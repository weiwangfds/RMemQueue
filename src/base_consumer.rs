use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use log::{debug, info};
use parking_lot::Mutex;

use crate::broker::Broker;
use crate::config::{config_keys, FromRmqConfig, RmqClientConfig};
use crate::consumer::{CommitMode, Consumer};
use crate::context::{ConsumerContext, DefaultConsumerContext, RebalanceEvent};
use crate::error::{RmqError, RmqResult};
use crate::message::BorrowedMessage;
use crate::topic_partition::{Offset, TopicPartitionList};

static MEMBER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

struct ConsumerState {
    subscribed_topics: Vec<String>,
    assigned_partitions: TopicPartitionList,
    position: HashMap<(Arc<str>, i32), i64>,
    paused: HashSet<(Arc<str>, i32)>,
    poll_start: AtomicUsize,
}

/// A synchronous message consumer that polls messages via [`BaseConsumer::poll`].
///
/// `BaseConsumer` is the lowest-level consumer implementation. It pulls messages
/// in a synchronous, blocking fashion using [`poll`](BaseConsumer::poll).
/// For use inside an async runtime such as tokio, prefer
/// [`StreamConsumer`](crate::stream_consumer::StreamConsumer) instead.
///
/// This type implements [`Clone`]. Cloning creates a **new** consumer instance
/// (with an independent `member_id`) that shares the same [`Broker`](crate::broker::Broker)
/// connection. The cloned instance does **not** inherit subscription or assignment state.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
/// use rmemqueue::{BaseConsumer, Consumer, RmqClientConfig, Message};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// let consumer = BaseConsumer::new(&config)?;
/// consumer.subscribe(&["my-topic"])?;
///
/// loop {
///     if let Some(result) = consumer.poll(Duration::from_secs(1)) {
///         let msg = result?;
///         println!("Received message: offset={}", msg.offset());
///     }
/// }
/// ```
pub struct BaseConsumer<C: ConsumerContext = DefaultConsumerContext> {
    broker: Arc<Broker>,
    group_id: Option<String>,
    member_id: String,
    context: Arc<C>,
    state: Mutex<ConsumerState>,
}

impl BaseConsumer<DefaultConsumerContext> {
    /// Creates a new `BaseConsumer` with the default [`ConsumerContext`].
    ///
    /// # Arguments
    ///
    /// * `config` — client configuration containing broker address, `group.id`, etc.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rmemqueue::base_consumer::BaseConsumer;
    /// use rmemqueue::config::RmqClientConfig;
    ///
    /// let config = RmqClientConfig::new();
    /// let consumer = BaseConsumer::new(&config)?;
    /// # Ok::<(), rmemqueue::error::RmqError>(())
    /// ```
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        Self::with_context(config, DefaultConsumerContext)
    }
}

impl<C: ConsumerContext> BaseConsumer<C> {
    /// Creates a new `BaseConsumer` with a custom [`ConsumerContext`].
    ///
    /// The context can be used to handle rebalance events and commit callbacks.
    /// See [`ConsumerContext`](crate::context::ConsumerContext) for details.
    pub fn with_context(config: &RmqClientConfig, context: C) -> RmqResult<Self> {
        let broker = crate::registry::BrokerRegistry::get_or_create(config)?;
        let group_id = config.get(config_keys::GROUP_ID).map(|s| s.to_owned());
        let member_id = format!(
            "consumer-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            MEMBER_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        Ok(Self {
            broker,
            group_id,
            member_id,
            context: Arc::new(context),
            state: Mutex::new(ConsumerState {
                subscribed_topics: Vec::new(),
                assigned_partitions: TopicPartitionList::new(),
                position: HashMap::new(),
                paused: HashSet::new(),
                poll_start: AtomicUsize::new(0),
            }),
        })
    }

    /// Blocks until a message is available or the timeout elapses.
    ///
    /// Internally polls all assigned (non-paused) partitions in round-robin order.
    ///
    /// **Warning**: do **not** call this method inside a tokio runtime — it will
    /// block the runtime thread. Use [`StreamConsumer::recv()`](crate::stream_consumer::StreamConsumer::recv)
    /// in async contexts instead.
    ///
    /// # Arguments
    ///
    /// * `timeout` — maximum duration to wait. Returns `None` if no message arrives
    ///   before the deadline.
    ///
    /// # Returns
    ///
    /// * `Some(Ok(msg))` — a message was successfully retrieved
    /// * `Some(Err(e))` — an error occurred while fetching
    /// * `None` — timed out, no message available
    ///
    /// # Example
    ///
/// ```ignore
/// use std::time::Duration;
/// use rmemqueue::{BaseConsumer, Consumer, RmqClientConfig, Message};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// let consumer = BaseConsumer::new(&config)?;
/// consumer.subscribe(&["my-topic"])?;
///
/// if let Some(result) = consumer.poll(Duration::from_secs(5)) {
///     let msg = result?;
///     println!("offset={}, payload={:?}", msg.offset(), msg.payload());
/// }
/// ```
    pub fn poll(&self, timeout: Duration) -> Option<RmqResult<BorrowedMessage<'_>>> {
        #[cfg(feature = "async")]
        {
            static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
            if tokio::runtime::Handle::try_current().is_ok() {
                if !WARNED.load(std::sync::atomic::Ordering::Relaxed) {
                    WARNED.store(true, std::sync::atomic::Ordering::Relaxed);
                    log::warn!(
                        "BaseConsumer::poll() called inside tokio runtime — \
                         this blocks the runtime thread. Use StreamConsumer::recv() instead."
                    );
                }
            }
        }
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(result) = self.poll_once() {
                return Some(result);
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let wait_time = remaining.min(Duration::from_millis(10));
            let topics = {
                let state = self.state.lock();
                state.subscribed_topics.clone()
            };
            if !topics.is_empty() {
                let _ = self.broker.wait_for_messages(&topics, wait_time);
            } else {
                std::thread::sleep(wait_time);
            }
        }
    }

    fn poll_once(&self) -> Option<RmqResult<BorrowedMessage<'_>>> {
        let (start_idx, elements_len) = {
            let state = self.state.lock();
            let len = state.assigned_partitions.elements().len();
            let start = if len > 0 {
                state.poll_start.load(Ordering::Relaxed) % len
            } else {
                0
            };
            (start, len)
        };

        if elements_len == 0 {
            return None;
        }

        for i in 0..elements_len {
            let idx = (start_idx + i) % elements_len;
            let (topic, partition, position, is_paused) = {
                let state = self.state.lock();
                let elements = state.assigned_partitions.elements();
                if idx >= elements.len() {
                    continue;
                }
                let elem = &elements[idx];
                let key = (Arc::from(elem.topic.as_str()), elem.partition);
                let is_paused = state.paused.contains(&key);
                let pos = state.position.get(&key).copied();
                (elem.topic.clone(), elem.partition, pos, is_paused)
            };

            if is_paused {
                continue;
            }

            match self.broker.fetch_one_from_position(
                &topic,
                partition,
                position,
                self.group_id.as_deref(),
            ) {
                Ok(Some(msg)) => {
                    let offset = msg.offset();
                    let key = (Arc::from(topic.as_str()), partition);
                    let mut state = self.state.lock();
                    state.position.insert(key, offset + 1);
                    state.poll_start.store(idx + 1, Ordering::Relaxed);
                    return Some(Ok(BorrowedMessage::new(msg)));
                }
                Ok(None) => continue,
                Err(RmqError::OffsetOutOfRange { offset, .. }) => {
                    debug!("consumer {} offset adjusted for {}/{}", self.member_id, topic, partition);
                    let key = (Arc::from(topic.as_str()), partition);
                    let mut state = self.state.lock();
                    state.position.insert(key, offset);
                }
                Err(RmqError::TopicNotFound(_) | RmqError::PartitionOutOfRange { .. }) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }

    /// Returns an infinite iterator that continuously polls for messages.
    ///
    /// The iterator internally calls [`poll`](BaseConsumer::poll) with a 1-second timeout.
    /// Useful for consuming messages in a `for` loop.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rmemqueue::{BaseConsumer, Consumer, RmqClientConfig, Message};
    ///
    /// let mut config = RmqClientConfig::new();
    /// config.set("broker.id", "my-broker");
    /// let consumer = BaseConsumer::new(&config)?;
    /// consumer.subscribe(&["my-topic"])?;
    ///
    /// for result in consumer.iter() {
    ///     let msg = result?;
    ///     println!("offset={}", msg.offset());
    /// }
    /// ```
    pub fn iter(&self) -> MessageIter<'_, C> {
        MessageIter { consumer: self }
    }

    #[cfg(feature = "async")]
    pub(crate) fn subscribed_topics(&self) -> Vec<String> {
        let state = self.state.lock();
        state.subscribed_topics.clone()
    }
}

/// Clones a `BaseConsumer`.
///
/// Creates a new consumer instance with an independent `member_id`. The clone does
/// **not** inherit subscription, assignment, or offset state — it is equivalent to
/// creating a brand-new consumer with the same configuration.
impl<C: ConsumerContext> Clone for BaseConsumer<C> {
    fn clone(&self) -> Self {
        let new_member_id = format!(
            "consumer-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            MEMBER_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        Self {
            broker: self.broker.clone(),
            group_id: None,
            member_id: new_member_id,
            context: self.context.clone(),
            state: Mutex::new(ConsumerState {
                subscribed_topics: Vec::new(),
                assigned_partitions: TopicPartitionList::new(),
                position: HashMap::new(),
                paused: HashSet::new(),
                poll_start: AtomicUsize::new(0),
            }),
        }
    }
}

impl FromRmqConfig for BaseConsumer<DefaultConsumerContext> {
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        BaseConsumer::new(config)
    }
}

/// A message iterator returned by [`BaseConsumer::iter`].
///
/// Implements [`Iterator`], calling [`BaseConsumer::poll`] with a 1-second timeout on
/// each [`next`](MessageIter::next) call. The iterator never terminates (unless an
/// unrecoverable error occurs).
pub struct MessageIter<'a, C: ConsumerContext> {
    consumer: &'a BaseConsumer<C>,
}

impl<'a, C: ConsumerContext> Iterator for MessageIter<'a, C> {
    type Item = RmqResult<BorrowedMessage<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.consumer.poll(Duration::from_secs(1))
    }
}

impl<C: ConsumerContext> Consumer for BaseConsumer<C> {
    fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }

    fn subscribe(&self, topics: &[&str]) -> RmqResult<()> {
        info!("consumer {} subscribed to {:?}", self.member_id, topics);
        let topic_strings: Vec<String> = topics.iter().map(|s| s.to_string()).collect();
        let assignment = if let Some(ref gid) = self.group_id {
            self.broker
                .join_group(gid, &self.member_id, &topic_strings)?
        } else {
            let mut tpl = TopicPartitionList::new();
            for topic in &topic_strings {
                if let Ok(meta) = self.broker.metadata(Some(topic)) {
                    for tm in &meta.topics {
                        for pm in &tm.partitions {
                            tpl.add_partition(&tm.name, pm.id);
                        }
                    }
                }
            }
            tpl
        };
        self.context
            .rebalance(&RebalanceEvent::Assigned(assignment.clone()));
        let mut state = self.state.lock();
        state.subscribed_topics = topic_strings;
        state.assigned_partitions = assignment;
        Ok(())
    }

    fn unsubscribe(&self) -> RmqResult<()> {
        info!("consumer {} unsubscribed", self.member_id);
        let old_assignment = {
            let state = self.state.lock();
            state.assigned_partitions.clone()
        };
        if let Some(ref gid) = self.group_id {
            self.broker.leave_group(gid, &self.member_id)?;
        }
        self.context
            .rebalance(&RebalanceEvent::Revoked(old_assignment));
        let mut state = self.state.lock();
        state.subscribed_topics.clear();
        state.assigned_partitions = TopicPartitionList::new();
        state.position.clear();
        state.paused.clear();
        Ok(())
    }

    fn subscription(&self) -> RmqResult<TopicPartitionList> {
        let state = self.state.lock();
        Ok(state.assigned_partitions.clone())
    }

    fn assign(&self, partitions: &TopicPartitionList) -> RmqResult<()> {
        let mut state = self.state.lock();
        state.assigned_partitions = partitions.clone();
        state.position.clear();
        Ok(())
    }

    fn assignment(&self) -> RmqResult<TopicPartitionList> {
        let state = self.state.lock();
        Ok(state.assigned_partitions.clone())
    }

    fn seek(&self, topic: &str, partition: i32, offset: Offset) -> RmqResult<()> {
        let offset_val = match offset {
            Offset::Beginning => {
                let (oldest, _) = self.broker.watermarks(topic, partition)?;
                oldest
            }
            Offset::End => {
                let (_, newest) = self.broker.watermarks(topic, partition)?;
                newest + 1
            }
            Offset::Offset(o) => o,
            Offset::Stored => {
                if let Some(ref gid) = self.group_id {
                    self.broker
                        .committed_offset(gid, topic, partition)?
                        .unwrap_or(0)
                } else {
                    0
                }
            }
            Offset::OffsetTail(tail) => {
                let (_, newest) = self.broker.watermarks(topic, partition)?;
                (newest + 1 - tail).max(0)
            }
        };
        let mut state = self.state.lock();
        state
            .position
            .insert((Arc::from(topic), partition), offset_val);
        Ok(())
    }

    fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> RmqResult<()> {
        if let Some(ref gid) = self.group_id {
            for elem in tpl.elements() {
                if let Offset::Offset(o) = elem.offset {
                    self.broker
                        .commit_offset(gid, &elem.topic, elem.partition, o)?;
                }
            }
        }
        self.context.commit_callback(Ok(()), tpl);
        let _ = mode;
        Ok(())
    }

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> RmqResult<()> {
        let mut state = self.state.lock();
        state.position.insert((Arc::from(topic), partition), offset);
        Ok(())
    }

    fn committed(&self) -> RmqResult<TopicPartitionList> {
        let assigned = {
            let state = self.state.lock();
            state.assigned_partitions.clone()
        };
        let mut tpl = TopicPartitionList::new();
        if let Some(ref gid) = self.group_id {
            for elem in assigned.elements() {
                let offset = self
                    .broker
                    .committed_offset(gid, &elem.topic, elem.partition)?;
                tpl.add_partition_offset(
                    &elem.topic,
                    elem.partition,
                    Offset::Offset(offset.unwrap_or(-1)),
                );
            }
        }
        Ok(tpl)
    }

    fn position(&self) -> RmqResult<TopicPartitionList> {
        let state = self.state.lock();
        let mut tpl = TopicPartitionList::new();
        for elem in state.assigned_partitions.elements() {
            let key = (Arc::from(elem.topic.as_str()), elem.partition);
            let offset = state.position.get(&key).copied().unwrap_or(-1);
            tpl.add_partition_offset(&elem.topic, elem.partition, Offset::Offset(offset));
        }
        Ok(tpl)
    }

    fn pause(&self, partitions: &TopicPartitionList) -> RmqResult<()> {
        let mut state = self.state.lock();
        for elem in partitions.elements() {
            state.paused.insert((Arc::from(elem.topic.as_str()), elem.partition));
        }
        Ok(())
    }

    fn resume(&self, partitions: &TopicPartitionList) -> RmqResult<()> {
        let mut state = self.state.lock();
        for elem in partitions.elements() {
            state.paused.remove(&(Arc::from(elem.topic.as_str()), elem.partition));
        }
        Ok(())
    }
}
