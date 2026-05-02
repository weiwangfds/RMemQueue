use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info};
use parking_lot::RwLock;

use crate::config::BrokerConfig;
use crate::error::{RmqError, RmqResult};
use crate::headers::OwnedHeaders;
use crate::message::StoredMessage;
use crate::metadata::{Metadata, PartitionMetadata, TopicMetadata};
use crate::partition::{PartitionConfig, PartitionNotify};
use crate::topic::Topic;

/// Broker backend trait defining the core message queue storage and retrieval operations.
pub trait BrokerBackend: Send + Sync + 'static {
    /// Ensure a topic exists with the given partition count. No-op if the topic already exists.
    fn ensure_topic(&self, topic: &str, num_partitions: i32) -> RmqResult<()>;

    /// Produce a message to the specified topic and partition, returning `(partition, offset, timestamp)`.
    fn produce(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64, i64)>;

    /// Fetch messages from a topic/partition starting at the given offset.
    fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_count: usize,
    ) -> RmqResult<Vec<Arc<StoredMessage>>>;

    /// Fetch a single message from a topic/partition at the given offset.
    fn fetch_one(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> RmqResult<Option<Arc<StoredMessage>>>;

    /// Get the low and high watermarks for a topic/partition.
    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)>;

    /// Get cluster or topic metadata.
    fn metadata(&self, broker_id: &str, topic: Option<&str>) -> RmqResult<Metadata>;

    /// Get partition notification objects for the given topics.
    fn get_partition_notifies(
        &self,
        topics: &[String],
    ) -> RmqResult<Vec<Arc<PartitionNotify>>>;

    /// Wait for new messages on the given topics within the timeout.
    fn wait_for_messages(&self, topics: &[String], timeout: Duration) -> RmqResult<bool>;

    /// Shut down the backend.
    fn shutdown(&self) -> RmqResult<()>;
}

// In-memory implementation

// SAFETY: parking_lot::RwLock is safe here because all topic operations
// (ensure_topic, produce, fetch) are O(1) and never hold across an await point.
struct InMemoryInner {
    topics: HashMap<String, Arc<Topic>>,
    shutdown: bool,
}

/// In-memory broker backend storing all data on the heap.
pub struct InMemoryBackend {
    inner: RwLock<InMemoryInner>,
    config: Arc<BrokerConfig>,
}

impl InMemoryBackend {
    pub(crate) fn new(config: Arc<BrokerConfig>) -> Self {
        Self {
            inner: RwLock::new(InMemoryInner {
                topics: HashMap::new(),
                shutdown: false,
            }),
            config,
        }
    }

    fn check_shutdown(&self) -> RmqResult<()> {
        if self.inner.read().shutdown {
            Err(RmqError::BrokerShutdown)
        } else {
            Ok(())
        }
    }

    fn make_partition_config(&self) -> PartitionConfig {
        PartitionConfig {
            max_capacity: self.config.buffer_capacity,
            retention_ms: self.config.retention_ms,
            retention_capacity: self.config.retention_capacity,
        }
    }

    fn get_topic(&self, topic: &str) -> RmqResult<Arc<Topic>> {
        self.inner
            .read()
            .topics
            .get(topic)
            .cloned()
            .ok_or_else(|| RmqError::TopicNotFound(topic.to_owned()))
    }

    fn topic_metadata(&self, topic: &Topic) -> TopicMetadata {
        let num_partitions = topic.partition_count();
        let mut partitions = Vec::with_capacity(num_partitions as usize);

        for i in 0..num_partitions {
            let (oldest, newest) = topic.watermarks(i).unwrap_or((0, -1));
            let count = topic
                .get_partition(i)
                .map(|p| p.message_count())
                .unwrap_or(0);

            partitions.push(PartitionMetadata {
                id: i,
                oldest_offset: oldest,
                newest_offset: newest,
                message_count: count,
            });
        }

        TopicMetadata {
            name: topic.name().to_owned(),
            partitions,
            error: None,
        }
    }
}

impl BrokerBackend for InMemoryBackend {
    fn ensure_topic(&self, topic: &str, num_partitions: i32) -> RmqResult<()> {
        self.check_shutdown()?;

        {
            let inner = self.inner.read();
            if inner.topics.contains_key(topic) {
                return Ok(());
            }
        }

        {
            let mut inner = self.inner.write();
            if inner.topics.contains_key(topic) {
                return Ok(());
            }

            let topic_config = self.make_partition_config();
            let t = Topic::new(topic.to_owned(), num_partitions, topic_config);
            inner.topics.insert(topic.to_owned(), Arc::new(t));
            info!("topic created: {} ({} partitions)", topic, num_partitions);
        }

        Ok(())
    }

    fn produce(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64, i64)> {
        self.check_shutdown()?;

        let ts = timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let t = self.get_topic(topic)?;
        let (partition_id, offset) = t.produce(partition, key, payload, headers, Some(ts))?;

        debug!("produce to {} partition {} offset {}", topic, partition_id, offset);
        Ok((partition_id, offset, ts))
    }

    fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_count: usize,
    ) -> RmqResult<Vec<Arc<StoredMessage>>> {
        self.check_shutdown()?;
        let t = self.get_topic(topic)?;
        t.fetch(partition, offset, max_count)
    }

    fn fetch_one(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> RmqResult<Option<Arc<StoredMessage>>> {
        self.check_shutdown()?;
        let t = self.get_topic(topic)?;
        t.fetch_one(partition, offset)
    }

    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)> {
        self.check_shutdown()?;
        let t = self.get_topic(topic)?;
        t.watermarks(partition)
    }

    fn metadata(&self, broker_id: &str, topic: Option<&str>) -> RmqResult<Metadata> {
        self.check_shutdown()?;

        let inner = self.inner.read();
        let topics = match topic {
            Some(name) => {
                let t = inner
                    .topics
                    .get(name)
                    .ok_or_else(|| RmqError::TopicNotFound(name.to_owned()))?;
                vec![self.topic_metadata(t)]
            }
            None => inner
                .topics
                .values()
                .map(|t| self.topic_metadata(t))
                .collect(),
        };

        Ok(Metadata {
            broker_id: broker_id.to_owned(),
            topics,
        })
    }

    fn get_partition_notifies(
        &self,
        topics: &[String],
    ) -> RmqResult<Vec<Arc<PartitionNotify>>> {
        let inner = self.inner.read();
        let mut notifies = Vec::new();
        for topic_name in topics {
            if let Some(topic) = inner.topics.get(topic_name) {
                let count = topic.partition_count();
                for i in 0..count {
                    if let Ok(notify) = topic.get_partition_notify(i) {
                        notifies.push(notify);
                    }
                }
            }
        }
        Ok(notifies)
    }

    fn wait_for_messages(&self, topics: &[String], timeout: Duration) -> RmqResult<bool> {
        let notifies = self.get_partition_notifies(topics)?;
        if notifies.is_empty() {
            std::thread::sleep(timeout);
            return Ok(false);
        }
        if notifies.len() == 1 {
            return Ok(notifies[0].wait(timeout));
        }
        let deadline = std::time::Instant::now() + timeout;
        for notify in &notifies {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            let timed_out = notify.wait(remaining);
            if !timed_out {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn shutdown(&self) -> RmqResult<()> {
        let mut inner = self.inner.write();
        inner.shutdown = true;
        info!("backend shutting down");
        // Wake up any waiting consumers
        for topic in inner.topics.values() {
            let count = topic.partition_count();
            for i in 0..count {
                if let Ok(notify) = topic.get_partition_notify(i) {
                    notify.notify();
                }
            }
        }
        Ok(())
    }
}

impl InMemoryBackend {
    /// Produce a typed (generic) message to the specified topic and partition.
    /// Returns `(partition, offset, timestamp)`.
    pub fn produce_typed<P: Send + Sync + 'static, K: Send + Sync + 'static>(
        &self,
        topic: &str,
        partition: i32,
        typed_payload: Arc<P>,
        typed_key: Option<Arc<K>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64, i64)> {
        self.check_shutdown()?;

        let ts = timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let t = self.get_topic(topic)?;
        let (partition_id, offset) = t.produce_typed(partition, typed_payload, typed_key, headers, Some(ts))?;

        debug!("produce_typed to {} partition {} offset {}", topic, partition_id, offset);
        Ok((partition_id, offset, ts))
    }
}
