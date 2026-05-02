use std::collections::HashMap;
use std::sync::Arc;

use log::{debug, error, info, warn};
use parking_lot::RwLock;

use crate::broker_backend::{BrokerBackend, InMemoryBackend};
use crate::config::{BrokerConfig, FromRmqConfig, RmqClientConfig};
use crate::consumer_group::ConsumerGroup;
use crate::error::{RmqError, RmqResult};
use crate::headers::OwnedHeaders;
use crate::message::StoredMessage;
use crate::offset_store::{InMemoryOffsetStore, OffsetStore};
use crate::partition::PartitionNotify;
use crate::partition_assignor::{PartitionAssignor, RoundRobinAssignor};
use crate::partitioner::{Partitioner, RoundRobinPartitioner};
use crate::topic_partition::TopicPartitionList;

/// Metadata returned after a record is successfully produced.
///
/// Contains the topic, partition, offset, and timestamp of the stored message.
pub struct RecordMetadata {
    /// The topic the message was written to.
    pub topic: String,
    /// The partition the message was written to.
    pub partition: i32,
    /// The offset of the message within the partition.
    pub offset: i64,
    /// The timestamp of the message (Unix milliseconds).
    pub timestamp: i64,
}

struct BrokerInner {
    consumer_groups: HashMap<String, Arc<ConsumerGroup>>,
    shutdown: bool,
}

/// The message broker at the heart of RMemQueue.
///
/// The broker is responsible for message storage, partition management, and consumer-group
/// coordination. It defaults to the in-memory backend ([`InMemoryBackend`]) but accepts a custom
/// backend via [`with_backend`](Broker::with_backend).
///
/// # Example
///
/// ```rust
/// use rmemqueue::{Broker, RmqClientConfig};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "test-broker");
///
/// let broker = Broker::new(config).unwrap();
/// ```
pub struct Broker {
    backend: Arc<dyn BrokerBackend>,
    in_memory: Option<Arc<InMemoryBackend>>,
    inner: RwLock<BrokerInner>,
    config: Arc<BrokerConfig>,
    #[allow(dead_code)]
    client_config: Arc<RmqClientConfig>,
    partitioner: Arc<dyn Partitioner>,
    assignor: Arc<dyn PartitionAssignor>,
    offset_store: Arc<dyn OffsetStore>,
}

impl Broker {
    /// Creates a new broker with the default in-memory backend and round-robin partitioner.
    ///
    /// # Arguments
    ///
    /// - `config` — Broker configuration; must contain `broker.id`.
    ///
    /// # Returns
    ///
    /// `Arc<Broker>` so it can be shared across producers and consumers.
    ///
    /// # Errors
    ///
    /// Returns [`RmqError::InvalidConfig`] if `broker.id` is missing or a value is invalid.
    pub fn new(config: RmqClientConfig) -> RmqResult<Arc<Self>> {
        let broker_config = BrokerConfig::from_config(&config)?;
        let broker_id = broker_config.broker_id.clone();
        let backend = Arc::new(InMemoryBackend::new(Arc::new(broker_config.clone())));
        info!("broker created: id={}", broker_id);
        Ok(Arc::new(Self {
            in_memory: Some(backend.clone()),
            backend,
            inner: RwLock::new(BrokerInner {
                consumer_groups: HashMap::new(),
                shutdown: false,
            }),
            config: Arc::new(broker_config),
            client_config: Arc::new(config),
            partitioner: Arc::new(RoundRobinPartitioner::new()),
            assignor: Arc::new(RoundRobinAssignor),
            offset_store: Arc::new(InMemoryOffsetStore::new()),
        }))
    }

    /// Creates a new broker with a custom storage backend.
    ///
    /// Useful when you need persistent storage or custom message management logic.
    ///
    /// # Arguments
    ///
    /// - `config` — Broker configuration; must contain `broker.id`.
    /// - `backend` — A type implementing [`BrokerBackend`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    /// use rmemqueue::{Broker, RmqClientConfig, BrokerBackend};
    ///
    /// // let my_backend = Arc::new(MyCustomBackend::new());
    /// // let broker = Broker::with_backend(config, my_backend).unwrap();
    /// ```
    pub fn with_backend(
        config: RmqClientConfig,
        backend: Arc<dyn BrokerBackend>,
    ) -> RmqResult<Arc<Self>> {
        let broker_config = BrokerConfig::from_config(&config)?;
        let broker_id = broker_config.broker_id.clone();
        info!("broker created with custom backend: id={}", broker_id);
        Ok(Arc::new(Self {
            in_memory: None,
            backend,
            inner: RwLock::new(BrokerInner {
                consumer_groups: HashMap::new(),
                shutdown: false,
            }),
            config: Arc::new(broker_config),
            client_config: Arc::new(config),
            partitioner: Arc::new(RoundRobinPartitioner::new()),
            assignor: Arc::new(RoundRobinAssignor),
            offset_store: Arc::new(InMemoryOffsetStore::new()),
        }))
    }

    /// Creates a new broker with a custom partitioner.
    ///
    /// By default the broker uses round-robin partitioning. Use this constructor to supply a
    /// custom [`Partitioner`] implementation.
    ///
    /// # Arguments
    ///
    /// - `config` — Broker configuration; must contain `broker.id`.
    /// - `partitioner` — A type implementing [`Partitioner`].
    pub fn with_partitioner(
        config: RmqClientConfig,
        partitioner: Arc<dyn Partitioner>,
    ) -> RmqResult<Arc<Self>> {
        let broker_config = BrokerConfig::from_config(&config)?;
        let broker_id = broker_config.broker_id.clone();
        let backend = Arc::new(InMemoryBackend::new(Arc::new(broker_config.clone())));
        info!("broker created with custom partitioner: id={}", broker_id);
        Ok(Arc::new(Self {
            in_memory: Some(backend.clone()),
            backend,
            inner: RwLock::new(BrokerInner {
                consumer_groups: HashMap::new(),
                shutdown: false,
            }),
            config: Arc::new(broker_config),
            client_config: Arc::new(config),
            partitioner,
            assignor: Arc::new(RoundRobinAssignor),
            offset_store: Arc::new(InMemoryOffsetStore::new()),
        }))
    }

    fn check_shutdown(&self) -> RmqResult<()> {
        if self.inner.read().shutdown {
            error!("operation attempted on shut down broker");
            Err(RmqError::BrokerShutdown)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_topic(&self, topic: &str, num_partitions: i32) -> RmqResult<()> {
        self.backend.ensure_topic(topic, num_partitions)
    }

    pub(crate) fn produce(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<RecordMetadata> {
        self.check_shutdown()?;
        self.ensure_topic(topic, self.config.default_num_partitions)?;
        debug!("produce request: topic={} partition={:?}", topic, partition);

        let num_p = {
            let meta = self.backend.metadata(&self.config.broker_id, Some(topic))?;
            meta.topics
                .first()
                .map(|t| t.partitions.len() as i32)
                .unwrap_or(self.config.default_num_partitions)
        };

        let p = match partition {
            Some(p) => p,
            None => self.partitioner.partition(topic, key.as_deref(), num_p),
        };

        let (partition_id, offset, ts) =
            self.backend.produce(topic, p, key, payload, headers, timestamp)?;

        Ok(RecordMetadata {
            topic: topic.to_owned(),
            partition: partition_id,
            offset,
            timestamp: ts,
        })
    }

    pub(crate) fn produce_typed<P: Send + Sync + 'static, K: Send + Sync + 'static>(
        &self,
        topic: &str,
        partition: Option<i32>,
        typed_payload: Arc<P>,
        typed_key: Option<Arc<K>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<RecordMetadata> {
        self.check_shutdown()?;
        let mem = self.in_memory.as_ref().ok_or_else(|| {
            RmqError::Custom("typed produce requires InMemoryBackend".to_owned())
        })?;
        self.ensure_topic(topic, self.config.default_num_partitions)?;
        debug!("produce_typed request: topic={} partition={:?}", topic, partition);

        let num_p = {
            let meta = self.backend.metadata(&self.config.broker_id, Some(topic))?;
            meta.topics
                .first()
                .map(|t| t.partitions.len() as i32)
                .unwrap_or(self.config.default_num_partitions)
        };

        let p = match partition {
            Some(p) => p,
            None => self.partitioner.partition(topic, None, num_p),
        };

        let (partition_id, offset, ts) =
            mem.produce_typed(topic, p, typed_payload, typed_key, headers, timestamp)?;

        Ok(RecordMetadata {
            topic: topic.to_owned(),
            partition: partition_id,
            offset,
            timestamp: ts,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_count: usize,
    ) -> RmqResult<Vec<Arc<StoredMessage>>> {
        self.backend.fetch(topic, partition, offset, max_count)
    }

    pub(crate) fn fetch_one_from_position(
        &self,
        topic: &str,
        partition: i32,
        position: Option<i64>,
        group_id: Option<&str>,
    ) -> RmqResult<Option<Arc<StoredMessage>>> {
        self.check_shutdown()?;

        let offset = match position {
            Some(o) => o,
            None => {
                if let Some(gid) = group_id {
                    let inner = self.inner.read();
                    if let Some(group) = inner.consumer_groups.get(gid) {
                        if let Ok(Some(committed)) = group.committed_offset(topic, partition) {
                            committed + 1
                        } else {
                            let (oldest, _) = self.backend.watermarks(topic, partition)?;
                            oldest
                        }
                    } else {
                        let (oldest, _) = self.backend.watermarks(topic, partition)?;
                        oldest
                    }
                } else {
                    let (oldest, _) = self.backend.watermarks(topic, partition)?;
                    oldest
                }
            }
        };

        match self.backend.fetch_one(topic, partition, offset) {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => Ok(None),
            Err(RmqError::OffsetOutOfRange { .. }) => {
                let (oldest, _) = self.backend.watermarks(topic, partition)?;
                warn!("offset out of range for {}/{}, adjusted to {}", topic, partition, oldest);
                Err(RmqError::OffsetOutOfRange {
                    topic: topic.to_owned(),
                    partition,
                    offset: oldest,
                })
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn metadata(&self, topic: Option<&str>) -> RmqResult<crate::metadata::Metadata> {
        self.check_shutdown()?;
        self.backend.metadata(&self.config.broker_id, topic)
    }

    /// Returns the watermarks (oldest and newest offsets) for the given partition.
    ///
    /// Returns a tuple `(oldest_offset, newest_offset)` representing the range of available
    /// messages in the partition.
    ///
    /// # Arguments
    ///
    /// - `topic` — Topic name.
    /// - `partition` — Partition index.
    pub fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)> {
        self.backend.watermarks(topic, partition)
    }

    /// Shuts down the broker.
    ///
    /// After shutdown, all producer and consumer operations will return
    /// [`RmqError::BrokerShutdown`].
    pub fn shutdown(&self) -> RmqResult<()> {
        {
            let mut inner = self.inner.write();
            inner.shutdown = true;
        }
        info!("broker shutting down: id={}", self.config.broker_id);
        self.backend.shutdown()
    }

    #[allow(dead_code)]
    pub(crate) fn config(&self) -> &BrokerConfig {
        &self.config
    }

    #[allow(dead_code)]
    pub(crate) fn client_config(&self) -> &RmqClientConfig {
        &self.client_config
    }

    #[allow(dead_code)]
    pub(crate) fn get_partition_notifies(
        &self,
        topics: &[String],
    ) -> RmqResult<Vec<Arc<PartitionNotify>>> {
        self.backend.get_partition_notifies(topics)
    }

    pub(crate) fn wait_for_messages(
        &self,
        topics: &[String],
        timeout: std::time::Duration,
    ) -> RmqResult<bool> {
        self.backend.wait_for_messages(topics, timeout)
    }

    pub(crate) fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        topics: &[String],
    ) -> RmqResult<TopicPartitionList> {
        self.check_shutdown()?;
        info!("member {} joining group {}", member_id, group_id);
        for topic in topics {
            self.ensure_topic(topic, self.config.default_num_partitions)?;
        }
        let group = {
            let mut inner = self.inner.write();
            inner
                .consumer_groups
                .entry(group_id.to_owned())
                .or_insert_with(|| {
                    Arc::new(ConsumerGroup::new(
                        group_id.to_owned(),
                        self.assignor.clone(),
                        self.offset_store.clone(),
                    ))
                })
                .clone()
        };

        let notifies = self.backend.get_partition_notifies(topics)?;
        let meta = self.backend.metadata(&self.config.broker_id, None)?;
        let mut tp_counts = HashMap::new();
        for topic in topics {
            if let Some(tm) = meta.topics.iter().find(|t| t.name == *topic) {
                tp_counts.insert(topic.clone(), tm.partitions.len() as i32);
            }
        }
        drop(notifies);

        group.join(member_id, topics, &tp_counts)
    }

    pub(crate) fn leave_group(&self, group_id: &str, member_id: &str) -> RmqResult<()> {
        self.check_shutdown()?;
        info!("member {} leaving group {}", member_id, group_id);
        let inner = self.inner.read();
        let group = inner
            .consumer_groups
            .get(group_id)
            .ok_or_else(|| RmqError::GroupNotFound(group_id.to_owned()))?;

        let meta = self.backend.metadata(&self.config.broker_id, None)?;
        let tp_counts: HashMap<String, i32> = meta
            .topics
            .iter()
            .map(|t| (t.name.clone(), t.partitions.len() as i32))
            .collect();
        group.leave(member_id, &tp_counts)
    }

    pub(crate) fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> RmqResult<()> {
        self.check_shutdown()?;
        debug!("commit offset {}/{}/{} group={}", topic, partition, offset, group_id);
        let inner = self.inner.read();
        let group = inner
            .consumer_groups
            .get(group_id)
            .ok_or_else(|| RmqError::GroupNotFound(group_id.to_owned()))?;
        group.commit_offset(topic, partition, offset)
    }

    pub(crate) fn committed_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> RmqResult<Option<i64>> {
        self.check_shutdown()?;
        let inner = self.inner.read();
        let group = inner
            .consumer_groups
            .get(group_id)
            .ok_or_else(|| RmqError::GroupNotFound(group_id.to_owned()))?;
        group.committed_offset(topic, partition)
    }
}

impl FromRmqConfig for Arc<Broker> {
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        Broker::new(config.clone())
    }
}
