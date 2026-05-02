use std::sync::Arc;
use std::time::Duration;

use crate::base_consumer::BaseConsumer;
use crate::broker::{Broker, RecordMetadata};
use crate::config::RmqClientConfig;
use crate::consumer::{CommitMode, Consumer};
use crate::error::{RmqError, RmqResult};
use crate::headers::OwnedHeaders;
use crate::message::StoredMessage;
use crate::topic_partition::{Offset, TopicPartitionList};

/// A borrowed message with typed payload and key, bypassing serialization.
///
/// Unlike [`BorrowedMessage`](crate::message::BorrowedMessage) which exposes raw bytes,
/// `TypedBorrowedMessage` gives direct access to the original Rust objects stored via
/// [`TypedProducer`]. This requires the broker to use an `InMemoryBackend`.
///
/// # Type parameters
///
/// * `P` — payload type
/// * `K` — key type (defaults to `()`)
pub struct TypedBorrowedMessage<'a, P, K = ()> {
    inner: Arc<StoredMessage>,
    _phantom: std::marker::PhantomData<(&'a P, &'a K)>,
}

impl<'a, P: 'static, K: 'static> TypedBorrowedMessage<'a, P, K> {
    /// Returns a reference to the typed payload `P`.
    ///
    /// # Panics
    ///
    /// Panics if the stored type does not match `P`.
    pub fn payload(&self) -> &P {
        self.inner
            .typed_payload_ref::<P>()
            .expect("typed payload type mismatch")
    }

    /// Returns a reference to the typed key `K`, or `None` if no key was set.
    pub fn key(&self) -> Option<&K> {
        self.inner.typed_key_ref::<K>()
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// Returns the partition id.
    pub fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// Returns the message offset within its partition.
    pub fn offset(&self) -> i64 {
        self.inner.offset()
    }

    /// Returns the message timestamp (milliseconds since Unix epoch).
    pub fn timestamp(&self) -> i64 {
        self.inner.timestamp()
    }

    /// Returns the message headers, if any were set.
    pub fn headers(&self) -> Option<&OwnedHeaders> {
        self.inner.headers()
    }
}

/// A typed producer that sends Rust objects directly, bypassing serialization.
///
/// Stores `Arc<P>` payloads and optional `Arc<K>` keys in memory, requiring the
/// broker to use an `InMemoryBackend`. Eliminates serialization overhead for
/// in-process messaging.
///
/// # Type parameters
///
/// * `P` — payload type
/// * `K` — key type (defaults to `()`)
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use rmemqueue::typed::TypedProducer;
/// use rmemqueue::config::RmqClientConfig;
///
/// let producer: TypedProducer<String> = TypedProducer::new(&RmqClientConfig::new())?;
/// producer.send("my-topic", Arc::new("hello".to_string()), None)?;
/// # Ok::<(), rmemqueue::error::RmqError>(())
/// ```
pub struct TypedProducer<P, K = ()> {
    broker: Arc<Broker>,
    _phantom: std::marker::PhantomData<(P, K)>,
}

impl<P: Send + Sync + 'static, K: Send + Sync + 'static> TypedProducer<P, K> {
    /// Creates a new `TypedProducer` from the given configuration.
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        let broker = crate::registry::BrokerRegistry::get_or_create(config)?;
        Ok(Self {
            broker,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Sends a typed message to the given topic with automatic partition assignment.
    pub fn send(
        &self,
        topic: &str,
        payload: Arc<P>,
        key: Option<Arc<K>>,
    ) -> RmqResult<RecordMetadata> {
        self.broker
            .produce_typed::<P, K>(topic, None, payload, key, None, None)
    }

    /// Sends a typed message to the given topic and specific partition.
    pub fn send_with_partition(
        &self,
        topic: &str,
        partition: i32,
        payload: Arc<P>,
        key: Option<Arc<K>>,
    ) -> RmqResult<RecordMetadata> {
        self.broker
            .produce_typed::<P, K>(topic, Some(partition), payload, key, None, None)
    }

    /// Sends a typed message with attached headers.
    pub fn send_with_headers(
        &self,
        topic: &str,
        payload: Arc<P>,
        key: Option<Arc<K>>,
        headers: OwnedHeaders,
    ) -> RmqResult<RecordMetadata> {
        self.broker
            .produce_typed::<P, K>(topic, None, payload, key, Some(headers), None)
    }

    /// Returns a reference to the underlying [`Broker`].
    pub fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }
}

impl<P: Send + Sync + 'static, K: Send + Sync + 'static> Clone for TypedProducer<P, K> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// A typed consumer that receives Rust objects directly, bypassing deserialization.
///
/// Wraps a [`BaseConsumer`](crate::base_consumer::BaseConsumer) and exposes typed
/// [`poll`](TypedConsumer::poll) that returns [`TypedBorrowedMessage`] instead of
/// raw-byte messages. Requires the broker to use an `InMemoryBackend`.
///
/// # Type parameters
///
/// * `P` — payload type
/// * `K` — key type (defaults to `()`)
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use rmemqueue::typed::TypedConsumer;
/// use rmemqueue::config::RmqClientConfig;
///
/// let consumer: TypedConsumer<String> = TypedConsumer::new(&RmqClientConfig::new())?;
/// consumer.subscribe(&["my-topic"])?;
///
/// if let Some(result) = consumer.poll(Duration::from_secs(5)) {
///     let msg = result?;
///     println!("payload={}", msg.payload());
/// }
/// # Ok::<(), rmemqueue::error::RmqError>(())
/// ```
pub struct TypedConsumer<P, K = ()> {
    inner: BaseConsumer,
    _phantom: std::marker::PhantomData<(P, K)>,
}

impl<P: Send + Sync + 'static, K: Send + Sync + 'static> TypedConsumer<P, K> {
    /// Creates a new `TypedConsumer` from the given configuration.
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        let inner = BaseConsumer::new(config)?;
        Ok(Self {
            inner,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Blocks until a typed message is available or the timeout elapses.
    ///
    /// Returns an error if the underlying message contains raw bytes instead of a
    /// typed payload (i.e. was produced by a non-typed producer).
    pub fn poll(&self, timeout: Duration) -> Option<RmqResult<TypedBorrowedMessage<'_, P, K>>> {
        match self.inner.poll(timeout) {
            Some(Ok(msg)) => {
                let stored = msg.inner_arc().clone();
                if stored.has_typed_payload() {
                    Some(Ok(TypedBorrowedMessage {
                        inner: stored,
                        _phantom: std::marker::PhantomData,
                    }))
                } else {
                    Some(Err(RmqError::Custom(
                        "message has raw bytes payload, not typed".to_owned(),
                    )))
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    /// Subscribes to the given list of topics.
    pub fn subscribe(&self, topics: &[&str]) -> RmqResult<()> {
        self.inner.subscribe(topics)
    }

    /// Unsubscribes from all topics and releases assigned partitions.
    pub fn unsubscribe(&self) -> RmqResult<()> {
        self.inner.unsubscribe()
    }

    /// Manually assigns the given partition list to this consumer.
    pub fn assign(&self, partitions: &TopicPartitionList) -> RmqResult<()> {
        self.inner.assign(partitions)
    }

    /// Returns the list of currently assigned partitions.
    pub fn assignment(&self) -> RmqResult<TopicPartitionList> {
        self.inner.assignment()
    }

    /// Seeks to the specified offset for the given topic-partition.
    pub fn seek(&self, topic: &str, partition: i32, offset: Offset) -> RmqResult<()> {
        self.inner.seek(topic, partition, offset)
    }

    /// Commits the offsets in the provided [`TopicPartitionList`].
    pub fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> RmqResult<()> {
        self.inner.commit(tpl, mode)
    }

    /// Returns a reference to the underlying [`Broker`].
    pub fn broker(&self) -> &Arc<Broker> {
        self.inner.broker()
    }
}

impl<P: Send + Sync + 'static, K: Send + Sync + 'static> Clone for TypedConsumer<P, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
