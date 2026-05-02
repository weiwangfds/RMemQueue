use log::debug;

use crate::base_producer::BaseProducer;
use crate::config::{FromRmqConfig, RmqClientConfig};
use crate::context::DeliveryResult;
use crate::error::RmqResult;
use crate::message::{OwnedMessage, Timestamp};
use crate::producer::Producer;
use crate::record::{FutureRecord, ToBytes};

/// Async producer providing an `async/await` message sending interface.
///
/// `FutureProducer` wraps a [`BaseProducer`](crate::base_producer::BaseProducer)
/// and exposes its synchronous `send` as an async method. Because the underlying
/// queue is in-memory, sends complete immediately, but the async interface
/// integrates seamlessly with async runtimes such as tokio.
///
/// Requires the `async` feature flag.
///
/// # Example
///
/// ```ignore
/// use rmemqueue::future_producer::FutureProducer;
/// use rmemqueue::config::RmqClientConfig;
/// use rmemqueue::record::FutureRecord;
///
/// let config = RmqClientConfig::new("localhost:5672");
/// let producer = FutureProducer::new(&config).unwrap();
///
/// let record = FutureRecord::to("my-topic").payload("hello".as_bytes());
/// let result = producer.send(record).await;
/// match result {
///     Ok(meta) => println!("Sent successfully: offset={}", meta.offset),
///     Err((e, _msg)) => eprintln!("Send failed: {}", e),
/// }
/// ```
pub struct FutureProducer {
    inner: BaseProducer,
}

fn to_delivery_error<K: ToBytes + ?Sized, P: ToBytes + ?Sized>(
    e: &crate::error::RmqError,
    record: &crate::record::BaseRecord<'_, K, P>,
) -> (crate::error::RmqError, OwnedMessage) {
    (
        e.clone(),
        OwnedMessage {
            payload: record.payload.map(|p| p.to_bytes().to_vec()),
            key: record.key.map(|k| k.to_bytes().to_vec()),
            topic: record.topic.to_owned(),
            partition: record.partition.unwrap_or(-1),
            offset: -1,
            timestamp: Timestamp::CreateTime(record.timestamp.unwrap_or(0)),
            headers: record.headers.clone(),
        },
    )
}

impl FutureProducer {
    /// Creates a new `FutureProducer`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rmemqueue::future_producer::FutureProducer;
    /// use rmemqueue::config::RmqClientConfig;
    ///
    /// let config = RmqClientConfig::new("localhost:5672");
    /// let producer = FutureProducer::new(&config).unwrap();
    /// ```
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        Ok(Self {
            inner: BaseProducer::new(config)?,
        })
    }

    /// Async send that returns a [`DeliveryResult`].
    ///
    /// On success, returns [`RecordMetadata`](crate::broker::RecordMetadata).
    /// On failure, returns the error together with the corresponding
    /// [`OwnedMessage`](crate::message::OwnedMessage).
    ///
    /// Because the underlying queue is in-memory, this method completes
    /// immediately, but the `async` interface makes it convenient to
    /// integrate with async runtimes (e.g. tokio).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rmemqueue::future_producer::FutureProducer;
    /// use rmemqueue::record::FutureRecord;
    /// use rmemqueue::config::RmqClientConfig;
    ///
    /// let producer = FutureProducer::new(&RmqClientConfig::new("localhost:5672")).unwrap();
    ///
    /// let record = FutureRecord::to("topic").payload("data".as_bytes());
    /// let result = producer.send(record).await;
    /// ```
    pub async fn send<K, P>(&self, record: FutureRecord<'_, K, P>) -> DeliveryResult
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        match self.inner.send(record) {
            Ok(meta) => {
                debug!(
                    "future send completed: {} partition {} offset {}",
                    meta.topic, meta.partition, meta.offset
                );
                Ok(meta)
            }
            Err((e, record)) => {
                debug!("future send failed: {}", e);
                Err(to_delivery_error(&e, &record))
            }
        }
    }
}

impl Clone for FutureProducer {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl FromRmqConfig for FutureProducer {
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        FutureProducer::new(config)
    }
}
