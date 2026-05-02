use std::sync::Arc;

use crate::broker::{Broker, RecordMetadata};
use crate::error::{RmqError, RmqResult};
use crate::metadata::Metadata;
use crate::record::{BaseRecord, ToBytes};

/// Core trait for message queue producers.
///
/// All producer types — [`BaseProducer`](crate::BaseProducer),
/// [`FutureProducer`](crate::FutureProducer), and
/// [`StreamProducer`](crate::StreamProducer) — implement this trait.
///
/// # Example
///
/// ```ignore
/// use rmemqueue::{Producer, BaseProducer, BaseRecord, RmqClientConfig};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// let producer = BaseProducer::new(&config).unwrap();
///
/// // Access the underlying broker
/// let broker = producer.broker();
///
/// // Send a message
/// let record = BaseRecord::to("my-topic").payload("hello".as_bytes());
/// let result = producer.send(record);
/// ```
pub trait Producer {
    /// Returns a shared reference to the underlying [`Broker`].
    fn broker(&self) -> &Arc<Broker>;

    /// Sends a record to the message queue.
    ///
    /// On success, returns [`RecordMetadata`] containing topic, partition, and offset.
    /// On failure, returns the error together with the original record so the caller
    /// can retry.
    ///
    /// # Example
    ///
/// ```ignore
/// use rmemqueue::{Producer, BaseProducer, BaseRecord};
///
/// let record = BaseRecord::to("topic").payload("data".as_bytes());
/// match producer.send(record) {
///     Ok(meta) => println!("Sent successfully: offset={}", meta.offset),
///     Err((e, _record)) => eprintln!("Send failed: {}", e),
/// }
/// ```
    fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<RecordMetadata, (RmqError, BaseRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized;

    /// Flushes pending records. This is a no-op for the in-memory queue implementation.
    fn flush(&self) -> RmqResult<()> {
        Ok(())
    }

    /// Returns the number of in-flight records. Always returns `0` for the in-memory queue.
    fn in_flight_count(&self) -> i32 {
        0
    }

    /// Queries metadata for the given topic.
    ///
    /// Pass `None` to retrieve metadata for all topics.
    fn metadata(&self, topic: Option<&str>) -> RmqResult<Metadata> {
        self.broker().metadata(topic)
    }

    /// Returns the low and high watermarks for the given topic and partition.
    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)> {
        self.broker().watermarks(topic, partition)
    }
}
