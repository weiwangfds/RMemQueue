#[cfg(feature = "async")]
use std::sync::Arc;

#[cfg(feature = "async")]
use crate::base_producer::BaseProducer;
#[cfg(feature = "async")]
use crate::broker::RecordMetadata;
#[cfg(feature = "async")]
use crate::config::{FromRmqConfig, RmqClientConfig};
#[cfg(feature = "async")]
use crate::error::{RmqError, RmqResult};
#[cfg(feature = "async")]
use crate::headers::OwnedHeaders;
#[cfg(feature = "async")]
use crate::producer::Producer;
#[cfg(feature = "async")]
use crate::record::{BaseRecord, ToBytes};

/// An owned record that stores its own key/payload buffers (unlike the borrowed [`BaseRecord`](crate::record::BaseRecord)).
///
/// Designed to be used as the `Item` type for [`futures_sink::Sink`], e.g. [`ProducerSink`].
///
/// Uses the builder pattern: create via [`to`](OwnedRecord::to), then chain field setters.
///
/// # Example
///
/// ```ignore
/// use rmemqueue::stream_producer::OwnedRecord;
/// use rmemqueue::headers::OwnedHeaders;
///
/// let record = OwnedRecord::to("my-topic")
///     .partition(0)
///     .payload(vec![1, 2, 3])
///     .key(vec![4, 5, 6])
///     .timestamp(1234567890)
///     .headers(OwnedHeaders::new().add("key", "value".as_bytes()));
/// ```
#[cfg(feature = "async")]
#[derive(Clone, Debug)]
pub struct OwnedRecord {
    /// Target topic name.
    pub topic: String,
    /// Target partition number. `None` lets the broker choose automatically.
    pub partition: Option<i32>,
    /// Message payload bytes.
    pub payload: Option<Vec<u8>>,
    /// Message key bytes.
    pub key: Option<Vec<u8>>,
    /// Message timestamp (Unix milliseconds).
    pub timestamp: Option<i64>,
    /// Message headers.
    pub headers: Option<OwnedHeaders>,
}

#[cfg(feature = "async")]
impl OwnedRecord {
    /// Creates an `OwnedRecord` targeting the given topic, with all other fields set to `None`.
    pub fn to(topic: &str) -> Self {
        Self {
            topic: topic.to_owned(),
            partition: None,
            payload: None,
            key: None,
            timestamp: None,
            headers: None,
        }
    }

    /// Sets the target partition.
    pub fn partition(mut self, p: i32) -> Self {
        self.partition = Some(p);
        self
    }

    /// Sets the message payload.
    pub fn payload(mut self, p: Vec<u8>) -> Self {
        self.payload = Some(p);
        self
    }

    /// Sets the message key.
    pub fn key(mut self, k: Vec<u8>) -> Self {
        self.key = Some(k);
        self
    }

    /// Sets the message timestamp (Unix milliseconds).
    pub fn timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Sets the message headers.
    pub fn headers(mut self, h: OwnedHeaders) -> Self {
        self.headers = Some(h);
        self
    }
}

#[cfg(feature = "async")]
impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> From<BaseRecord<'a, K, P>> for OwnedRecord {
    fn from(record: BaseRecord<'a, K, P>) -> Self {
        Self {
            topic: record.topic.to_owned(),
            partition: record.partition,
            payload: record.payload.map(|p| p.to_bytes().to_vec()),
            key: record.key.map(|k| k.to_bytes().to_vec()),
            timestamp: record.timestamp,
            headers: record.headers,
        }
    }
}

// ---------------------------------------------------------------------------
// StreamProducer — async producer with Sink support
// ---------------------------------------------------------------------------

/// Async producer with [`futures_sink::Sink`] support via [`sink`](StreamProducer::sink).
///
/// Wraps a [`BaseProducer`](crate::base_producer::BaseProducer) and implements [`Producer`].
/// Also provides a [`sink`](StreamProducer::sink) method that returns a [`ProducerSink`]
/// implementing `Sink<OwnedRecord>`.
///
/// Requires the `async` feature flag.
///
/// # Example
///
/// ```ignore
/// use rmemqueue::stream_producer::StreamProducer;
/// use rmemqueue::config::RmqClientConfig;
/// use rmemqueue::record::BaseRecord;
/// use rmemqueue::producer::Producer;
///
/// let config = RmqClientConfig::new("localhost:5672");
/// let producer = StreamProducer::new(&config).unwrap();
///
/// let record = BaseRecord::to("my-topic").payload("hello".as_bytes());
/// let result = producer.send_record(record);
/// ```
#[cfg(feature = "async")]
#[derive(Clone)]
pub struct StreamProducer {
    inner: Arc<BaseProducer>,
}

#[cfg(feature = "async")]
impl StreamProducer {
    /// Creates a new `StreamProducer`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rmemqueue::stream_producer::StreamProducer;
    /// use rmemqueue::config::RmqClientConfig;
    ///
    /// let config = RmqClientConfig::new("localhost:5672");
    /// let producer = StreamProducer::new(&config).unwrap();
    /// ```
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        Ok(Self {
            inner: Arc::new(BaseProducer::new(config)?),
        })
    }

    /// Sends a [`BaseRecord`] directly, returning metadata on success or the record on failure.
    pub fn send_record<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<RecordMetadata, (RmqError, BaseRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.inner.send(record)
    }

    /// Send an [`OwnedRecord`], returning metadata on success or the record on failure.
    pub fn send(&self, record: OwnedRecord) -> Result<RecordMetadata, (RmqError, OwnedRecord)> {
        let result = {
            let mut base: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(&record.topic);
            if let Some(ref p) = record.payload {
                base = base.payload(p.as_slice());
            }
            if let Some(ref k) = record.key {
                base = base.key(k.as_slice());
            }
            if let Some(p) = record.partition {
                base = base.partition(p);
            }
            if let Some(ts) = record.timestamp {
                base = base.timestamp(ts);
            }
            if let Some(ref h) = record.headers {
                base = base.headers(h.clone());
            }
            self.inner.send(base).map_err(|(e, _)| e)
        };
        result.map_err(|e| (e, record))
    }

    /// Returns a [`ProducerSink`] that implements `Sink<OwnedRecord>`.
    ///
    /// Use this to integrate with async stream combinators.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rmemqueue::stream_producer::{StreamProducer, OwnedRecord};
    /// use rmemqueue::config::RmqClientConfig;
    /// use futures::SinkExt;
    ///
    /// let producer = StreamProducer::new(&RmqClientConfig::new("localhost:5672")).unwrap();
    /// let mut sink = producer.sink();
    ///
    /// let record = OwnedRecord::to("topic").payload(vec![1, 2, 3]);
    /// sink.send(record).await.unwrap();
    /// ```
    pub fn sink(&self) -> ProducerSink {
        ProducerSink {
            inner: self.clone(),
        }
    }
}

#[cfg(feature = "async")]
impl Producer for StreamProducer {
    fn broker(&self) -> &std::sync::Arc<crate::broker::Broker> {
        self.inner.broker()
    }

    fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<RecordMetadata, (RmqError, BaseRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.inner.send(record)
    }
}

#[cfg(feature = "async")]
impl FromRmqConfig for StreamProducer {
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        StreamProducer::new(config)
    }
}

// ---------------------------------------------------------------------------
// ProducerSink — Sink<OwnedRecord> adapter
// ---------------------------------------------------------------------------

/// `Sink<OwnedRecord>` adapter created via [`StreamProducer::sink`].
///
/// All methods complete immediately (in-memory, no buffering).
/// Success metadata is not surfaced through the `Sink` trait;
/// use [`StreamProducer::send`] instead if you need metadata.
///
/// Requires the `async` feature flag.
///
/// # Example
///
/// ```ignore
/// use rmemqueue::stream_producer::{StreamProducer, OwnedRecord, ProducerSink};
/// use rmemqueue::config::RmqClientConfig;
/// use futures::SinkExt;
///
/// let producer = StreamProducer::new(&RmqClientConfig::new("localhost:5672")).unwrap();
/// let mut sink: ProducerSink = producer.sink();
///
/// let record = OwnedRecord::to("topic").payload(vec![1, 2, 3]);
/// sink.send(record).await.unwrap();
/// ```
#[cfg(feature = "async")]
pub struct ProducerSink {
    inner: StreamProducer,
}

#[cfg(feature = "async")]
impl futures_sink::Sink<OwnedRecord> for ProducerSink {
    type Error = (RmqError, OwnedRecord);

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: OwnedRecord,
    ) -> Result<(), Self::Error> {
        self.get_mut().inner.send(item).map(|_| ())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
