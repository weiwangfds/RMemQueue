use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use crate::headers::OwnedHeaders;
use crate::record::FromBytes;

/// Represents the timestamp of a message, indicating when it was created or appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp {
    /// Timestamp is not available.
    NotAvailable,
    /// Timestamp set by the producer when the message was created (milliseconds).
    CreateTime(i64),
    /// Timestamp set by the broker when the message was appended to the log (milliseconds).
    LogAppendTime(i64),
}

impl Timestamp {
    /// Converts the timestamp to milliseconds.
    ///
    /// Returns `None` if the timestamp is not available.
    pub fn to_millis(self) -> Option<i64> {
        match self {
            Timestamp::NotAvailable => None,
            Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => Some(ms),
        }
    }

    /// Returns a `CreateTime` timestamp from the current system time with millisecond precision.
    pub fn now() -> Timestamp {
        Timestamp::CreateTime(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        )
    }
}

/// A trait defining the common access interface for queue messages.
pub trait Message {
    /// Returns the message key as a byte slice, or `None` if absent.
    fn key(&self) -> Option<&[u8]>;
    /// Returns the message payload as a byte slice, or `None` if absent.
    fn payload(&self) -> Option<&[u8]>;
    /// Returns the topic name this message belongs to.
    fn topic(&self) -> &str;
    /// Returns the partition index this message resides in.
    fn partition(&self) -> i32;
    /// Returns the offset of this message within its partition.
    fn offset(&self) -> i64;
    /// Returns the timestamp of this message.
    fn timestamp(&self) -> Timestamp;
    /// Returns the headers attached to this message, or `None` if absent.
    fn headers(&self) -> Option<&OwnedHeaders>;

    /// Decodes the payload bytes into type `T`.
    ///
    /// Returns `None` if the payload is absent or deserialization fails.
    fn decode_payload<T: FromBytes>(&self) -> Option<T> {
        self.payload().and_then(FromBytes::from_bytes)
    }

    /// Decodes the key bytes into type `T`.
    ///
    /// Returns `None` if the key is absent or deserialization fails.
    fn decode_key<T: FromBytes>(&self) -> Option<T> {
        self.key().and_then(FromBytes::from_bytes)
    }
}

// ---------------------------------------------------------------------------
// StoredMessage (internal)
// ---------------------------------------------------------------------------

/// Internal storage representation of a message, holding all metadata and content.
pub struct StoredMessage {
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    typed_payload: Option<Arc<dyn Any + Send + Sync>>,
    typed_key: Option<Arc<dyn Any + Send + Sync>>,
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    timestamp: i64,
    headers: Option<OwnedHeaders>,
    #[allow(dead_code)]
    created_at: Instant,
}

impl StoredMessage {
    pub(crate) fn new(
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        topic: Arc<str>,
        partition: i32,
        offset: i64,
        timestamp: i64,
        headers: Option<OwnedHeaders>,
        created_at: Instant,
    ) -> Self {
        Self {
            key,
            payload,
            typed_payload: None,
            typed_key: None,
            topic,
            partition,
            offset,
            timestamp,
            headers,
            created_at,
        }
    }

    pub(crate) fn new_typed<P: Send + Sync + 'static, K: Send + Sync + 'static>(
        typed_payload: Arc<P>,
        typed_key: Option<Arc<K>>,
        topic: Arc<str>,
        partition: i32,
        offset: i64,
        timestamp: i64,
        headers: Option<OwnedHeaders>,
        created_at: Instant,
    ) -> Self {
        Self {
            key: None,
            payload: None,
            typed_payload: Some(typed_payload),
            typed_key: typed_key.map(|k| k as Arc<dyn Any + Send + Sync>),
            topic,
            partition,
            offset,
            timestamp,
            headers,
            created_at,
        }
    }

    /// Returns the message key as a byte slice.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the message payload as a byte slice.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// Returns the topic name this message belongs to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition index this message resides in.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset of this message within its partition.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the raw timestamp value in milliseconds.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns a reference to the headers, if any.
    pub fn headers(&self) -> Option<&OwnedHeaders> {
        self.headers.as_ref()
    }

    /// Returns the `Instant` at which this message was created.
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    pub(crate) fn typed_payload_ref<P: 'static>(&self) -> Option<&P> {
        self.typed_payload
            .as_ref()
            .and_then(|arc| arc.downcast_ref::<P>())
    }

    pub(crate) fn typed_key_ref<K: 'static>(&self) -> Option<&K> {
        self.typed_key
            .as_ref()
            .and_then(|arc| arc.downcast_ref::<K>())
    }

    #[allow(dead_code)]
    pub(crate) fn typed_payload_arc<P: Send + Sync + 'static>(&self) -> Option<Arc<P>> {
        self.typed_payload
            .as_ref()
            .and_then(|arc| arc.clone().downcast::<P>().ok())
    }

    #[allow(dead_code)]
    pub(crate) fn typed_key_arc<K: Send + Sync + 'static>(&self) -> Option<Arc<K>> {
        self.typed_key
            .as_ref()
            .and_then(|arc| arc.clone().downcast::<K>().ok())
    }

    pub(crate) fn has_typed_payload(&self) -> bool {
        self.typed_payload.is_some()
    }

    pub(crate) fn to_owned_message(&self) -> OwnedMessage {
        OwnedMessage {
            payload: self.payload.clone(),
            key: self.key.clone(),
            topic: self.topic.to_string(),
            partition: self.partition,
            offset: self.offset,
            timestamp: Timestamp::CreateTime(self.timestamp),
            headers: self.headers.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// OwnedMessage
// ---------------------------------------------------------------------------

/// An owned message that can be used independently of the storage layer.
#[derive(Clone, Debug)]
pub struct OwnedMessage {
    /// The message payload bytes.
    pub payload: Option<Vec<u8>>,
    /// The message key bytes.
    pub key: Option<Vec<u8>>,
    /// The topic name this message belongs to.
    pub topic: String,
    /// The partition index this message resides in.
    pub partition: i32,
    /// The offset of this message within its partition.
    pub offset: i64,
    /// The timestamp of this message.
    pub timestamp: Timestamp,
    /// The headers attached to this message.
    pub headers: Option<OwnedHeaders>,
}

impl Message for OwnedMessage {
    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn headers(&self) -> Option<&OwnedHeaders> {
        self.headers.as_ref()
    }
}

// ---------------------------------------------------------------------------
// BorrowedMessage
// ---------------------------------------------------------------------------

/// A borrowed message backed by an `Arc<StoredMessage>`, sharing the storage lifetime.
pub struct BorrowedMessage<'a> {
    inner: Arc<StoredMessage>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> BorrowedMessage<'a> {
    pub(crate) fn new(inner: Arc<StoredMessage>) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Detaches this borrowed message into an independent [`OwnedMessage`].
    ///
    /// The returned `OwnedMessage` owns its data and can outlive the original reference.
    pub fn detach(&self) -> OwnedMessage {
        self.inner.to_owned_message()
    }

    pub(crate) fn inner_arc(&self) -> &Arc<StoredMessage> {
        &self.inner
    }
}

impl<'a> Message for BorrowedMessage<'a> {
    fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    fn payload(&self) -> Option<&[u8]> {
        self.inner.payload()
    }

    fn topic(&self) -> &str {
        self.inner.topic()
    }

    fn partition(&self) -> i32 {
        self.inner.partition()
    }

    fn offset(&self) -> i64 {
        self.inner.offset()
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::CreateTime(self.inner.timestamp())
    }

    fn headers(&self) -> Option<&OwnedHeaders> {
        self.inner.headers()
    }
}
