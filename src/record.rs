use crate::headers::OwnedHeaders;

/// A trait for converting a type into a byte slice.
///
/// Used to serialize keys and payloads into `&[u8]` for writing to the message queue.
pub trait ToBytes {
    /// Converts `self` into a byte slice reference.
    fn to_bytes(&self) -> &[u8];
}

/// A trait for deserializing a type from a byte slice.
///
/// Used to decode `&[u8]` read from the message queue into concrete types.
pub trait FromBytes: Sized {
    /// Attempts to construct `Self` from a byte slice, returning `None` on failure.
    fn from_bytes(data: &[u8]) -> Option<Self>;
}

impl FromBytes for Vec<u8> {
    fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(data.to_vec())
    }
}

impl FromBytes for String {
    fn from_bytes(data: &[u8]) -> Option<Self> {
        String::from_utf8(data.to_vec()).ok()
    }
}

impl FromBytes for () {
    fn from_bytes(_data: &[u8]) -> Option<Self> {
        Some(())
    }
}

impl<const N: usize> FromBytes for [u8; N] {
    fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() == N {
            let mut arr = [0u8; N];
            arr.copy_from_slice(data);
            Some(arr)
        } else {
            None
        }
    }
}

impl ToBytes for [u8] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for str {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<T: ToBytes + ?Sized> ToBytes for &T {
    fn to_bytes(&self) -> &[u8] {
        (*self).to_bytes()
    }
}

impl ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}

macro_rules! impl_to_bytes_array {
    ($($n:literal),*) => {
        $(
            impl ToBytes for [u8; $n] {
                fn to_bytes(&self) -> &[u8] {
                    self
                }
            }
        )*
    };
}

impl_to_bytes_array!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31, 32
);

/// A base record for constructing messages to be sent to the queue.
///
/// Generic over key type `K` and payload type `P`, both of which must implement [`ToBytes`].
/// Uses a builder pattern with chainable setter methods.
///
/// # Example
///
/// ```ignore
/// use crate::record::BaseRecord;
///
/// let record = BaseRecord::to("my_topic")
///     .payload(b"hello")
///     .key(b"key1")
///     .partition(0)
///     .timestamp(1234567890);
/// ```
#[derive(Clone, Debug)]
pub struct BaseRecord<'a, K: ToBytes + ?Sized = [u8], P: ToBytes + ?Sized = [u8]> {
    /// The target topic name.
    pub topic: &'a str,
    /// The target partition index; `None` lets the queue choose automatically.
    pub partition: Option<i32>,
    /// The message payload.
    pub payload: Option<&'a P>,
    /// The message key.
    pub key: Option<&'a K>,
    /// The message timestamp in milliseconds; `None` uses the current time.
    pub timestamp: Option<i64>,
    /// The message headers.
    pub headers: Option<OwnedHeaders>,
}

impl<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized> BaseRecord<'a, K, P> {
    /// Creates a new `BaseRecord` targeting the given topic, with all other fields set to defaults.
    pub fn to(topic: &'a str) -> Self {
        Self {
            topic,
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
    pub fn payload(mut self, p: &'a P) -> Self {
        self.payload = Some(p);
        self
    }

    /// Sets the message key.
    pub fn key(mut self, k: &'a K) -> Self {
        self.key = Some(k);
        self
    }

    /// Sets the message timestamp in milliseconds.
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

/// Type alias for backward compatibility — `FutureRecord` is now identical to `BaseRecord`.
pub type FutureRecord<'a, K = [u8], P = [u8]> = BaseRecord<'a, K, P>;

/// A wrapper type that enables JSON serialization via serde.
#[cfg(feature = "serde")]
#[derive(Debug, Clone)]
pub struct SerdeJson<T>(pub T);

/// Serializes a value to JSON bytes using serde.
///
/// Returns an empty `Vec` if serialization fails.
#[cfg(feature = "serde")]
pub fn to_json_bytes<T: serde::Serialize + ?Sized>(val: &T) -> Vec<u8> {
    serde_json::to_vec(val).unwrap_or_default()
}

/// Deserializes a value from JSON bytes using serde.
///
/// Returns `None` if deserialization fails.
#[cfg(feature = "serde")]
pub fn from_json_bytes<T: serde::de::DeserializeOwned>(data: &[u8]) -> Option<T> {
    serde_json::from_slice(data).ok()
}

#[cfg(feature = "serde")]
impl<T: serde::de::DeserializeOwned> FromBytes for SerdeJson<T> {
    fn from_bytes(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok().map(SerdeJson)
    }
}
