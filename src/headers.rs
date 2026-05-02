/// A single header entry consisting of a key string and an optional byte value.
#[derive(Clone, Debug)]
pub struct Header {
    /// The header key name.
    pub key: String,
    /// The header value as raw bytes, or `None` if the header has no value.
    pub value: Option<Vec<u8>>,
}

/// An owned, growable collection of message headers.
///
/// Supports builder-pattern chaining via [`OwnedHeaders::insert`].
///
/// # Example
///
/// ```ignore
/// use crate::headers::{Header, OwnedHeaders};
///
/// let headers = OwnedHeaders::new()
///     .insert(Header { key: "trace-id".into(), value: Some(b"123".to_vec()) })
///     .insert(Header { key: "source".into(), value: None });
/// ```
#[derive(Clone, Debug, Default)]
pub struct OwnedHeaders {
    headers: Vec<Header>,
}

impl OwnedHeaders {
    /// Creates a new empty `OwnedHeaders`.
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
        }
    }

    /// Creates a new empty `OwnedHeaders` with at least the specified capacity.
    pub fn new_with_capacity(cap: usize) -> Self {
        Self {
            headers: Vec::with_capacity(cap),
        }
    }

    /// Appends a header, returning `self` for chaining.
    pub fn insert(mut self, header: Header) -> Self {
        self.headers.push(header);
        self
    }

    /// Returns the first header matching the given key, or `None` if not found.
    pub fn get(&self, key: &str) -> Option<&Header> {
        self.headers.iter().find(|h| h.key == key)
    }

    /// Returns the header at the given index, or `None` if out of bounds.
    pub fn get_at(&self, idx: usize) -> Option<&Header> {
        self.headers.get(idx)
    }

    /// Returns an iterator over all headers.
    pub fn iter(&self) -> impl Iterator<Item = &Header> {
        self.headers.iter()
    }

    /// Returns the number of headers.
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// Returns `true` if there are no headers.
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Returns the total number of headers (alias for [`len`](OwnedHeaders::len)).
    pub fn count(&self) -> usize {
        self.headers.len()
    }
}
