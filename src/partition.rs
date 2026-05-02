use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::trace;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::error::{RmqError, RmqResult};
use crate::headers::OwnedHeaders;
use crate::message::StoredMessage;

/// Eviction policy trait for the partition buffer, determining when to remove the oldest message.
pub trait EvictionPolicy: Send + Sync {
    /// Returns `true` if the front (oldest) message in the buffer should be evicted.
    /// `buffer_len` is the current number of messages in the buffer.
    /// `front_msg` is the oldest message in the buffer.
    fn should_evict(&self, buffer_len: usize, front_msg: &StoredMessage) -> bool;
}

/// Time-based eviction policy that evicts messages whose age exceeds the configured threshold.
pub struct TimeEviction {
    /// Message retention time in milliseconds.
    pub retention_ms: u64,
}

impl EvictionPolicy for TimeEviction {
    fn should_evict(&self, _buffer_len: usize, front_msg: &StoredMessage) -> bool {
        front_msg.created_at().elapsed().as_millis() as u64 > self.retention_ms
    }
}

/// Capacity-based eviction policy that evicts messages when the buffer count exceeds the threshold.
pub struct CapacityEviction {
    /// Maximum number of messages to retain.
    pub retention_capacity: usize,
}

impl EvictionPolicy for CapacityEviction {
    fn should_evict(&self, buffer_len: usize, _front_msg: &StoredMessage) -> bool {
        buffer_len > self.retention_capacity
    }
}

pub(crate) struct PartitionConfig {
    pub max_capacity: usize,
    pub retention_ms: Option<u64>,
    pub retention_capacity: Option<usize>,
}

impl PartitionConfig {
    fn build_policies(&self) -> Vec<Box<dyn EvictionPolicy>> {
        let mut policies: Vec<Box<dyn EvictionPolicy>> = Vec::new();
        if let Some(retention_ms) = self.retention_ms {
            policies.push(Box::new(TimeEviction { retention_ms }));
        }
        if let Some(retention_capacity) = self.retention_capacity {
            policies.push(Box::new(CapacityEviction { retention_capacity }));
        }
        policies
    }
}

/// Partition notification mechanism used to wake waiting consumers when new messages arrive.
/// This type is public because `BrokerBackend` needs to expose it.
pub struct PartitionNotify {
    mutex: Mutex<()>,
    condvar: Condvar,
    #[cfg(feature = "async")]
    async_notify: tokio::sync::Notify,
}

impl PartitionNotify {
    /// Create a new partition notification instance.
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
            #[cfg(feature = "async")]
            async_notify: tokio::sync::Notify::new(),
        }
    }

    /// Notify all waiting consumers that new messages have arrived.
    pub fn notify(&self) {
        self.condvar.notify_all();
        #[cfg(feature = "async")]
        self.async_notify.notify_waiters();
    }

    /// Block until timeout or notified. Returns `true` if the wait timed out, `false` if woken.
    pub fn wait(&self, timeout: Duration) -> bool {
        let mut guard = self.mutex.lock();
        self.condvar.wait_for(&mut guard, timeout).timed_out()
    }

    /// Returns a reference to the async notification handle for tokio-based waiting.
    #[cfg(feature = "async")]
    pub fn async_notify(&self) -> &tokio::sync::Notify {
        &self.async_notify
    }
}

pub(crate) struct Partition {
    topic_name: Arc<str>,
    partition_id: i32,
    // SAFETY: parking_lot::RwLock is used instead of tokio::sync::RwLock because:
    // 1. All operations (append, read_one, read) complete in O(1) — nanosecond-scale
    // 2. No .await point exists between lock acquisition and release
    // 3. VecDeque push_back/get are non-async and never yield
    // 4. Using tokio::sync::RwLock here would add unnecessary async overhead
    buffer: RwLock<PartitionBuffer>,
    notify: Arc<PartitionNotify>,
}

struct PartitionBuffer {
    log: VecDeque<Arc<StoredMessage>>,
    next_offset: i64,
    max_capacity: usize,
    policies: Vec<Box<dyn EvictionPolicy>>,
}

impl Partition {
    pub fn new(topic_name: Arc<str>, partition_id: i32, config: PartitionConfig) -> Self {
        let policies = config.build_policies();
        let max_capacity = config.max_capacity;
        Self {
            topic_name,
            partition_id,
            buffer: RwLock::new(PartitionBuffer {
                log: VecDeque::new(),
                next_offset: 0,
                max_capacity,
                policies,
            }),
            notify: Arc::new(PartitionNotify::new()),
        }
    }

    pub fn notify(&self) -> &Arc<PartitionNotify> {
        &self.notify
    }

    pub fn append(
        &self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64)> {
        let ts = timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let mut buf = self.buffer.write();

        let offset = buf.next_offset;
        buf.next_offset += 1;
        trace!("append to {}/{} offset={}", self.topic_name, self.partition_id, offset);

        let msg = Arc::new(StoredMessage::new(
            key,
            payload,
            Arc::clone(&self.topic_name),
            self.partition_id,
            offset,
            ts,
            headers,
            Instant::now(),
        ));

        buf.log.push_back(msg);

        if buf.max_capacity > 0 && buf.log.len() >= buf.max_capacity {
            buf.log.pop_front();
        }

        Self::evict_buffer(&mut buf);

        self.notify.notify();

        Ok((self.partition_id, offset))
    }

    pub fn append_typed<P: Send + Sync + 'static, K: Send + Sync + 'static>(
        &self,
        typed_payload: Arc<P>,
        typed_key: Option<Arc<K>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64)> {
        let ts = timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let mut buf = self.buffer.write();

        let offset = buf.next_offset;
        buf.next_offset += 1;
        trace!("append_typed to {}/{} offset={}", self.topic_name, self.partition_id, offset);

        let msg = Arc::new(StoredMessage::new_typed(
            typed_payload,
            typed_key,
            Arc::clone(&self.topic_name),
            self.partition_id,
            offset,
            ts,
            headers,
            Instant::now(),
        ));

        buf.log.push_back(msg);

        if buf.max_capacity > 0 && buf.log.len() >= buf.max_capacity {
            buf.log.pop_front();
        }

        Self::evict_buffer(&mut buf);

        self.notify.notify();

        Ok((self.partition_id, offset))
    }

    pub fn read_one(&self, offset: i64) -> RmqResult<Option<Arc<StoredMessage>>> {
        let buf = self.buffer.read();

        if buf.log.is_empty() {
            if offset == 0 || offset == buf.next_offset {
                return Ok(None);
            }
            trace!("read_one offset out of range: {}/{} offset={}", self.topic_name, self.partition_id, offset);
            return Err(RmqError::OffsetOutOfRange {
                topic: self.topic_name.to_string(),
                partition: self.partition_id,
                offset,
            });
        }

        let oldest = buf.next_offset - buf.log.len() as i64;

        if offset < oldest || offset > buf.next_offset {
            trace!("read_one offset out of range: {}/{} offset={}", self.topic_name, self.partition_id, offset);
            return Err(RmqError::OffsetOutOfRange {
                topic: self.topic_name.to_string(),
                partition: self.partition_id,
                offset,
            });
        }

        if offset == buf.next_offset {
            return Ok(None);
        }

        let idx = (offset - oldest) as usize;
        Ok(Some(Arc::clone(&buf.log[idx])))
    }

    #[allow(dead_code)]
    pub fn read(&self, offset: i64, max_count: usize) -> RmqResult<Vec<Arc<StoredMessage>>> {
        let buf = self.buffer.read();

        if buf.log.is_empty() {
            if offset == 0 || offset == buf.next_offset {
                return Ok(Vec::new());
            }
            return Err(RmqError::OffsetOutOfRange {
                topic: self.topic_name.to_string(),
                partition: self.partition_id,
                offset,
            });
        }

        let oldest = buf.next_offset - buf.log.len() as i64;

        if offset < oldest || offset > buf.next_offset {
            return Err(RmqError::OffsetOutOfRange {
                topic: self.topic_name.to_string(),
                partition: self.partition_id,
                offset,
            });
        }

        if offset == buf.next_offset {
            return Ok(Vec::new());
        }

        let start = (offset - oldest) as usize;
        let end = std::cmp::min(start + max_count, buf.log.len());

        Ok(buf.log.range(start..end).cloned().collect())
    }

    pub fn watermarks(&self) -> (i64, i64) {
        let buf = self.buffer.read();
        let newest = buf.next_offset - 1;
        let oldest = if buf.log.is_empty() {
            0
        } else {
            buf.next_offset - buf.log.len() as i64
        };
        (oldest, newest)
    }

    pub fn message_count(&self) -> i64 {
        let buf = self.buffer.read();
        buf.log.len() as i64
    }

    #[allow(dead_code)]
    pub fn evict(&self) {
        let mut buf = self.buffer.write();
        Self::evict_buffer(&mut buf);
    }

    fn evict_buffer(buf: &mut PartitionBuffer) {
        let initial_len = buf.log.len();
        while let Some(front) = buf.log.front() {
            let should_evict = buf.policies.iter().any(|p| p.should_evict(buf.log.len(), front));
            if should_evict {
                buf.log.pop_front();
            } else {
                break;
            }
        }
        if buf.log.len() < initial_len {
            trace!("evicting messages from buffer, {} messages remaining", buf.log.len());
        }
    }
}
