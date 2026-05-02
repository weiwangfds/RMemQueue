use crate::error::RmqResult;

/// Consumer group offset store trait for persisting committed offsets.
pub trait OffsetStore: Send + Sync + 'static {
    /// Commit the consumed offset for a consumer group on a specific topic-partition.
    fn commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> RmqResult<()>;
    /// Retrieve the last committed offset for a consumer group on a specific topic-partition.
    fn committed(&self, group_id: &str, topic: &str, partition: i32) -> RmqResult<Option<i64>>;
}

/// In-memory HashMap-based offset store implementation.
pub struct InMemoryOffsetStore {
    offsets: parking_lot::RwLock<std::collections::HashMap<(String, String, i32), i64>>,
}

impl InMemoryOffsetStore {
    /// Create a new in-memory offset store instance.
    pub fn new() -> Self {
        Self {
            offsets: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemoryOffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetStore for InMemoryOffsetStore {
    fn commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> RmqResult<()> {
        let mut offsets = self.offsets.write();
        offsets.insert((group_id.to_owned(), topic.to_owned(), partition), offset);
        Ok(())
    }

    fn committed(&self, group_id: &str, topic: &str, partition: i32) -> RmqResult<Option<i64>> {
        let offsets = self.offsets.read();
        Ok(offsets
            .get(&(group_id.to_owned(), topic.to_owned(), partition))
            .copied())
    }
}
