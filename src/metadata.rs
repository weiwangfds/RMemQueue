use crate::error::RmqError;

/// Cluster metadata containing broker node information and topic list.
#[derive(Debug, Clone)]
pub struct Metadata {
    /// Broker node identifier.
    pub broker_id: String,
    /// List of topic metadata entries.
    pub topics: Vec<TopicMetadata>,
}

/// Topic metadata containing partition information and optional error.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic name.
    pub name: String,
    /// List of partition metadata entries.
    pub partitions: Vec<PartitionMetadata>,
    /// Topic-level error (e.g. topic does not exist).
    pub error: Option<RmqError>,
}

/// Partition metadata containing offset range and message count.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Partition identifier.
    pub id: i32,
    /// Oldest available offset in the partition (low watermark).
    pub oldest_offset: i64,
    /// Offset of the newest message in the partition (high watermark).
    pub newest_offset: i64,
    /// Number of messages currently stored in the partition.
    pub message_count: i64,
}
