/// Error type for RMemQueue operations.
///
/// Covers all error conditions that can occur during message production, consumption,
/// configuration parsing, and broker operations.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RmqError {
    /// The requested topic does not exist.
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    /// The requested partition index is out of the valid range.
    #[error("Partition out of range: topic={topic}, partition={partition}")]
    PartitionOutOfRange { topic: String, partition: i32 },

    /// The requested message offset is out of the valid range.
    #[error("Offset out of range: topic={topic}, partition={partition}, offset={offset}")]
    OffsetOutOfRange {
        topic: String,
        partition: i32,
        offset: i64,
    },

    /// The requested consumer group does not exist.
    #[error("Consumer group not found: {0}")]
    GroupNotFound(String),

    /// The consumer is already subscribed to the given topics.
    #[error("Already subscribed to: {0:?}")]
    AlreadySubscribed(Vec<String>),

    /// The consumer is not subscribed to any topic.
    #[error("Not subscribed")]
    NotSubscribed,

    /// The broker has been shut down and refuses new operations.
    #[error("Broker is shut down")]
    BrokerShutdown,

    /// The partition buffer is full; no more messages can be written.
    #[error("Buffer full: topic={topic}, partition={partition}")]
    BufferFull { topic: String, partition: i32 },

    /// A configuration value is missing or invalid.
    #[error("{0}")]
    InvalidConfig(String),

    /// A custom, user-defined error.
    #[error("{0}")]
    Custom(String),
}

/// Common result type for RMemQueue operations.
///
/// On success contains `T`; on failure contains [`RmqError`].
pub type RmqResult<T> = Result<T, RmqError>;
