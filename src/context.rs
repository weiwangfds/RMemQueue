use crate::broker::RecordMetadata;
use crate::error::{RmqError, RmqResult};
use crate::message::OwnedMessage;
use crate::topic_partition::TopicPartitionList;

/// Client context trait providing pluggable callback hooks.
pub trait ClientContext: Send + Sync + 'static {
    /// Called when the client encounters an error. The default implementation ignores it.
    fn error(&self, error: RmqError, reason: &str) {
        let _ = (error, reason); // suppress unused
    }
}

/// Default client context with all callbacks as no-ops.
pub struct DefaultClientContext;
impl ClientContext for DefaultClientContext {}

/// Producer context trait providing message delivery result callbacks.
pub trait ProducerContext: ClientContext {
    /// Called when a message delivery completes (success or failure).
    fn delivery(&self, result: &DeliveryResult, metadata: RecordMetadata);
}

/// Default producer context with delivery callback as a no-op.
pub struct DefaultProducerContext;
impl ClientContext for DefaultProducerContext {}
impl ProducerContext for DefaultProducerContext {
    fn delivery(&self, _result: &DeliveryResult, _metadata: RecordMetadata) {}
}

/// Consumer context trait providing rebalance and offset commit callbacks.
pub trait ConsumerContext: ClientContext {
    /// Called when the partition assignment for the consumer group changes.
    fn rebalance(&self, _event: &RebalanceEvent) {}
    /// Called when an offset commit completes.
    fn commit_callback(&self, _result: RmqResult<()>, _offsets: &TopicPartitionList) {}
}

/// Default consumer context with all callbacks as no-ops.
pub struct DefaultConsumerContext;
impl ClientContext for DefaultConsumerContext {}
impl ConsumerContext for DefaultConsumerContext {}

/// Type alias for message delivery results: success yields record metadata, failure yields the error and original message.
pub type DeliveryResult = Result<RecordMetadata, (RmqError, OwnedMessage)>;

/// Rebalance event indicating partition assignment or revocation.
#[derive(Debug)]
pub enum RebalanceEvent {
    /// Partitions have been assigned to this consumer.
    Assigned(TopicPartitionList),
    /// Partitions have been revoked from this consumer.
    Revoked(TopicPartitionList),
}
