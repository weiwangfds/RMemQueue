use std::sync::Arc;

use crate::broker::Broker;
use crate::error::RmqResult;
use crate::message::{BorrowedMessage, Message};
use crate::metadata::Metadata;
use crate::topic_partition::{Offset, TopicPartitionList};

/// The mode used when committing consumer offsets.
///
/// Controls the behavior of [`Consumer::commit`] when persisting offsets.
///
/// # Variants
///
/// - [`CommitMode::Sync`] — blocks until the commit completes
/// - [`CommitMode::Async`] — fires the commit request and returns immediately
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Synchronous commit — blocks until the offset is fully committed.
    Sync,
    /// Asynchronous commit — returns immediately after dispatching the request.
    Async,
}

/// Core consumer trait defining all message consumption operations.
///
/// Provides topic subscription, partition assignment, offset management,
/// metadata queries, and pause/resume capabilities. All consumer types
/// (e.g. [`BaseConsumer`](crate::base_consumer::BaseConsumer),
/// [`StreamConsumer`](crate::stream_consumer::StreamConsumer)) implement this trait.
///
/// # Method groups
///
/// | Group | Methods |
/// |-------|---------|
/// | Subscription | [`subscribe`](Consumer::subscribe), [`unsubscribe`](Consumer::unsubscribe), [`subscription`](Consumer::subscription) |
/// | Partition assignment | [`assign`](Consumer::assign), [`assignment`](Consumer::assignment) |
/// | Offset management | [`seek`](Consumer::seek), [`commit`](Consumer::commit), [`commit_message`](Consumer::commit_message), [`store_offset`](Consumer::store_offset), [`committed`](Consumer::committed), [`position`](Consumer::position) |
/// | Metadata | [`metadata`](Consumer::metadata), [`watermarks`](Consumer::watermarks) |
/// | Pause / resume | [`pause`](Consumer::pause), [`resume`](Consumer::resume) |
///
/// # Example
///
/// ```no_run
/// use rmemqueue::consumer::Consumer;
/// # use rmemqueue::base_consumer::BaseConsumer;
/// # fn example(consumer: &BaseConsumer) -> rmemqueue::error::RmqResult<()> {
/// consumer.subscribe(&["my-topic"])?;
/// let tpl = consumer.subscription()?;
/// println!("Subscribed partitions: {:?}", tpl);
/// # Ok(())
/// # }
/// ```
pub trait Consumer {
    /// Returns a reference to the [`Broker`] this consumer is connected to.
    fn broker(&self) -> &Arc<Broker>;

    // ── Subscription management ─────────────────────────────

    /// Subscribes to the given list of topics.
    ///
    /// If a `group.id` is configured the consumer joins the consumer group and
    /// the broker coordinates partition assignment; without a `group.id` all
    /// partitions are assigned directly to this consumer.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use rmemqueue::consumer::Consumer;
    /// # fn example<C: Consumer>(consumer: &C) -> rmemqueue::error::RmqResult<()> {
    /// consumer.subscribe(&["orders", "payments"])?;
    /// # Ok(())
    /// # }
    /// ```
    fn subscribe(&self, topics: &[&str]) -> RmqResult<()>;

    /// Unsubscribes from all topics and releases assigned partitions.
    ///
    /// Triggers a rebalance notification if the consumer belongs to a consumer group.
    fn unsubscribe(&self) -> RmqResult<()>;

    /// Returns the current list of subscribed partitions.
    fn subscription(&self) -> RmqResult<TopicPartitionList>;

    // ── Partition assignment ────────────────────────────────

    /// Manually assigns the given partition list to this consumer.
    ///
    /// Clears any stored position state. Use this for scenarios that do not
    /// require consumer-group coordination.
    fn assign(&self, partitions: &TopicPartitionList) -> RmqResult<()>;

    /// Returns the list of currently assigned partitions.
    fn assignment(&self) -> RmqResult<TopicPartitionList>;

    // ── Offset management ───────────────────────────────────

    /// Seeks to the specified offset for the given topic-partition.
    ///
    /// # Arguments
    ///
    /// * `topic` — topic name
    /// * `partition` — partition id
    /// * `offset` — target offset (supports `Beginning`, `End`, `Offset`, `Stored`, `OffsetTail`)
    fn seek(&self, topic: &str, partition: i32, offset: Offset) -> RmqResult<()>;

    /// Commits the offsets in the provided [`TopicPartitionList`].
    ///
    /// The `mode` parameter controls synchronous vs asynchronous commit.
    /// Requires a configured `group.id` to take effect.
    fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> RmqResult<()>;

    /// Commits the offset corresponding to a single message.
    ///
    /// Automatically constructs a [`TopicPartitionList`] with `message.offset + 1` and commits it.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use rmemqueue::consumer::{Consumer, CommitMode};
    /// # use rmemqueue::message::BorrowedMessage;
    /// # fn example<C: Consumer>(consumer: &C, msg: &BorrowedMessage<'_>) -> rmemqueue::error::RmqResult<()> {
    /// consumer.commit_message(msg, CommitMode::Async)?;
    /// # Ok(())
    /// # }
    /// ```
    fn commit_message(&self, msg: &BorrowedMessage<'_>, mode: CommitMode) -> RmqResult<()> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            msg.topic(),
            msg.partition(),
            Offset::Offset(msg.offset() + 1),
        );
        self.commit(&tpl, mode)
    }

    /// Stores an offset for the given topic-partition locally (not committed to the broker).
    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> RmqResult<()>;

    /// Returns the committed offsets for all assigned partitions.
    ///
    /// Requires a configured `group.id`; returns an empty list otherwise.
    fn committed(&self) -> RmqResult<TopicPartitionList>;

    /// Returns the current consumption position for all assigned partitions.
    fn position(&self) -> RmqResult<TopicPartitionList>;

    // ── Metadata ────────────────────────────────────────────

    /// Queries cluster metadata.
    ///
    /// When `topic` is `Some`, returns metadata only for that topic; otherwise returns
    /// metadata for all topics.
    fn metadata(&self, topic: Option<&str>) -> RmqResult<Metadata> {
        self.broker().metadata(topic)
    }

    /// Queries the watermarks (low and high) for the given topic-partition.
    ///
    /// Returns `(oldest_offset, newest_offset)`.
    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)> {
        self.broker().watermarks(topic, partition)
    }

    // ── Pause / resume ──────────────────────────────────────

    /// Pauses consumption for the specified partitions.
    ///
    /// Paused partitions are skipped by [`poll`](crate::base_consumer::BaseConsumer::poll).
    fn pause(&self, partitions: &TopicPartitionList) -> RmqResult<()>;

    /// Resumes consumption for the specified partitions.
    fn resume(&self, partitions: &TopicPartitionList) -> RmqResult<()>;
}

/// Macro to delegate `Consumer` trait implementation to an inner field.
/// Usage: `delegate_consumer!(MyType, inner_field_name);`
#[macro_export]
macro_rules! delegate_consumer {
    ($ty:ty, $field:ident) => {
        impl $crate::consumer::Consumer for $ty {
            fn broker(&self) -> &std::sync::Arc<$crate::broker::Broker> {
                self.$field.broker()
            }

            fn subscribe(&self, topics: &[&str]) -> $crate::error::RmqResult<()> {
                self.$field.subscribe(topics)
            }

            fn unsubscribe(&self) -> $crate::error::RmqResult<()> {
                self.$field.unsubscribe()
            }

            fn subscription(&self) -> $crate::error::RmqResult<$crate::topic_partition::TopicPartitionList> {
                self.$field.subscription()
            }

            fn assign(&self, partitions: &$crate::topic_partition::TopicPartitionList) -> $crate::error::RmqResult<()> {
                self.$field.assign(partitions)
            }

            fn assignment(&self) -> $crate::error::RmqResult<$crate::topic_partition::TopicPartitionList> {
                self.$field.assignment()
            }

            fn seek(&self, topic: &str, partition: i32, offset: $crate::topic_partition::Offset) -> $crate::error::RmqResult<()> {
                self.$field.seek(topic, partition, offset)
            }

            fn commit(&self, tpl: &$crate::topic_partition::TopicPartitionList, mode: $crate::consumer::CommitMode) -> $crate::error::RmqResult<()> {
                self.$field.commit(tpl, mode)
            }

            fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> $crate::error::RmqResult<()> {
                self.$field.store_offset(topic, partition, offset)
            }

            fn committed(&self) -> $crate::error::RmqResult<$crate::topic_partition::TopicPartitionList> {
                self.$field.committed()
            }

            fn position(&self) -> $crate::error::RmqResult<$crate::topic_partition::TopicPartitionList> {
                self.$field.position()
            }

            fn pause(&self, partitions: &$crate::topic_partition::TopicPartitionList) -> $crate::error::RmqResult<()> {
                self.$field.pause(partitions)
            }

            fn resume(&self, partitions: &$crate::topic_partition::TopicPartitionList) -> $crate::error::RmqResult<()> {
                self.$field.resume(partitions)
            }
        }
    };
}
