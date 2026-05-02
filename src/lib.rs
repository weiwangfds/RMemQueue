//! RMemQueue — A Kafka-like in-memory message queue for inter-thread communication in Rust.
//!
//! RMemQueue provides a publish-subscribe messaging model inspired by Apache Kafka,
//! supporting topics, partitions, consumer groups, and offset commits.
//!
//! # Features
//!
//! - **In-memory storage**: All messages are held in memory, ideal for testing, embedded use, or
//!   high-performance ephemeral messaging between threads.
//! - **Partition support**: Each topic can have multiple partitions with configurable partitioning
//!   strategies.
//! - **Consumer groups**: Group-based consumption with automatic partition assignment.
//! - **Pluggable backends**: Swap the storage backend via the [`BrokerBackend`] trait.
//! - **Sync & async**: Both synchronous and asynchronous (`async` feature) producers and consumers.
//!
//! # Example
//!
//! ```rust
//! use rmemqueue::{Broker, RmqClientConfig, BaseProducer, BaseConsumer, Consumer};
//!
//! // Create a config and set the broker ID
//! let mut config = RmqClientConfig::new();
//! config.set("broker.id", "test-broker");
//!
//! // Create a Broker
//! let broker = Broker::new(config).unwrap();
//! ```
//!
//! [`BrokerBackend`]: crate::broker_backend::BrokerBackend

pub mod config;
pub mod error;
pub mod headers;
pub mod message;
pub mod metadata;
pub mod offset_store;
pub mod partition_assignor;
pub mod record;
pub mod topic_partition;

pub mod broker;
pub mod broker_backend;
pub(crate) mod consumer_group;
pub mod context;
pub(crate) mod partition;
pub mod partitioner;
pub(crate) mod registry;
pub(crate) mod topic;

pub mod base_producer;
#[cfg(feature = "async")]
pub mod future_producer;
pub mod producer;
#[cfg(feature = "async")]
pub mod stream_producer;

pub mod base_consumer;
pub mod consumer;
pub mod typed;
#[cfg(feature = "async")]
pub mod stream_consumer;

pub use broker::{Broker, RecordMetadata};
pub use broker_backend::{BrokerBackend, InMemoryBackend};
pub use config::{FromRmqConfig, RmqClientConfig};
pub use context::{
    ClientContext, ConsumerContext, DefaultClientContext, DefaultConsumerContext,
    DefaultProducerContext, DeliveryResult, ProducerContext, RebalanceEvent,
};
pub use error::{RmqError, RmqResult};
pub use headers::{Header, OwnedHeaders};
pub use message::{BorrowedMessage, Message, OwnedMessage, StoredMessage, Timestamp};
pub use metadata::{Metadata, PartitionMetadata, TopicMetadata};
pub use offset_store::{InMemoryOffsetStore, OffsetStore};
pub use partition_assignor::{PartitionAssignor, RoundRobinAssignor};
pub use partitioner::{
    ConsistentPartitioner, Partitioner, RandomPartitioner, RoundRobinPartitioner,
};
pub use partition::{EvictionPolicy, TimeEviction, CapacityEviction};
pub use record::{BaseRecord, FromBytes, FutureRecord, ToBytes};
#[cfg(feature = "serde")]
pub use record::{from_json_bytes, to_json_bytes, SerdeJson};
pub use topic_partition::{Offset, TopicPartitionElem, TopicPartitionList};

pub use base_consumer::BaseConsumer;
pub use base_producer::BaseProducer;
pub use consumer::{CommitMode, Consumer};
#[cfg(feature = "async")]
pub use future_producer::FutureProducer;
pub use producer::Producer;
#[cfg(feature = "async")]
pub use stream_consumer::{MessageStream, StreamConsumer};
#[cfg(feature = "async")]
pub use stream_producer::{OwnedRecord, ProducerSink, StreamProducer};
pub use typed::{TypedBorrowedMessage, TypedConsumer, TypedProducer};
