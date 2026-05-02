use std::sync::Arc;

use log::debug;

use crate::broker::{Broker, RecordMetadata};
use crate::config::{FromRmqConfig, RmqClientConfig};
use crate::context::{DefaultProducerContext, DeliveryResult, ProducerContext};
use crate::error::{RmqError, RmqResult};
use crate::message::{OwnedMessage, Timestamp};
use crate::producer::Producer;
use crate::record::{BaseRecord, ToBytes};

/// Synchronous base producer implementing [`Producer`] and [`Clone`].
///
/// `BaseProducer` is the core producer implementation that writes messages
/// directly to the in-memory queue via [`Broker`](crate::broker::Broker).
/// Supports injecting custom callback logic through a [`ProducerContext`].
///
/// # Example
///
/// ```ignore
/// use rmemqueue::{BaseProducer, RmqClientConfig, Producer, BaseRecord};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// let producer = BaseProducer::new(&config).unwrap();
///
/// let record = BaseRecord::to("my-topic").payload("hello".as_bytes());
/// let result = producer.send(record);
/// ```
pub struct BaseProducer<C: ProducerContext = DefaultProducerContext> {
    broker: Arc<Broker>,
    context: Arc<C>,
}

impl BaseProducer {
    /// Creates a new `BaseProducer` with the default producer context.
    ///
    /// # Example
    ///
/// ```ignore
/// use rmemqueue::{BaseProducer, RmqClientConfig};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// let producer = BaseProducer::new(&config).unwrap();
/// ```
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        Self::with_context(config, DefaultProducerContext)
    }
}

impl<C: ProducerContext> BaseProducer<C> {
    /// Creates a new `BaseProducer` with a custom [`ProducerContext`].
    ///
    /// A custom context allows you to provide callback logic that runs when
    /// a message delivery succeeds or fails.
    ///
    /// # Example
    ///
/// ```ignore
/// use rmemqueue::{BaseProducer, RmqClientConfig, DefaultProducerContext};
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// let producer = BaseProducer::with_context(&config, DefaultProducerContext).unwrap();
/// ```
    pub fn with_context(config: &RmqClientConfig, context: C) -> RmqResult<Self> {
        let broker = crate::registry::BrokerRegistry::get_or_create(config)?;
        Ok(Self {
            broker,
            context: Arc::new(context),
        })
    }
}

impl<C: ProducerContext> Producer for BaseProducer<C> {
    fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }

    fn send<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<RecordMetadata, (RmqError, BaseRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let key_bytes = record.key.map(|k| k.to_bytes().to_vec());
        let payload_bytes = record.payload.map(|p| p.to_bytes().to_vec());
        let headers_for_produce = record.headers.clone();

        match self.broker.produce(
            record.topic,
            record.partition,
            key_bytes,
            payload_bytes,
            headers_for_produce,
            record.timestamp,
        ) {
            Ok(meta) => {
                debug!("sent message to {} partition {} offset {}", meta.topic, meta.partition, meta.offset);
                let cb_result: DeliveryResult = Ok(RecordMetadata {
                    topic: meta.topic.clone(),
                    partition: meta.partition,
                    offset: meta.offset,
                    timestamp: meta.timestamp,
                });
                self.context.delivery(&cb_result, RecordMetadata {
                    topic: meta.topic.clone(),
                    partition: meta.partition,
                    offset: meta.offset,
                    timestamp: meta.timestamp,
                });
                Ok(meta)
            }
            Err(e) => {
                debug!("send failed for {}: {}", record.topic, e);
                let owned = OwnedMessage {
                    payload: record.payload.map(|p| p.to_bytes().to_vec()),
                    key: record.key.map(|k| k.to_bytes().to_vec()),
                    topic: record.topic.to_owned(),
                    partition: record.partition.unwrap_or(-1),
                    offset: -1,
                    timestamp: Timestamp::CreateTime(record.timestamp.unwrap_or(0)),
                    headers: record.headers.clone(),
                };
                let delivery_meta = RecordMetadata {
                    topic: record.topic.to_owned(),
                    partition: record.partition.unwrap_or(-1),
                    offset: -1,
                    timestamp: record.timestamp.unwrap_or(0),
                };
                self.context
                    .delivery(&Err((e.clone(), owned)), delivery_meta);
                Err((e, record))
            }
        }
    }
}

impl<C: ProducerContext> Clone for BaseProducer<C> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            context: self.context.clone(),
        }
    }
}

impl FromRmqConfig for BaseProducer {
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        BaseProducer::new(config)
    }
}
