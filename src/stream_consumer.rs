#[cfg(feature = "async")]
use std::sync::Arc;

#[cfg(feature = "async")]
use crate::base_consumer::BaseConsumer;
#[cfg(feature = "async")]
use crate::config::{FromRmqConfig, RmqClientConfig};
#[cfg(feature = "async")]
use crate::consumer::Consumer;
#[cfg(feature = "async")]
use crate::delegate_consumer;
#[cfg(feature = "async")]
use crate::error::RmqResult;
#[cfg(feature = "async")]
use crate::message::OwnedMessage;

/// An async-aware message consumer suitable for use within tokio runtimes.
///
/// `StreamConsumer` wraps a [`BaseConsumer`](crate::base_consumer::BaseConsumer) and
/// exposes both an async [`recv`](StreamConsumer::recv) method and a
/// [`futures::Stream`](futures_core::Stream) adapter via [`stream`](StreamConsumer::stream).
///
/// **Requires** the `async` feature flag.
///
/// # Example
///
/// ```ignore
/// use rmemqueue::{StreamConsumer, Consumer, RmqClientConfig, Message};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), rmemqueue::error::RmqError> {
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// config.set("group.id", "my-group");
/// let consumer = StreamConsumer::new(&config)?;
/// consumer.subscribe(&["my-topic"])?;
///
/// loop {
///     let msg = consumer.recv().await?;
///     println!("Received: offset={}", msg.offset());
/// }
/// # }
/// ```
#[cfg(feature = "async")]
#[derive(Clone)]
pub struct StreamConsumer {
    inner: Arc<BaseConsumer>,
}

#[cfg(feature = "async")]
impl StreamConsumer {
    /// Creates a new `StreamConsumer` from the given configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rmemqueue::stream_consumer::StreamConsumer;
    /// use rmemqueue::config::RmqClientConfig;
    ///
    /// let consumer = StreamConsumer::new(&RmqClientConfig::new())?;
    /// # Ok::<(), rmemqueue::error::RmqError>(())
    /// ```
    pub fn new(config: &RmqClientConfig) -> RmqResult<Self> {
        Ok(Self {
            inner: Arc::new(BaseConsumer::new(config)?),
        })
    }

    /// Asynchronously receives the next available message.
    ///
    /// Awaits notification from the broker that new messages are available, then
    /// returns the next [`OwnedMessage`]. This method is safe to call inside a
    /// tokio runtime.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rmemqueue::{StreamConsumer, Consumer, RmqClientConfig, Message};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), rmemqueue::error::RmqError> {
    /// let mut config = RmqClientConfig::new();
    /// config.set("broker.id", "my-broker");
    /// config.set("group.id", "my-group");
    /// let consumer = StreamConsumer::new(&config)?;
    /// consumer.subscribe(&["my-topic"])?;
    /// let msg = consumer.recv().await?;
    /// println!("offset={}", msg.offset());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv(&self) -> RmqResult<OwnedMessage> {
        loop {
            match self.inner.poll(std::time::Duration::from_millis(0)) {
                Some(Ok(msg)) => return Ok(msg.detach()),
                Some(Err(e)) => return Err(e),
                None => {}
            }

            let topics = self.inner.subscribed_topics();

            if topics.is_empty() {
                tokio::task::yield_now().await;
                continue;
            }

            let notifies = self.inner.broker().get_partition_notifies(&topics)?;
            if notifies.is_empty() {
                tokio::task::yield_now().await;
                continue;
            }

            let notified_fut = async {
                for notify in &notifies {
                    notify.async_notify().notified().await;
                }
            };
            let timeout = tokio::time::sleep(std::time::Duration::from_secs(30));

            tokio::select! {
                _ = notified_fut => {}
                _ = timeout => {}
            }
        }
    }

    /// Returns a [`MessageStream`] that implements [`futures::Stream`](futures_core::Stream).
    ///
    /// Useful with `futures_util::StreamExt` or `tokio_stream::StreamExt`.
    ///
    /// # Example
    ///
/// ```ignore
/// use rmemqueue::{StreamConsumer, Consumer, RmqClientConfig, Message};
/// use futures_util::StreamExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), rmemqueue::error::RmqError> {
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// config.set("group.id", "my-group");
/// let consumer = StreamConsumer::new(&config)?;
/// consumer.subscribe(&["my-topic"])?;
/// let mut stream = consumer.stream();
/// while let Some(result) = stream.next().await {
///     let msg = result?;
///     println!("offset={}", msg.offset());
/// }
/// # Ok(())
/// # }
/// ```
    pub fn stream(&self) -> MessageStream<'_> {
        MessageStream { consumer: self }
    }
}

#[cfg(feature = "async")]
delegate_consumer!(StreamConsumer, inner);

#[cfg(feature = "async")]
impl FromRmqConfig for StreamConsumer {
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        StreamConsumer::new(config)
    }
}

/// A [`futures::Stream`](futures_core::Stream) adapter returned by [`StreamConsumer::stream`].
///
/// Yields [`OwnedMessage`](crate::message::OwnedMessage) items as they become available.
/// Requires the `async` feature flag.
#[cfg(feature = "async")]
pub struct MessageStream<'a> {
    consumer: &'a StreamConsumer,
}

/// [`futures::Stream`] implementation for [`MessageStream`].
///
/// Each call to [`poll_next`](futures_core::Stream::poll_next) attempts to fetch one
/// message from the underlying [`BaseConsumer`](crate::base_consumer::BaseConsumer).
/// If no message is immediately available the implementation registers a waker
/// that is triggered once the broker signals new data.
#[cfg(feature = "async")]
impl<'a> futures_core::Stream for MessageStream<'a> {
    type Item = RmqResult<OwnedMessage>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.consumer.inner.poll(std::time::Duration::from_millis(0)) {
            Some(Ok(msg)) => std::task::Poll::Ready(Some(Ok(msg.detach()))),
            Some(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
            None => {
                let topics = this.consumer.inner.subscribed_topics();
                if topics.is_empty() {
                    cx.waker().wake_by_ref();
                    return std::task::Poll::Pending;
                }

                let notifies = match this.consumer.inner.broker().get_partition_notifies(&topics) {
                    Ok(n) if !n.is_empty() => n,
                    _ => {
                        cx.waker().wake_by_ref();
                        return std::task::Poll::Pending;
                    }
                };

                for notify in &notifies {
                    notify.async_notify().notify_one();
                }

                match this.consumer.inner.poll(std::time::Duration::from_millis(0)) {
                    Some(Ok(msg)) => std::task::Poll::Ready(Some(Ok(msg.detach()))),
                    Some(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
                    None => {
                        let waker = cx.waker().clone();
                        let notifies = notifies.clone();
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = async {
                                    for notify in &notifies {
                                        notify.async_notify().notified().await;
                                    }
                                } => {}
                                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                            }
                            waker.wake();
                        });
                        std::task::Poll::Pending
                    }
                }
            }
        }
    }
}
