use std::collections::HashMap;

use crate::error::{RmqError, RmqResult};

/// Client configuration for RMemQueue.
///
/// Stores all configuration options as key-value pairs, used to initialise [`Broker`], producers,
/// and consumers.
///
/// # Example
///
/// ```rust
/// use rmemqueue::config::RmqClientConfig;
///
/// let mut config = RmqClientConfig::new();
/// config.set("broker.id", "my-broker");
/// config.set("default.num.partitions", "3");
/// ```
///
/// [`Broker`]: crate::Broker
#[derive(Clone, Debug, Default)]
pub struct RmqClientConfig {
    conf_map: HashMap<String, String>,
}

impl RmqClientConfig {
    /// Creates a new, empty configuration.
    ///
    /// All values start at their defaults. Set required parameters (e.g. `broker.id`) via
    /// [`set`](RmqClientConfig::set).
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the value for the given configuration key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.conf_map.get(key).map(|s| s.as_str())
    }

    /// Sets a configuration key-value pair.
    ///
    /// Overwrites the existing value if the key is already present.
    /// Returns `&mut Self` for chained calls.
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) -> &mut Self {
        self.conf_map.insert(key.into(), value.into());
        self
    }

    /// Removes a configuration key.
    ///
    /// Does nothing if the key does not exist.
    /// Returns `&mut Self` for chained calls.
    pub fn remove(&mut self, key: &str) -> &mut Self {
        self.conf_map.remove(key);
        self
    }
}

/// Trait for constructing a type from an [`RmqClientConfig`].
///
/// Types that implement this trait can parse themselves from a configuration object.
/// For example, [`Broker`] implements `FromRmqConfig`.
///
/// [`Broker`]: crate::Broker
pub trait FromRmqConfig: Sized {
    /// Constructs an instance from the given configuration.
    ///
    /// Returns [`RmqError::InvalidConfig`] if required fields are missing or values are invalid.
    ///
    /// [`RmqError::InvalidConfig`]: crate::RmqError::InvalidConfig
    fn from_config(config: &RmqClientConfig) -> RmqResult<Self>;
}

/// Predefined configuration key constants.
///
/// Contains all recognised RMemQueue configuration keys for use with [`RmqClientConfig::set`].
///
/// # Example
///
/// ```rust
/// use rmemqueue::config::{RmqClientConfig, config_keys};
///
/// let mut config = RmqClientConfig::new();
/// config.set(config_keys::BROKER_ID, "broker-1");
/// config.set(config_keys::NUM_PARTITIONS, "4");
/// ```
pub mod config_keys {
    /// Unique identifier for the broker. **Required.**
    pub const BROKER_ID: &str = "broker.id";

    /// Default number of partitions per topic. Defaults to `1`.
    pub const NUM_PARTITIONS: &str = "default.num.partitions";

    /// Buffer capacity (in messages) per partition. Defaults to `10000`.
    pub const BUFFER_CAPACITY: &str = "partition.buffer.capacity";

    /// Data retention policy. Accepted values: `"none"`, `"time"`, `"capacity"`. Defaults to `"none"`.
    pub const RETENTION_POLICY: &str = "retention.policy";

    /// Capacity-based retention limit (message count). Only effective when `retention.policy` is `"capacity"`.
    pub const RETENTION_CAPACITY: &str = "retention.capacity";

    /// Time-based retention duration in milliseconds. Only effective when `retention.policy` is `"time"`.
    pub const RETENTION_MS: &str = "retention.ms";

    /// Consumer group session timeout in milliseconds. Defaults to `30000`.
    pub const GROUP_SESSION_TIMEOUT: &str = "group.session.timeout.ms";

    /// Consumer group identifier. Used when a consumer joins a group.
    pub const GROUP_ID: &str = "group.id";
}

#[derive(Clone, Debug)]
pub(crate) struct BrokerConfig {
    pub broker_id: String,
    pub default_num_partitions: i32,
    pub buffer_capacity: usize,
    #[allow(dead_code)]
    pub retention_policy: String,
    pub retention_ms: Option<u64>,
    pub retention_capacity: Option<usize>,
    #[allow(dead_code)]
    pub group_session_timeout_ms: u64,
}

impl BrokerConfig {
    pub fn from_config(config: &RmqClientConfig) -> RmqResult<Self> {
        let broker_id = config
            .get(config_keys::BROKER_ID)
            .map(|s| s.to_owned())
            .ok_or_else(|| {
                RmqError::InvalidConfig(format!("{} is required", config_keys::BROKER_ID))
            })?;

        let default_num_partitions = config
            .get(config_keys::NUM_PARTITIONS)
            .map(|s| s.parse::<i32>())
            .transpose()
            .map_err(|e| {
                RmqError::InvalidConfig(format!("Invalid {}: {}", config_keys::NUM_PARTITIONS, e))
            })?
            .unwrap_or(1);

        let buffer_capacity = config
            .get(config_keys::BUFFER_CAPACITY)
            .map(|s| s.parse::<usize>())
            .transpose()
            .map_err(|e| {
                RmqError::InvalidConfig(format!("Invalid {}: {}", config_keys::BUFFER_CAPACITY, e))
            })?
            .unwrap_or(10000);

        let retention_policy = config
            .get(config_keys::RETENTION_POLICY)
            .map(|s| s.to_owned())
            .unwrap_or_else(|| "none".to_owned());

        let retention_capacity = config
            .get(config_keys::RETENTION_CAPACITY)
            .map(|s| s.parse::<usize>())
            .transpose()
            .map_err(|e| {
                RmqError::InvalidConfig(format!(
                    "Invalid {}: {}",
                    config_keys::RETENTION_CAPACITY,
                    e
                ))
            })?;

        let retention_ms = config
            .get(config_keys::RETENTION_MS)
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|e| {
                RmqError::InvalidConfig(format!("Invalid {}: {}", config_keys::RETENTION_MS, e))
            })?;

        let group_session_timeout_ms = config
            .get(config_keys::GROUP_SESSION_TIMEOUT)
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|e| {
                RmqError::InvalidConfig(format!(
                    "Invalid {}: {}",
                    config_keys::GROUP_SESSION_TIMEOUT,
                    e
                ))
            })?
            .unwrap_or(30000);

        Ok(Self {
            broker_id,
            default_num_partitions,
            buffer_capacity,
            retention_policy,
            retention_capacity,
            retention_ms,
            group_session_timeout_ms,
        })
    }
}
