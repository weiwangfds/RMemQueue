use std::collections::HashMap;
use std::sync::{Arc, Weak};

use log::{debug, info};
use once_cell::sync::Lazy;
use parking_lot::RwLock;

use crate::broker::Broker;
use crate::config::RmqClientConfig;
use crate::error::RmqResult;

static REGISTRY: Lazy<RwLock<HashMap<String, Weak<Broker>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub(crate) struct BrokerRegistry;

impl BrokerRegistry {
    pub fn get_or_create(config: &RmqClientConfig) -> RmqResult<Arc<Broker>> {
        let broker_id = config
            .get("broker.id")
            .expect("broker.id is required (validated by BrokerConfig)")
            .to_owned();

        {
            let registry = REGISTRY.read();
            if let Some(weak) = registry.get(&broker_id) {
                if let Some(strong) = weak.upgrade() {
                    debug!("reusing existing broker: {}", broker_id);
                    return Ok(strong);
                }
            }
        }

        let broker = Broker::new(config.clone())?;
        {
            let mut registry = REGISTRY.write();
            info!("creating new broker: {}", broker_id);
            registry.insert(broker_id, Arc::downgrade(&broker));
        }
        Ok(broker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_shares_broker() {
        let mut config1 = RmqClientConfig::new();
        config1.set("broker.id", "shared-test-broker");
        config1.set("default.num.partitions", "1");

        let mut config2 = RmqClientConfig::new();
        config2.set("broker.id", "shared-test-broker");
        config2.set("default.num.partitions", "1");

        let b1 = BrokerRegistry::get_or_create(&config1).unwrap();
        let b2 = BrokerRegistry::get_or_create(&config2).unwrap();
        assert!(Arc::ptr_eq(&b1, &b2));
    }

    #[test]
    fn test_registry_different_ids() {
        let mut config1 = RmqClientConfig::new();
        config1.set("broker.id", "diff-broker-a");

        let mut config2 = RmqClientConfig::new();
        config2.set("broker.id", "diff-broker-b");

        let b1 = BrokerRegistry::get_or_create(&config1).unwrap();
        let b2 = BrokerRegistry::get_or_create(&config2).unwrap();
        assert!(!Arc::ptr_eq(&b1, &b2));
    }
}
