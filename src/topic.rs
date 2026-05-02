use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use parking_lot::RwLock;

use crate::error::{RmqError, RmqResult};
use crate::headers::OwnedHeaders;
use crate::message::StoredMessage;
use crate::partition::{Partition, PartitionConfig, PartitionNotify};

pub(crate) struct Topic {
    name: Arc<str>,
    partitions: RwLock<Vec<Arc<Partition>>>,
    partition_count: AtomicI32,
    #[allow(dead_code)]
    config: PartitionConfig,
}

impl Topic {
    pub fn new(name: String, num_partitions: i32, config: PartitionConfig) -> Self {
        let name_arc: Arc<str> = Arc::from(name);
        let partitions: Vec<Arc<Partition>> = (0..num_partitions)
            .map(|id| {
                Arc::new(Partition::new(
                    Arc::clone(&name_arc),
                    id,
                    PartitionConfig {
                        max_capacity: config.max_capacity,
                        retention_ms: config.retention_ms,
                        retention_capacity: config.retention_capacity,
                    },
                ))
            })
            .collect();

        Self {
            name: name_arc,
            partition_count: AtomicI32::new(num_partitions),
            partitions: RwLock::new(partitions),
            config,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn partition_count(&self) -> i32 {
        self.partition_count.load(Ordering::Relaxed)
    }

    pub fn get_partition(&self, id: i32) -> RmqResult<Arc<Partition>> {
        let partitions = self.partitions.read();
        if id < 0 || id as usize >= partitions.len() {
            return Err(RmqError::PartitionOutOfRange {
                topic: self.name.to_string(),
                partition: id,
            });
        }
        Ok(Arc::clone(&partitions[id as usize]))
    }

    pub fn get_partition_notify(&self, id: i32) -> RmqResult<Arc<PartitionNotify>> {
        let p = self.get_partition(id)?;
        Ok(Arc::clone(p.notify()))
    }

    pub fn produce(
        &self,
        partition: i32,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64)> {
        let p = self.get_partition(partition)?;
        p.append(key, payload, headers, timestamp)
    }

    pub fn produce_typed<P: Send + Sync + 'static, K: Send + Sync + 'static>(
        &self,
        partition: i32,
        typed_payload: Arc<P>,
        typed_key: Option<Arc<K>>,
        headers: Option<OwnedHeaders>,
        timestamp: Option<i64>,
    ) -> RmqResult<(i32, i64)> {
        let p = self.get_partition(partition)?;
        p.append_typed(typed_payload, typed_key, headers, timestamp)
    }

    #[allow(dead_code)]
    pub fn fetch(
        &self,
        partition: i32,
        offset: i64,
        max_count: usize,
    ) -> RmqResult<Vec<Arc<StoredMessage>>> {
        let p = self.get_partition(partition)?;
        p.read(offset, max_count)
    }

    pub fn fetch_one(&self, partition: i32, offset: i64) -> RmqResult<Option<Arc<StoredMessage>>> {
        let p = self.get_partition(partition)?;
        p.read_one(offset)
    }

    pub fn watermarks(&self, partition: i32) -> RmqResult<(i64, i64)> {
        let p = self.get_partition(partition)?;
        Ok(p.watermarks())
    }
}
