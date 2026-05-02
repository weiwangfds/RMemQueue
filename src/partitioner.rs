use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

/// Partition selection strategy trait defining how messages are routed to partitions.
pub trait Partitioner: Send + Sync {
    /// Select a target partition based on topic, message key, and total partition count.
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32;
}

// ---------------------------------------------------------------------------
// Consistent hash partitioner
// ---------------------------------------------------------------------------

/// Consistent hash partitioner using FNV-1a hashing to ensure the same key always maps to the same partition.
/// Falls back to round-robin when the message key is `None`.
pub struct ConsistentPartitioner {
    fallback: RoundRobinPartitioner,
}

impl ConsistentPartitioner {
    /// Create a new consistent hash partitioner.
    pub fn new() -> Self {
        Self {
            fallback: RoundRobinPartitioner::new(),
        }
    }
}

impl Default for ConsistentPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for ConsistentPartitioner {
    fn partition(&self, _topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        if num_partitions <= 0 {
            return 0;
        }
        match key {
            Some(k) => {
                let hash = fnv1a_32(k);
                (hash % num_partitions as u32) as i32
            }
            None => self.fallback.partition(_topic, None, num_partitions),
        }
    }
}

/// FNV-1a 32-bit hash — fast, simple, good distribution.
fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 2166136261;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

// ---------------------------------------------------------------------------
// Round-robin partitioner
// ---------------------------------------------------------------------------

/// Round-robin partitioner that cycles through partitions sequentially.
pub struct RoundRobinPartitioner {
    counter: AtomicI32,
}

impl RoundRobinPartitioner {
    /// Create a new round-robin partitioner.
    pub fn new() -> Self {
        Self {
            counter: AtomicI32::new(0),
        }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for RoundRobinPartitioner {
    fn partition(&self, _topic: &str, _key: Option<&[u8]>, num_partitions: i32) -> i32 {
        if num_partitions <= 0 {
            return 0;
        }
        let prev = self.counter.fetch_add(1, Ordering::Relaxed);
        ((prev % num_partitions) + num_partitions) % num_partitions
    }
}

// ---------------------------------------------------------------------------
// Random partitioner
// ---------------------------------------------------------------------------

static RANDOM_SEED: AtomicU64 = AtomicU64::new(0);

/// Pseudo-random partition selector using a deterministic counter with a large-prime step
/// to approximate random behaviour without requiring the `rand` crate.
pub struct RandomPartitioner {
    state: AtomicI32,
}

impl RandomPartitioner {
    /// Create a new random partitioner seeded from the current timestamp.
    pub fn new() -> Self {
        let base = RANDOM_SEED.fetch_add(1, Ordering::Relaxed);
        let seed = (base as i32)
            .wrapping_mul(1103515245)
            .wrapping_add(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i32)
                    .unwrap_or(1),
            );
        Self {
            state: AtomicI32::new(if seed == 0 { 1 } else { seed }),
        }
    }
}

impl Default for RandomPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for RandomPartitioner {
    fn partition(&self, _topic: &str, _key: Option<&[u8]>, num_partitions: i32) -> i32 {
        if num_partitions <= 0 {
            return 0;
        }
        let prev = self
            .state
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                Some(x.wrapping_mul(1103515245).wrapping_add(12345))
            })
            .unwrap();
        ((prev.abs()) % num_partitions + num_partitions) % num_partitions
    }
}
