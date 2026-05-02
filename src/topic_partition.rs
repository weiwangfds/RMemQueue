/// Represents a partition offset, either as a symbolic position or an absolute/relative value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offset {
    /// Start consuming from the beginning of the partition (oldest available message).
    Beginning,
    /// Start consuming from the end of the partition (next produced message).
    End,
    /// Use the last committed (stored) offset for the consumer group.
    Stored,
    /// An absolute offset value within the partition.
    Offset(i64),
    /// A relative offset counted backwards from the current end of the partition.
    OffsetTail(i64),
}

/// A single topic-partition pair with an associated offset.
#[derive(Debug, Clone)]
pub struct TopicPartitionElem {
    /// The topic name.
    pub topic: String,
    /// The partition index within the topic.
    pub partition: i32,
    /// The offset position within the partition.
    pub offset: Offset,
}

/// An owned, growable list of [`TopicPartitionElem`] entries.
#[derive(Debug, Clone, Default)]
pub struct TopicPartitionList {
    elements: Vec<TopicPartitionElem>,
}

impl TopicPartitionList {
    /// Creates a new empty `TopicPartitionList`.
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Creates a new empty `TopicPartitionList` with at least the specified capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            elements: Vec::with_capacity(cap),
        }
    }

    /// Adds a topic-partition with a default [`Offset::Stored`] offset.
    ///
    /// Returns a mutable reference to the newly added element.
    pub fn add_partition(&mut self, topic: &str, partition: i32) -> &mut TopicPartitionElem {
        self.elements.push(TopicPartitionElem {
            topic: topic.to_owned(),
            partition,
            offset: Offset::Stored,
        });
        self.elements.last_mut().unwrap()
    }

    /// Adds a topic-partition with the specified [`Offset`].
    pub fn add_partition_offset(&mut self, topic: &str, partition: i32, offset: Offset) {
        self.elements.push(TopicPartitionElem {
            topic: topic.to_owned(),
            partition,
            offset,
        });
    }

    /// Finds the element matching the given topic and partition, or `None` if not found.
    pub fn find_partition(&self, topic: &str, partition: i32) -> Option<&TopicPartitionElem> {
        self.elements
            .iter()
            .find(|e| e.topic == topic && e.partition == partition)
    }

    /// Returns a slice of all [`TopicPartitionElem`] entries.
    pub fn elements(&self) -> &[TopicPartitionElem] {
        &self.elements
    }

    /// Returns references to all elements belonging to the specified topic.
    pub fn elements_for_topic(&self, topic: &str) -> Vec<&TopicPartitionElem> {
        self.elements.iter().filter(|e| e.topic == topic).collect()
    }

    /// Returns the total number of elements in the list.
    pub fn count(&self) -> usize {
        self.elements.len()
    }
}
