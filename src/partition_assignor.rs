use std::collections::HashMap;

use crate::error::RmqResult;
use crate::topic_partition::TopicPartitionList;

/// Partition assignment strategy trait defining how partitions are distributed among consumer group members.
pub trait PartitionAssignor: Send + Sync + 'static {
    /// Returns the name of this assignment strategy.
    fn name(&self) -> &str;

    /// Compute partition assignments for all members.
    /// `member_topics`: member ID → list of subscribed topics.
    /// `partition_counts`: topic name → number of partitions.
    /// Returns: member ID → assigned `TopicPartitionList`.
    fn assign(
        &self,
        member_topics: &HashMap<String, Vec<String>>,
        partition_counts: &HashMap<String, i32>,
    ) -> RmqResult<HashMap<String, TopicPartitionList>>;
}

/// Round-robin assignor that distributes partitions evenly among members subscribed to each topic.
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    /// Returns the strategy name "roundrobin".
    fn name(&self) -> &str {
        "roundrobin"
    }

    fn assign(
        &self,
        member_topics: &HashMap<String, Vec<String>>,
        partition_counts: &HashMap<String, i32>,
    ) -> RmqResult<HashMap<String, TopicPartitionList>> {
        let mut sorted_members: Vec<&str> = member_topics.keys().map(|s| s.as_str()).collect();
        sorted_members.sort();

        let mut assignments: HashMap<String, TopicPartitionList> = sorted_members
            .iter()
            .map(|&m| (m.to_owned(), TopicPartitionList::new()))
            .collect();

        let all_topics: std::collections::HashSet<&str> = member_topics
            .values()
            .flat_map(|v| v.iter().map(|s| s.as_str()))
            .collect();

        for topic in &all_topics {
            let num_partitions = match partition_counts.get(*topic) {
                Some(&n) => n,
                None => continue,
            };

            let subscribed: Vec<&str> = sorted_members
                .iter()
                .filter(|m| {
                    member_topics
                        .get(**m)
                        .map(|t| t.iter().any(|t2| t2 == *topic))
                        .unwrap_or(false)
                })
                .copied()
                .collect();

            let sub_count = subscribed.len();
            if sub_count == 0 || num_partitions == 0 {
                continue;
            }

            for p in 0..num_partitions {
                let member_idx = p as usize % sub_count;
                let member_id = subscribed[member_idx];
                assignments
                    .get_mut(member_id)
                    .unwrap()
                    .add_partition(topic, p);
            }
        }

        Ok(assignments)
    }
}
