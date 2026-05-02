use std::collections::HashMap;
use std::sync::Arc;

use log::{debug, info, warn};
use parking_lot::RwLock;

use crate::error::{RmqError, RmqResult};
use crate::offset_store::OffsetStore;
use crate::partition_assignor::PartitionAssignor;
use crate::topic_partition::TopicPartitionList;

struct GroupMember {
    #[allow(dead_code)]
    member_id: String,
    #[allow(dead_code)]
    subscribed_topics: Vec<String>,
}

struct GroupState {
    members: HashMap<String, GroupMember>,
    member_topics: HashMap<String, Vec<String>>,
    assignments: HashMap<String, TopicPartitionList>,
}

pub(crate) struct ConsumerGroup {
    #[allow(dead_code)]
    group_id: String,
    state: RwLock<GroupState>,
    assignor: Arc<dyn PartitionAssignor>,
    offset_store: Arc<dyn OffsetStore>,
}

impl ConsumerGroup {
    pub fn new(
        group_id: String,
        assignor: Arc<dyn PartitionAssignor>,
        offset_store: Arc<dyn OffsetStore>,
    ) -> Self {
        Self {
            group_id,
            state: RwLock::new(GroupState {
                members: HashMap::new(),
                member_topics: HashMap::new(),
                assignments: HashMap::new(),
            }),
            assignor,
            offset_store,
        }
    }

    #[allow(dead_code)]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn join(
        &self,
        member_id: &str,
        topics: &[String],
        topic_partition_counts: &HashMap<String, i32>,
    ) -> RmqResult<TopicPartitionList> {
        let mut state = self.state.write();
        state.members.insert(
            member_id.to_owned(),
            GroupMember {
                member_id: member_id.to_owned(),
                subscribed_topics: topics.to_vec(),
            },
        );
        state
            .member_topics
            .insert(member_id.to_owned(), topics.to_vec());
        self.rebalance(&mut state, topic_partition_counts);
        let assignment = state
            .assignments
            .get(member_id)
            .cloned()
            .ok_or_else(|| RmqError::Custom(format!("no assignment for member {}", member_id)))?;
        info!("member {} joined group {}, assigned {} partitions", member_id, self.group_id, assignment.count());
        Ok(assignment)
    }

    pub fn leave(
        &self,
        member_id: &str,
        topic_partition_counts: &HashMap<String, i32>,
    ) -> RmqResult<()> {
        let mut state = self.state.write();
        state.members.remove(member_id);
        state.member_topics.remove(member_id);
        state.assignments.remove(member_id);
        info!("member {} left group {}", member_id, self.group_id);
        if !state.members.is_empty() {
            self.rebalance(&mut state, topic_partition_counts);
        }
        Ok(())
    }

    fn rebalance(
        &self,
        state: &mut GroupState,
        topic_partition_counts: &HashMap<String, i32>,
    ) {
        if state.members.is_empty() {
            state.assignments.clear();
            return;
        }
        debug!("rebalancing group {} with {} members", self.group_id, state.members.len());
        match self
            .assignor
            .assign(&state.member_topics, topic_partition_counts)
        {
            Ok(new_assignments) => state.assignments = new_assignments,
            Err(_) => {
                warn!("rebalance failed for group {}, keeping existing assignments", self.group_id);
            }
        }
    }

    pub fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> RmqResult<()> {
        self.offset_store
            .commit(&self.group_id, topic, partition, offset)
    }

    pub fn committed_offset(&self, topic: &str, partition: i32) -> RmqResult<Option<i64>> {
        self.offset_store.committed(&self.group_id, topic, partition)
    }

    #[allow(dead_code)]
    pub fn assignment(&self, member_id: &str) -> Option<TopicPartitionList> {
        let state = self.state.read();
        state.assignments.get(member_id).cloned()
    }
}
