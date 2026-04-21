use async_trait::async_trait;
use greenmqtt_core::{NodeId, ReplicatedRangeDescriptor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSplitResult {
    pub left_range_id: String,
    pub right_range_id: String,
}

#[async_trait]
pub trait RangeControlClient: Send + Sync {
    async fn bootstrap_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String>;
    async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()>;
    async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()>;
    async fn recover_range(&self, range_id: &str, new_leader_node_id: NodeId)
        -> anyhow::Result<()>;
    async fn split_range(&self, range_id: &str, split_key: Vec<u8>)
        -> anyhow::Result<RangeSplitResult>;
    async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String>;
    async fn drain_range(&self, range_id: &str) -> anyhow::Result<()>;
    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()>;
}

