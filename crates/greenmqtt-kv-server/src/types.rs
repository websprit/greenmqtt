use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    NodeId, RangeReconfigurationState, ReconfigurationPhase, ReplicatedRangeDescriptor,
    ServiceShardLifecycle,
};
use greenmqtt_kv_engine::{KvRangeSnapshot, KvRangeSpace};
use greenmqtt_kv_raft::{RaftMessage, RaftNode, RaftStatusSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;


#[derive(Clone)]
pub struct HostedRange {
    pub descriptor: ReplicatedRangeDescriptor,
    pub raft: Arc<dyn RaftNode>,
    pub space: Arc<dyn KvRangeSpace>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostedRangeStatus {
    pub descriptor: ReplicatedRangeDescriptor,
    pub raft: RaftStatusSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedRangeSnapshot {
    pub snapshot: KvRangeSnapshot,
    pub entries: Vec<(Bytes, Bytes)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaLagSnapshot {
    pub node_id: NodeId,
    pub lag: u64,
    pub match_index: u64,
    pub next_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeHealthSnapshot {
    pub range_id: String,
    pub lifecycle: ServiceShardLifecycle,
    pub role: greenmqtt_kv_raft::RaftNodeRole,
    pub current_term: u64,
    pub leader_node_id: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub latest_snapshot_index: Option<u64>,
    pub replica_lag: Vec<ReplicaLagSnapshot>,
    pub reconfiguration: RangeReconfigurationState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZombieRangeSnapshot {
    pub range_id: String,
    pub lifecycle: ServiceShardLifecycle,
    pub leader_node_id: Option<NodeId>,
}

#[async_trait]
pub trait KvRangeHost: Send + Sync {
    async fn add_range(&self, range: HostedRange) -> anyhow::Result<()>;
    async fn remove_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>>;
    async fn open_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>>;
    async fn range(&self, range_id: &str) -> anyhow::Result<Option<HostedRangeStatus>>;
    async fn list_ranges(&self) -> anyhow::Result<Vec<HostedRangeStatus>>;
    async fn local_leader_ranges(
        &self,
        local_node_id: NodeId,
    ) -> anyhow::Result<Vec<HostedRangeStatus>>;
}

#[derive(Clone, Default)]
pub struct MemoryKvRangeHost {
    pub(crate) ranges: Arc<RwLock<BTreeMap<String, HostedRange>>>,
}

#[async_trait]
pub trait RangeLifecycleManager: Send + Sync {
    async fn create_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<HostedRange>;
    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ReplicaTransport: Send + Sync {
    async fn send(
        &self,
        from_node_id: NodeId,
        target_node_id: NodeId,
        range_id: &str,
        message: &RaftMessage,
    ) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct ReplicaRuntime {
    pub(crate) host: Arc<dyn KvRangeHost>,
    pub(crate) transport: Arc<dyn ReplicaTransport>,
    pub(crate) lifecycle: Option<Arc<dyn RangeLifecycleManager>>,
    pub(crate) pending: Arc<Mutex<VecDeque<PendingOutbound>>>,
    pub(crate) pending_reconfig: Arc<Mutex<BTreeMap<String, PendingReconfiguration>>>,
    pub(crate) send_timeout: Duration,
    pub(crate) max_pending: usize,
    pub(crate) snapshot_threshold: u64,
    pub(crate) compact_threshold: usize,
}

pub struct ReplicaRuntimeHandle {
    pub(crate) shutdown: Option<oneshot::Sender<()>>,
    pub(crate) task: JoinHandle<anyhow::Result<()>>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingOutbound {
    pub(crate) from_node_id: NodeId,
    pub(crate) target_node_id: NodeId,
    pub(crate) range_id: String,
    pub(crate) message: RaftMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingReconfiguration {
    pub old_voters: Vec<NodeId>,
    pub old_learners: Vec<NodeId>,
    pub joint_voters: Vec<NodeId>,
    pub joint_learners: Vec<NodeId>,
    pub target_voters: Vec<NodeId>,
    pub target_learners: Vec<NodeId>,
    pub current_voters: Vec<NodeId>,
    pub current_learners: Vec<NodeId>,
    pub phase: ReconfigurationPhase,
    pub blocked_on_catch_up: bool,
}
