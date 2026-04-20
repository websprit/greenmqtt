use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, RangeId, RangeReplica, ReplicaRole};
use serde::{Deserialize, Serialize};


pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftNodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftClusterConfig {
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
}

impl RaftClusterConfig {
    pub fn replicas(&self) -> Vec<RangeReplica> {
        let mut replicas = Vec::with_capacity(self.voters.len() + self.learners.len());
        replicas.extend(self.voters.iter().copied().map(|node_id| {
            RangeReplica::new(
                node_id,
                ReplicaRole::Voter,
                greenmqtt_core::ReplicaSyncState::Probing,
            )
        }));
        replicas.extend(self.learners.iter().copied().map(|node_id| {
            RangeReplica::new(
                node_id,
                ReplicaRole::Learner,
                greenmqtt_core::ReplicaSyncState::Probing,
            )
        }));
        replicas
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshot {
    pub range_id: RangeId,
    pub term: Term,
    pub index: LogIndex,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftStatusSnapshot {
    pub local_node_id: NodeId,
    pub role: RaftNodeRole,
    pub current_term: Term,
    pub last_log_index: LogIndex,
    pub log_len: usize,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub leader_node_id: Option<NodeId>,
    pub cluster_config: RaftClusterConfig,
    pub config_transition: Option<RaftConfigTransitionState>,
    pub replica_progress: Vec<ReplicaProgressSnapshot>,
    pub latest_snapshot_index: Option<LogIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaProgressSnapshot {
    pub node_id: NodeId,
    pub match_index: LogIndex,
    pub next_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredRaftState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub leader_node_id: Option<NodeId>,
    pub cluster_config: RaftClusterConfig,
    pub config_transition: Option<RaftConfigTransitionState>,
    pub log: Vec<RaftLogEntry>,
    pub latest_snapshot: Option<RaftSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredRaftHardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub cluster_config: RaftClusterConfig,
    pub config_transition: Option<RaftConfigTransitionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredRaftProgressState {
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub leader_node_id: Option<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftConfigChange {
    ReplaceVoters {
        voters: Vec<NodeId>,
    },
    ReplaceLearners {
        learners: Vec<NodeId>,
    },
    ReplaceCluster {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    },
    EnterJointConsensus {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    },
    FinalizeJointConsensus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftConfigTransitionPhase {
    JointConsensus,
    Finalizing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftConfigTransitionState {
    pub old_config: RaftClusterConfig,
    pub joint_config: RaftClusterConfig,
    pub final_config: RaftClusterConfig,
    pub phase: RaftConfigTransitionPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: RaftSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboundRaftMessage {
    pub target_node_id: NodeId,
    pub message: RaftMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftMessage {
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshot(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

#[async_trait]
pub trait RaftNode: Send + Sync {
    fn local_node_id(&self) -> NodeId;
    fn range_id(&self) -> &RangeId;

    async fn tick(&self) -> anyhow::Result<()>;
    async fn status(&self) -> anyhow::Result<RaftStatusSnapshot>;
    async fn receive(&self, from: NodeId, message: RaftMessage) -> anyhow::Result<()>;
    async fn propose(&self, command: Bytes) -> anyhow::Result<LogIndex>;
    async fn read_index(&self) -> anyhow::Result<LogIndex>;
    async fn transfer_leadership(&self, target: NodeId) -> anyhow::Result<()>;
    async fn change_cluster_config(&self, change: RaftConfigChange) -> anyhow::Result<()>;
    async fn install_snapshot(&self, snapshot: RaftSnapshot) -> anyhow::Result<()>;
    async fn latest_snapshot(&self) -> anyhow::Result<Option<RaftSnapshot>>;
    async fn recover(&self) -> anyhow::Result<()>;
    async fn drain_outbox(&self) -> anyhow::Result<Vec<OutboundRaftMessage>>;
    async fn committed_entries(&self) -> anyhow::Result<Vec<RaftLogEntry>>;
    async fn mark_applied(&self, up_to_index: LogIndex) -> anyhow::Result<()>;
    async fn compact_log_to_snapshot(&self) -> anyhow::Result<usize>;
}
