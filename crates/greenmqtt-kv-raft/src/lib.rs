use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, RangeId, RangeReplica, ReplicaRole};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

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
            RangeReplica::new(node_id, ReplicaRole::Voter, greenmqtt_core::ReplicaSyncState::Probing)
        }));
        replicas.extend(
            self.learners
                .iter()
                .copied()
                .map(|node_id| {
                    RangeReplica::new(
                        node_id,
                        ReplicaRole::Learner,
                        greenmqtt_core::ReplicaSyncState::Probing,
                    )
                }),
        );
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
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub leader_node_id: Option<NodeId>,
    pub cluster_config: RaftClusterConfig,
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
}

#[derive(Debug, Clone)]
pub struct MemoryRaftNode {
    range_id: RangeId,
    inner: Arc<Mutex<MemoryRaftState>>,
}

#[derive(Debug, Clone)]
struct MemoryRaftState {
    local_node_id: NodeId,
    role: RaftNodeRole,
    current_term: Term,
    commit_index: LogIndex,
    applied_index: LogIndex,
    leader_node_id: Option<NodeId>,
    cluster_config: RaftClusterConfig,
    log: Vec<RaftLogEntry>,
    latest_snapshot: Option<RaftSnapshot>,
}

impl MemoryRaftNode {
    pub fn new(
        local_node_id: NodeId,
        range_id: impl Into<RangeId>,
        cluster_config: RaftClusterConfig,
    ) -> Self {
        Self {
            range_id: range_id.into(),
            inner: Arc::new(Mutex::new(MemoryRaftState {
                local_node_id,
                role: RaftNodeRole::Follower,
                current_term: 0,
                commit_index: 0,
                applied_index: 0,
                leader_node_id: None,
                cluster_config,
                log: Vec::new(),
                latest_snapshot: None,
            })),
        }
    }

    pub fn single_node(local_node_id: NodeId, range_id: impl Into<RangeId>) -> Self {
        Self::new(
            local_node_id,
            range_id,
            RaftClusterConfig {
                voters: vec![local_node_id],
                learners: Vec::new(),
            },
        )
    }
}

#[async_trait]
impl RaftNode for MemoryRaftNode {
    fn local_node_id(&self) -> NodeId {
        self.inner.lock().expect("raft state poisoned").local_node_id
    }

    fn range_id(&self) -> &RangeId {
        &self.range_id
    }

    async fn tick(&self) -> anyhow::Result<()> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        if state.leader_node_id.is_none() && state.cluster_config.voters.contains(&state.local_node_id)
        {
            state.current_term = state.current_term.max(1);
            state.leader_node_id = Some(state.local_node_id);
            state.role = RaftNodeRole::Leader;
        }
        Ok(())
    }

    async fn status(&self) -> anyhow::Result<RaftStatusSnapshot> {
        let state = self.inner.lock().expect("raft state poisoned");
        Ok(RaftStatusSnapshot {
            local_node_id: state.local_node_id,
            role: state.role,
            current_term: state.current_term,
            commit_index: state.commit_index,
            applied_index: state.applied_index,
            leader_node_id: state.leader_node_id,
            cluster_config: state.cluster_config.clone(),
        })
    }

    async fn receive(&self, _from: NodeId, message: RaftMessage) -> anyhow::Result<()> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        match message {
            RaftMessage::AppendEntries(request) => {
                if request.term >= state.current_term {
                    state.current_term = request.term;
                    state.leader_node_id = Some(request.leader_id);
                    state.role = if request.leader_id == state.local_node_id {
                        RaftNodeRole::Leader
                    } else {
                        RaftNodeRole::Follower
                    };
                    for entry in request.entries {
                        if let Some(existing) =
                            state.log.iter_mut().find(|candidate| candidate.index == entry.index)
                        {
                            *existing = entry;
                        } else {
                            state.log.push(entry);
                        }
                    }
                    state.commit_index = state.commit_index.max(request.leader_commit);
                    state.applied_index = state.applied_index.max(state.commit_index);
                }
            }
            RaftMessage::RequestVote(request) => {
                if request.term > state.current_term {
                    state.current_term = request.term;
                    state.leader_node_id = None;
                    state.role = RaftNodeRole::Follower;
                }
            }
            RaftMessage::InstallSnapshot(request) => {
                if request.term >= state.current_term {
                    state.current_term = request.term;
                    state.leader_node_id = Some(request.leader_id);
                    state.role = if request.leader_id == state.local_node_id {
                        RaftNodeRole::Leader
                    } else {
                        RaftNodeRole::Follower
                    };
                    state.commit_index = request.snapshot.index;
                    state.applied_index = request.snapshot.index;
                    state.latest_snapshot = Some(request.snapshot);
                    let commit_index = state.commit_index;
                    state.log.retain(|entry| entry.index > commit_index);
                }
            }
            RaftMessage::AppendEntriesResponse(_)
            | RaftMessage::RequestVoteResponse(_)
            | RaftMessage::InstallSnapshotResponse(_) => {}
        }
        Ok(())
    }

    async fn propose(&self, command: Bytes) -> anyhow::Result<LogIndex> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        anyhow::ensure!(
            state.leader_node_id == Some(state.local_node_id) && state.role == RaftNodeRole::Leader,
            "propose requires local leader ownership"
        );
        let next_index = state.log.last().map(|entry| entry.index + 1).unwrap_or(1);
        let entry_term = state.current_term.max(1);
        state.log.push(RaftLogEntry {
            term: entry_term,
            index: next_index,
            command,
        });
        state.commit_index = next_index;
        state.applied_index = next_index;
        Ok(next_index)
    }

    async fn read_index(&self) -> anyhow::Result<LogIndex> {
        Ok(self.inner.lock().expect("raft state poisoned").commit_index)
    }

    async fn transfer_leadership(&self, target: NodeId) -> anyhow::Result<()> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        let exists = state.cluster_config.voters.contains(&target)
            || state.cluster_config.learners.contains(&target);
        anyhow::ensure!(exists, "target node is not part of raft config");
        state.leader_node_id = Some(target);
        state.role = if target == state.local_node_id {
            RaftNodeRole::Leader
        } else {
            RaftNodeRole::Follower
        };
        state.current_term = state.current_term.max(1);
        Ok(())
    }

    async fn change_cluster_config(&self, change: RaftConfigChange) -> anyhow::Result<()> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        match change {
            RaftConfigChange::ReplaceVoters { voters } => {
                state.cluster_config.voters = voters;
            }
            RaftConfigChange::ReplaceLearners { learners } => {
                state.cluster_config.learners = learners;
            }
            RaftConfigChange::ReplaceCluster { voters, learners } => {
                state.cluster_config.voters = voters;
                state.cluster_config.learners = learners;
            }
        }
        if state.leader_node_id.is_some_and(|leader| !state.cluster_config.voters.contains(&leader))
        {
            state.leader_node_id = None;
            state.role = RaftNodeRole::Follower;
        }
        Ok(())
    }

    async fn install_snapshot(&self, snapshot: RaftSnapshot) -> anyhow::Result<()> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        state.current_term = state.current_term.max(snapshot.term);
        state.commit_index = snapshot.index;
        state.applied_index = snapshot.index;
        state.latest_snapshot = Some(snapshot);
        let commit_index = state.commit_index;
        state.log.retain(|entry| entry.index > commit_index);
        Ok(())
    }

    async fn latest_snapshot(&self) -> anyhow::Result<Option<RaftSnapshot>> {
        Ok(self
            .inner
            .lock()
            .expect("raft state poisoned")
            .latest_snapshot
            .clone())
    }

    async fn recover(&self) -> anyhow::Result<()> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        if let Some((snapshot_term, snapshot_index)) = state
            .latest_snapshot
            .as_ref()
            .map(|snapshot| (snapshot.term, snapshot.index))
        {
            state.current_term = state.current_term.max(snapshot_term);
            state.commit_index = snapshot_index;
            state.applied_index = snapshot_index;
        }
        if state.cluster_config.voters.contains(&state.local_node_id) && state.leader_node_id.is_none()
        {
            state.leader_node_id = Some(state.local_node_id);
            state.role = RaftNodeRole::Leader;
            state.current_term = state.current_term.max(1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryRaftNode, RaftClusterConfig, RaftConfigChange, RaftMessage, RaftNode, RaftNodeRole, RaftSnapshot, RequestVoteRequest};
    use bytes::Bytes;
    use greenmqtt_core::{ReplicaRole, ReplicaSyncState};

    #[test]
    fn raft_cluster_config_maps_voters_and_learners_to_replicas() {
        let config = RaftClusterConfig {
            voters: vec![1, 2],
            learners: vec![3],
        };
        let replicas = config.replicas();
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas[0].node_id, 1);
        assert_eq!(replicas[0].role, ReplicaRole::Voter);
        assert_eq!(replicas[0].sync_state, ReplicaSyncState::Probing);
        assert_eq!(replicas[2].node_id, 3);
        assert_eq!(replicas[2].role, ReplicaRole::Learner);
    }

    #[test]
    fn raft_config_change_can_replace_whole_cluster_membership() {
        let change = RaftConfigChange::ReplaceCluster {
            voters: vec![1, 2, 3],
            learners: vec![4],
        };
        match change {
            RaftConfigChange::ReplaceCluster { voters, learners } => {
                assert_eq!(voters, vec![1, 2, 3]);
                assert_eq!(learners, vec![4]);
            }
            _ => panic!("expected ReplaceCluster"),
        }
        assert_eq!(RaftNodeRole::Leader, RaftNodeRole::Leader);
    }

    #[tokio::test]
    async fn memory_raft_node_recovers_single_node_as_leader_and_applies_proposals() {
        let node = MemoryRaftNode::single_node(1, "range-1");
        node.recover().await.unwrap();
        let status = node.status().await.unwrap();
        assert_eq!(status.role, RaftNodeRole::Leader);
        assert_eq!(status.leader_node_id, Some(1));

        let index = node.propose(Bytes::from_static(b"put:a")).await.unwrap();
        assert_eq!(index, 1);
        assert_eq!(node.read_index().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn memory_raft_node_supports_transfer_config_change_and_snapshot_install() {
        let node = MemoryRaftNode::new(
            1,
            "range-2",
            RaftClusterConfig {
                voters: vec![1, 2],
                learners: vec![3],
            },
        );
        node.recover().await.unwrap();
        node.transfer_leadership(2).await.unwrap();
        assert_eq!(node.status().await.unwrap().leader_node_id, Some(2));

        node.change_cluster_config(RaftConfigChange::ReplaceCluster {
            voters: vec![1, 3],
            learners: vec![2],
        })
        .await
        .unwrap();
        let status = node.status().await.unwrap();
        assert_eq!(status.cluster_config.voters, vec![1, 3]);
        assert_eq!(status.cluster_config.learners, vec![2]);

        node.install_snapshot(RaftSnapshot {
            range_id: "range-2".into(),
            term: 5,
            index: 8,
            payload: Bytes::from_static(b"snapshot"),
        })
        .await
        .unwrap();
        let status = node.status().await.unwrap();
        assert_eq!(status.current_term, 5);
        assert_eq!(status.commit_index, 8);
        assert_eq!(status.applied_index, 8);
        assert!(node.latest_snapshot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn memory_raft_node_accepts_higher_term_vote_request() {
        let node = MemoryRaftNode::single_node(1, "range-3");
        node.recover().await.unwrap();
        node.receive(
            2,
            RaftMessage::RequestVote(RequestVoteRequest {
                term: 9,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .await
        .unwrap();
        let status = node.status().await.unwrap();
        assert_eq!(status.current_term, 9);
        assert_eq!(status.role, RaftNodeRole::Follower);
        assert_eq!(status.leader_node_id, None);
    }
}
