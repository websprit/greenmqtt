use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, RangeId, RangeReplica, ReplicaRole};
use metrics::counter;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
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
    pub log: Vec<RaftLogEntry>,
    pub latest_snapshot: Option<RaftSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredRaftHardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub cluster_config: RaftClusterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredRaftProgressState {
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub leader_node_id: Option<NodeId>,
}

const HARD_STATE_PREFIX: &[u8] = b"hard\0";
const PROGRESS_STATE_PREFIX: &[u8] = b"progress\0";
const SNAPSHOT_META_PREFIX: &[u8] = b"snapshot\0";
const LOG_ENTRY_PREFIX: &[u8] = b"log\0";

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

#[async_trait]
pub trait RaftStateStore: Send + Sync {
    async fn load(&self, range_id: &str) -> anyhow::Result<Option<StoredRaftState>>;
    async fn save(&self, range_id: &str, state: StoredRaftState) -> anyhow::Result<()>;
    async fn delete(&self, range_id: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct MemoryRaftStateStore {
    inner: Arc<Mutex<HashMap<RangeId, StoredRaftState>>>,
}

#[derive(Clone)]
pub struct RocksDbRaftStateStore {
    db: Arc<DB>,
}

const STORED_STATE_PREFIX: &[u8] = b"state\0";

fn stored_state_key(range_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(STORED_STATE_PREFIX.len() + range_id.len());
    key.extend_from_slice(STORED_STATE_PREFIX);
    key.extend_from_slice(range_id.as_bytes());
    key
}

fn scoped_state_key(prefix: &[u8], range_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + range_id.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(range_id.as_bytes());
    key
}

fn scoped_log_prefix(range_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(LOG_ENTRY_PREFIX.len() + range_id.len() + 1);
    key.extend_from_slice(LOG_ENTRY_PREFIX);
    key.extend_from_slice(range_id.as_bytes());
    key.push(0);
    key
}

fn scoped_log_entry_key(range_id: &str, index: LogIndex) -> Vec<u8> {
    let mut key = scoped_log_prefix(range_id);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

#[async_trait]
impl RaftStateStore for MemoryRaftStateStore {
    async fn load(&self, range_id: &str) -> anyhow::Result<Option<StoredRaftState>> {
        Ok(self
            .inner
            .lock()
            .expect("raft state store poisoned")
            .get(range_id)
            .cloned())
    }

    async fn save(&self, range_id: &str, state: StoredRaftState) -> anyhow::Result<()> {
        self.inner
            .lock()
            .expect("raft state store poisoned")
            .insert(range_id.to_string(), state);
        Ok(())
    }

    async fn delete(&self, range_id: &str) -> anyhow::Result<()> {
        self.inner
            .lock()
            .expect("raft state store poisoned")
            .remove(range_id);
        Ok(())
    }
}

impl RocksDbRaftStateStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        Ok(Self {
            db: Arc::new(DB::open(&options, path)?),
        })
    }
}

#[async_trait]
impl RaftStateStore for RocksDbRaftStateStore {
    async fn load(&self, range_id: &str) -> anyhow::Result<Option<StoredRaftState>> {
        if let Some(bytes) = self.db.get(stored_state_key(range_id))? {
            return Ok(Some(bincode::deserialize(&bytes)?));
        }

        let Some(hard_state_bytes) = self.db.get(scoped_state_key(HARD_STATE_PREFIX, range_id))?
        else {
            return Ok(None);
        };
        let hard_state: StoredRaftHardState = bincode::deserialize(&hard_state_bytes)?;
        let progress_state = self
            .db
            .get(scoped_state_key(PROGRESS_STATE_PREFIX, range_id))?
            .map(|bytes| bincode::deserialize::<StoredRaftProgressState>(&bytes))
            .transpose()?
            .unwrap_or_default();
        let latest_snapshot = self
            .db
            .get(scoped_state_key(SNAPSHOT_META_PREFIX, range_id))?
            .map(|bytes| bincode::deserialize::<RaftSnapshot>(&bytes))
            .transpose()?;
        let log_prefix = scoped_log_prefix(range_id);
        let mut log = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&log_prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&log_prefix) {
                break;
            }
            log.push(bincode::deserialize::<RaftLogEntry>(&value)?);
        }
        Ok(Some(StoredRaftState {
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for,
            commit_index: progress_state.commit_index,
            applied_index: progress_state.applied_index,
            leader_node_id: progress_state.leader_node_id,
            cluster_config: hard_state.cluster_config,
            log,
            latest_snapshot,
        }))
    }

    async fn save(&self, range_id: &str, state: StoredRaftState) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(
            scoped_state_key(HARD_STATE_PREFIX, range_id),
            bincode::serialize(&StoredRaftHardState {
                current_term: state.current_term,
                voted_for: state.voted_for,
                cluster_config: state.cluster_config.clone(),
            })?,
        );
        batch.put(
            scoped_state_key(PROGRESS_STATE_PREFIX, range_id),
            bincode::serialize(&StoredRaftProgressState {
                commit_index: state.commit_index,
                applied_index: state.applied_index,
                leader_node_id: state.leader_node_id,
            })?,
        );
        let snapshot_key = scoped_state_key(SNAPSHOT_META_PREFIX, range_id);
        if let Some(snapshot) = &state.latest_snapshot {
            batch.put(snapshot_key, bincode::serialize(snapshot)?);
        } else {
            batch.delete(snapshot_key);
        }
        let log_prefix = scoped_log_prefix(range_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&log_prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&log_prefix) {
                break;
            }
            batch.delete(key);
        }
        for entry in &state.log {
            batch.put(
                scoped_log_entry_key(range_id, entry.index),
                bincode::serialize(entry)?,
            );
        }
        batch.delete(stored_state_key(range_id));
        self.db.write(batch)?;
        Ok(())
    }

    async fn delete(&self, range_id: &str) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete(stored_state_key(range_id));
        batch.delete(scoped_state_key(HARD_STATE_PREFIX, range_id));
        batch.delete(scoped_state_key(PROGRESS_STATE_PREFIX, range_id));
        batch.delete(scoped_state_key(SNAPSHOT_META_PREFIX, range_id));
        let log_prefix = scoped_log_prefix(range_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&log_prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&log_prefix) {
                break;
            }
            batch.delete(key);
        }
        self.db.write(batch)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MemoryRaftNode {
    range_id: RangeId,
    inner: Arc<Mutex<MemoryRaftState>>,
    state_store: Arc<dyn RaftStateStore>,
}

#[derive(Debug, Clone)]
struct MemoryRaftState {
    local_node_id: NodeId,
    role: RaftNodeRole,
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,
    applied_index: LogIndex,
    leader_node_id: Option<NodeId>,
    cluster_config: RaftClusterConfig,
    log: Vec<RaftLogEntry>,
    latest_snapshot: Option<RaftSnapshot>,
    match_indexes: HashMap<NodeId, LogIndex>,
    next_indexes: HashMap<NodeId, LogIndex>,
    election_elapsed_ticks: u64,
    election_timeout_ticks: u64,
    heartbeat_elapsed_ticks: u64,
    votes_granted: HashSet<NodeId>,
    leader_lease_acks: HashSet<NodeId>,
    leader_lease_ticks_remaining: u64,
    committed_queue: VecDeque<RaftLogEntry>,
    outbox: Vec<OutboundRaftMessage>,
}

impl MemoryRaftNode {
    fn initial_election_timeout(local_node_id: NodeId, range_id: &str) -> u64 {
        let range_seed = range_id.as_bytes().iter().fold(0u64, |seed, byte| {
            seed.wrapping_mul(33).wrapping_add(u64::from(*byte))
        });
        3 + ((local_node_id.wrapping_add(range_seed)) % 5)
    }

    pub fn with_state_store(
        local_node_id: NodeId,
        range_id: impl Into<RangeId>,
        cluster_config: RaftClusterConfig,
        state_store: Arc<dyn RaftStateStore>,
    ) -> Self {
        let range_id = range_id.into();
        let election_timeout_ticks = Self::initial_election_timeout(local_node_id, &range_id);
        Self {
            range_id: range_id.clone(),
            inner: Arc::new(Mutex::new(MemoryRaftState {
                local_node_id,
                role: RaftNodeRole::Follower,
                current_term: 0,
                voted_for: None,
                commit_index: 0,
                applied_index: 0,
                leader_node_id: None,
                cluster_config,
                log: Vec::new(),
                latest_snapshot: None,
                match_indexes: HashMap::from([(local_node_id, 0)]),
                next_indexes: HashMap::from([(local_node_id, 1)]),
                election_elapsed_ticks: 0,
                election_timeout_ticks,
                heartbeat_elapsed_ticks: 0,
                votes_granted: HashSet::new(),
                leader_lease_acks: HashSet::from([local_node_id]),
                leader_lease_ticks_remaining: 0,
                committed_queue: VecDeque::new(),
                outbox: Vec::new(),
            })),
            state_store,
        }
    }

    pub fn new(
        local_node_id: NodeId,
        range_id: impl Into<RangeId>,
        cluster_config: RaftClusterConfig,
    ) -> Self {
        Self::with_state_store(
            local_node_id,
            range_id,
            cluster_config,
            Arc::new(MemoryRaftStateStore::default()),
        )
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

    fn quorum_size(state: &MemoryRaftState) -> usize {
        (state.cluster_config.voters.len() / 2) + 1
    }

    fn last_log_index(state: &MemoryRaftState) -> LogIndex {
        state
            .log
            .last()
            .map(|entry| entry.index)
            .or_else(|| {
                state
                    .latest_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.index)
            })
            .unwrap_or(0)
    }

    fn reset_replication_progress(state: &mut MemoryRaftState) {
        let local_index = Self::last_log_index(state).max(state.commit_index);
        let previous_match_indexes = state.match_indexes.clone();
        let previous_next_indexes = state.next_indexes.clone();
        let mut match_indexes = HashMap::new();
        let mut next_indexes = HashMap::new();
        for node_id in state
            .cluster_config
            .voters
            .iter()
            .chain(state.cluster_config.learners.iter())
        {
            let index = if *node_id == state.local_node_id {
                local_index
            } else {
                previous_match_indexes.get(node_id).copied().unwrap_or(0)
            };
            match_indexes.insert(*node_id, index);
            next_indexes.insert(
                *node_id,
                previous_next_indexes
                    .get(node_id)
                    .copied()
                    .unwrap_or(index.saturating_add(1)),
            );
        }
        state.match_indexes = match_indexes;
        state.next_indexes = next_indexes;
    }

    fn maybe_advance_commit(state: &mut MemoryRaftState) {
        if state.role != RaftNodeRole::Leader || state.leader_node_id != Some(state.local_node_id) {
            return;
        }
        if state.cluster_config.voters.is_empty() {
            return;
        }
        let mut voter_matches = state
            .cluster_config
            .voters
            .iter()
            .map(|node_id| state.match_indexes.get(node_id).copied().unwrap_or(0))
            .collect::<Vec<_>>();
        voter_matches.sort_unstable_by(|left, right| right.cmp(left));
        let majority_index = voter_matches[Self::quorum_size(state) - 1];
        if majority_index > state.commit_index {
            let previous_commit = state.commit_index;
            state.commit_index = majority_index;
            Self::enqueue_newly_committed_entries(state, previous_commit);
        }
    }

    fn replica_match_index(state: &MemoryRaftState, node_id: NodeId) -> LogIndex {
        state.match_indexes.get(&node_id).copied().unwrap_or(0)
    }

    fn replica_caught_up_to_commit(state: &MemoryRaftState, node_id: NodeId) -> bool {
        Self::replica_match_index(state, node_id) >= state.commit_index
    }

    fn replica_caught_up_to_tail(state: &MemoryRaftState, node_id: NodeId) -> bool {
        Self::replica_match_index(state, node_id) >= Self::last_log_index(state)
    }

    fn enqueue_newly_committed_entries(state: &mut MemoryRaftState, previous_commit: LogIndex) {
        let start = previous_commit.saturating_add(1);
        let end = state.commit_index;
        if start > end {
            return;
        }
        for entry in state
            .log
            .iter()
            .filter(|entry| entry.index >= start && entry.index <= end)
        {
            if state
                .committed_queue
                .back()
                .is_some_and(|queued| queued.index >= entry.index)
            {
                continue;
            }
            state.committed_queue.push_back(entry.clone());
        }
    }

    fn rebuild_committed_queue(state: &mut MemoryRaftState) {
        state.committed_queue.clear();
        for entry in state
            .log
            .iter()
            .filter(|entry| entry.index > state.applied_index && entry.index <= state.commit_index)
        {
            state.committed_queue.push_back(entry.clone());
        }
    }

    fn entry_term_at(state: &MemoryRaftState, index: LogIndex) -> Term {
        if index == 0 {
            return 0;
        }
        if let Some(snapshot) = &state.latest_snapshot {
            if snapshot.index == index {
                return snapshot.term;
            }
        }
        state
            .log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
            .unwrap_or(0)
    }

    fn last_log_term(state: &MemoryRaftState) -> Term {
        state
            .log
            .last()
            .map(|entry| entry.term)
            .or_else(|| state.latest_snapshot.as_ref().map(|snapshot| snapshot.term))
            .unwrap_or(0)
    }

    fn candidate_log_is_up_to_date(
        state: &MemoryRaftState,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> bool {
        let local_last_term = Self::last_log_term(state);
        let local_last_index = Self::last_log_index(state);
        last_log_term > local_last_term
            || (last_log_term == local_last_term && last_log_index >= local_last_index)
    }

    fn begin_election(state: &mut MemoryRaftState) {
        state.current_term += 1;
        state.role = RaftNodeRole::Candidate;
        state.leader_node_id = None;
        state.voted_for = Some(state.local_node_id);
        state.election_elapsed_ticks = 0;
        state.heartbeat_elapsed_ticks = 0;
        state.election_timeout_ticks = 3 + ((state.local_node_id + state.current_term) % 5);
        state.votes_granted.clear();
        state.votes_granted.insert(state.local_node_id);
        state.leader_lease_ticks_remaining = 0;
        state.leader_lease_acks.clear();
    }

    fn on_leader_elected(state: &mut MemoryRaftState) {
        state.role = RaftNodeRole::Leader;
        state.leader_node_id = Some(state.local_node_id);
        state.heartbeat_elapsed_ticks = 0;
        state.election_elapsed_ticks = 0;
        state.leader_lease_acks.clear();
        state.leader_lease_acks.insert(state.local_node_id);
        state.leader_lease_ticks_remaining = if Self::quorum_size(state) == 1 { 3 } else { 0 };
        Self::reset_replication_progress(state);
    }

    fn note_election(&self) {
        counter!("kv_raft_election_total", "range_id" => self.range_id.clone()).increment(1);
    }

    fn note_stepdown(&self) {
        counter!("kv_raft_stepdown_total", "range_id" => self.range_id.clone()).increment(1);
    }

    fn note_snapshot_install(&self) {
        counter!("kv_raft_snapshot_install_total", "range_id" => self.range_id.clone())
            .increment(1);
    }

    fn note_leader_lease_ack(state: &mut MemoryRaftState, node_id: NodeId) {
        if !state.cluster_config.voters.contains(&node_id) {
            return;
        }
        state.leader_lease_acks.insert(node_id);
        if state.leader_lease_acks.len() >= Self::quorum_size(state) {
            state.leader_lease_ticks_remaining = 3;
        }
    }

    fn enqueue_request_votes(state: &mut MemoryRaftState) {
        let last_log_index = Self::last_log_index(state);
        let last_log_term = Self::last_log_term(state);
        for target_node_id in &state.cluster_config.voters {
            if *target_node_id == state.local_node_id {
                continue;
            }
            state.outbox.push(OutboundRaftMessage {
                target_node_id: *target_node_id,
                message: RaftMessage::RequestVote(RequestVoteRequest {
                    term: state.current_term,
                    candidate_id: state.local_node_id,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
    }

    fn append_entries_request_for(
        state: &MemoryRaftState,
        target_node_id: NodeId,
    ) -> AppendEntriesRequest {
        let next_index = state
            .next_indexes
            .get(&target_node_id)
            .copied()
            .unwrap_or_else(|| Self::last_log_index(state) + 1);
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = Self::entry_term_at(state, prev_log_index);
        let entries = state
            .log
            .iter()
            .filter(|entry| entry.index >= next_index)
            .cloned()
            .collect();
        AppendEntriesRequest {
            term: state.current_term,
            leader_id: state.local_node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: state.commit_index,
        }
    }

    fn enqueue_append_entries(state: &mut MemoryRaftState, target_node_id: NodeId) {
        if target_node_id == state.local_node_id {
            return;
        }
        state.outbox.push(OutboundRaftMessage {
            target_node_id,
            message: RaftMessage::AppendEntries(Self::append_entries_request_for(
                state,
                target_node_id,
            )),
        });
    }

    fn enqueue_replication_round(state: &mut MemoryRaftState) {
        let targets = state
            .cluster_config
            .voters
            .iter()
            .chain(state.cluster_config.learners.iter())
            .copied()
            .filter(|node_id| *node_id != state.local_node_id)
            .collect::<Vec<_>>();
        for target in targets {
            Self::enqueue_append_entries(state, target);
        }
    }

    fn stored_state(state: &MemoryRaftState) -> StoredRaftState {
        StoredRaftState {
            current_term: state.current_term,
            voted_for: state.voted_for,
            commit_index: state.commit_index,
            applied_index: state.applied_index,
            leader_node_id: state.leader_node_id,
            cluster_config: state.cluster_config.clone(),
            log: state.log.clone(),
            latest_snapshot: state.latest_snapshot.clone(),
        }
    }
}

#[async_trait]
impl RaftNode for MemoryRaftNode {
    fn local_node_id(&self) -> NodeId {
        self.inner
            .lock()
            .expect("raft state poisoned")
            .local_node_id
    }

    fn range_id(&self) -> &RangeId {
        &self.range_id
    }

    async fn tick(&self) -> anyhow::Result<()> {
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            if state.role == RaftNodeRole::Leader {
                state.leader_lease_ticks_remaining =
                    state.leader_lease_ticks_remaining.saturating_sub(1);
                state.heartbeat_elapsed_ticks += 1;
                if state.heartbeat_elapsed_ticks >= 2 {
                    state.heartbeat_elapsed_ticks = 0;
                    state.leader_lease_acks.clear();
                    let local_node_id = state.local_node_id;
                    state.leader_lease_acks.insert(local_node_id);
                    Self::enqueue_replication_round(&mut state);
                }
            } else {
                state.election_elapsed_ticks += 1;
            }
            if state.leader_node_id.is_none()
                && state.cluster_config.voters.contains(&state.local_node_id)
                && state.cluster_config.voters.len() == 1
            {
                state.current_term = state.current_term.max(1);
                state.voted_for = Some(state.local_node_id);
                Self::on_leader_elected(&mut state);
            } else if state.role != RaftNodeRole::Leader
                && state.cluster_config.voters.contains(&state.local_node_id)
                && state.election_elapsed_ticks >= state.election_timeout_ticks
            {
                Self::begin_election(&mut state);
                Self::enqueue_request_votes(&mut state);
                self.note_election();
            }
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(())
    }

    async fn status(&self) -> anyhow::Result<RaftStatusSnapshot> {
        let state = self.inner.lock().expect("raft state poisoned");
        Ok(RaftStatusSnapshot {
            local_node_id: state.local_node_id,
            role: state.role,
            current_term: state.current_term,
            last_log_index: Self::last_log_index(&state),
            log_len: state.log.len(),
            commit_index: state.commit_index,
            applied_index: state.applied_index,
            leader_node_id: state.leader_node_id,
            cluster_config: state.cluster_config.clone(),
            replica_progress: state
                .cluster_config
                .voters
                .iter()
                .chain(state.cluster_config.learners.iter())
                .copied()
                .map(|node_id| ReplicaProgressSnapshot {
                    node_id,
                    match_index: state.match_indexes.get(&node_id).copied().unwrap_or(0),
                    next_index: state.next_indexes.get(&node_id).copied().unwrap_or(1),
                })
                .collect(),
            latest_snapshot_index: state
                .latest_snapshot
                .as_ref()
                .map(|snapshot| snapshot.index),
        })
    }

    async fn receive(&self, from: NodeId, message: RaftMessage) -> anyhow::Result<()> {
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            match message {
                RaftMessage::AppendEntries(request) => {
                    if request.term >= state.current_term {
                        let local_last_index = Self::last_log_index(&state);
                        let prev_matches = request.prev_log_index == 0
                            || (request.prev_log_index <= local_last_index
                                && Self::entry_term_at(&state, request.prev_log_index)
                                    == request.prev_log_term);
                        if !prev_matches {
                            let response_term = state.current_term.max(request.term);
                            let response_match = request.prev_log_index.saturating_sub(1);
                            state.outbox.push(OutboundRaftMessage {
                                target_node_id: from,
                                message: RaftMessage::AppendEntriesResponse(
                                    AppendEntriesResponse {
                                        term: response_term,
                                        success: false,
                                        match_index: response_match,
                                    },
                                ),
                            });
                        } else {
                            if request.term > state.current_term {
                                state.voted_for = None;
                                state.votes_granted.clear();
                                state.leader_lease_ticks_remaining = 0;
                                state.leader_lease_acks.clear();
                            }
                            state.current_term = request.term;
                            state.leader_node_id = Some(request.leader_id);
                            state.role = if request.leader_id == state.local_node_id {
                                RaftNodeRole::Leader
                            } else {
                                RaftNodeRole::Follower
                            };
                            state.election_elapsed_ticks = 0;
                            state.heartbeat_elapsed_ticks = 0;
                            state.leader_lease_ticks_remaining = 0;
                            state.leader_lease_acks.clear();
                            for entry in request.entries {
                                if let Some(position) = state
                                    .log
                                    .iter()
                                    .position(|candidate| candidate.index == entry.index)
                                {
                                    if state.log[position].term != entry.term {
                                        state.log.truncate(position);
                                        state.log.push(entry);
                                    }
                                } else {
                                    state.log.push(entry);
                                }
                            }
                            let previous_commit = state.commit_index;
                            state.commit_index = state
                                .commit_index
                                .max(request.leader_commit.min(Self::last_log_index(&state)));
                            Self::enqueue_newly_committed_entries(&mut state, previous_commit);
                            let local_node_id = state.local_node_id;
                            let local_match = Self::last_log_index(&state).max(state.commit_index);
                            state.match_indexes.insert(local_node_id, local_match);
                            let response_term = state.current_term;
                            let response_match = Self::last_log_index(&state);
                            state.outbox.push(OutboundRaftMessage {
                                target_node_id: from,
                                message: RaftMessage::AppendEntriesResponse(
                                    AppendEntriesResponse {
                                        term: response_term,
                                        success: true,
                                        match_index: response_match,
                                    },
                                ),
                            });
                        }
                    }
                }
                RaftMessage::RequestVote(request) => {
                    if request.term > state.current_term {
                        state.current_term = request.term;
                        state.leader_node_id = None;
                        state.role = RaftNodeRole::Follower;
                        state.voted_for = None;
                        state.votes_granted.clear();
                        state.election_elapsed_ticks = 0;
                        state.leader_lease_ticks_remaining = 0;
                        state.leader_lease_acks.clear();
                        self.note_stepdown();
                    }
                    if request.term == state.current_term
                        && Self::candidate_log_is_up_to_date(
                            &state,
                            request.last_log_index,
                            request.last_log_term,
                        )
                        && state
                            .voted_for
                            .is_none_or(|candidate| candidate == request.candidate_id)
                    {
                        state.voted_for = Some(request.candidate_id);
                        state.role = RaftNodeRole::Follower;
                        state.leader_node_id = None;
                        state.election_elapsed_ticks = 0;
                        let response_term = state.current_term;
                        state.outbox.push(OutboundRaftMessage {
                            target_node_id: from,
                            message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                                term: response_term,
                                vote_granted: true,
                            }),
                        });
                    } else {
                        let response_term = state.current_term;
                        state.outbox.push(OutboundRaftMessage {
                            target_node_id: from,
                            message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                                term: response_term,
                                vote_granted: false,
                            }),
                        });
                    }
                }
                RaftMessage::InstallSnapshot(request) => {
                    if request.term >= state.current_term {
                        if request.term > state.current_term {
                            state.voted_for = None;
                            state.votes_granted.clear();
                            state.leader_lease_ticks_remaining = 0;
                            state.leader_lease_acks.clear();
                            self.note_stepdown();
                        }
                        state.current_term = request.term;
                        state.leader_node_id = Some(request.leader_id);
                        state.role = if request.leader_id == state.local_node_id {
                            RaftNodeRole::Leader
                        } else {
                            RaftNodeRole::Follower
                        };
                        state.election_elapsed_ticks = 0;
                        state.heartbeat_elapsed_ticks = 0;
                        state.leader_lease_ticks_remaining = 0;
                        state.leader_lease_acks.clear();
                        state.commit_index = request.snapshot.index;
                        state.applied_index = request.snapshot.index;
                        state.latest_snapshot = Some(request.snapshot);
                        let local_node_id = state.local_node_id;
                        let commit_index = state.commit_index;
                        state.match_indexes.insert(local_node_id, commit_index);
                        state.committed_queue.clear();
                        self.note_snapshot_install();
                    }
                }
                RaftMessage::AppendEntriesResponse(response) => {
                    if response.term > state.current_term {
                        state.current_term = response.term;
                        state.leader_node_id = None;
                        state.role = RaftNodeRole::Follower;
                        state.voted_for = None;
                        state.votes_granted.clear();
                        state.leader_lease_ticks_remaining = 0;
                        state.leader_lease_acks.clear();
                        self.note_stepdown();
                    } else if state.leader_node_id == Some(state.local_node_id)
                        && state.role == RaftNodeRole::Leader
                    {
                        if response.success {
                            let current = state.match_indexes.get(&from).copied().unwrap_or(0);
                            if response.match_index > current {
                                state.match_indexes.insert(from, response.match_index);
                            }
                            state.next_indexes.insert(from, response.match_index + 1);
                            Self::maybe_advance_commit(&mut state);
                            Self::note_leader_lease_ack(&mut state, from);
                            if state.next_indexes.get(&from).copied().unwrap_or(1)
                                <= Self::last_log_index(&state)
                            {
                                Self::enqueue_append_entries(&mut state, from);
                            }
                        } else {
                            let fallback = response.match_index.saturating_add(1);
                            let next_index = state.next_indexes.get(&from).copied().unwrap_or(1);
                            state
                                .next_indexes
                                .insert(from, next_index.min(fallback).max(1));
                            Self::enqueue_append_entries(&mut state, from);
                        }
                    }
                }
                RaftMessage::RequestVoteResponse(response) => {
                    if response.term > state.current_term {
                        state.current_term = response.term;
                        state.leader_node_id = None;
                        state.role = RaftNodeRole::Follower;
                        state.voted_for = None;
                        state.votes_granted.clear();
                        state.leader_lease_ticks_remaining = 0;
                        state.leader_lease_acks.clear();
                        self.note_stepdown();
                    } else if state.role == RaftNodeRole::Candidate && response.vote_granted {
                        state.votes_granted.insert(from);
                        if state.votes_granted.len() >= Self::quorum_size(&state) {
                            Self::on_leader_elected(&mut state);
                            Self::enqueue_replication_round(&mut state);
                        }
                    }
                }
                RaftMessage::InstallSnapshotResponse(_) => {}
            }
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(())
    }

    async fn propose(&self, command: Bytes) -> anyhow::Result<LogIndex> {
        let (next_index, persisted) = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            anyhow::ensure!(
                state.leader_node_id == Some(state.local_node_id)
                    && state.role == RaftNodeRole::Leader,
                "propose requires local leader ownership"
            );
            let next_index = state.log.last().map(|entry| entry.index + 1).unwrap_or(1);
            let entry_term = state.current_term.max(1);
            state.log.push(RaftLogEntry {
                term: entry_term,
                index: next_index,
                command,
            });
            let local_node_id = state.local_node_id;
            state.match_indexes.insert(local_node_id, next_index);
            if Self::quorum_size(&state) == 1 {
                let previous_commit = state.commit_index;
                state.commit_index = next_index;
                Self::enqueue_newly_committed_entries(&mut state, previous_commit);
            } else {
                Self::enqueue_replication_round(&mut state);
            }
            (next_index, Self::stored_state(&state))
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(next_index)
    }

    async fn read_index(&self) -> anyhow::Result<LogIndex> {
        let state = self.inner.lock().expect("raft state poisoned");
        anyhow::ensure!(
            state.role == RaftNodeRole::Leader && state.leader_node_id == Some(state.local_node_id),
            "read index requires local leader ownership"
        );
        anyhow::ensure!(
            state.leader_lease_ticks_remaining > 0,
            "read index requires an active leader lease"
        );
        Ok(state.commit_index)
    }

    async fn transfer_leadership(&self, target: NodeId) -> anyhow::Result<()> {
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            let exists = state.cluster_config.voters.contains(&target)
                || state.cluster_config.learners.contains(&target);
            anyhow::ensure!(exists, "target node is not part of raft config");
            anyhow::ensure!(
                state.cluster_config.voters.contains(&target),
                "target node must be a voter before leadership transfer"
            );
            anyhow::ensure!(
                Self::replica_caught_up_to_tail(&state, target),
                "target node is not caught up to the leader log tail"
            );
            state.leader_node_id = Some(target);
            state.role = if target == state.local_node_id {
                RaftNodeRole::Leader
            } else {
                RaftNodeRole::Follower
            };
            state.voted_for = None;
            state.votes_granted.clear();
            state.leader_lease_ticks_remaining = 0;
            state.leader_lease_acks.clear();
            state.current_term = state.current_term.max(1);
            Self::reset_replication_progress(&mut state);
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(())
    }

    async fn change_cluster_config(&self, change: RaftConfigChange) -> anyhow::Result<()> {
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            let (next_voters, next_learners) = match change {
                RaftConfigChange::ReplaceVoters { voters } => {
                    (voters, state.cluster_config.learners.clone())
                }
                RaftConfigChange::ReplaceLearners { learners } => {
                    (state.cluster_config.voters.clone(), learners)
                }
                RaftConfigChange::ReplaceCluster { voters, learners } => (voters, learners),
            };
            for node_id in &next_voters {
                anyhow::ensure!(
                    Self::replica_caught_up_to_commit(&state, *node_id),
                    "replica {node_id} is not caught up to commit index"
                );
            }
            state.cluster_config.voters = next_voters;
            state.cluster_config.learners = next_learners;
            if state
                .leader_node_id
                .is_some_and(|leader| !state.cluster_config.voters.contains(&leader))
            {
                state.leader_node_id = None;
                state.role = RaftNodeRole::Follower;
            }
            state.votes_granted.clear();
            state.leader_lease_ticks_remaining = 0;
            state.leader_lease_acks.clear();
            Self::reset_replication_progress(&mut state);
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(())
    }

    async fn install_snapshot(&self, snapshot: RaftSnapshot) -> anyhow::Result<()> {
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            state.current_term = state.current_term.max(snapshot.term);
            state.commit_index = snapshot.index;
            state.applied_index = snapshot.index;
            state.latest_snapshot = Some(snapshot);
            state.votes_granted.clear();
            state.leader_lease_ticks_remaining = 0;
            state.leader_lease_acks.clear();
            let local_node_id = state.local_node_id;
            let commit_index = state.commit_index;
            state.match_indexes.insert(local_node_id, commit_index);
            state.committed_queue.clear();
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
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
        let restored = self.state_store.load(&self.range_id).await?;
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            if let Some(restored) = restored {
                state.current_term = restored.current_term;
                state.voted_for = restored.voted_for;
                state.commit_index = restored.commit_index;
                state.applied_index = restored.applied_index;
                state.leader_node_id = restored.leader_node_id;
                state.cluster_config = restored.cluster_config;
                state.log = restored.log;
                state.latest_snapshot = restored.latest_snapshot;
            } else if let Some((snapshot_term, snapshot_index)) = state
                .latest_snapshot
                .as_ref()
                .map(|snapshot| (snapshot.term, snapshot.index))
            {
                state.current_term = state.current_term.max(snapshot_term);
                state.commit_index = snapshot_index;
                state.applied_index = snapshot_index;
            }
            if state.cluster_config.voters.len() == 1
                && state.cluster_config.voters.contains(&state.local_node_id)
                && state.leader_node_id.is_none()
            {
                state.current_term = state.current_term.max(1);
                Self::on_leader_elected(&mut state);
            } else {
                state.role = if state.leader_node_id == Some(state.local_node_id) {
                    RaftNodeRole::Leader
                } else {
                    RaftNodeRole::Follower
                };
                state.leader_lease_ticks_remaining = 0;
                state.leader_lease_acks.clear();
            }
            state.votes_granted.clear();
            Self::reset_replication_progress(&mut state);
            Self::rebuild_committed_queue(&mut state);
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(())
    }

    async fn drain_outbox(&self) -> anyhow::Result<Vec<OutboundRaftMessage>> {
        let mut state = self.inner.lock().expect("raft state poisoned");
        Ok(std::mem::take(&mut state.outbox))
    }

    async fn committed_entries(&self) -> anyhow::Result<Vec<RaftLogEntry>> {
        let state = self.inner.lock().expect("raft state poisoned");
        Ok(state.committed_queue.iter().cloned().collect())
    }

    async fn mark_applied(&self, up_to_index: LogIndex) -> anyhow::Result<()> {
        let persisted = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            state.applied_index = state.applied_index.max(up_to_index.min(state.commit_index));
            while state
                .committed_queue
                .front()
                .is_some_and(|entry| entry.index <= state.applied_index)
            {
                state.committed_queue.pop_front();
            }
            Self::stored_state(&state)
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(())
    }

    async fn compact_log_to_snapshot(&self) -> anyhow::Result<usize> {
        let (removed, persisted) = {
            let mut state = self.inner.lock().expect("raft state poisoned");
            let Some(snapshot_index) = state
                .latest_snapshot
                .as_ref()
                .map(|snapshot| snapshot.index)
            else {
                return Ok(0);
            };
            anyhow::ensure!(
                state.applied_index >= snapshot_index,
                "cannot compact log before snapshot is safely applied"
            );
            let before = state.log.len();
            state.log.retain(|entry| entry.index > snapshot_index);
            let removed = before.saturating_sub(state.log.len());
            state
                .committed_queue
                .retain(|entry| entry.index > snapshot_index);
            (removed, Self::stored_state(&state))
        };
        self.state_store.save(&self.range_id, persisted).await?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, MemoryRaftNode,
        MemoryRaftStateStore, RaftClusterConfig, RaftConfigChange, RaftMessage, RaftNode,
        RaftNodeRole, RaftSnapshot, RaftStateStore, RequestVoteRequest, RequestVoteResponse,
        RocksDbRaftStateStore,
    };
    use bytes::Bytes;
    use greenmqtt_core::{NodeId, ReplicaRole, ReplicaSyncState};
    use std::sync::Arc;

    async fn elect_local_leader(node: &MemoryRaftNode) {
        for _ in 0..8 {
            node.tick().await.unwrap();
        }
        if node.status().await.unwrap().role == RaftNodeRole::Candidate {
            node.receive(
                2,
                RaftMessage::RequestVoteResponse(RequestVoteResponse {
                    term: node.status().await.unwrap().current_term,
                    vote_granted: true,
                }),
            )
            .await
            .unwrap();
        }
        assert_eq!(node.status().await.unwrap().role, RaftNodeRole::Leader);
    }

    async fn deliver_outbox_once(from: NodeId, source: &MemoryRaftNode, target: &MemoryRaftNode) {
        for outbound in source.drain_outbox().await.unwrap() {
            if outbound.target_node_id == target.local_node_id() {
                target.receive(from, outbound.message).await.unwrap();
            }
        }
    }

    async fn deliver_outbox_to_targets(
        from: NodeId,
        source: &MemoryRaftNode,
        targets: &[&MemoryRaftNode],
    ) {
        for outbound in source.drain_outbox().await.unwrap() {
            for target in targets {
                if outbound.target_node_id == target.local_node_id() {
                    target
                        .receive(from, outbound.message.clone())
                        .await
                        .unwrap();
                    break;
                }
            }
        }
    }

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

    #[tokio::test]
    async fn memory_raft_node_starts_election_after_timeout() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let node = MemoryRaftNode::with_state_store(
            1,
            "range-election",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            store.clone(),
        );
        node.recover().await.unwrap();
        assert_eq!(node.status().await.unwrap().role, RaftNodeRole::Follower);

        for _ in 0..8 {
            node.tick().await.unwrap();
        }

        let status = node.status().await.unwrap();
        assert_eq!(status.role, RaftNodeRole::Candidate);
        assert_eq!(status.current_term, 1);
        assert_eq!(status.leader_node_id, None);
        let persisted = store.load("range-election").await.unwrap().unwrap();
        assert_eq!(persisted.voted_for, Some(1));
    }

    #[tokio::test]
    async fn memory_raft_node_becomes_leader_after_majority_votes() {
        let node = MemoryRaftNode::new(
            1,
            "range-election-win",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        for _ in 0..8 {
            node.tick().await.unwrap();
        }
        assert_eq!(node.status().await.unwrap().role, RaftNodeRole::Candidate);

        node.receive(
            2,
            RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        )
        .await
        .unwrap();

        let status = node.status().await.unwrap();
        assert_eq!(status.role, RaftNodeRole::Leader);
        assert_eq!(status.leader_node_id, Some(1));
    }

    #[tokio::test]
    async fn memory_raft_node_emits_request_vote_messages_during_election() {
        let node = MemoryRaftNode::new(
            1,
            "range-election-messages",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        for _ in 0..8 {
            node.tick().await.unwrap();
        }
        let outbox = node.drain_outbox().await.unwrap();
        assert_eq!(outbox.len(), 2);
        assert!(outbox.iter().all(|message| matches!(
            &message.message,
            RaftMessage::RequestVote(RequestVoteRequest {
                term: 1,
                candidate_id: 1,
                ..
            })
        )));
    }

    #[tokio::test]
    async fn memory_raft_node_grants_vote_once_per_term_and_persists_choice() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let node = MemoryRaftNode::with_state_store(
            1,
            "range-vote",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            store.clone(),
        );
        node.recover().await.unwrap();

        node.receive(
            2,
            RaftMessage::RequestVote(RequestVoteRequest {
                term: 4,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .await
        .unwrap();
        let persisted = store.load("range-vote").await.unwrap().unwrap();
        assert_eq!(persisted.current_term, 4);
        assert_eq!(persisted.voted_for, Some(2));

        node.receive(
            3,
            RaftMessage::RequestVote(RequestVoteRequest {
                term: 4,
                candidate_id: 3,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .await
        .unwrap();
        let persisted = store.load("range-vote").await.unwrap().unwrap();
        assert_eq!(persisted.voted_for, Some(2));
    }

    #[tokio::test]
    async fn memory_raft_node_requires_majority_acks_before_commit_in_three_replica_cluster() {
        let node = MemoryRaftNode::new(
            1,
            "range-4",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;

        let index = node.propose(Bytes::from_static(b"put:a")).await.unwrap();
        assert_eq!(index, 1);
        let status = node.status().await.unwrap();
        assert_eq!(status.role, RaftNodeRole::Leader);
        assert_eq!(status.commit_index, 0);
        assert_eq!(status.applied_index, 0);
        assert!(node.read_index().await.is_err());

        node.receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: status.current_term,
                success: true,
                match_index: index,
            }),
        )
        .await
        .unwrap();

        let committed = node.status().await.unwrap();
        assert_eq!(committed.commit_index, 1);
        assert_eq!(committed.applied_index, 0);
        assert_eq!(node.read_index().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn memory_raft_node_rebuilds_committed_queue_from_persisted_progress() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let node = MemoryRaftNode::with_state_store(
            1,
            "range-queue",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            store.clone(),
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        let index = node.propose(Bytes::from_static(b"put:q")).await.unwrap();
        node.receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: node.status().await.unwrap().current_term,
                success: true,
                match_index: index,
            }),
        )
        .await
        .unwrap();
        assert_eq!(node.committed_entries().await.unwrap().len(), 1);

        let restored = MemoryRaftNode::with_state_store(
            1,
            "range-queue",
            RaftClusterConfig::default(),
            store.clone(),
        );
        restored.recover().await.unwrap();
        assert_eq!(restored.committed_entries().await.unwrap().len(), 1);

        restored.mark_applied(1).await.unwrap();
        assert!(restored.committed_entries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_raft_node_emits_heartbeats_on_leader_ticks() {
        let node = MemoryRaftNode::new(
            1,
            "range-heartbeats",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        let _ = node.drain_outbox().await.unwrap();

        node.tick().await.unwrap();
        assert!(node.drain_outbox().await.unwrap().is_empty());
        node.tick().await.unwrap();
        let outbox = node.drain_outbox().await.unwrap();
        assert_eq!(outbox.len(), 2);
        assert!(outbox.iter().all(|message| matches!(
            &message.message,
            RaftMessage::AppendEntries(AppendEntriesRequest { entries, .. }) if entries.is_empty()
        )));
    }

    #[tokio::test]
    async fn memory_raft_node_read_index_requires_active_leader_lease() {
        let node = MemoryRaftNode::new(
            1,
            "range-read-lease",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        assert!(node.read_index().await.is_err());

        let _ = node.drain_outbox().await.unwrap();
        node.tick().await.unwrap();
        node.tick().await.unwrap();
        let heartbeat = node.drain_outbox().await.unwrap();
        assert!(heartbeat.iter().any(|message| message.target_node_id == 2));
        node.receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: node.status().await.unwrap().current_term,
                success: true,
                match_index: 0,
            }),
        )
        .await
        .unwrap();
        assert_eq!(node.read_index().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn memory_raft_node_rejects_follower_read_index() {
        let node = MemoryRaftNode::new(
            2,
            "range-read-follower",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        assert!(node.read_index().await.is_err());
    }

    #[tokio::test]
    async fn memory_raft_node_replicates_log_entries_via_outbox_exchange() {
        let leader_store = Arc::new(MemoryRaftStateStore::default());
        let follower_store = Arc::new(MemoryRaftStateStore::default());
        let leader = MemoryRaftNode::with_state_store(
            1,
            "range-repl",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            leader_store,
        );
        let follower = MemoryRaftNode::with_state_store(
            2,
            "range-repl",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            follower_store.clone(),
        );
        leader.recover().await.unwrap();
        follower.recover().await.unwrap();
        elect_local_leader(&leader).await;
        let _ = leader.drain_outbox().await.unwrap();

        leader.propose(Bytes::from_static(b"put:k")).await.unwrap();
        deliver_outbox_once(1, &leader, &follower).await;
        let persisted = follower_store.load("range-repl").await.unwrap().unwrap();
        assert_eq!(persisted.log.len(), 1);
        assert_eq!(persisted.log[0].command, Bytes::from_static(b"put:k"));
    }

    #[tokio::test]
    async fn memory_raft_node_rewinds_next_index_after_failed_replication_response() {
        let leader = MemoryRaftNode::new(
            1,
            "range-retry",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        leader.recover().await.unwrap();
        elect_local_leader(&leader).await;
        let _ = leader.drain_outbox().await.unwrap();

        leader.propose(Bytes::from_static(b"cmd1")).await.unwrap();
        let _ = leader.drain_outbox().await.unwrap();
        leader
            .receive(
                2,
                RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term: 1,
                    success: true,
                    match_index: 1,
                }),
            )
            .await
            .unwrap();
        let _ = leader.drain_outbox().await.unwrap();

        leader.propose(Bytes::from_static(b"cmd2")).await.unwrap();
        let second_round = leader.drain_outbox().await.unwrap();
        let incremental = second_round
            .iter()
            .find_map(|message| match &message.message {
                RaftMessage::AppendEntries(request) if message.target_node_id == 2 => Some(request),
                _ => None,
            })
            .unwrap();
        assert_eq!(incremental.prev_log_index, 1);
        assert_eq!(incremental.entries.len(), 1);

        leader
            .receive(
                2,
                RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term: 1,
                    success: false,
                    match_index: 0,
                }),
            )
            .await
            .unwrap();
        let retry_round = leader.drain_outbox().await.unwrap();
        let retry = retry_round
            .iter()
            .find_map(|message| match &message.message {
                RaftMessage::AppendEntries(request) if message.target_node_id == 2 => Some(request),
                _ => None,
            })
            .unwrap();
        assert_eq!(retry.prev_log_index, 0);
        assert_eq!(retry.entries.len(), 2);
    }

    #[tokio::test]
    async fn memory_raft_node_rewinds_conflicting_follower_log_suffix() {
        let leader = MemoryRaftNode::new(
            1,
            "range-conflict",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        let follower_store = Arc::new(MemoryRaftStateStore::default());
        let follower = MemoryRaftNode::with_state_store(
            2,
            "range-conflict",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            follower_store.clone(),
        );
        follower
            .receive(
                9,
                RaftMessage::AppendEntries(AppendEntriesRequest {
                    term: 1,
                    leader_id: 9,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![super::RaftLogEntry {
                        term: 9,
                        index: 1,
                        command: Bytes::from_static(b"old"),
                    }],
                    leader_commit: 1,
                }),
            )
            .await
            .unwrap();

        leader.recover().await.unwrap();
        elect_local_leader(&leader).await;
        let _ = leader.drain_outbox().await.unwrap();
        leader.propose(Bytes::from_static(b"new")).await.unwrap();
        deliver_outbox_once(1, &leader, &follower).await;

        let persisted = follower_store
            .load("range-conflict")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.log.len(), 1);
        assert_eq!(persisted.log[0].term, 1);
        assert_eq!(persisted.log[0].command, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn memory_raft_node_catches_up_and_promotes_learner() {
        let leader = MemoryRaftNode::new(
            1,
            "range-learner",
            RaftClusterConfig {
                voters: vec![1, 2],
                learners: vec![3],
            },
        );
        let learner_store = Arc::new(MemoryRaftStateStore::default());
        let learner = MemoryRaftNode::with_state_store(
            3,
            "range-learner",
            RaftClusterConfig {
                voters: vec![1, 2],
                learners: vec![3],
            },
            learner_store.clone(),
        );
        leader.recover().await.unwrap();
        learner.recover().await.unwrap();
        elect_local_leader(&leader).await;
        let _ = leader.drain_outbox().await.unwrap();

        leader
            .propose(Bytes::from_static(b"put:learner"))
            .await
            .unwrap();
        deliver_outbox_once(1, &leader, &learner).await;
        let persisted = learner_store.load("range-learner").await.unwrap().unwrap();
        assert_eq!(persisted.log.len(), 1);
        assert_eq!(persisted.log[0].command, Bytes::from_static(b"put:learner"));

        leader
            .change_cluster_config(RaftConfigChange::ReplaceCluster {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            })
            .await
            .unwrap();
        let status = leader.status().await.unwrap();
        assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
        assert!(status.cluster_config.learners.is_empty());
    }

    #[tokio::test]
    async fn memory_raft_node_blocks_leadership_transfer_to_uncaught_up_replica() {
        let node = MemoryRaftNode::new(
            1,
            "range-unsafe-transfer",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        node.propose(Bytes::from_static(b"cmd")).await.unwrap();
        let error = node.transfer_leadership(2).await.unwrap_err().to_string();
        assert!(error.contains("not caught up"));
    }

    #[tokio::test]
    async fn memory_raft_node_blocks_replica_config_change_until_target_is_caught_up() {
        let node = MemoryRaftNode::new(
            1,
            "range-unsafe-config",
            RaftClusterConfig {
                voters: vec![1, 2],
                learners: vec![3],
            },
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        let index = node.propose(Bytes::from_static(b"cmd")).await.unwrap();
        node.receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: node.status().await.unwrap().current_term,
                success: true,
                match_index: index,
            }),
        )
        .await
        .unwrap();

        let error = node
            .change_cluster_config(RaftConfigChange::ReplaceCluster {
                voters: vec![1, 3],
                learners: Vec::new(),
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("not caught up"));
    }

    #[tokio::test]
    async fn leader_crash_before_commit_keeps_uncommitted_entry_out_of_committed_queue() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let leader = MemoryRaftNode::with_state_store(
            1,
            "range-crash-before-commit",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            store.clone(),
        );
        leader.recover().await.unwrap();
        elect_local_leader(&leader).await;
        leader.propose(Bytes::from_static(b"cmd")).await.unwrap();

        let restored = MemoryRaftNode::with_state_store(
            1,
            "range-crash-before-commit",
            RaftClusterConfig::default(),
            store,
        );
        restored.recover().await.unwrap();
        let status = restored.status().await.unwrap();
        assert_eq!(status.commit_index, 0);
        assert_eq!(status.applied_index, 0);
        assert!(restored.committed_entries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn leader_crash_after_quorum_commit_before_apply_preserves_committed_queue() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let leader = MemoryRaftNode::with_state_store(
            1,
            "range-crash-after-commit",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            store.clone(),
        );
        leader.recover().await.unwrap();
        elect_local_leader(&leader).await;
        let index = leader.propose(Bytes::from_static(b"cmd")).await.unwrap();
        leader
            .receive(
                2,
                RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term: leader.status().await.unwrap().current_term,
                    success: true,
                    match_index: index,
                }),
            )
            .await
            .unwrap();

        let restored = MemoryRaftNode::with_state_store(
            1,
            "range-crash-after-commit",
            RaftClusterConfig::default(),
            store,
        );
        restored.recover().await.unwrap();
        let status = restored.status().await.unwrap();
        assert_eq!(status.commit_index, 1);
        assert_eq!(status.applied_index, 0);
        assert_eq!(restored.committed_entries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn follower_restart_during_catch_up_recovers_and_continues_replication() {
        let follower_store = Arc::new(MemoryRaftStateStore::default());
        let leader = MemoryRaftNode::new(
            1,
            "range-catchup-restart",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        let follower = MemoryRaftNode::with_state_store(
            2,
            "range-catchup-restart",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            follower_store.clone(),
        );
        leader.recover().await.unwrap();
        follower.recover().await.unwrap();
        elect_local_leader(&leader).await;

        leader.propose(Bytes::from_static(b"one")).await.unwrap();
        deliver_outbox_once(1, &leader, &follower).await;

        let restarted = MemoryRaftNode::with_state_store(
            2,
            "range-catchup-restart",
            RaftClusterConfig::default(),
            follower_store.clone(),
        );
        restarted.recover().await.unwrap();
        leader.propose(Bytes::from_static(b"two")).await.unwrap();
        deliver_outbox_once(1, &leader, &restarted).await;

        let persisted = follower_store
            .load("range-catchup-restart")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.log.len(), 2);
        assert_eq!(persisted.log[0].command, Bytes::from_static(b"one"));
        assert_eq!(persisted.log[1].command, Bytes::from_static(b"two"));
    }

    #[tokio::test]
    async fn partitioned_leader_steps_down_on_higher_term_append_entries_response() {
        let leader = MemoryRaftNode::new(
            1,
            "range-stepdown",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        leader.recover().await.unwrap();
        elect_local_leader(&leader).await;
        leader
            .receive(
                2,
                RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term: leader.status().await.unwrap().current_term + 1,
                    success: false,
                    match_index: 0,
                }),
            )
            .await
            .unwrap();
        let status = leader.status().await.unwrap();
        assert_eq!(status.role, RaftNodeRole::Follower);
        assert_eq!(status.leader_node_id, None);
    }

    #[tokio::test]
    async fn memory_raft_node_prevents_split_brain_after_higher_term_leader_emerges() {
        let stale_leader = MemoryRaftNode::new(
            1,
            "range-split-brain",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        let replacement = MemoryRaftNode::new(
            2,
            "range-split-brain",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        );
        stale_leader.recover().await.unwrap();
        replacement.recover().await.unwrap();
        elect_local_leader(&stale_leader).await;

        for _ in 0..8 {
            replacement.tick().await.unwrap();
        }
        if replacement.status().await.unwrap().role == RaftNodeRole::Candidate {
            replacement
                .receive(
                    3,
                    RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term: replacement.status().await.unwrap().current_term,
                        vote_granted: true,
                    }),
                )
                .await
                .unwrap();
        }
        assert_eq!(replacement.status().await.unwrap().role, RaftNodeRole::Leader);
        let new_term = replacement.status().await.unwrap().current_term;

        stale_leader
            .receive(
                2,
                RaftMessage::AppendEntries(AppendEntriesRequest {
                    term: new_term,
                    leader_id: 2,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                }),
            )
            .await
            .unwrap();

        let stale_status = stale_leader.status().await.unwrap();
        let replacement_status = replacement.status().await.unwrap();
        assert_eq!(stale_status.role, RaftNodeRole::Follower);
        assert_eq!(stale_status.leader_node_id, Some(2));
        assert_eq!(replacement_status.role, RaftNodeRole::Leader);
        assert_eq!(replacement_status.leader_node_id, Some(2));
    }

    #[tokio::test]
    async fn membership_change_uses_safe_staging_before_final_voter_set() {
        let node = MemoryRaftNode::new(
            1,
            "range-membership-stage",
            RaftClusterConfig {
                voters: vec![1],
                learners: Vec::new(),
            },
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;

        node.change_cluster_config(RaftConfigChange::ReplaceCluster {
            voters: vec![1],
            learners: vec![2, 3],
        })
        .await
        .unwrap();
        let staged = node.status().await.unwrap();
        assert_eq!(staged.cluster_config.voters, vec![1]);
        assert_eq!(staged.cluster_config.learners, vec![2, 3]);

        node.change_cluster_config(RaftConfigChange::ReplaceCluster {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        })
        .await
        .unwrap();
        let final_status = node.status().await.unwrap();
        assert_eq!(final_status.cluster_config.voters, vec![1, 2, 3]);
        assert!(final_status.cluster_config.learners.is_empty());
    }

    #[tokio::test]
    async fn three_replica_reference_soak_survives_restarts_and_repeated_replication() {
        let leader_store = Arc::new(MemoryRaftStateStore::default());
        let follower2_store = Arc::new(MemoryRaftStateStore::default());
        let follower3_store = Arc::new(MemoryRaftStateStore::default());
        let leader = MemoryRaftNode::with_state_store(
            1,
            "range-soak",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            leader_store,
        );
        let mut follower2 = MemoryRaftNode::with_state_store(
            2,
            "range-soak",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            follower2_store.clone(),
        );
        let follower3 = MemoryRaftNode::with_state_store(
            3,
            "range-soak",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            follower3_store.clone(),
        );
        leader.recover().await.unwrap();
        follower2.recover().await.unwrap();
        follower3.recover().await.unwrap();
        elect_local_leader(&leader).await;

        async fn pump_cluster(
            leader: &MemoryRaftNode,
            follower2: &MemoryRaftNode,
            follower3: &MemoryRaftNode,
        ) {
            for _ in 0..8 {
                leader.tick().await.unwrap();
                deliver_outbox_to_targets(1, leader, &[follower2, follower3]).await;
                deliver_outbox_once(2, follower2, leader).await;
                deliver_outbox_once(3, follower3, leader).await;
            }
        }

        for index in 0..32 {
            leader
                .propose(Bytes::from(format!("cmd-{index}").into_bytes()))
                .await
                .unwrap();
            pump_cluster(&leader, &follower2, &follower3).await;

            if index % 8 == 7 {
                follower2 = MemoryRaftNode::with_state_store(
                    2,
                    "range-soak",
                    RaftClusterConfig::default(),
                    follower2_store.clone(),
                );
                follower2.recover().await.unwrap();
            }
        }

        leader.propose(Bytes::from_static(b"final-sync")).await.unwrap();
        pump_cluster(&leader, &follower2, &follower3).await;

        let leader_status = leader.status().await.unwrap();
        let follower2_status = follower2.status().await.unwrap();
        let follower3_status = follower3.status().await.unwrap();
        assert!(leader_status.commit_index >= 32);
        assert!(follower2_status.last_log_index >= 32);
        assert!(follower3_status.last_log_index >= 32);
    }

    #[tokio::test]
    async fn snapshot_install_after_log_truncation_restores_follower_state() {
        let leader = MemoryRaftNode::single_node(1, "range-snapshot-trunc");
        leader.recover().await.unwrap();
        leader.propose(Bytes::from_static(b"a")).await.unwrap();
        leader.mark_applied(1).await.unwrap();
        leader.propose(Bytes::from_static(b"b")).await.unwrap();
        leader.mark_applied(2).await.unwrap();
        leader
            .install_snapshot(RaftSnapshot {
                range_id: "range-snapshot-trunc".into(),
                term: 1,
                index: 2,
                payload: Bytes::from_static(b"snapshot"),
            })
            .await
            .unwrap();
        leader.compact_log_to_snapshot().await.unwrap();

        let follower = MemoryRaftNode::new(
            2,
            "range-snapshot-trunc",
            RaftClusterConfig {
                voters: vec![1, 2],
                learners: Vec::new(),
            },
        );
        follower.recover().await.unwrap();
        follower
            .receive(
                1,
                RaftMessage::InstallSnapshot(InstallSnapshotRequest {
                    term: 1,
                    leader_id: 1,
                    snapshot: leader.latest_snapshot().await.unwrap().unwrap(),
                }),
            )
            .await
            .unwrap();
        let status = follower.status().await.unwrap();
        assert_eq!(status.commit_index, 2);
        assert_eq!(status.applied_index, 2);
    }

    #[tokio::test]
    async fn memory_raft_node_restores_persisted_state_from_state_store() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let node = MemoryRaftNode::with_state_store(
            1,
            "range-5",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: vec![4],
            },
            store.clone(),
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        let index = node.propose(Bytes::from_static(b"put:x")).await.unwrap();
        node.receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: node.status().await.unwrap().current_term,
                success: true,
                match_index: index,
            }),
        )
        .await
        .unwrap();
        node.receive(
            3,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: node.status().await.unwrap().current_term,
                success: true,
                match_index: index,
            }),
        )
        .await
        .unwrap();
        node.change_cluster_config(RaftConfigChange::ReplaceCluster {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        })
        .await
        .unwrap();
        node.transfer_leadership(3).await.unwrap();

        let restored =
            MemoryRaftNode::with_state_store(1, "range-5", RaftClusterConfig::default(), store);
        restored.recover().await.unwrap();
        let status = restored.status().await.unwrap();
        assert_eq!(status.current_term, 1);
        assert_eq!(status.commit_index, 1);
        assert_eq!(status.applied_index, 0);
        assert_eq!(status.leader_node_id, Some(3));
        assert_eq!(status.role, RaftNodeRole::Follower);
        assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
        assert!(status.cluster_config.learners.is_empty());
    }

    #[tokio::test]
    async fn memory_raft_node_persists_applied_index_after_mark_applied() {
        let store = Arc::new(MemoryRaftStateStore::default());
        let node = MemoryRaftNode::with_state_store(
            1,
            "range-applied",
            RaftClusterConfig {
                voters: vec![1],
                learners: Vec::new(),
            },
            store.clone(),
        );
        node.recover().await.unwrap();
        let index = node.propose(Bytes::from_static(b"put:z")).await.unwrap();
        node.mark_applied(index).await.unwrap();

        let restored = MemoryRaftNode::with_state_store(
            1,
            "range-applied",
            RaftClusterConfig::default(),
            store,
        );
        restored.recover().await.unwrap();
        let status = restored.status().await.unwrap();
        assert_eq!(status.commit_index, 1);
        assert_eq!(status.applied_index, 1);
    }

    #[tokio::test]
    async fn rocksdb_raft_state_store_restores_persisted_state() {
        let tempdir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDbRaftStateStore::open(tempdir.path()).unwrap());
        let node = MemoryRaftNode::with_state_store(
            1,
            "range-6",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
            store.clone(),
        );
        node.recover().await.unwrap();
        elect_local_leader(&node).await;
        let index = node.propose(Bytes::from_static(b"put:y")).await.unwrap();
        node.receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: node.status().await.unwrap().current_term,
                success: true,
                match_index: index,
            }),
        )
        .await
        .unwrap();
        node.transfer_leadership(2).await.unwrap();

        let restored =
            MemoryRaftNode::with_state_store(1, "range-6", RaftClusterConfig::default(), store);
        restored.recover().await.unwrap();
        let status = restored.status().await.unwrap();
        assert_eq!(status.current_term, 1);
        assert_eq!(status.commit_index, 1);
        assert_eq!(status.applied_index, 0);
        assert_eq!(status.leader_node_id, Some(2));
        assert_eq!(status.role, RaftNodeRole::Follower);
        assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn rocksdb_raft_state_store_splits_hard_progress_and_log_state() {
        let tempdir = tempfile::tempdir().unwrap();
        let store = RocksDbRaftStateStore::open(tempdir.path()).unwrap();
        store
            .save(
                "range-7",
                super::StoredRaftState {
                    current_term: 3,
                    voted_for: Some(2),
                    commit_index: 11,
                    applied_index: 11,
                    leader_node_id: Some(2),
                    cluster_config: RaftClusterConfig {
                        voters: vec![1, 2, 3],
                        learners: vec![4],
                    },
                    log: vec![
                        super::RaftLogEntry {
                            term: 2,
                            index: 10,
                            command: Bytes::from_static(b"a"),
                        },
                        super::RaftLogEntry {
                            term: 3,
                            index: 11,
                            command: Bytes::from_static(b"b"),
                        },
                    ],
                    latest_snapshot: Some(RaftSnapshot {
                        range_id: "range-7".into(),
                        term: 2,
                        index: 9,
                        payload: Bytes::from_static(b"snapshot"),
                    }),
                },
            )
            .await
            .unwrap();

        assert!(store
            .db
            .get(super::scoped_state_key(super::HARD_STATE_PREFIX, "range-7"))
            .unwrap()
            .is_some());
        assert!(store
            .db
            .get(super::scoped_state_key(
                super::PROGRESS_STATE_PREFIX,
                "range-7"
            ))
            .unwrap()
            .is_some());
        assert!(store
            .db
            .get(super::scoped_state_key(
                super::SNAPSHOT_META_PREFIX,
                "range-7"
            ))
            .unwrap()
            .is_some());
        assert!(store
            .db
            .get(super::scoped_log_entry_key("range-7", 10))
            .unwrap()
            .is_some());
        assert!(store
            .db
            .get(super::scoped_log_entry_key("range-7", 11))
            .unwrap()
            .is_some());
        assert!(store
            .db
            .get(super::stored_state_key("range-7"))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn memory_raft_node_compacts_log_only_after_snapshot_is_applied() {
        let node = MemoryRaftNode::single_node(1, "range-compact");
        node.recover().await.unwrap();
        node.propose(Bytes::from_static(b"a")).await.unwrap();
        node.mark_applied(1).await.unwrap();
        node.propose(Bytes::from_static(b"b")).await.unwrap();
        node.mark_applied(2).await.unwrap();
        node.install_snapshot(RaftSnapshot {
            range_id: "range-compact".into(),
            term: 1,
            index: 1,
            payload: Bytes::from_static(b"snapshot"),
        })
        .await
        .unwrap();
        let removed = node.compact_log_to_snapshot().await.unwrap();
        assert_eq!(removed, 1);
        let status = node.status().await.unwrap();
        assert_eq!(status.log_len, 1);
        assert_eq!(status.last_log_index, 2);
    }
}
