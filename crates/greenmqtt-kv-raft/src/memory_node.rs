use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, RangeId};
use metrics::counter;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use crate::*;

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
    config_transition: Option<RaftConfigTransitionState>,
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
                config_transition: None,
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
        let majority_index = if let Some(transition) = state.config_transition.as_ref() {
            if state.commit_index < transition.joint_config_index {
                Self::majority_match_index(state, &transition.old_config.voters)
            } else if transition.target_config_index > 0
                && state.commit_index >= transition.target_config_index
            {
                Self::majority_match_index(state, &transition.final_config.voters)
            } else {
                Self::joint_majority_commit_index(state, transition)
            }
        } else {
            Self::majority_match_index(state, &state.cluster_config.voters)
        };
        if majority_index > state.commit_index {
            let previous_commit = state.commit_index;
            state.commit_index = majority_index;
            Self::apply_committed_config_entries(state, previous_commit);
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

    fn majority_match_index(state: &MemoryRaftState, voters: &[NodeId]) -> LogIndex {
        if voters.is_empty() {
            return 0;
        }
        let mut voter_matches = voters
            .iter()
            .map(|node_id| state.match_indexes.get(node_id).copied().unwrap_or(0))
            .collect::<Vec<_>>();
        voter_matches.sort_unstable_by(|left, right| right.cmp(left));
        voter_matches[voters.len() / 2]
    }

    fn voter_quorum_caught_up_to_index(
        state: &MemoryRaftState,
        voters: &[NodeId],
        required_index: LogIndex,
    ) -> bool {
        Self::majority_match_index(state, voters) >= required_index
    }

    fn joint_majority_commit_index(
        state: &MemoryRaftState,
        transition: &RaftConfigTransitionState,
    ) -> LogIndex {
        let old_majority = Self::majority_match_index(state, &transition.old_config.voters);
        let final_majority = Self::majority_match_index(state, &transition.final_config.voters);
        old_majority.min(final_majority)
    }

    fn cluster_config_union(
        left: &RaftClusterConfig,
        right: &RaftClusterConfig,
    ) -> RaftClusterConfig {
        let mut voters = left.voters.clone();
        voters.extend(right.voters.iter().copied());
        voters.sort_unstable();
        voters.dedup();

        let mut learners = left.learners.clone();
        learners.extend(right.learners.iter().copied());
        learners.retain(|node_id| !voters.contains(node_id));
        learners.sort_unstable();
        learners.dedup();

        RaftClusterConfig { voters, learners }
    }

    fn config_caught_up_to_commit(state: &MemoryRaftState, config: &RaftClusterConfig) -> bool {
        config
            .voters
            .iter()
            .chain(config.learners.iter())
            .all(|node_id| Self::replica_caught_up_to_commit(state, *node_id))
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

    fn apply_committed_config_entries(state: &mut MemoryRaftState, previous_commit: LogIndex) {
        let committed_entries = state
            .log
            .iter()
            .filter(|entry| entry.index > previous_commit && entry.index <= state.commit_index)
            .cloned()
            .collect::<Vec<_>>();
        for entry in committed_entries {
            let Some(config_change) = entry.config_change else {
                continue;
            };
            match config_change {
                RaftConfigLogEntry::JointConfig {
                    old_config,
                    joint_config,
                    final_config,
                } => {
                    state.cluster_config = joint_config.clone();
                    let target_config_index = state
                        .config_transition
                        .as_ref()
                        .map(|transition| transition.target_config_index)
                        .unwrap_or(0);
                    let phase = if target_config_index > 0 {
                        RaftConfigTransitionPhase::Finalizing
                    } else {
                        RaftConfigTransitionPhase::JointConsensus
                    };
                    state.config_transition = Some(RaftConfigTransitionState {
                        old_config,
                        joint_config,
                        final_config,
                        joint_config_index: entry.index,
                        target_config_index,
                        phase,
                    });
                }
                RaftConfigLogEntry::TargetConfig {
                    old_config,
                    joint_config,
                    final_config,
                    joint_config_index,
                } => {
                    state.cluster_config = final_config.clone();
                    state.config_transition = Some(RaftConfigTransitionState {
                        old_config,
                        joint_config,
                        final_config: final_config.clone(),
                        joint_config_index,
                        target_config_index: entry.index,
                        phase: RaftConfigTransitionPhase::Finalizing,
                    });
                    if state
                        .leader_node_id
                        .is_some_and(|leader| !final_config.voters.contains(&leader))
                    {
                        state.leader_node_id = None;
                        state.role = RaftNodeRole::Follower;
                    }
                }
            }
        }
    }

    fn rebuild_config_state_from_log(state: &mut MemoryRaftState) {
        let committed_index = state.commit_index;
        let applied_index = state.applied_index;
        state.config_transition = None;
        Self::apply_committed_config_entries(state, 0);
        let pending_entries = state
            .log
            .iter()
            .filter(|entry| entry.index > committed_index)
            .cloned()
            .collect::<Vec<_>>();
        for entry in pending_entries {
            let Some(config_change) = entry.config_change else {
                continue;
            };
            match config_change {
                RaftConfigLogEntry::JointConfig {
                    old_config,
                    joint_config,
                    final_config,
                } => {
                    state.config_transition = Some(RaftConfigTransitionState {
                        old_config,
                        joint_config,
                        final_config,
                        joint_config_index: entry.index,
                        target_config_index: 0,
                        phase: RaftConfigTransitionPhase::JointConsensus,
                    });
                }
                RaftConfigLogEntry::TargetConfig {
                    old_config,
                    joint_config,
                    final_config,
                    joint_config_index,
                } => {
                    state.config_transition = Some(RaftConfigTransitionState {
                        old_config,
                        joint_config,
                        final_config,
                        joint_config_index,
                        target_config_index: entry.index,
                        phase: RaftConfigTransitionPhase::Finalizing,
                    });
                }
            }
        }
        if state.config_transition.as_ref().is_some_and(|transition| {
            transition.target_config_index > 0 && applied_index >= transition.target_config_index
        }) {
            state.config_transition = None;
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
            config_transition: state.config_transition.clone(),
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
            config_transition: state.config_transition.clone(),
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
                            Self::apply_committed_config_entries(&mut state, previous_commit);
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
                config_change: None,
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
            match change {
                RaftConfigChange::ReplaceVoters { voters } => {
                    for node_id in &voters {
                        anyhow::ensure!(
                            Self::replica_caught_up_to_commit(&state, *node_id),
                            "replica {node_id} is not caught up to commit index"
                        );
                    }
                    state.cluster_config.voters = voters;
                    state.config_transition = None;
                }
                RaftConfigChange::ReplaceLearners { learners } => {
                    state.cluster_config.learners = learners;
                    state.config_transition = None;
                }
                RaftConfigChange::ReplaceCluster { voters, learners } => {
                    for node_id in &voters {
                        anyhow::ensure!(
                            Self::replica_caught_up_to_commit(&state, *node_id),
                            "replica {node_id} is not caught up to commit index"
                        );
                    }
                    state.cluster_config.voters = voters;
                    state.cluster_config.learners = learners;
                    state.config_transition = None;
                }
                RaftConfigChange::EnterJointConsensus { voters, learners } => {
                    anyhow::ensure!(
                        state.config_transition.is_none(),
                        "joint consensus change already in progress"
                    );
                    let final_config = RaftClusterConfig { voters, learners };
                    let old_config = state.cluster_config.clone();
                    let joint_config = Self::cluster_config_union(&old_config, &final_config);
                    anyhow::ensure!(
                        Self::config_caught_up_to_commit(&state, &joint_config),
                        "joint config is not caught up to commit index"
                    );
                    let next_index = Self::last_log_index(&state).saturating_add(1);
                    let entry_term = state.current_term.max(1);
                    state.log.push(RaftLogEntry {
                        term: entry_term,
                        index: next_index,
                        config_change: Some(RaftConfigLogEntry::JointConfig {
                            old_config: old_config.clone(),
                            joint_config: joint_config.clone(),
                            final_config: final_config.clone(),
                        }),
                        command: Bytes::new(),
                    });
                    state.config_transition = Some(RaftConfigTransitionState {
                        old_config,
                        joint_config,
                        final_config,
                        joint_config_index: next_index,
                        target_config_index: 0,
                        phase: RaftConfigTransitionPhase::JointConsensus,
                    });
                    let local_node_id = state.local_node_id;
                    state.match_indexes.insert(local_node_id, next_index);
                    if Self::quorum_size(&state) == 1 {
                        let previous_commit = state.commit_index;
                        state.commit_index = next_index;
                        Self::apply_committed_config_entries(&mut state, previous_commit);
                        Self::enqueue_newly_committed_entries(&mut state, previous_commit);
                    } else {
                        Self::enqueue_replication_round(&mut state);
                    }
                }
                RaftConfigChange::FinalizeJointConsensus => {
                    let transition = state
                        .config_transition
                        .clone()
                        .ok_or_else(|| anyhow::anyhow!("no joint consensus change in progress"))?;
                    anyhow::ensure!(
                        transition.target_config_index == 0,
                        "target config entry already proposed"
                    );
                    anyhow::ensure!(
                        state.commit_index >= transition.joint_config_index,
                        "joint config entry must commit before finalizing"
                    );
                    let required_index = Self::last_log_index(&state);
                    let single_node_old_config = transition.old_config.voters.len() == 1
                        && transition.old_config.voters.contains(&state.local_node_id);
                    if !single_node_old_config {
                        anyhow::ensure!(
                            Self::voter_quorum_caught_up_to_index(
                                &state,
                                &transition.old_config.voters,
                                required_index,
                            ),
                            "old config has not satisfied dual-majority catch-up"
                        );
                        anyhow::ensure!(
                            Self::voter_quorum_caught_up_to_index(
                                &state,
                                &transition.final_config.voters,
                                required_index,
                            ),
                            "final config has not satisfied dual-majority catch-up"
                        );
                    }
                    if state
                        .leader_node_id
                        .is_some_and(|leader| !transition.final_config.voters.contains(&leader))
                    {
                        anyhow::bail!(
                            "current leader must transfer before leaving the finalized voter set"
                        );
                    }
                    let next_index = Self::last_log_index(&state).saturating_add(1);
                    let entry_term = state.current_term.max(1);
                    state.log.push(RaftLogEntry {
                        term: entry_term,
                        index: next_index,
                        config_change: Some(RaftConfigLogEntry::TargetConfig {
                            old_config: transition.old_config.clone(),
                            joint_config: transition.joint_config.clone(),
                            final_config: transition.final_config.clone(),
                            joint_config_index: transition.joint_config_index,
                        }),
                        command: Bytes::new(),
                    });
                    state.config_transition = Some(RaftConfigTransitionState {
                        old_config: transition.old_config,
                        joint_config: transition.joint_config,
                        final_config: transition.final_config,
                        joint_config_index: transition.joint_config_index,
                        target_config_index: next_index,
                        phase: RaftConfigTransitionPhase::Finalizing,
                    });
                    let local_node_id = state.local_node_id;
                    state.match_indexes.insert(local_node_id, next_index);
                    if Self::quorum_size(&state) == 1 {
                        let previous_commit = state.commit_index;
                        state.commit_index = next_index;
                        Self::apply_committed_config_entries(&mut state, previous_commit);
                        Self::enqueue_newly_committed_entries(&mut state, previous_commit);
                    } else {
                        Self::enqueue_replication_round(&mut state);
                    }
                }
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
                state.config_transition = restored.config_transition;
                state.log = restored.log;
                state.latest_snapshot = restored.latest_snapshot;
                Self::rebuild_config_state_from_log(&mut state);
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
            if state.config_transition.as_ref().is_some_and(|transition| {
                transition.target_config_index > 0
                    && state.applied_index >= transition.target_config_index
            }) {
                state.config_transition = None;
            }
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
