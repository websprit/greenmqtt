use super::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, MemoryRaftNode,
    MemoryRaftStateStore, RaftClusterConfig, RaftConfigChange, RaftConfigTransitionPhase,
    RaftMessage, RaftNode, RaftNodeRole, RaftSnapshot, RaftStateStore, RequestVoteRequest, RequestVoteResponse,
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
async fn joint_consensus_transition_survives_restart() {
    let store = Arc::new(MemoryRaftStateStore::default());
    let node = MemoryRaftNode::with_state_store(
        1,
        "range-joint-restart",
        RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
        store.clone(),
    );
    node.recover().await.unwrap();
    node.change_cluster_config(RaftConfigChange::EnterJointConsensus {
        voters: vec![2, 3],
        learners: Vec::new(),
    })
    .await
    .unwrap();

    let restarted = MemoryRaftNode::with_state_store(
        1,
        "range-joint-restart",
        RaftClusterConfig::default(),
        store,
    );
    restarted.recover().await.unwrap();
    let status = restarted.status().await.unwrap();
    let transition = status.config_transition.expect("joint transition");
    assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
    assert_eq!(transition.old_config.voters, vec![1, 2]);
    assert_eq!(transition.final_config.voters, vec![2, 3]);
    assert_eq!(transition.phase, RaftConfigTransitionPhase::JointConsensus);
}

#[tokio::test]
async fn joint_consensus_rejects_repeated_enter_requests() {
    let node = MemoryRaftNode::new(
        1,
        "range-joint-repeat",
        RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
    );
    node.recover().await.unwrap();
    node.change_cluster_config(RaftConfigChange::EnterJointConsensus {
        voters: vec![2, 3],
        learners: Vec::new(),
    })
    .await
    .unwrap();

    let error = node
        .change_cluster_config(RaftConfigChange::EnterJointConsensus {
            voters: vec![2, 3],
            learners: Vec::new(),
        })
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("already in progress"));
}

#[tokio::test]
async fn joint_consensus_blocks_finalizing_away_current_leader() {
    let node = MemoryRaftNode::new(
        1,
        "range-joint-leader",
        RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
    );
    node.recover().await.unwrap();
    elect_local_leader(&node).await;
    node.change_cluster_config(RaftConfigChange::EnterJointConsensus {
        voters: vec![2, 3],
        learners: Vec::new(),
    })
    .await
    .unwrap();

    let error = node
        .change_cluster_config(RaftConfigChange::FinalizeJointConsensus)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("transfer before leaving"));
    assert!(node.status().await.unwrap().config_transition.is_some());
}

#[tokio::test]
async fn joint_consensus_requires_dual_majority_before_commit_and_finalize() {
    let node = MemoryRaftNode::new(
        1,
        "range-joint-dual-majority",
        RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
    );
    node.recover().await.unwrap();
    elect_local_leader(&node).await;
    node.change_cluster_config(RaftConfigChange::EnterJointConsensus {
        voters: vec![1, 3],
        learners: Vec::new(),
    })
    .await
    .unwrap();

    node.propose(Bytes::from_static(b"joint-cmd")).await.unwrap();
    node.receive(
        2,
        RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
            term: node.status().await.unwrap().current_term,
            success: true,
            match_index: 1,
        }),
    )
    .await
    .unwrap();

    let status = node.status().await.unwrap();
    assert_eq!(status.commit_index, 0);

    let error = node
        .change_cluster_config(RaftConfigChange::FinalizeJointConsensus)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("dual-majority catch-up"));

    node.receive(
        3,
        RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
            term: node.status().await.unwrap().current_term,
            success: true,
            match_index: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(node.status().await.unwrap().commit_index, 1);
    node.change_cluster_config(RaftConfigChange::FinalizeJointConsensus)
        .await
        .unwrap();
    let final_status = node.status().await.unwrap();
    assert_eq!(final_status.cluster_config.voters, vec![1, 3]);
    assert!(final_status.config_transition.is_none());
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
                config_transition: None,
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
