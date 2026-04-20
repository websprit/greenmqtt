use super::{
    AppliedRangeSnapshot, HostedRange, KvRangeHost, MemoryKvRangeHost, RangeLifecycleManager,
    ReplicaRuntime, ReplicaTransport,
};
use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    RangeBoundary, RangeReplica, ReconfigurationPhase, ReplicaRole, ReplicaSyncState,
    ReplicatedRangeDescriptor, ServiceShardKey, ServiceShardLifecycle,
};
use greenmqtt_kv_balance::{execute_balance_commands, BalanceCommand};
use greenmqtt_kv_engine::{KvEngine, MemoryKvEngine};
use greenmqtt_kv_raft::{
    AppendEntriesResponse, MemoryRaftNode, RaftClusterConfig, RaftMessage, RaftNode,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[derive(Clone)]
struct TestRangeLifecycleManager {
    engine: MemoryKvEngine,
    retired: Arc<Mutex<Vec<String>>>,
}

impl TestRangeLifecycleManager {
    fn new(engine: MemoryKvEngine) -> Self {
        Self {
            engine,
            retired: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn retired(&self) -> Vec<String> {
        self.retired.lock().expect("retired poisoned").clone()
    }
}

#[async_trait]
impl RangeLifecycleManager for TestRangeLifecycleManager {
    async fn create_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<HostedRange> {
        self.engine
            .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
            self.engine.open_range(&descriptor.id).await?,
        );
        let raft = Arc::new(MemoryRaftNode::single_node(
            descriptor.leader_node_id.unwrap_or(1),
            &descriptor.id,
        ));
        raft.recover().await?;
        Ok(HostedRange {
            descriptor,
            raft,
            space,
        })
    }

    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.engine.destroy_range(range_id).await?;
        self.retired
            .lock()
            .expect("retired poisoned")
            .push(range_id.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn memory_range_host_tracks_ranges_and_reports_local_leaders() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-1".into(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-1").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-1"));
    raft.recover().await.unwrap();
    let host = MemoryKvRangeHost::default();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-1",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            1,
            Some(1),
            vec![
                RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(2, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(3, ReplicaRole::Voter, ReplicaSyncState::Replicating),
            ],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space,
    })
    .await
    .unwrap();

    assert_eq!(host.list_ranges().await.unwrap().len(), 1);
    assert_eq!(host.local_leader_ranges(1).await.unwrap().len(), 1);
    assert_eq!(host.local_leader_ranges(2).await.unwrap().len(), 0);
    assert!(host.range("range-1").await.unwrap().is_some());
}

#[derive(Default)]
struct RecordingTransport {
    sent: Arc<Mutex<Vec<(u64, u64, String, RaftMessage)>>>,
}

#[async_trait]
impl ReplicaTransport for RecordingTransport {
    async fn send(
        &self,
        from_node_id: u64,
        target_node_id: u64,
        range_id: &str,
        message: &RaftMessage,
    ) -> anyhow::Result<()> {
        self.sent.lock().expect("transport poisoned").push((
            from_node_id,
            target_node_id,
            range_id.to_string(),
            message.clone(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn replica_runtime_ticks_ranges_and_sends_outbound_messages() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-r".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-r").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-r",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-r",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space,
    })
    .await
    .unwrap();

    let transport = Arc::new(RecordingTransport::default());
    let runtime = ReplicaRuntime::new(host, transport.clone());
    let mut sent = 0usize;
    for _ in 0..8 {
        sent += runtime.tick_once().await.unwrap();
    }
    assert!(sent > 0);
    let sent_messages = transport.sent.lock().expect("transport poisoned");
    assert!(sent_messages
        .iter()
        .any(|(from, target, range_id, message)| {
            *from == 1
                && *target == 2
                && range_id == "range-r"
                && matches!(message, RaftMessage::RequestVote(_))
        }));
}

#[derive(Default)]
struct FlakyTransport {
    attempts: AtomicUsize,
    sent: Arc<Mutex<Vec<(u64, u64, String, RaftMessage)>>>,
}

#[async_trait]
impl ReplicaTransport for FlakyTransport {
    async fn send(
        &self,
        from_node_id: u64,
        target_node_id: u64,
        range_id: &str,
        message: &RaftMessage,
    ) -> anyhow::Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            anyhow::bail!("synthetic transport failure");
        }
        self.sent.lock().expect("transport poisoned").push((
            from_node_id,
            target_node_id,
            range_id.to_string(),
            message.clone(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn replica_runtime_retries_failed_outbound_messages() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-retry".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-retry").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-retry",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-retry",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space,
    })
    .await
    .unwrap();
    let transport = Arc::new(FlakyTransport::default());
    let runtime = ReplicaRuntime::with_config(
        host,
        transport.clone(),
        None,
        Duration::from_secs(1),
        8,
        64,
        64,
    );

    let mut sent = 0usize;
    for _ in 0..8 {
        sent += runtime.tick_once().await.unwrap();
    }
    assert!(sent > 0);
    assert!(transport.attempts.load(Ordering::SeqCst) >= 2);
    assert!(!transport
        .sent
        .lock()
        .expect("transport poisoned")
        .is_empty());
}

#[tokio::test]
async fn replica_runtime_applies_committed_entries_and_marks_progress() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-apply".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-apply").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-apply"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-apply",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host.clone(), Arc::new(RecordingTransport::default()));

    let command = bincode::serialize(&vec![greenmqtt_kv_engine::KvMutation {
        key: b"alpha".as_slice().into(),
        value: Some(b"one".as_slice().into()),
    }])
    .unwrap();
    raft.propose(command.into()).await.unwrap();
    let applied = runtime.apply_once().await.unwrap();
    assert_eq!(applied, 1);

    let opened = host.open_range("range-apply").await.unwrap().unwrap();
    assert_eq!(
        opened
            .space
            .reader()
            .get(b"alpha")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"one"
    );
    let status = opened.raft.status().await.unwrap();
    assert_eq!(status.commit_index, 1);
    assert_eq!(status.applied_index, 1);
}

#[tokio::test]
async fn replica_runtime_generates_snapshot_from_applied_state() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-snapshot".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-snapshot").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-snapshot"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-snapshot",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host.clone(), Arc::new(RecordingTransport::default()));

    let command = bincode::serialize(&vec![greenmqtt_kv_engine::KvMutation {
        key: b"alpha".as_slice().into(),
        value: Some(b"one".as_slice().into()),
    }])
    .unwrap();
    raft.propose(command.into()).await.unwrap();
    runtime.apply_once().await.unwrap();
    let generated = runtime.snapshot_once(1).await.unwrap();
    assert_eq!(generated, 1);

    let snapshot = raft.latest_snapshot().await.unwrap().unwrap();
    assert_eq!(snapshot.index, 1);
    let payload: AppliedRangeSnapshot = bincode::deserialize(&snapshot.payload).unwrap();
    assert_eq!(payload.snapshot.range_id, "range-snapshot");
    assert_eq!(payload.snapshot.layout_version, 1);
    assert_eq!(payload.entries.len(), 1);
}

#[tokio::test]
async fn replica_runtime_compacts_logs_after_snapshot_safety_point() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-compact".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-compact").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-compact"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-compact",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host.clone(), Arc::new(RecordingTransport::default()));

    for payload in [b"a".as_slice(), b"b".as_slice()] {
        let command = bincode::serialize(&vec![greenmqtt_kv_engine::KvMutation {
            key: payload.into(),
            value: Some(payload.into()),
        }])
        .unwrap();
        raft.propose(command.into()).await.unwrap();
        runtime.apply_once().await.unwrap();
    }
    runtime.snapshot_once(1).await.unwrap();
    let compacted = runtime.compact_once(1).await.unwrap();
    assert!(compacted > 0);
    let status = raft.status().await.unwrap();
    assert!(status.log_len <= 1);
}

#[tokio::test]
async fn replica_runtime_spawn_runs_background_tick_loop() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-loop".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-loop").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-loop",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-loop",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space,
    })
    .await
    .unwrap();

    let transport = Arc::new(RecordingTransport::default());
    let runtime = Arc::new(ReplicaRuntime::new(host, transport.clone()));
    let handle = runtime.spawn(Duration::from_millis(5));
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown().await.unwrap();

    assert!(!transport
        .sent
        .lock()
        .expect("transport poisoned")
        .is_empty());
}

#[tokio::test]
async fn replica_runtime_change_replicas_updates_raft_config() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-config".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-config").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-config"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-config",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-config", vec![1, 2, 3], vec![4])
        .await
        .unwrap();
    runtime
        .change_replicas("range-config", vec![1, 2, 3], vec![4])
        .await
        .unwrap();
    let status = raft.status().await.unwrap();
    assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
    assert_eq!(status.cluster_config.learners, vec![4]);
}

#[tokio::test]
async fn replica_runtime_accepts_repeated_request_while_final_config_is_committed_but_not_applied()
{
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-config-finalizing".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-config-finalizing").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-config-finalizing"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-config-finalizing",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-config-finalizing", vec![1, 2], Vec::new())
        .await
        .unwrap();
    runtime
        .change_replicas("range-config-finalizing", vec![1, 2], Vec::new())
        .await
        .unwrap();

    let status = raft.status().await.unwrap();
    let transition = status
        .config_transition
        .clone()
        .expect("finalizing transition");
    assert_eq!(
        transition.phase,
        greenmqtt_kv_raft::RaftConfigTransitionPhase::Finalizing
    );
    runtime
        .change_replicas("range-config-finalizing", vec![1, 2], Vec::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn replica_runtime_change_replicas_stages_new_voter_as_learner_first() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-stage".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-stage").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-stage"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-stage",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-stage", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let status = raft.status().await.unwrap();
    assert_eq!(status.cluster_config.voters, vec![1]);
    assert_eq!(status.cluster_config.learners, vec![2]);
}

#[tokio::test]
async fn replica_runtime_blocks_switching_reconfig_target_while_stage_is_pending() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-stage-guard".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-stage-guard").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-stage-guard"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-stage-guard",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-stage-guard", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let error = runtime
        .change_replicas("range-stage-guard", vec![1, 3], Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("config change already in progress"));
}

#[tokio::test]
async fn replica_runtime_allows_new_reconfig_target_after_finalize() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-stage-finalize".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-stage-finalize").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-stage-finalize"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-stage-finalize",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-stage-finalize", vec![1, 2], Vec::new())
        .await
        .unwrap();
    runtime
        .change_replicas("range-stage-finalize", vec![1, 2], Vec::new())
        .await
        .unwrap();
    runtime.apply_once().await.unwrap();
    runtime
        .change_replicas("range-stage-finalize", vec![1, 3], Vec::new())
        .await
        .unwrap();
    let status = raft.status().await.unwrap();
    assert_eq!(status.cluster_config.voters, vec![1, 2]);
    assert_eq!(status.cluster_config.learners, vec![3]);
}

#[tokio::test]
async fn replica_runtime_blocks_finalize_until_staged_replica_catches_up() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-stage-catchup".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-stage-catchup").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-stage-catchup"));
    raft.recover().await.unwrap();
    let _ = raft.propose(Bytes::from_static(b"cmd")).await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-stage-catchup",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-stage-catchup", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let error = runtime
        .change_replicas("range-stage-catchup", vec![1, 2], Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("entering joint config"));
    let pending = runtime
        .pending_reconfiguration("range-stage-catchup")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.phase, ReconfigurationPhase::JointConsensus);
    assert!(pending.blocked_on_catch_up);

    let status = raft.status().await.unwrap();
    raft.receive(
        2,
        RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
            term: status.current_term,
            success: true,
            match_index: status.commit_index,
        }),
    )
    .await
    .unwrap();

    runtime
        .change_replicas("range-stage-catchup", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let final_status = raft.status().await.unwrap();
    assert_eq!(final_status.cluster_config.voters, vec![1, 2]);
    assert!(final_status.cluster_config.learners.is_empty());
    assert!(runtime
        .pending_reconfiguration("range-stage-catchup")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn replica_runtime_tracks_pending_reconfiguration_phase() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-stage-state".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-stage-state").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-stage-state"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-stage-state",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-stage-state", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let pending = runtime
        .pending_reconfiguration("range-stage-state")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.phase, ReconfigurationPhase::StagingLearners);
    assert_eq!(pending.target_voters, vec![1, 2]);
    assert_eq!(pending.current_voters, vec![1]);
    assert_eq!(pending.current_learners, vec![2]);
    assert!(pending.blocked_on_catch_up);
}

#[tokio::test]
async fn replica_runtime_exposes_joint_consensus_pending_state() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-joint-state".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-joint-state").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-joint-state",
        RaftClusterConfig {
            voters: vec![1, 2],
            learners: vec![3],
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-joint-state",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![
                RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(2, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(3, ReplicaRole::Learner, ReplicaSyncState::Replicating),
            ],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-joint-state", vec![2, 3], Vec::new())
        .await
        .unwrap();
    let pending = runtime
        .pending_reconfiguration("range-joint-state")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.phase, ReconfigurationPhase::JointConsensus);
    assert_eq!(pending.old_voters, vec![1, 2]);
    assert_eq!(pending.joint_voters, vec![1, 2, 3]);
    assert_eq!(pending.target_voters, vec![2, 3]);
    assert!(!pending.blocked_on_catch_up);
}

#[tokio::test]
async fn replica_runtime_blocks_finalize_until_dual_majority_is_reached() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-dual-majority".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-dual-majority").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-dual-majority",
        RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-dual-majority",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![
                RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(2, ReplicaRole::Voter, ReplicaSyncState::Replicating),
            ],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));

    for _ in 0..8 {
        raft.tick().await.unwrap();
    }
    raft.receive(
        2,
        RaftMessage::RequestVoteResponse(greenmqtt_kv_raft::RequestVoteResponse {
            term: raft.status().await.unwrap().current_term,
            vote_granted: true,
        }),
    )
    .await
    .unwrap();

    runtime
        .change_replicas("range-dual-majority", vec![1, 3], Vec::new())
        .await
        .unwrap();
    runtime
        .change_replicas("range-dual-majority", vec![1, 3], Vec::new())
        .await
        .unwrap();

    raft.propose(
        bincode::serialize(&Vec::<greenmqtt_kv_engine::KvMutation>::new())
            .unwrap()
            .into(),
    )
    .await
    .unwrap();
    raft.receive(
        2,
        RaftMessage::AppendEntriesResponse(greenmqtt_kv_raft::AppendEntriesResponse {
            term: raft.status().await.unwrap().current_term,
            success: true,
            match_index: 1,
        }),
    )
    .await
    .unwrap();

    let error = runtime
        .change_replicas("range-dual-majority", vec![1, 3], Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("dual-majority catch-up"));
    let pending = runtime
        .pending_reconfiguration("range-dual-majority")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.phase, ReconfigurationPhase::JointConsensus);
    assert!(pending.blocked_on_catch_up);

    raft.receive(
        3,
        RaftMessage::AppendEntriesResponse(greenmqtt_kv_raft::AppendEntriesResponse {
            term: raft.status().await.unwrap().current_term,
            success: true,
            match_index: 1,
        }),
    )
    .await
    .unwrap();

    runtime
        .change_replicas("range-dual-majority", vec![1, 3], Vec::new())
        .await
        .unwrap();
    let final_status = raft.status().await.unwrap();
    assert_eq!(final_status.cluster_config.voters, vec![1, 3]);
    assert!(runtime
        .pending_reconfiguration("range-dual-majority")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn replica_runtime_survives_repeated_replica_reconfiguration_cycles() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-reconfig-loop".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-reconfig-loop").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-reconfig-loop"));
    raft.recover().await.unwrap();
    let _ = raft.propose(Bytes::from_static(b"cmd")).await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-reconfig-loop",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));

    for target in [2u64, 3, 2, 3] {
        runtime
            .change_replicas("range-reconfig-loop", vec![1, target], Vec::new())
            .await
            .unwrap();
        let status = raft.status().await.unwrap();
        raft.receive(
            target,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: status.current_term,
                success: true,
                match_index: status.commit_index,
            }),
        )
        .await
        .unwrap();
        runtime
            .change_replicas("range-reconfig-loop", vec![1, target], Vec::new())
            .await
            .unwrap();
        runtime.apply_once().await.unwrap();
        runtime
            .change_replicas("range-reconfig-loop", vec![1], Vec::new())
            .await
            .unwrap();
        runtime
            .change_replicas("range-reconfig-loop", vec![1], Vec::new())
            .await
            .unwrap();
        runtime.apply_once().await.unwrap();
    }

    let final_status = raft.status().await.unwrap();
    assert_eq!(final_status.cluster_config.voters, vec![1]);
    assert!(final_status.cluster_config.learners.is_empty());
    assert!(runtime
        .pending_reconfiguration("range-reconfig-loop")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn replica_runtime_transfer_leadership_updates_raft_state() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-leader".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-leader").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-leader"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-leader",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-leader", vec![1, 2], Vec::new())
        .await
        .unwrap();
    runtime
        .change_replicas("range-leader", vec![1, 2], Vec::new())
        .await
        .unwrap();
    runtime
        .transfer_leadership("range-leader", 2)
        .await
        .unwrap();
    let status = raft.status().await.unwrap();
    assert_eq!(status.leader_node_id, Some(2));
    assert_eq!(status.role, greenmqtt_kv_raft::RaftNodeRole::Follower);
}

#[tokio::test]
async fn replica_runtime_applies_balance_command_to_raft_node() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-balance".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-balance").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-balance"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-balance",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-balance", vec![1, 2], Vec::new())
        .await
        .unwrap();
    runtime
        .change_replicas("range-balance", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let applied = runtime
        .apply_balance_command(&BalanceCommand::TransferLeadership {
            range_id: "range-balance".into(),
            from_node_id: 1,
            to_node_id: 2,
        })
        .await
        .unwrap();
    assert!(applied);
    let status = raft.status().await.unwrap();
    assert_eq!(status.leader_node_id, Some(2));
}

#[tokio::test]
async fn replica_runtime_applies_change_replicas_balance_command() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-balance-replicas".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-balance-replicas").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-balance-replicas"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-balance-replicas",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    let staged = runtime
        .apply_balance_command(&BalanceCommand::ChangeReplicas {
            range_id: "range-balance-replicas".into(),
            voters: vec![1, 2, 3],
            learners: vec![4],
        })
        .await
        .unwrap();
    assert!(staged);
    let status = raft.status().await.unwrap();
    assert_eq!(status.cluster_config.voters, vec![1]);
    assert_eq!(status.cluster_config.learners, vec![2, 3, 4]);
    let applied = runtime
        .apply_balance_command(&BalanceCommand::ChangeReplicas {
            range_id: "range-balance-replicas".into(),
            voters: vec![1, 2, 3],
            learners: vec![4],
        })
        .await
        .unwrap();
    assert!(applied);
    let status = raft.status().await.unwrap();
    assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
    assert_eq!(status.cluster_config.learners, vec![4]);
}

#[tokio::test]
async fn replica_runtime_applies_recover_range_balance_command() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-balance-recover".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-balance-recover").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-balance-recover",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-balance-recover",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    let applied = runtime
        .apply_balance_command(&BalanceCommand::RecoverRange {
            range_id: "range-balance-recover".into(),
            new_leader_node_id: 2,
        })
        .await
        .unwrap();
    assert!(applied);
    let status = raft.status().await.unwrap();
    assert_eq!(status.leader_node_id, Some(2));
}

#[tokio::test]
async fn replica_runtime_blocks_removing_current_local_leader_from_voters() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-block-remove".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-block-remove").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-block-remove"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-block-remove",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    runtime
        .change_replicas("range-block-remove", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let error = runtime
        .change_replicas("range-block-remove", vec![2], Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("refusing to remove current local leader")
            || error.contains("config change already in progress")
    );
}

#[tokio::test]
async fn replica_runtime_executes_balance_command_batches() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-batch".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-batch").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-batch"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-batch",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    let applied = execute_balance_commands(
        &runtime,
        &[
            BalanceCommand::ChangeReplicas {
                range_id: "range-batch".into(),
                voters: vec![1, 2],
                learners: vec![3],
            },
            BalanceCommand::ChangeReplicas {
                range_id: "range-batch".into(),
                voters: vec![1, 2],
                learners: vec![3],
            },
            BalanceCommand::TransferLeadership {
                range_id: "range-batch".into(),
                from_node_id: 1,
                to_node_id: 2,
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(applied, 3);
    let status = raft.status().await.unwrap();
    assert_eq!(status.cluster_config.voters, vec![1, 2]);
    assert_eq!(status.cluster_config.learners, vec![3]);
    assert_eq!(status.leader_node_id, Some(2));
}

#[tokio::test]
async fn replica_runtime_health_snapshot_reports_replica_lag() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-health".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-health").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-health",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
    ));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-health",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft.clone(),
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    for _ in 0..8 {
        raft.tick().await.unwrap();
    }
    raft.receive(
        2,
        RaftMessage::RequestVoteResponse(greenmqtt_kv_raft::RequestVoteResponse {
            term: raft.status().await.unwrap().current_term,
            vote_granted: true,
        }),
    )
    .await
    .unwrap();
    raft.propose(
        bincode::serialize(&Vec::<greenmqtt_kv_engine::KvMutation>::new())
            .unwrap()
            .into(),
    )
    .await
    .unwrap();
    raft.receive(
        2,
        RaftMessage::AppendEntriesResponse(greenmqtt_kv_raft::AppendEntriesResponse {
            term: raft.status().await.unwrap().current_term,
            success: true,
            match_index: 1,
        }),
    )
    .await
    .unwrap();

    let health = runtime.health_snapshot().await.unwrap();
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].range_id, "range-health");
    assert!(health[0]
        .replica_lag
        .iter()
        .any(|replica| replica.node_id == 3));
}

#[tokio::test]
async fn replica_runtime_debug_dump_includes_range_and_replica_progress() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-dump".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-dump").await.unwrap(),
    );
    let raft = Arc::new(MemoryRaftNode::single_node(1, "range-dump"));
    raft.recover().await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-dump",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space,
    })
    .await
    .unwrap();
    let runtime = ReplicaRuntime::new(host, Arc::new(RecordingTransport::default()));
    let dump = runtime.debug_dump().await.unwrap();
    assert!(dump.contains("range=range-dump"));
    assert!(dump.contains("replica=1"));
}

#[tokio::test]
async fn replica_runtime_split_range_moves_data_into_serving_children() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
            range_id: "range-split".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let source = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-split").await.unwrap(),
    );
    source
        .writer()
        .apply(vec![
            greenmqtt_kv_engine::KvMutation {
                key: Bytes::from_static(b"apple"),
                value: Some(Bytes::from_static(b"1")),
            },
            greenmqtt_kv_engine::KvMutation {
                key: Bytes::from_static(b"orange"),
                value: Some(Bytes::from_static(b"2")),
            },
        ])
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-split",
            ServiceShardKey::retain("tenant-a"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: Arc::new(MemoryRaftNode::single_node(1, "range-split")),
        space: source,
    })
    .await
    .unwrap();
    let lifecycle = Arc::new(TestRangeLifecycleManager::new(engine.clone()));
    let runtime = ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(RecordingTransport::default()),
        Some(lifecycle.clone()),
        Duration::from_secs(1),
        64,
        64,
        64,
    );

    let (left_id, right_id) = runtime
        .split_range("range-split", b"m".to_vec())
        .await
        .unwrap();

    assert!(host.open_range("range-split").await.unwrap().is_none());
    let left = host.open_range(&left_id).await.unwrap().unwrap();
    let right = host.open_range(&right_id).await.unwrap().unwrap();
    assert_eq!(left.descriptor.lifecycle, ServiceShardLifecycle::Serving);
    assert_eq!(right.descriptor.lifecycle, ServiceShardLifecycle::Serving);
    assert_eq!(left.descriptor.epoch, 3);
    assert_eq!(right.descriptor.epoch, 3);
    assert_eq!(
        left.space.reader().get(b"apple").await.unwrap().unwrap(),
        Bytes::from_static(b"1")
    );
    assert!(left.space.reader().get(b"orange").await.unwrap().is_none());
    assert_eq!(
        right.space.reader().get(b"orange").await.unwrap().unwrap(),
        Bytes::from_static(b"2")
    );
    assert_eq!(lifecycle.retired(), vec!["range-split".to_string()]);
}

#[tokio::test]
async fn replica_runtime_merge_ranges_moves_data_into_serving_merged_range() {
    let engine = MemoryKvEngine::default();
    for (range_id, boundary, key, value) in [
        (
            "range-left",
            RangeBoundary::new(None, Some(b"m".to_vec())),
            b"apple".as_slice(),
            b"1".as_slice(),
        ),
        (
            "range-right",
            RangeBoundary::new(Some(b"m".to_vec()), None),
            b"orange".as_slice(),
            b"2".as_slice(),
        ),
    ] {
        engine
            .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
                range_id: range_id.into(),
                boundary: boundary.clone(),
            })
            .await
            .unwrap();
        let space = engine.open_range(range_id).await.unwrap();
        space
            .writer()
            .apply(vec![greenmqtt_kv_engine::KvMutation {
                key: Bytes::copy_from_slice(key),
                value: Some(Bytes::copy_from_slice(value)),
            }])
            .await
            .unwrap();
    }

    let host = Arc::new(MemoryKvRangeHost::default());
    for (range_id, boundary) in [
        ("range-left", RangeBoundary::new(None, Some(b"m".to_vec()))),
        ("range-right", RangeBoundary::new(Some(b"m".to_vec()), None)),
    ] {
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                range_id,
                ServiceShardKey::retain("tenant-a"),
                boundary,
                2,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            raft: Arc::new(MemoryRaftNode::single_node(1, range_id)),
            space: Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
                engine.open_range(range_id).await.unwrap(),
            ),
        })
        .await
        .unwrap();
    }
    let lifecycle = Arc::new(TestRangeLifecycleManager::new(engine.clone()));
    let runtime = ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(RecordingTransport::default()),
        Some(lifecycle.clone()),
        Duration::from_secs(1),
        64,
        64,
        64,
    );

    let merged_id = runtime
        .merge_ranges("range-left", "range-right")
        .await
        .unwrap();

    assert!(host.open_range("range-left").await.unwrap().is_none());
    assert!(host.open_range("range-right").await.unwrap().is_none());
    let merged = host.open_range(&merged_id).await.unwrap().unwrap();
    assert_eq!(merged.descriptor.lifecycle, ServiceShardLifecycle::Serving);
    assert_eq!(merged.descriptor.epoch, 4);
    assert_eq!(
        merged.space.reader().get(b"apple").await.unwrap().unwrap(),
        Bytes::from_static(b"1")
    );
    assert_eq!(
        merged.space.reader().get(b"orange").await.unwrap().unwrap(),
        Bytes::from_static(b"2")
    );
    assert_eq!(
        lifecycle.retired(),
        vec!["range-left".to_string(), "range-right".to_string()]
    );
}

#[tokio::test]
async fn replica_runtime_bootstrap_range_creates_hosted_range() {
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let lifecycle = Arc::new(TestRangeLifecycleManager::new(engine.clone()));
    let runtime = ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(RecordingTransport::default()),
        Some(lifecycle),
        Duration::from_secs(1),
        64,
        64,
        64,
    );

    let range_id = runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-bootstrap",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Bootstrapping,
        ))
        .await
        .unwrap();

    assert_eq!(range_id, "range-bootstrap");
    let hosted = host.open_range("range-bootstrap").await.unwrap().unwrap();
    assert_eq!(hosted.descriptor.id, "range-bootstrap");
}
