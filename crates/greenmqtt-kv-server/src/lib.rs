use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, ReplicatedRangeDescriptor, ServiceShardLifecycle};
use greenmqtt_kv_balance::{BalanceCommand, BalanceCommandExecutor};
use greenmqtt_kv_engine::{KvMutation, KvRangeSnapshot, KvRangeSpace};
use greenmqtt_kv_raft::{
    RaftConfigChange, RaftLogEntry, RaftMessage, RaftNode, RaftSnapshot, RaftStatusSnapshot,
};
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
    ranges: Arc<RwLock<BTreeMap<String, HostedRange>>>,
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
    host: Arc<dyn KvRangeHost>,
    transport: Arc<dyn ReplicaTransport>,
    pending: Arc<Mutex<VecDeque<PendingOutbound>>>,
    send_timeout: Duration,
    max_pending: usize,
    snapshot_threshold: u64,
    compact_threshold: usize,
}

pub struct ReplicaRuntimeHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<anyhow::Result<()>>,
}

#[derive(Debug, Clone)]
struct PendingOutbound {
    from_node_id: NodeId,
    target_node_id: NodeId,
    range_id: String,
    message: RaftMessage,
}

impl ReplicaRuntime {
    pub fn new(host: Arc<dyn KvRangeHost>, transport: Arc<dyn ReplicaTransport>) -> Self {
        Self {
            host,
            transport,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            send_timeout: Duration::from_secs(1),
            max_pending: 1024,
            snapshot_threshold: 64,
            compact_threshold: 64,
        }
    }

    pub fn with_config(
        host: Arc<dyn KvRangeHost>,
        transport: Arc<dyn ReplicaTransport>,
        send_timeout: Duration,
        max_pending: usize,
        snapshot_threshold: u64,
        compact_threshold: usize,
    ) -> Self {
        Self {
            host,
            transport,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            send_timeout,
            max_pending,
            snapshot_threshold,
            compact_threshold,
        }
    }

    fn push_pending(&self, pending: PendingOutbound) {
        let mut guard = self.pending.lock().expect("replica runtime poisoned");
        guard.push_back(pending);
        while guard.len() > self.max_pending {
            guard.pop_front();
        }
    }

    async fn flush_pending(&self) -> anyhow::Result<usize> {
        let pending = {
            let mut guard = self.pending.lock().expect("replica runtime poisoned");
            guard.drain(..).collect::<Vec<_>>()
        };
        let mut sent = 0usize;
        for entry in pending {
            let result = tokio::time::timeout(
                self.send_timeout,
                self.transport.send(
                    entry.from_node_id,
                    entry.target_node_id,
                    &entry.range_id,
                    &entry.message,
                ),
            )
            .await;
            match result {
                Ok(Ok(())) => sent += 1,
                Ok(Err(_)) | Err(_) => self.push_pending(entry),
            }
        }
        Ok(sent)
    }

    pub async fn tick_once(&self) -> anyhow::Result<usize> {
        let mut sent = self.flush_pending().await?;
        let statuses = self.host.list_ranges().await?;
        for status in statuses {
            let Some(range) = self.host.open_range(&status.descriptor.id).await? else {
                continue;
            };
            range.raft.tick().await?;
            let from_node_id = range.raft.local_node_id();
            for outbound in range.raft.drain_outbox().await? {
                let pending = PendingOutbound {
                    from_node_id,
                    target_node_id: outbound.target_node_id,
                    range_id: status.descriptor.id.clone(),
                    message: outbound.message,
                };
                let result = tokio::time::timeout(
                    self.send_timeout,
                    self.transport.send(
                        pending.from_node_id,
                        pending.target_node_id,
                        &pending.range_id,
                        &pending.message,
                    ),
                )
                .await;
                match result {
                    Ok(Ok(())) => sent += 1,
                    Ok(Err(_)) | Err(_) => self.push_pending(pending),
                }
            }
        }
        Ok(sent)
    }

    async fn apply_entry(range: &HostedRange, entry: &RaftLogEntry) -> anyhow::Result<()> {
        let mutations: Vec<KvMutation> = bincode::deserialize(&entry.command)?;
        range.space.writer().apply(mutations).await
    }

    pub async fn apply_once(&self) -> anyhow::Result<usize> {
        let statuses = self.host.list_ranges().await?;
        let mut applied = 0usize;
        for status in statuses {
            let Some(range) = self.host.open_range(&status.descriptor.id).await? else {
                continue;
            };
            let entries = range.raft.committed_entries().await?;
            if entries.is_empty() {
                continue;
            }
            let mut last_applied = None;
            for entry in &entries {
                Self::apply_entry(&range, entry).await?;
                last_applied = Some(entry.index);
                applied += 1;
            }
            if let Some(index) = last_applied {
                range.raft.mark_applied(index).await?;
            }
        }
        Ok(applied)
    }

    pub async fn snapshot_once(&self, min_applied_since_snapshot: u64) -> anyhow::Result<usize> {
        let statuses = self.host.list_ranges().await?;
        let mut generated = 0usize;
        for status in statuses {
            if status.raft.role != greenmqtt_kv_raft::RaftNodeRole::Leader {
                continue;
            }
            let Some(range) = self.host.open_range(&status.descriptor.id).await? else {
                continue;
            };
            let latest_snapshot_index = range
                .raft
                .latest_snapshot()
                .await?
                .map(|snapshot| snapshot.index)
                .unwrap_or(0);
            if status
                .raft
                .applied_index
                .saturating_sub(latest_snapshot_index)
                < min_applied_since_snapshot
            {
                continue;
            }
            let snapshot = range.space.snapshot().await?;
            let applied_snapshot = AppliedRangeSnapshot {
                snapshot: snapshot.clone(),
                entries: range.space.reader().scan(range.space.boundary(), usize::MAX).await?,
            };
            let payload = bincode::serialize(&applied_snapshot)?;
            range.raft
                .install_snapshot(RaftSnapshot {
                    range_id: status.descriptor.id.clone(),
                    term: status.raft.current_term,
                    index: status.raft.applied_index,
                    payload: payload.into(),
                })
                .await?;
            generated += 1;
        }
        Ok(generated)
    }

    pub async fn compact_once(&self, min_log_entries: usize) -> anyhow::Result<usize> {
        let statuses = self.host.list_ranges().await?;
        let mut compacted = 0usize;
        for status in statuses {
            if status.raft.log_len < min_log_entries {
                continue;
            }
            let Some(range) = self.host.open_range(&status.descriptor.id).await? else {
                continue;
            };
            if range.raft.latest_snapshot().await?.is_none() {
                continue;
            }
            let removed = range.raft.compact_log_to_snapshot().await?;
            if removed > 0 {
                range.space.writer().compact(None).await?;
                compacted += removed;
            }
        }
        Ok(compacted)
    }

    pub async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        let range = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found"))?;
        range
            .raft
            .change_cluster_config(RaftConfigChange::ReplaceCluster { voters, learners })
            .await
    }

    pub async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        let range = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found"))?;
        range.raft.transfer_leadership(target_node_id).await
    }

    pub async fn recover_range(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        let range = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found"))?;
        range
            .raft
            .change_cluster_config(RaftConfigChange::ReplaceCluster {
                voters: range
                    .raft
                    .status()
                    .await?
                    .cluster_config
                    .voters
                    .into_iter()
                    .collect(),
                learners: range
                    .raft
                    .status()
                    .await?
                    .cluster_config
                    .learners
                    .into_iter()
                    .collect(),
            })
            .await?;
        range.raft.transfer_leadership(new_leader_node_id).await
    }

    pub async fn apply_balance_command(&self, command: &BalanceCommand) -> anyhow::Result<bool> {
        match command {
            BalanceCommand::ChangeReplicas {
                range_id,
                voters,
                learners,
            } => {
                self.change_replicas(range_id, voters.clone(), learners.clone())
                    .await?;
                Ok(true)
            }
            BalanceCommand::TransferLeadership {
                range_id,
                to_node_id,
                ..
            } => {
                self.transfer_leadership(range_id, *to_node_id).await?;
                Ok(true)
            }
            BalanceCommand::RecoverRange {
                range_id,
                new_leader_node_id,
            } => {
                self.recover_range(range_id, *new_leader_node_id).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub async fn run_once(&self) -> anyhow::Result<(usize, usize, usize, usize)> {
        let sent = self.tick_once().await?;
        let applied = self.apply_once().await?;
        let snapshotted = self.snapshot_once(self.snapshot_threshold).await?;
        let compacted = self.compact_once(self.compact_threshold).await?;
        Ok((sent, applied, snapshotted, compacted))
    }

    pub fn spawn(self: Arc<Self>, interval: Duration) -> ReplicaRuntimeHandle {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _ = self.run_once().await?;
                    }
                    _ = &mut shutdown_rx => {
                        return Ok(());
                    }
                }
            }
        });
        ReplicaRuntimeHandle {
            shutdown: Some(shutdown_tx),
            task,
        }
    }
}

#[async_trait]
impl BalanceCommandExecutor for ReplicaRuntime {
    async fn execute(&self, command: &BalanceCommand) -> anyhow::Result<bool> {
        self.apply_balance_command(command).await
    }
}

impl ReplicaRuntimeHandle {
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await?
    }
}

#[async_trait]
impl KvRangeHost for MemoryKvRangeHost {
    async fn add_range(&self, range: HostedRange) -> anyhow::Result<()> {
        self.ranges
            .write()
            .expect("range host poisoned")
            .insert(range.descriptor.id.clone(), range);
        Ok(())
    }

    async fn remove_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>> {
        Ok(self
            .ranges
            .write()
            .expect("range host poisoned")
            .remove(range_id))
    }

    async fn open_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>> {
        Ok(self
            .ranges
            .read()
            .expect("range host poisoned")
            .get(range_id)
            .cloned())
    }

    async fn range(&self, range_id: &str) -> anyhow::Result<Option<HostedRangeStatus>> {
        let maybe_range = self
            .ranges
            .read()
            .expect("range host poisoned")
            .get(range_id)
            .cloned();
        match maybe_range {
            Some(range) => Ok(Some(HostedRangeStatus {
                descriptor: range.descriptor,
                raft: range.raft.status().await?,
            })),
            None => Ok(None),
        }
    }

    async fn list_ranges(&self) -> anyhow::Result<Vec<HostedRangeStatus>> {
        let ranges: Vec<_> = self
            .ranges
            .read()
            .expect("range host poisoned")
            .values()
            .cloned()
            .collect();
        let mut statuses = Vec::with_capacity(ranges.len());
        for range in ranges {
            statuses.push(HostedRangeStatus {
                descriptor: range.descriptor,
                raft: range.raft.status().await?,
            });
        }
        Ok(statuses)
    }

    async fn local_leader_ranges(
        &self,
        local_node_id: NodeId,
    ) -> anyhow::Result<Vec<HostedRangeStatus>> {
        let ranges: Vec<_> = self
            .ranges
            .read()
            .expect("range host poisoned")
            .values()
            .cloned()
            .collect();
        let mut statuses = Vec::new();
        for range in ranges {
            let raft = range.raft.status().await?;
            if raft.leader_node_id == Some(local_node_id)
                && range.descriptor.lifecycle == ServiceShardLifecycle::Serving
            {
                statuses.push(HostedRangeStatus {
                    descriptor: range.descriptor,
                    raft,
                });
            }
        }
        Ok(statuses)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppliedRangeSnapshot, HostedRange, KvRangeHost, MemoryKvRangeHost, ReplicaRuntime,
        ReplicaTransport,
    };
    use async_trait::async_trait;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_balance::BalanceCommand;
    use greenmqtt_kv_engine::{KvEngine, MemoryKvEngine};
    use greenmqtt_kv_raft::{MemoryRaftNode, RaftMessage, RaftNode};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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
        let runtime =
            ReplicaRuntime::with_config(host, transport.clone(), Duration::from_secs(1), 8, 64, 64);

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

        assert!(!transport.sent.lock().expect("transport poisoned").is_empty());
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
        let status = raft.status().await.unwrap();
        assert_eq!(status.cluster_config.voters, vec![1, 2, 3]);
        assert_eq!(status.cluster_config.learners, vec![4]);
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
        runtime.transfer_leadership("range-leader", 2).await.unwrap();
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
}
