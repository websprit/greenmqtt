use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, RangeBoundary, ReplicatedRangeDescriptor, ServiceShardLifecycle};
use greenmqtt_kv_balance::{BalanceCommand, BalanceCommandExecutor};
use greenmqtt_kv_engine::{KvMutation, KvRangeSnapshot, KvRangeSpace};
use greenmqtt_kv_raft::{
    RaftConfigChange, RaftLogEntry, RaftMessage, RaftNode, RaftSnapshot, RaftStatusSnapshot,
};
use metrics::gauge;
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
    pub role: greenmqtt_kv_raft::RaftNodeRole,
    pub current_term: u64,
    pub leader_node_id: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub latest_snapshot_index: Option<u64>,
    pub replica_lag: Vec<ReplicaLagSnapshot>,
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
    ranges: Arc<RwLock<BTreeMap<String, HostedRange>>>,
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
    host: Arc<dyn KvRangeHost>,
    transport: Arc<dyn ReplicaTransport>,
    lifecycle: Option<Arc<dyn RangeLifecycleManager>>,
    pending: Arc<Mutex<VecDeque<PendingOutbound>>>,
    pending_reconfig: Arc<Mutex<BTreeMap<String, PendingReconfiguration>>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingReconfiguration {
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
}

impl ReplicaRuntime {
    pub fn new(host: Arc<dyn KvRangeHost>, transport: Arc<dyn ReplicaTransport>) -> Self {
        Self {
            host,
            transport,
            lifecycle: None,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            pending_reconfig: Arc::new(Mutex::new(BTreeMap::new())),
            send_timeout: Duration::from_secs(1),
            max_pending: 1024,
            snapshot_threshold: 64,
            compact_threshold: 64,
        }
    }

    pub fn with_config(
        host: Arc<dyn KvRangeHost>,
        transport: Arc<dyn ReplicaTransport>,
        lifecycle: Option<Arc<dyn RangeLifecycleManager>>,
        send_timeout: Duration,
        max_pending: usize,
        snapshot_threshold: u64,
        compact_threshold: usize,
    ) -> Self {
        Self {
            host,
            transport,
            lifecycle,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            pending_reconfig: Arc::new(Mutex::new(BTreeMap::new())),
            send_timeout,
            max_pending,
            snapshot_threshold,
            compact_threshold,
        }
    }

    async fn replace_range(&self, range: HostedRange) -> anyhow::Result<()> {
        self.host.add_range(range).await
    }

    async fn rewrite_descriptor<F>(&self, range_id: &str, update: F) -> anyhow::Result<HostedRange>
    where
        F: FnOnce(&mut ReplicatedRangeDescriptor),
    {
        let mut range = self
            .host
            .remove_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found"))?;
        update(&mut range.descriptor);
        self.host.add_range(range.clone()).await?;
        Ok(range)
    }

    pub async fn validate_serving_fence(
        &self,
        range_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<HostedRange> {
        let range = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found"))?;
        if let Some(expected_epoch) = expected_epoch {
            anyhow::ensure!(
                range.descriptor.epoch == expected_epoch,
                "range epoch mismatch: expected={}, actual={}",
                expected_epoch,
                range.descriptor.epoch
            );
        }
        anyhow::ensure!(
            range.descriptor.lifecycle == ServiceShardLifecycle::Serving,
            "range lifecycle is not serving: {:?}",
            range.descriptor.lifecycle
        );
        Ok(range)
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
                entries: range
                    .space
                    .reader()
                    .scan(range.space.boundary(), usize::MAX)
                    .await?,
            };
            let payload = bincode::serialize(&applied_snapshot)?;
            range
                .raft
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

    pub async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<(String, String)> {
        let lifecycle = self
            .lifecycle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("range lifecycle manager unavailable"))?;
        let source = self.validate_serving_fence(range_id, None).await?;
        anyhow::ensure!(
            source.descriptor.boundary.contains(&split_key),
            "split key outside range boundary"
        );
        anyhow::ensure!(
            source.descriptor.boundary.start_key.as_deref() != Some(split_key.as_slice()),
            "split key must advance lower boundary"
        );
        let left_id = format!("{range_id}-left");
        let right_id = format!("{range_id}-right");
        let source = self
            .rewrite_descriptor(range_id, |descriptor| {
                descriptor.epoch += 1;
                descriptor.lifecycle = ServiceShardLifecycle::Draining;
            })
            .await?;
        let child_epoch = source.descriptor.epoch + 1;
        let left = lifecycle
            .create_range(ReplicatedRangeDescriptor::new(
                left_id.clone(),
                source.descriptor.shard.clone(),
                RangeBoundary::new(
                    source.descriptor.boundary.start_key.clone(),
                    Some(split_key.clone()),
                ),
                child_epoch,
                source.descriptor.config_version,
                source.descriptor.leader_node_id,
                source.descriptor.replicas.clone(),
                source.descriptor.commit_index,
                source.descriptor.applied_index,
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await?;
        let right = lifecycle
            .create_range(ReplicatedRangeDescriptor::new(
                right_id.clone(),
                source.descriptor.shard.clone(),
                RangeBoundary::new(
                    Some(split_key.clone()),
                    source.descriptor.boundary.end_key.clone(),
                ),
                child_epoch,
                source.descriptor.config_version,
                source.descriptor.leader_node_id,
                source.descriptor.replicas.clone(),
                source.descriptor.commit_index,
                source.descriptor.applied_index,
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await?;
        let left_entries: Vec<(Bytes, Bytes)> = source
            .space
            .reader()
            .scan(&left.descriptor.boundary, usize::MAX)
            .await?;
        let right_entries: Vec<(Bytes, Bytes)> = source
            .space
            .reader()
            .scan(&right.descriptor.boundary, usize::MAX)
            .await?;
        if !left_entries.is_empty() {
            left.space
                .writer()
                .apply(
                    left_entries
                        .into_iter()
                        .map(|(key, value)| KvMutation {
                            key,
                            value: Some(value),
                        })
                        .collect(),
                )
                .await?;
        }
        if !right_entries.is_empty() {
            right.space
                .writer()
                .apply(
                    right_entries
                        .into_iter()
                        .map(|(key, value)| KvMutation {
                            key,
                            value: Some(value),
                        })
                        .collect(),
                )
                .await?;
        }
        let mut left = left;
        left.descriptor.lifecycle = ServiceShardLifecycle::Serving;
        let mut right = right;
        right.descriptor.lifecycle = ServiceShardLifecycle::Serving;
        self.replace_range(left).await?;
        self.replace_range(right).await?;
        let _ = self.host.remove_range(range_id).await?;
        lifecycle.retire_range(range_id).await?;
        Ok((left_id, right_id))
    }

    pub async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        let lifecycle = self
            .lifecycle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("range lifecycle manager unavailable"))?;
        let left = self.validate_serving_fence(left_range_id, None).await?;
        let right = self.validate_serving_fence(right_range_id, None).await?;
        anyhow::ensure!(
            left.descriptor.shard == right.descriptor.shard
                && left.descriptor.boundary.end_key == right.descriptor.boundary.start_key,
            "ranges are not merge-adjacent siblings"
        );
        let left = self
            .rewrite_descriptor(left_range_id, |descriptor| {
                descriptor.epoch += 1;
                descriptor.lifecycle = ServiceShardLifecycle::Draining;
            })
            .await?;
        let right = self
            .rewrite_descriptor(right_range_id, |descriptor| {
                descriptor.epoch += 1;
                descriptor.lifecycle = ServiceShardLifecycle::Draining;
            })
            .await?;
        let merged_id = format!("merge-{left_range_id}-{right_range_id}");
        let merged = lifecycle
            .create_range(ReplicatedRangeDescriptor::new(
                merged_id.clone(),
                left.descriptor.shard.clone(),
                RangeBoundary::new(
                    left.descriptor.boundary.start_key.clone(),
                    right.descriptor.boundary.end_key.clone(),
                ),
                left.descriptor.epoch.max(right.descriptor.epoch) + 1,
                left.descriptor.config_version.max(right.descriptor.config_version),
                left.descriptor.leader_node_id.or(right.descriptor.leader_node_id),
                left.descriptor.replicas.clone(),
                left.descriptor.commit_index.max(right.descriptor.commit_index),
                left.descriptor.applied_index.max(right.descriptor.applied_index),
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await?;
        let mut merged_mutations: Vec<KvMutation> = Vec::new();
        merged_mutations.extend(
            left.space
                .reader()
                .scan(left.space.boundary(), usize::MAX)
                .await?
                .into_iter()
                .map(|(key, value)| KvMutation {
                    key,
                    value: Some(value),
                }),
        );
        merged_mutations.extend(
            right
                .space
                .reader()
                .scan(right.space.boundary(), usize::MAX)
                .await?
                .into_iter()
                .map(|(key, value)| KvMutation {
                    key,
                    value: Some(value),
                }),
        );
        let mut merged = merged;
        if !merged_mutations.is_empty() {
            merged.space.writer().apply(merged_mutations).await?;
        }
        merged.descriptor.lifecycle = ServiceShardLifecycle::Serving;
        self.replace_range(merged).await?;
        let _ = self.host.remove_range(left_range_id).await?;
        let _ = self.host.remove_range(right_range_id).await?;
        lifecycle.retire_range(left_range_id).await?;
        lifecycle.retire_range(right_range_id).await?;
        Ok(merged_id)
    }

    pub async fn bootstrap_range(
        &self,
        mut descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        let lifecycle = self
            .lifecycle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("range lifecycle manager unavailable"))?;
        anyhow::ensure!(
            self.host.open_range(&descriptor.id).await?.is_none(),
            "range `{}` already exists",
            descriptor.id
        );
        descriptor.lifecycle = ServiceShardLifecycle::Serving;
        let range = lifecycle.create_range(descriptor.clone()).await?;
        self.replace_range(range).await?;
        Ok(descriptor.id)
    }

    pub async fn drain_range(&self, range_id: &str) -> anyhow::Result<()> {
        let _ = self
            .rewrite_descriptor(range_id, |descriptor| {
                descriptor.lifecycle = ServiceShardLifecycle::Draining;
                descriptor.epoch += 1;
            })
            .await?;
        Ok(())
    }

    pub async fn retire_range(&self, range_id: &str) -> anyhow::Result<bool> {
        let lifecycle = self
            .lifecycle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("range lifecycle manager unavailable"))?;
        let removed = self.host.remove_range(range_id).await?;
        if removed.is_none() {
            return Ok(false);
        }
        lifecycle.retire_range(range_id).await?;
        Ok(true)
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
        let status = range.raft.status().await?;
        let current_voters = status.cluster_config.voters;
        let current_learners = status.cluster_config.learners;

        let mut desired_voters = voters;
        desired_voters.sort_unstable();
        desired_voters.dedup();
        let mut desired_learners = learners;
        desired_learners.sort_unstable();
        desired_learners.dedup();
        let desired_target = PendingReconfiguration {
            voters: desired_voters.clone(),
            learners: desired_learners.clone(),
        };
        let pending_target = self
            .pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .get(range_id)
            .cloned();
        if let Some(pending) = pending_target.as_ref() {
            anyhow::ensure!(
                pending == &desired_target,
                "config change already in progress for range `{range_id}`"
            );
        }

        let mut staged_learners = current_learners.clone();
        for node_id in desired_voters.iter().chain(desired_learners.iter()) {
            if !current_voters.contains(node_id) && !staged_learners.contains(node_id) {
                staged_learners.push(*node_id);
            }
        }
        if staged_learners != current_learners {
            self.pending_reconfig
                .lock()
                .expect("replica runtime poisoned")
                .insert(range_id.to_string(), desired_target);
            return range
                .raft
                .change_cluster_config(RaftConfigChange::ReplaceCluster {
                    voters: current_voters,
                    learners: staged_learners,
                })
                .await;
        }

        if let Some(local_leader) = status.leader_node_id {
            if current_voters.contains(&local_leader)
                && !desired_voters.contains(&local_leader)
                && local_leader == range.raft.local_node_id()
            {
                anyhow::bail!(
                    "refusing to remove current local leader from voters before leadership transfer"
                );
            }
        }

        range
            .raft
            .change_cluster_config(RaftConfigChange::ReplaceCluster {
                voters: desired_voters,
                learners: desired_learners,
            })
            .await?;
        self.pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .remove(range_id);
        Ok(())
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
        let _ = self.health_snapshot().await?;
        Ok((sent, applied, snapshotted, compacted))
    }

    pub async fn health_snapshot(&self) -> anyhow::Result<Vec<RangeHealthSnapshot>> {
        let statuses = self.host.list_ranges().await?;
        Ok(statuses
            .into_iter()
            .map(|status| {
                let lag = status
                    .raft
                    .replica_progress
                    .iter()
                    .map(|progress| ReplicaLagSnapshot {
                        node_id: progress.node_id,
                        lag: status
                            .raft
                            .last_log_index
                            .saturating_sub(progress.match_index),
                        match_index: progress.match_index,
                        next_index: progress.next_index,
                    })
                    .collect();
                let health = RangeHealthSnapshot {
                    range_id: status.descriptor.id,
                    role: status.raft.role,
                    current_term: status.raft.current_term,
                    leader_node_id: status.raft.leader_node_id,
                    commit_index: status.raft.commit_index,
                    applied_index: status.raft.applied_index,
                    latest_snapshot_index: status.raft.latest_snapshot_index,
                    replica_lag: lag,
                };
                let role_value = match health.role {
                    greenmqtt_kv_raft::RaftNodeRole::Follower => 0.0,
                    greenmqtt_kv_raft::RaftNodeRole::Candidate => 1.0,
                    greenmqtt_kv_raft::RaftNodeRole::Leader => 2.0,
                };
                gauge!("kv_raft_current_term", "range_id" => health.range_id.clone())
                    .set(health.current_term as f64);
                gauge!("kv_raft_role", "range_id" => health.range_id.clone()).set(role_value);
                gauge!("kv_raft_leader_id", "range_id" => health.range_id.clone())
                    .set(health.leader_node_id.unwrap_or_default() as f64);
                gauge!("kv_raft_commit_index", "range_id" => health.range_id.clone())
                    .set(health.commit_index as f64);
                gauge!("kv_raft_applied_index", "range_id" => health.range_id.clone())
                    .set(health.applied_index as f64);
                for replica in &health.replica_lag {
                    gauge!(
                        "kv_raft_replication_lag",
                        "range_id" => health.range_id.clone(),
                        "replica_id" => replica.node_id.to_string()
                    )
                    .set(replica.lag as f64);
                }
                health
            })
            .collect())
    }

    pub async fn debug_dump(&self) -> anyhow::Result<String> {
        let health = self.health_snapshot().await?;
        let mut lines = Vec::new();
        for range in health {
            lines.push(format!(
                "range={} role={:?} term={} leader={:?} commit={} applied={} snapshot={:?}",
                range.range_id,
                range.role,
                range.current_term,
                range.leader_node_id,
                range.commit_index,
                range.applied_index,
                range.latest_snapshot_index
            ));
            for replica in range.replica_lag {
                lines.push(format!(
                    "  replica={} lag={} match={} next={}",
                    replica.node_id, replica.lag, replica.match_index, replica.next_index
                ));
            }
        }
        Ok(lines.join("\n"))
    }

    pub async fn list_zombie_ranges(&self) -> anyhow::Result<Vec<ZombieRangeSnapshot>> {
        let statuses = self.host.list_ranges().await?;
        Ok(statuses
            .into_iter()
            .filter(|status| {
                status.descriptor.lifecycle != ServiceShardLifecycle::Serving
                    || status.raft.leader_node_id.is_none()
            })
            .map(|status| ZombieRangeSnapshot {
                range_id: status.descriptor.id,
                lifecycle: status.descriptor.lifecycle,
                leader_node_id: status.raft.leader_node_id,
            })
            .collect())
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
        AppliedRangeSnapshot, HostedRange, KvRangeHost, MemoryKvRangeHost, RangeLifecycleManager,
        ReplicaRuntime, ReplicaTransport,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_balance::{execute_balance_commands, BalanceCommand};
    use greenmqtt_kv_engine::{KvEngine, MemoryKvEngine};
    use greenmqtt_kv_raft::{MemoryRaftNode, RaftMessage, RaftNode};
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
        runtime
            .change_replicas("range-stage-finalize", vec![1, 3], Vec::new())
            .await
            .unwrap();
        let status = raft.status().await.unwrap();
        assert_eq!(status.cluster_config.voters, vec![1, 2]);
        assert_eq!(status.cluster_config.learners, vec![3]);
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
                vec![RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating)],
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
            space.writer()
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
}
