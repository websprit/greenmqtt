use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    NodeId, RangeBoundary, RangeReconfigurationState, ReconfigurationPhase,
    ReplicatedRangeDescriptor, ServiceShardLifecycle,
};
use greenmqtt_kv_balance::{BalanceCommand, BalanceCommandExecutor};
use greenmqtt_kv_engine::KvMutation;
use greenmqtt_kv_raft::{RaftConfigChange, RaftLogEntry, RaftSnapshot, RaftStatusSnapshot};
use metrics::gauge;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::*;

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

    fn desired_replicas_caught_up(
        status: &RaftStatusSnapshot,
        voters: &[NodeId],
        learners: &[NodeId],
    ) -> bool {
        voters
            .iter()
            .chain(learners.iter())
            .all(|node_id| {
                status
                    .replica_progress
                    .iter()
                    .find(|replica| replica.node_id == *node_id)
                    .map(|replica| replica.match_index >= status.commit_index)
                    .unwrap_or(false)
            })
    }

    fn config_caught_up(status: &RaftStatusSnapshot, voters: &[NodeId], learners: &[NodeId]) -> bool {
        voters
            .iter()
            .chain(learners.iter())
            .all(|node_id| {
                status
                    .replica_progress
                    .iter()
                    .find(|replica| replica.node_id == *node_id)
                    .map(|replica| replica.match_index >= status.commit_index)
                    .unwrap_or(false)
            })
    }

    fn voter_quorum_caught_up_to_index(
        status: &RaftStatusSnapshot,
        voters: &[NodeId],
        required_index: u64,
    ) -> bool {
        if voters.is_empty() {
            return false;
        }
        let mut matches = voters
            .iter()
            .map(|node_id| {
                status
                    .replica_progress
                    .iter()
                    .find(|replica| replica.node_id == *node_id)
                    .map(|replica| replica.match_index)
                    .unwrap_or(0)
            })
            .collect::<Vec<_>>();
        matches.sort_unstable_by(|left, right| right.cmp(left));
        matches[voters.len() / 2] >= required_index
    }

    fn pending_from_status(status: &RaftStatusSnapshot) -> Option<PendingReconfiguration> {
        let transition = status.config_transition.as_ref()?;
        Some(PendingReconfiguration {
            old_voters: transition.old_config.voters.clone(),
            old_learners: transition.old_config.learners.clone(),
            joint_voters: transition.joint_config.voters.clone(),
            joint_learners: transition.joint_config.learners.clone(),
            target_voters: transition.final_config.voters.clone(),
            target_learners: transition.final_config.learners.clone(),
            current_voters: status.cluster_config.voters.clone(),
            current_learners: status.cluster_config.learners.clone(),
            phase: match transition.phase {
                greenmqtt_kv_raft::RaftConfigTransitionPhase::JointConsensus => {
                    ReconfigurationPhase::JointConsensus
                }
                greenmqtt_kv_raft::RaftConfigTransitionPhase::Finalizing => {
                    ReconfigurationPhase::Finalizing
                }
            },
            blocked_on_catch_up: !Self::config_caught_up(
                status,
                &transition.joint_config.voters,
                &transition.joint_config.learners,
            ) || !Self::voter_quorum_caught_up_to_index(
                status,
                &transition.old_config.voters,
                status.last_log_index,
            ) || !Self::voter_quorum_caught_up_to_index(
                status,
                &transition.final_config.voters,
                status.last_log_index,
            ),
        })
    }

    pub async fn pending_reconfiguration(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<PendingReconfiguration>> {
        if let Some(pending) = self
            .pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .get(range_id)
            .cloned()
        {
            return Ok(Some(pending));
        }
        let Some(status) = self.host.range(range_id).await? else {
            return Ok(None);
        };
        Ok(Self::pending_from_status(&status.raft))
    }

    pub async fn list_pending_reconfigurations(
        &self,
    ) -> anyhow::Result<BTreeMap<String, PendingReconfiguration>> {
        let mut pending = self
            .pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .clone();
        for status in self.host.list_ranges().await? {
            if let Some(derived) = Self::pending_from_status(&status.raft) {
                pending.entry(status.descriptor.id.clone()).or_insert(derived);
            }
        }
        Ok(pending)
    }

    pub async fn reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        let Some(status) = self.host.range(range_id).await? else {
            return Ok(None);
        };
        let pending = self
            .pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .get(range_id)
            .cloned()
            .or_else(|| Self::pending_from_status(&status.raft));
        Ok(Some(RangeReconfigurationState {
            range_id: range_id.to_string(),
            old_voters: pending
                .as_ref()
                .map(|pending| pending.old_voters.clone())
                .unwrap_or_else(|| status.raft.cluster_config.voters.clone()),
            old_learners: pending
                .as_ref()
                .map(|pending| pending.old_learners.clone())
                .unwrap_or_else(|| status.raft.cluster_config.learners.clone()),
            current_voters: status.raft.cluster_config.voters.clone(),
            current_learners: status.raft.cluster_config.learners.clone(),
            joint_voters: pending
                .as_ref()
                .map(|pending| pending.joint_voters.clone())
                .unwrap_or_else(|| status.raft.cluster_config.voters.clone()),
            joint_learners: pending
                .as_ref()
                .map(|pending| pending.joint_learners.clone())
                .unwrap_or_else(|| status.raft.cluster_config.learners.clone()),
            pending_voters: pending
                .as_ref()
                .map(|pending| pending.target_voters.clone())
                .unwrap_or_default(),
            pending_learners: pending
                .as_ref()
                .map(|pending| pending.target_learners.clone())
                .unwrap_or_default(),
            phase: pending.as_ref().map(|pending| pending.phase.clone()),
            blocked_on_catch_up: pending
                .as_ref()
                .map(|pending| pending.blocked_on_catch_up)
                .unwrap_or(false),
        }))
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
        let left_id = format!("{range_id}-left");
        let right_id = format!("{range_id}-right");
        if self.host.open_range(range_id).await?.is_none()
            && self.host.open_range(&left_id).await?.is_some()
            && self.host.open_range(&right_id).await?.is_some()
        {
            return Ok((left_id, right_id));
        }
        let source = self.validate_serving_fence(range_id, None).await?;
        anyhow::ensure!(
            source.descriptor.boundary.contains(&split_key),
            "split key outside range boundary"
        );
        anyhow::ensure!(
            source.descriptor.boundary.start_key.as_deref() != Some(split_key.as_slice()),
            "split key must advance lower boundary"
        );
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
        let merged_id = format!("merge-{left_range_id}-{right_range_id}");
        if self.host.open_range(left_range_id).await?.is_none()
            && self.host.open_range(right_range_id).await?.is_none()
            && self.host.open_range(&merged_id).await?.is_some()
        {
            return Ok(merged_id);
        }
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
        if let Some(existing) = self.host.open_range(&descriptor.id).await? {
            anyhow::ensure!(
                existing.descriptor.shard == descriptor.shard
                    && existing.descriptor.boundary == descriptor.boundary,
                "range `{}` already exists with different descriptor",
                descriptor.id
            );
            return Ok(descriptor.id);
        }
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
        if let Some(range) = self.host.open_range(range_id).await? {
            if range.descriptor.lifecycle == ServiceShardLifecycle::Draining {
                return Ok(());
            }
        }
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
            return Ok(true);
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
        let current_voters = status.cluster_config.voters.clone();
        let current_learners = status.cluster_config.learners.clone();

        let mut desired_voters = voters;
        desired_voters.sort_unstable();
        desired_voters.dedup();
        let mut desired_learners = learners;
        desired_learners.sort_unstable();
        desired_learners.dedup();
        let desired_target = (desired_voters.clone(), desired_learners.clone());
        let pending_target = self
            .pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .get(range_id)
            .cloned();
        if let Some(pending) = pending_target.as_ref() {
            anyhow::ensure!(
                (pending.target_voters.clone(), pending.target_learners.clone()) == desired_target,
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
                .insert(
                    range_id.to_string(),
                    PendingReconfiguration {
                        old_voters: current_voters.clone(),
                        old_learners: current_learners.clone(),
                        joint_voters: current_voters.clone(),
                        joint_learners: staged_learners.clone(),
                        target_voters: desired_voters,
                        target_learners: desired_learners,
                        current_voters: current_voters.clone(),
                        current_learners: staged_learners.clone(),
                        phase: ReconfigurationPhase::StagingLearners,
                        blocked_on_catch_up: true,
                    },
                );
            return range
                .raft
                .change_cluster_config(RaftConfigChange::ReplaceCluster {
                    voters: current_voters,
                    learners: staged_learners,
                })
                .await;
        }

        if current_voters == desired_voters
            && current_learners == desired_learners
            && status.config_transition.is_none()
        {
            self.pending_reconfig
                .lock()
                .expect("replica runtime poisoned")
                .remove(range_id);
            return Ok(());
        }

        let status_transition = status.config_transition.clone();
        if let Some(transition) = status_transition {
            let blocked_on_catch_up = !Self::voter_quorum_caught_up_to_index(
                &status,
                &transition.old_config.voters,
                status.last_log_index,
            ) || !Self::voter_quorum_caught_up_to_index(
                &status,
                &transition.final_config.voters,
                status.last_log_index,
            );
            self.pending_reconfig
                .lock()
                .expect("replica runtime poisoned")
                .insert(
                    range_id.to_string(),
                    PendingReconfiguration {
                        old_voters: transition.old_config.voters.clone(),
                        old_learners: transition.old_config.learners.clone(),
                        joint_voters: transition.joint_config.voters.clone(),
                        joint_learners: transition.joint_config.learners.clone(),
                        target_voters: transition.final_config.voters.clone(),
                        target_learners: transition.final_config.learners.clone(),
                        current_voters: current_voters.clone(),
                        current_learners: current_learners.clone(),
                        phase: ReconfigurationPhase::JointConsensus,
                        blocked_on_catch_up,
                    },
                );
            if blocked_on_catch_up {
                anyhow::bail!("config change blocked on dual-majority catch-up for range `{range_id}`");
            }
            range
                .raft
                .change_cluster_config(RaftConfigChange::FinalizeJointConsensus)
                .await?;
            self.pending_reconfig
                .lock()
                .expect("replica runtime poisoned")
                .remove(range_id);
            return Ok(());
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

        let joint_voters = {
            let mut voters = current_voters.clone();
            voters.extend(desired_voters.iter().copied());
            voters.sort_unstable();
            voters.dedup();
            voters
        };
        let joint_learners = {
            let mut learners = current_learners.clone();
            learners.extend(desired_learners.iter().copied());
            learners.retain(|node_id| !joint_voters.contains(node_id));
            learners.sort_unstable();
            learners.dedup();
            learners
        };
        let blocked_on_catch_up =
            !Self::desired_replicas_caught_up(&status, &joint_voters, &joint_learners);
        self.pending_reconfig
            .lock()
            .expect("replica runtime poisoned")
            .insert(
                range_id.to_string(),
                PendingReconfiguration {
                    old_voters: current_voters.clone(),
                    old_learners: current_learners.clone(),
                    joint_voters: joint_voters.clone(),
                    joint_learners: joint_learners.clone(),
                    target_voters: desired_voters.clone(),
                    target_learners: desired_learners.clone(),
                    current_voters: current_voters.clone(),
                    current_learners: current_learners.clone(),
                    phase: ReconfigurationPhase::JointConsensus,
                    blocked_on_catch_up,
                },
            );
        if blocked_on_catch_up {
            anyhow::bail!("config change blocked before entering joint config for range `{range_id}`");
        }
        range
            .raft
            .change_cluster_config(RaftConfigChange::EnterJointConsensus {
                voters: desired_voters,
                learners: desired_learners,
            })
            .await?;
        let joint_status = range.raft.status().await?;
        if joint_status.cluster_config.voters == desired_target.0
            && joint_status.cluster_config.learners == desired_target.1
            && joint_voters == desired_target.0
            && joint_learners == desired_target.1
        {
            range
                .raft
                .change_cluster_config(RaftConfigChange::FinalizeJointConsensus)
                .await?;
            self.pending_reconfig
                .lock()
                .expect("replica runtime poisoned")
                .remove(range_id);
        }
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
        let status = range.raft.status().await?;
        if status.leader_node_id == Some(target_node_id) {
            return Ok(());
        }
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
        let status = range.raft.status().await?;
        if status.leader_node_id == Some(new_leader_node_id) {
            return Ok(());
        }
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
        let mut health_entries = Vec::with_capacity(statuses.len());
        for status in statuses {
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
                range_id: status.descriptor.id.clone(),
                lifecycle: status.descriptor.lifecycle,
                role: status.raft.role,
                current_term: status.raft.current_term,
                leader_node_id: status.raft.leader_node_id,
                commit_index: status.raft.commit_index,
                applied_index: status.raft.applied_index,
                latest_snapshot_index: status.raft.latest_snapshot_index,
                replica_lag: lag,
                reconfiguration: self
                    .reconfiguration_state(&status.descriptor.id)
                    .await?
                    .expect("range state should exist for listed range"),
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
            health_entries.push(health);
        }
        Ok(health_entries)
    }

    pub async fn debug_dump(&self) -> anyhow::Result<String> {
        let health = self.health_snapshot().await?;
        let mut lines = Vec::new();
        for range in health {
                lines.push(format!(
                "range={} lifecycle={:?} role={:?} term={} leader={:?} commit={} applied={} snapshot={:?} current_voters={:?} current_learners={:?} pending_voters={:?} pending_learners={:?} phase={:?} blocked_on_catch_up={}",
                range.range_id,
                range.lifecycle,
                range.role,
                range.current_term,
                range.leader_node_id,
                range.commit_index,
                range.applied_index,
                range.latest_snapshot_index,
                range.reconfiguration.current_voters,
                range.reconfiguration.current_learners,
                range.reconfiguration.pending_voters,
                range.reconfiguration.pending_learners,
                range.reconfiguration.phase,
                range.reconfiguration.blocked_on_catch_up
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
