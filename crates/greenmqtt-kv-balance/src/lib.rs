use async_trait::async_trait;
use greenmqtt_cluster::ClusterWorkflowController;
use greenmqtt_core::{
    BalancerState, ClusterNodeLifecycle, ControlPlaneRegistry, NodeId, RangeBoundary, RangeReplica,
    ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, ServiceEndpoint, ServiceKind,
    ServiceShardAssignment, ServiceShardKey, ServiceShardLifecycle, ServiceShardTransition,
    ServiceShardTransitionKind,
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BalanceCommand {
    BootstrapRange {
        range_id: String,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    },
    MigrateShard {
        shard: ServiceShardKey,
        from_node_id: NodeId,
        to_node_id: NodeId,
    },
    FailoverShard {
        shard: ServiceShardKey,
        from_node_id: NodeId,
        to_node_id: NodeId,
    },
    RecordBalancerState {
        name: String,
    },
    ChangeReplicas {
        range_id: String,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    },
    TransferLeadership {
        range_id: String,
        from_node_id: NodeId,
        to_node_id: NodeId,
    },
    CleanupReplicas {
        range_id: String,
        removed_node_ids: Vec<NodeId>,
    },
    RecoverRange {
        range_id: String,
        new_leader_node_id: NodeId,
    },
    SplitRange {
        source_range_id: String,
        left_range_id: String,
        right_range_id: String,
    },
    MergeRanges {
        left_range_id: String,
        right_range_id: String,
        merged_range_id: String,
    },
}

#[async_trait]
pub trait BalanceCommandExecutor: Send + Sync {
    async fn execute(&self, command: &BalanceCommand) -> anyhow::Result<bool>;
}

pub async fn execute_balance_commands(
    executor: &dyn BalanceCommandExecutor,
    commands: &[BalanceCommand],
) -> anyhow::Result<usize> {
    let mut applied = 0usize;
    for command in commands {
        if executor.execute(command).await? {
            applied += 1;
        }
    }
    Ok(applied)
}

#[derive(Clone)]
pub struct BalanceCoordinator {
    controller: Arc<dyn BalanceController>,
    executor: Arc<dyn BalanceCommandExecutor>,
}

impl BalanceCoordinator {
    pub fn new(
        controller: Arc<dyn BalanceController>,
        executor: Arc<dyn BalanceCommandExecutor>,
    ) -> Self {
        Self {
            controller,
            executor,
        }
    }

    pub async fn bootstrap_missing_ranges(
        &self,
        desired_ranges: Vec<ReplicatedRangeDescriptor>,
    ) -> anyhow::Result<usize> {
        let commands = self
            .controller
            .bootstrap_missing_ranges(desired_ranges)
            .await?;
        execute_balance_commands(self.executor.as_ref(), &commands).await
    }

    pub async fn reconcile_replica_counts(
        &self,
        voters_per_range: usize,
        learners_per_range: usize,
    ) -> anyhow::Result<usize> {
        let commands = self
            .controller
            .reconcile_replica_counts(voters_per_range, learners_per_range)
            .await?;
        execute_balance_commands(self.executor.as_ref(), &commands).await
    }

    pub async fn rebalance_range_leaders(&self) -> anyhow::Result<usize> {
        let commands = self.controller.rebalance_range_leaders().await?;
        execute_balance_commands(self.executor.as_ref(), &commands).await
    }

    pub async fn cleanup_unreachable_replicas(&self) -> anyhow::Result<usize> {
        let commands = self.controller.cleanup_unreachable_replicas().await?;
        execute_balance_commands(self.executor.as_ref(), &commands).await
    }

    pub async fn recover_ranges(&self) -> anyhow::Result<usize> {
        let commands = self.controller.recover_ranges().await?;
        execute_balance_commands(self.executor.as_ref(), &commands).await
    }
}

#[async_trait]
pub trait BalanceController: Send + Sync {
    async fn bootstrap_missing_ranges(
        &self,
        desired_ranges: Vec<ReplicatedRangeDescriptor>,
    ) -> anyhow::Result<Vec<BalanceCommand>>;

    async fn failover_offline_assignments(&self) -> anyhow::Result<Vec<BalanceCommand>>;

    async fn record_balancer_state(
        &self,
        name: &str,
        state: BalancerState,
    ) -> anyhow::Result<BalanceCommand>;

    async fn reconcile_replica_counts(
        &self,
        voters_per_range: usize,
        learners_per_range: usize,
    ) -> anyhow::Result<Vec<BalanceCommand>>;

    async fn rebalance_range_leaders(&self) -> anyhow::Result<Vec<BalanceCommand>>;

    async fn cleanup_unreachable_replicas(&self) -> anyhow::Result<Vec<BalanceCommand>>;

    async fn recover_ranges(&self) -> anyhow::Result<Vec<BalanceCommand>>;

    async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<Option<BalanceCommand>>;

    async fn merge_adjacent_ranges(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<BalanceCommand>>;
}

#[derive(Clone)]
pub struct MetadataBalanceController {
    registry: Arc<dyn ControlPlaneRegistry>,
    default_voters_per_range: usize,
    default_learners_per_range: usize,
}

impl MetadataBalanceController {
    pub fn new(registry: Arc<dyn ControlPlaneRegistry>) -> Self {
        Self {
            registry,
            default_voters_per_range: 3,
            default_learners_per_range: 0,
        }
    }

    async fn select_serving_member(
        &self,
        service_kind: ServiceKind,
        excluded_node_id: Option<NodeId>,
    ) -> anyhow::Result<Option<(NodeId, ServiceEndpoint)>> {
        let mut candidates = self.registry.list_members().await?;
        candidates.sort_by_key(|member| member.node_id);
        Ok(candidates
            .into_iter()
            .filter(|member| member.lifecycle == ClusterNodeLifecycle::Serving)
            .filter(|member| excluded_node_id != Some(member.node_id))
            .find_map(|member| {
                member
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.kind == service_kind)
                    .cloned()
                    .map(|endpoint| (member.node_id, endpoint))
            }))
    }

    async fn serving_member_ids(&self) -> anyhow::Result<Vec<NodeId>> {
        let mut members = self.registry.list_members().await?;
        members.sort_by_key(|member| member.node_id);
        Ok(members
            .into_iter()
            .filter(|member| member.lifecycle == ClusterNodeLifecycle::Serving)
            .map(|member| member.node_id)
            .collect())
    }

    fn sort_key(boundary: &RangeBoundary) -> Vec<u8> {
        boundary.start_key.clone().unwrap_or_default()
    }

    pub async fn migrate_leaving_assignments(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Vec<BalanceCommand>> {
        let assignments = self.registry.list_assignments(None).await?;
        let mut commands = Vec::new();
        for assignment in assignments
            .into_iter()
            .filter(|assignment| assignment.owner_node_id() == node_id)
        {
            let Some((target_node_id, endpoint)) = self
                .select_serving_member(assignment.shard.service_kind(), Some(node_id))
                .await?
            else {
                continue;
            };
            self.registry
                .apply_transition(ServiceShardTransition::new(
                    ServiceShardTransitionKind::Migration,
                    assignment.shard.clone(),
                    Some(node_id),
                    ServiceShardAssignment::new(
                        assignment.shard.clone(),
                        ServiceEndpoint::new(
                            assignment.shard.service_kind(),
                            target_node_id,
                            endpoint.endpoint,
                        ),
                        assignment.epoch + 1,
                        assignment.fencing_token + 1,
                        ServiceShardLifecycle::Draining,
                    ),
                ))
                .await?;
            commands.push(BalanceCommand::MigrateShard {
                shard: assignment.shard,
                from_node_id: node_id,
                to_node_id: target_node_id,
            });
        }
        Ok(commands)
    }

    pub async fn failover_assignments_from(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Vec<BalanceCommand>> {
        let assignments = self.registry.list_assignments(None).await?;
        let mut commands = Vec::new();
        for assignment in assignments
            .into_iter()
            .filter(|assignment| assignment.owner_node_id() == node_id)
        {
            let Some((target_node_id, endpoint)) = self
                .select_serving_member(assignment.shard.service_kind(), Some(node_id))
                .await?
            else {
                continue;
            };
            self.registry
                .apply_transition(ServiceShardTransition::new(
                    ServiceShardTransitionKind::Failover,
                    assignment.shard.clone(),
                    Some(node_id),
                    ServiceShardAssignment::new(
                        assignment.shard.clone(),
                        ServiceEndpoint::new(
                            assignment.shard.service_kind(),
                            target_node_id,
                            endpoint.endpoint,
                        ),
                        assignment.epoch + 1,
                        assignment.fencing_token + 1,
                        ServiceShardLifecycle::Recovering,
                    ),
                ))
                .await?;
            commands.push(BalanceCommand::FailoverShard {
                shard: assignment.shard,
                from_node_id: node_id,
                to_node_id: target_node_id,
            });
        }
        Ok(commands)
    }
}

#[async_trait]
impl BalanceController for MetadataBalanceController {
    async fn bootstrap_missing_ranges(
        &self,
        desired_ranges: Vec<ReplicatedRangeDescriptor>,
    ) -> anyhow::Result<Vec<BalanceCommand>> {
        let mut commands = Vec::new();
        for mut desired in desired_ranges {
            if self.registry.resolve_range(&desired.id).await?.is_some() {
                continue;
            }
            let (target_node_id, endpoint) = self
                .select_serving_member(desired.shard.service_kind(), None)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("no serving member available for {:?}", desired.shard)
                })?;
            if desired.leader_node_id.is_none() {
                desired.leader_node_id = Some(target_node_id);
            }
            if desired.replicas.is_empty() {
                desired.replicas.push(RangeReplica::new(
                    target_node_id,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                ));
            }
            self.registry.upsert_range(desired.clone()).await?;
            if self
                .registry
                .resolve_assignment(&desired.shard)
                .await?
                .is_none()
            {
                self.registry
                    .apply_transition(ServiceShardTransition::new(
                        ServiceShardTransitionKind::Bootstrap,
                        desired.shard.clone(),
                        None,
                        ServiceShardAssignment::new(
                            desired.shard.clone(),
                            ServiceEndpoint::new(
                                desired.shard.service_kind(),
                                target_node_id,
                                endpoint.endpoint,
                            ),
                            desired.epoch.max(1),
                            desired.config_version.max(1),
                            ServiceShardLifecycle::Bootstrapping,
                        ),
                    ))
                    .await?;
            }
            commands.push(BalanceCommand::BootstrapRange {
                range_id: desired.id,
                shard: desired.shard,
                target_node_id,
            });
        }
        Ok(commands)
    }

    async fn failover_offline_assignments(&self) -> anyhow::Result<Vec<BalanceCommand>> {
        let assignments = self.registry.list_assignments(None).await?;
        let mut commands = Vec::new();
        for assignment in assignments {
            let Some(owner) = self
                .registry
                .resolve_member(assignment.owner_node_id())
                .await?
            else {
                continue;
            };
            if owner.lifecycle == ClusterNodeLifecycle::Serving {
                continue;
            }
            let Some((target_node_id, endpoint)) = self
                .select_serving_member(assignment.shard.service_kind(), Some(owner.node_id))
                .await?
            else {
                continue;
            };
            self.registry
                .apply_transition(ServiceShardTransition::new(
                    ServiceShardTransitionKind::Failover,
                    assignment.shard.clone(),
                    Some(owner.node_id),
                    ServiceShardAssignment::new(
                        assignment.shard.clone(),
                        ServiceEndpoint::new(
                            assignment.shard.service_kind(),
                            target_node_id,
                            endpoint.endpoint,
                        ),
                        assignment.epoch + 1,
                        assignment.fencing_token + 1,
                        ServiceShardLifecycle::Recovering,
                    ),
                ))
                .await?;
            commands.push(BalanceCommand::FailoverShard {
                shard: assignment.shard,
                from_node_id: owner.node_id,
                to_node_id: target_node_id,
            });
        }
        Ok(commands)
    }

    async fn record_balancer_state(
        &self,
        name: &str,
        state: BalancerState,
    ) -> anyhow::Result<BalanceCommand> {
        self.registry.upsert_balancer_state(name, state).await?;
        Ok(BalanceCommand::RecordBalancerState {
            name: name.to_string(),
        })
    }

    async fn reconcile_replica_counts(
        &self,
        voters_per_range: usize,
        learners_per_range: usize,
    ) -> anyhow::Result<Vec<BalanceCommand>> {
        let serving_members = self.serving_member_ids().await?;
        let mut commands = Vec::new();
        for mut range in self.registry.list_ranges(None).await? {
            let mut voters: Vec<_> = range
                .replicas
                .iter()
                .filter(|replica| replica.role == ReplicaRole::Voter)
                .map(|replica| replica.node_id)
                .collect();
            let mut learners: Vec<_> = range
                .replicas
                .iter()
                .filter(|replica| replica.role == ReplicaRole::Learner)
                .map(|replica| replica.node_id)
                .collect();

            for node_id in &serving_members {
                if voters.len() >= voters_per_range {
                    break;
                }
                if !voters.contains(node_id) && !learners.contains(node_id) {
                    voters.push(*node_id);
                }
            }
            while voters.len() > voters_per_range {
                voters.pop();
            }
            for node_id in &serving_members {
                if learners.len() >= learners_per_range {
                    break;
                }
                if !voters.contains(node_id) && !learners.contains(node_id) {
                    learners.push(*node_id);
                }
            }
            while learners.len() > learners_per_range {
                learners.pop();
            }

            let new_replicas = voters
                .iter()
                .copied()
                .map(|node_id| {
                    RangeReplica::new(node_id, ReplicaRole::Voter, ReplicaSyncState::Replicating)
                })
                .chain(learners.iter().copied().map(|node_id| {
                    RangeReplica::new(node_id, ReplicaRole::Learner, ReplicaSyncState::Replicating)
                }))
                .collect::<Vec<_>>();
            if new_replicas != range.replicas {
                range.replicas = new_replicas;
                self.registry.upsert_range(range.clone()).await?;
                commands.push(BalanceCommand::ChangeReplicas {
                    range_id: range.id,
                    voters,
                    learners,
                });
            }
        }
        Ok(commands)
    }

    async fn rebalance_range_leaders(&self) -> anyhow::Result<Vec<BalanceCommand>> {
        let serving_members = self.serving_member_ids().await?;
        if serving_members.len() < 2 {
            return Ok(Vec::new());
        }
        let mut ranges = self.registry.list_ranges(None).await?;
        let mut leader_counts = serving_members
            .iter()
            .copied()
            .map(|node_id| (node_id, 0usize))
            .collect::<std::collections::BTreeMap<_, _>>();
        for range in &ranges {
            if let Some(leader_node_id) = range.leader_node_id {
                if let Some(count) = leader_counts.get_mut(&leader_node_id) {
                    *count += 1;
                }
            }
        }
        let (&max_node, &max_count) = leader_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .expect("leader counts not empty");
        let (&min_node, &min_count) = leader_counts
            .iter()
            .min_by_key(|(_, count)| *count)
            .expect("leader counts not empty");
        if max_count <= min_count + 1 {
            return Ok(Vec::new());
        }
        let mut commands = Vec::new();
        if let Some(range) = ranges.iter_mut().find(|range| {
            range.leader_node_id == Some(max_node)
                && range.replicas.iter().any(|replica| {
                    replica.node_id == min_node && replica.role == ReplicaRole::Voter
                })
        }) {
            range.leader_node_id = Some(min_node);
            self.registry.upsert_range(range.clone()).await?;
            commands.push(BalanceCommand::TransferLeadership {
                range_id: range.id.clone(),
                from_node_id: max_node,
                to_node_id: min_node,
            });
        }
        Ok(commands)
    }

    async fn cleanup_unreachable_replicas(&self) -> anyhow::Result<Vec<BalanceCommand>> {
        let members = self
            .registry
            .list_members()
            .await?
            .into_iter()
            .map(|member| (member.node_id, member.lifecycle))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut commands = Vec::new();
        for mut range in self.registry.list_ranges(None).await? {
            let removed_node_ids = range
                .replicas
                .iter()
                .filter(|replica| {
                    members
                        .get(&replica.node_id)
                        .is_none_or(|lifecycle| *lifecycle != ClusterNodeLifecycle::Serving)
                })
                .map(|replica| replica.node_id)
                .collect::<Vec<_>>();
            if removed_node_ids.is_empty() {
                continue;
            }
            range
                .replicas
                .retain(|replica| !removed_node_ids.contains(&replica.node_id));
            if range
                .leader_node_id
                .is_some_and(|leader| removed_node_ids.contains(&leader))
            {
                range.leader_node_id = range
                    .replicas
                    .iter()
                    .find(|replica| replica.role == ReplicaRole::Voter)
                    .map(|replica| replica.node_id);
            }
            self.registry.upsert_range(range.clone()).await?;
            commands.push(BalanceCommand::CleanupReplicas {
                range_id: range.id,
                removed_node_ids,
            });
        }
        Ok(commands)
    }

    async fn recover_ranges(&self) -> anyhow::Result<Vec<BalanceCommand>> {
        let mut commands = Vec::new();
        for mut range in self.registry.list_ranges(None).await? {
            if range.lifecycle != ServiceShardLifecycle::Recovering
                || range.leader_node_id.is_some()
            {
                continue;
            }
            if let Some((target_node_id, _)) = self
                .select_serving_member(range.shard.service_kind(), None)
                .await?
            {
                range.leader_node_id = Some(target_node_id);
                self.registry.upsert_range(range.clone()).await?;
                commands.push(BalanceCommand::RecoverRange {
                    range_id: range.id,
                    new_leader_node_id: target_node_id,
                });
            }
        }
        Ok(commands)
    }

    async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<Option<BalanceCommand>> {
        let Some(range) = self.registry.resolve_range(range_id).await? else {
            return Ok(None);
        };
        anyhow::ensure!(range.contains_key(&split_key), "split key outside range");
        anyhow::ensure!(
            range.boundary.start_key.as_deref() != Some(split_key.as_slice()),
            "split key must advance lower boundary"
        );
        let left = ReplicatedRangeDescriptor::new(
            format!("{range_id}-left"),
            range.shard.clone(),
            RangeBoundary::new(range.boundary.start_key.clone(), Some(split_key.clone())),
            range.epoch + 1,
            range.config_version,
            range.leader_node_id,
            range.replicas.clone(),
            range.commit_index,
            range.applied_index,
            range.lifecycle.clone(),
        );
        let right = ReplicatedRangeDescriptor::new(
            format!("{range_id}-right"),
            range.shard.clone(),
            RangeBoundary::new(Some(split_key), range.boundary.end_key.clone()),
            range.epoch + 1,
            range.config_version,
            range.leader_node_id,
            range.replicas,
            range.commit_index,
            range.applied_index,
            range.lifecycle,
        );
        let _ = self.registry.remove_range(range_id).await?;
        let _ = self.registry.upsert_range(left.clone()).await?;
        let _ = self.registry.upsert_range(right.clone()).await?;
        Ok(Some(BalanceCommand::SplitRange {
            source_range_id: range_id.to_string(),
            left_range_id: left.id,
            right_range_id: right.id,
        }))
    }

    async fn merge_adjacent_ranges(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<BalanceCommand>> {
        let mut ranges = self.registry.list_shard_ranges(shard).await?;
        ranges.sort_by_key(|range| Self::sort_key(&range.boundary));
        for pair in ranges.windows(2) {
            let left = &pair[0];
            let right = &pair[1];
            if left.boundary.end_key != right.boundary.start_key {
                continue;
            }
            let merged = ReplicatedRangeDescriptor::new(
                format!("merge-{}-{}", left.id, right.id),
                shard.clone(),
                RangeBoundary::new(
                    left.boundary.start_key.clone(),
                    right.boundary.end_key.clone(),
                ),
                left.epoch.max(right.epoch) + 1,
                left.config_version.max(right.config_version),
                left.leader_node_id.or(right.leader_node_id),
                left.replicas.clone(),
                left.commit_index.max(right.commit_index),
                left.applied_index.max(right.applied_index),
                left.lifecycle.clone(),
            );
            let _ = self.registry.remove_range(&left.id).await?;
            let _ = self.registry.remove_range(&right.id).await?;
            let _ = self.registry.upsert_range(merged.clone()).await?;
            return Ok(Some(BalanceCommand::MergeRanges {
                left_range_id: left.id.clone(),
                right_range_id: right.id.clone(),
                merged_range_id: merged.id,
            }));
        }
        Ok(None)
    }
}

#[async_trait]
impl ClusterWorkflowController for MetadataBalanceController {
    async fn on_member_observed(
        &self,
        member: &greenmqtt_core::ClusterNodeMembership,
    ) -> anyhow::Result<()> {
        if member.lifecycle == ClusterNodeLifecycle::Serving {
            let _ = self
                .reconcile_replica_counts(
                    self.default_voters_per_range,
                    self.default_learners_per_range,
                )
                .await?;
            let _ = self.rebalance_range_leaders().await?;
        }
        Ok(())
    }

    async fn on_member_removed(&self, node_id: NodeId) -> anyhow::Result<()> {
        let _ = self.failover_assignments_from(node_id).await?;
        let _ = self.cleanup_unreachable_replicas().await?;
        let _ = self.recover_ranges().await?;
        Ok(())
    }

    async fn on_lifecycle_changed(
        &self,
        node_id: NodeId,
        lifecycle: ClusterNodeLifecycle,
    ) -> anyhow::Result<()> {
        match lifecycle {
            ClusterNodeLifecycle::Serving => {
                let _ = self
                    .reconcile_replica_counts(
                        self.default_voters_per_range,
                        self.default_learners_per_range,
                    )
                    .await?;
                let _ = self.rebalance_range_leaders().await?;
            }
            ClusterNodeLifecycle::Leaving => {
                let _ = self.migrate_leaving_assignments(node_id).await?;
            }
            ClusterNodeLifecycle::Offline | ClusterNodeLifecycle::Suspect => {
                let _ = self.failover_assignments_from(node_id).await?;
                let _ = self.cleanup_unreachable_replicas().await?;
                let _ = self.recover_ranges().await?;
            }
            ClusterNodeLifecycle::Joining => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        execute_balance_commands, BalanceCommand, BalanceCommandExecutor, BalanceController,
        BalanceCoordinator, MetadataBalanceController,
    };
    use async_trait::async_trait;
    use greenmqtt_cluster::ClusterWorkflowController;
    use greenmqtt_core::{
        BalancerState, BalancerStateRegistry, ClusterMembershipRegistry, ClusterNodeLifecycle,
        ClusterNodeMembership, RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState,
        ReplicatedRangeDescriptor, ReplicatedRangeRegistry, ServiceEndpoint,
        ServiceEndpointRegistry, ServiceKind, ServiceShardAssignment, ServiceShardKey,
        ServiceShardLifecycle,
    };
    use greenmqtt_rpc::StaticServiceEndpointRegistry;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn controller_bootstraps_missing_range_and_assignment() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    7,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();

        let controller = MetadataBalanceController::new(registry.clone());
        let commands = controller
            .bootstrap_missing_ranges(vec![ReplicatedRangeDescriptor::new(
                "retain-range-1",
                ServiceShardKey::retain("t1"),
                RangeBoundary::full(),
                1,
                1,
                None,
                Vec::new(),
                0,
                0,
                ServiceShardLifecycle::Bootstrapping,
            )])
            .await
            .unwrap();

        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            BalanceCommand::BootstrapRange {
                range_id,
                target_node_id: 7,
                ..
            } if range_id == "retain-range-1"
        ));
        assert!(registry
            .resolve_range("retain-range-1")
            .await
            .unwrap()
            .is_some());
        assert!(registry
            .resolve_assignment(&ServiceShardKey::retain("t1"))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn controller_fails_over_assignment_from_offline_member() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Offline,
                vec![ServiceEndpoint::new(
                    ServiceKind::Dist,
                    7,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Dist,
                    9,
                    "http://127.0.0.1:50090",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_assignment(ServiceShardAssignment::new(
                ServiceShardKey::dist("t1"),
                ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
                1,
                10,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let controller = MetadataBalanceController::new(registry.clone());
        let commands = controller.failover_offline_assignments().await.unwrap();
        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            BalanceCommand::FailoverShard {
                from_node_id: 7,
                to_node_id: 9,
                ..
            }
        ));
        assert_eq!(
            registry
                .resolve_assignment(&ServiceShardKey::dist("t1"))
                .await
                .unwrap()
                .unwrap()
                .owner_node_id(),
            9
        );
    }

    #[tokio::test]
    async fn controller_records_balancer_state_through_registry() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let controller = MetadataBalanceController::new(registry.clone());
        let command = controller
            .record_balancer_state(
                "leader-balance",
                BalancerState {
                    disabled: false,
                    load_rules: BTreeMap::from([("max_skew".into(), "1".into())]),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            command,
            BalanceCommand::RecordBalancerState { ref name } if name == "leader-balance"
        ));
        assert_eq!(
            registry
                .resolve_balancer_state("leader-balance")
                .await
                .unwrap()
                .unwrap()
                .load_rules["max_skew"],
            "1"
        );
    }

    #[tokio::test]
    async fn controller_reconciles_replica_counts() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        for node_id in [7, 9, 11] {
            registry
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(
                        ServiceKind::Retain,
                        node_id,
                        format!("http://127.0.0.1:50{node_id}"),
                    )],
                ))
                .await
                .unwrap();
        }
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "retain-range-1",
                ServiceShardKey::retain("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![RangeReplica::new(
                    7,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let controller = MetadataBalanceController::new(registry.clone());
        let commands = controller.reconcile_replica_counts(2, 1).await.unwrap();
        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            BalanceCommand::ChangeReplicas { voters, learners, .. }
                if voters.len() == 2 && learners.len() == 1
        ));
    }

    #[tokio::test]
    async fn controller_rebalances_leaders_between_serving_nodes() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        for node_id in [7, 9] {
            registry
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(
                        ServiceKind::Dist,
                        node_id,
                        format!("http://127.0.0.1:50{node_id}"),
                    )],
                ))
                .await
                .unwrap();
        }
        for range_id in ["dist-range-1", "dist-range-2"] {
            registry
                .upsert_range(ReplicatedRangeDescriptor::new(
                    range_id,
                    ServiceShardKey::dist("t1"),
                    RangeBoundary::full(),
                    1,
                    1,
                    Some(7),
                    vec![
                        RangeReplica::new(7, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                        RangeReplica::new(9, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                    ],
                    0,
                    0,
                    ServiceShardLifecycle::Serving,
                ))
                .await
                .unwrap();
        }
        let controller = MetadataBalanceController::new(registry.clone());
        let commands = controller.rebalance_range_leaders().await.unwrap();
        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            BalanceCommand::TransferLeadership {
                from_node_id: 7,
                to_node_id: 9,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn controller_cleans_unreachable_replicas_and_recovers_leader() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Offline,
                vec![ServiceEndpoint::new(
                    ServiceKind::Inbox,
                    7,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Inbox,
                    9,
                    "http://127.0.0.1:50090",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "inbox-range-1",
                ServiceShardKey::inbox("t1", "s1"),
                RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![
                    RangeReplica::new(7, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                    RangeReplica::new(9, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                ],
                0,
                0,
                ServiceShardLifecycle::Recovering,
            ))
            .await
            .unwrap();
        let controller = MetadataBalanceController::new(registry.clone());
        let cleanup = controller.cleanup_unreachable_replicas().await.unwrap();
        assert_eq!(cleanup.len(), 1);
        assert_eq!(
            registry
                .resolve_range("inbox-range-1")
                .await
                .unwrap()
                .unwrap()
                .leader_node_id,
            Some(9)
        );
    }

    #[tokio::test]
    async fn controller_recovers_range_without_leader() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Inbox,
                    9,
                    "http://127.0.0.1:50090",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "inbox-range-2",
                ServiceShardKey::inbox("t1", "s2"),
                RangeBoundary::full(),
                1,
                1,
                None,
                vec![RangeReplica::new(
                    9,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Recovering,
            ))
            .await
            .unwrap();
        let controller = MetadataBalanceController::new(registry.clone());
        let recovery = controller.recover_ranges().await.unwrap();
        assert_eq!(recovery.len(), 1);
        assert!(matches!(
            &recovery[0],
            BalanceCommand::RecoverRange {
                range_id,
                new_leader_node_id: 9
            } if range_id == "inbox-range-2"
        ));
    }

    #[tokio::test]
    async fn workflow_observe_serving_member_triggers_replica_and_leader_rebalance() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        for node_id in [7, 9] {
            registry
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(
                        ServiceKind::Retain,
                        node_id,
                        format!("http://127.0.0.1:50{node_id}"),
                    )],
                ))
                .await
                .unwrap();
        }
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "retain-range-1",
                ServiceShardKey::retain("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![RangeReplica::new(
                    7,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let controller = MetadataBalanceController::new(registry.clone());
        controller
            .on_member_observed(
                &registry
                    .resolve_member(9)
                    .await
                    .unwrap()
                    .expect("member exists"),
            )
            .await
            .unwrap();
        let range = registry
            .resolve_range("retain-range-1")
            .await
            .unwrap()
            .unwrap();
        assert!(range.replicas.len() >= 2);
    }

    #[tokio::test]
    async fn workflow_lifecycle_change_migrates_and_fails_over_assignments() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        for node_id in [7, 9, 11] {
            registry
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(
                        ServiceKind::Dist,
                        node_id,
                        format!("http://127.0.0.1:50{node_id}"),
                    )],
                ))
                .await
                .unwrap();
        }
        registry
            .upsert_assignment(ServiceShardAssignment::new(
                ServiceShardKey::dist("t1"),
                ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
                1,
                10,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let controller = MetadataBalanceController::new(registry.clone());
        let _ = registry
            .set_member_lifecycle(7, ClusterNodeLifecycle::Leaving)
            .await
            .unwrap();
        controller
            .on_lifecycle_changed(7, ClusterNodeLifecycle::Leaving)
            .await
            .unwrap();
        assert_eq!(
            registry
                .resolve_assignment(&ServiceShardKey::dist("t1"))
                .await
                .unwrap()
                .unwrap()
                .owner_node_id(),
            9
        );

        registry
            .upsert_assignment(ServiceShardAssignment::new(
                ServiceShardKey::dist("t2"),
                ServiceEndpoint::new(ServiceKind::Dist, 9, "http://127.0.0.1:50090"),
                1,
                10,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let _ = registry
            .set_member_lifecycle(9, ClusterNodeLifecycle::Offline)
            .await
            .unwrap();
        controller
            .on_lifecycle_changed(9, ClusterNodeLifecycle::Offline)
            .await
            .unwrap();
        assert_eq!(
            registry
                .resolve_assignment(&ServiceShardKey::dist("t2"))
                .await
                .unwrap()
                .unwrap()
                .owner_node_id(),
            11
        );
    }

    #[tokio::test]
    async fn controller_splits_range_into_left_and_right_descriptors() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "retain-range-1",
                ServiceShardKey::retain("t1"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
                1,
                1,
                Some(7),
                vec![RangeReplica::new(
                    7,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let controller = MetadataBalanceController::new(registry.clone());
        let command = controller
            .split_range("retain-range-1", b"m".to_vec())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            command,
            BalanceCommand::SplitRange {
                source_range_id,
                ..
            } if source_range_id == "retain-range-1"
        ));
        assert_eq!(
            registry
                .list_shard_ranges(&ServiceShardKey::retain("t1"))
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn controller_merges_adjacent_ranges_for_same_shard() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let shard = ServiceShardKey::retain("t1");
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "retain-range-left",
                shard.clone(),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"m".to_vec())),
                1,
                1,
                Some(7),
                vec![RangeReplica::new(
                    7,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "retain-range-right",
                shard.clone(),
                RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec())),
                1,
                1,
                Some(7),
                vec![RangeReplica::new(
                    7,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let controller = MetadataBalanceController::new(registry.clone());
        let command = controller
            .merge_adjacent_ranges(&shard)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            command,
            BalanceCommand::MergeRanges {
                left_range_id,
                right_range_id,
                ..
            } if left_range_id == "retain-range-left" && right_range_id == "retain-range-right"
        ));
        assert_eq!(registry.list_shard_ranges(&shard).await.unwrap().len(), 1);
    }

    #[derive(Default)]
    struct RecordingExecutor {
        commands: Mutex<Vec<BalanceCommand>>,
    }

    #[async_trait]
    impl BalanceCommandExecutor for RecordingExecutor {
        async fn execute(&self, command: &BalanceCommand) -> anyhow::Result<bool> {
            self.commands
                .lock()
                .expect("executor poisoned")
                .push(command.clone());
            Ok(true)
        }
    }

    #[tokio::test]
    async fn execute_balance_commands_applies_all_supported_commands() {
        let executor = RecordingExecutor::default();
        let commands = vec![
            BalanceCommand::ChangeReplicas {
                range_id: "r1".into(),
                voters: vec![1, 2, 3],
                learners: vec![4],
            },
            BalanceCommand::TransferLeadership {
                range_id: "r1".into(),
                from_node_id: 1,
                to_node_id: 2,
            },
        ];
        let applied = execute_balance_commands(&executor, &commands)
            .await
            .unwrap();
        assert_eq!(applied, 2);
        assert_eq!(
            executor.commands.lock().expect("executor poisoned").len(),
            2
        );
    }

    #[tokio::test]
    async fn balance_coordinator_executes_controller_output_against_executor() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    7,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();
        let controller: Arc<dyn BalanceController> =
            Arc::new(MetadataBalanceController::new(registry.clone()));
        let executor = Arc::new(RecordingExecutor::default());
        let coordinator = BalanceCoordinator::new(controller, executor.clone());

        let applied = coordinator
            .bootstrap_missing_ranges(vec![ReplicatedRangeDescriptor::new(
                "retain-range-coord",
                ServiceShardKey::retain("t1"),
                RangeBoundary::full(),
                1,
                1,
                None,
                Vec::new(),
                0,
                0,
                ServiceShardLifecycle::Bootstrapping,
            )])
            .await
            .unwrap();
        assert_eq!(applied, 1);
        assert_eq!(
            executor.commands.lock().expect("executor poisoned").len(),
            1
        );
    }
}
