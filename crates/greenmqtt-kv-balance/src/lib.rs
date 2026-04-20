use async_trait::async_trait;
use greenmqtt_core::{
    BalancerState, ClusterNodeLifecycle, ControlPlaneRegistry, NodeId, RangeReplica, ReplicaRole,
    ReplicaSyncState, ReplicatedRangeDescriptor, ServiceEndpoint, ServiceKind,
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
    FailoverShard {
        shard: ServiceShardKey,
        from_node_id: NodeId,
        to_node_id: NodeId,
    },
    RecordBalancerState {
        name: String,
    },
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
}

#[derive(Clone)]
pub struct MetadataBalanceController {
    registry: Arc<dyn ControlPlaneRegistry>,
}

impl MetadataBalanceController {
    pub fn new(registry: Arc<dyn ControlPlaneRegistry>) -> Self {
        Self { registry }
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
                .ok_or_else(|| anyhow::anyhow!("no serving member available for {:?}", desired.shard))?;
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
            if self.registry.resolve_assignment(&desired.shard).await?.is_none() {
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
            let Some(owner) = self.registry.resolve_member(assignment.owner_node_id()).await? else {
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
}

#[cfg(test)]
mod tests {
    use super::{BalanceCommand, BalanceController, MetadataBalanceController};
    use greenmqtt_core::{
        BalancerState, BalancerStateRegistry, ClusterMembershipRegistry, ClusterNodeLifecycle,
        ClusterNodeMembership, RangeBoundary, ReplicatedRangeDescriptor,
        ReplicatedRangeRegistry, ServiceEndpoint, ServiceKind, ServiceEndpointRegistry,
        ServiceShardAssignment, ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_rpc::StaticServiceEndpointRegistry;
    use std::collections::BTreeMap;
    use std::sync::Arc;

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
        assert!(registry.resolve_range("retain-range-1").await.unwrap().is_some());
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
}
