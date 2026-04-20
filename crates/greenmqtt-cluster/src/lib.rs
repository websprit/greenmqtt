mod failure_detector;
mod membership;

use async_trait::async_trait;
pub use failure_detector::{FailureDetector, FailureDetectorConfig, FailureDetectorSnapshot};
use foca::Config as FocaConfig;
use greenmqtt_core::{
    ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership, Lifecycle, NodeId,
    ServiceEndpoint, ServiceKind,
};
pub use membership::MemberList;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub bind_addr: String,
    pub seeds: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterEngine {
    Foca,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub members: Vec<ClusterNodeMembership>,
}

#[async_trait]
pub trait ClusterControl: Send + Sync {
    async fn snapshot(&self) -> anyhow::Result<ClusterSnapshot>;
}

#[async_trait]
pub trait ClusterWorkflowController: Send + Sync {
    async fn on_member_observed(&self, member: &ClusterNodeMembership) -> anyhow::Result<()>;

    async fn on_member_removed(&self, node_id: NodeId) -> anyhow::Result<()>;

    async fn on_lifecycle_changed(
        &self,
        node_id: NodeId,
        lifecycle: ClusterNodeLifecycle,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdvertisedService {
    pub kind: ServiceKind,
    pub endpoint: String,
}

impl AdvertisedService {
    pub fn new(kind: ServiceKind, endpoint: impl Into<String>) -> Self {
        Self {
            kind,
            endpoint: endpoint.into(),
        }
    }

    pub fn into_endpoint(self, node_id: NodeId) -> ServiceEndpoint {
        ServiceEndpoint::new(self.kind, node_id, self.endpoint)
    }
}

pub struct ClusterCoordinator {
    local_node_id: NodeId,
    members: MemberList,
    detector: FailureDetector,
    registry: Arc<dyn ClusterMembershipRegistry>,
    workflow: Option<Arc<dyn ClusterWorkflowController>>,
}

impl ClusterCoordinator {
    pub fn new(
        local_node_id: NodeId,
        detector: FailureDetector,
        registry: Arc<dyn ClusterMembershipRegistry>,
    ) -> Self {
        Self::with_workflow(local_node_id, detector, registry, None)
    }

    pub fn with_workflow(
        local_node_id: NodeId,
        detector: FailureDetector,
        registry: Arc<dyn ClusterMembershipRegistry>,
        workflow: Option<Arc<dyn ClusterWorkflowController>>,
    ) -> Self {
        Self {
            local_node_id,
            members: MemberList::default(),
            detector,
            registry,
            workflow,
        }
    }

    pub async fn advertise_local_services(
        &mut self,
        epoch: u64,
        lifecycle: ClusterNodeLifecycle,
        services: Vec<AdvertisedService>,
    ) -> anyhow::Result<ClusterNodeMembership> {
        let membership = ClusterNodeMembership::new(
            self.local_node_id,
            epoch,
            lifecycle,
            services
                .into_iter()
                .map(|service| service.into_endpoint(self.local_node_id))
                .collect(),
        );
        self.members.upsert(membership.clone());
        let _ = self.registry.upsert_member(membership.clone()).await?;
        if let Some(workflow) = &self.workflow {
            workflow.on_member_observed(&membership).await?;
        }
        Ok(membership)
    }

    pub async fn sync_members(
        &mut self,
        members: Vec<ClusterNodeMembership>,
    ) -> anyhow::Result<usize> {
        let mut changed = 0usize;
        for member in members {
            if let Some(workflow) = &self.workflow {
                workflow.on_member_observed(&member).await?;
            }
            if self.members.upsert(member.clone()) != Some(member.clone()) {
                changed += 1;
            }
            let _ = self.registry.upsert_member(member).await?;
        }
        Ok(changed)
    }

    pub async fn apply_detector_input(
        &mut self,
        input: failure_detector::DetectorInput,
    ) -> anyhow::Result<Vec<failure_detector::DetectorOutput>> {
        let outputs = self.detector.step(input);
        for output in &outputs {
            match output {
                failure_detector::DetectorOutput::MembershipChanged(member)
                | failure_detector::DetectorOutput::BumpIncarnationAndRefute(member) => {
                    self.members.upsert(member.clone());
                    let _ = self.registry.upsert_member(member.clone()).await?;
                    if let Some(workflow) = &self.workflow {
                        workflow.on_member_observed(member).await?;
                    }
                }
                failure_detector::DetectorOutput::MembershipRemoved(node_id) => {
                    self.members.remove(*node_id);
                    let _ = self.registry.remove_member(*node_id).await?;
                    if let Some(workflow) = &self.workflow {
                        workflow.on_member_removed(*node_id).await?;
                    }
                }
                failure_detector::DetectorOutput::Send { .. }
                | failure_detector::DetectorOutput::ScheduleTimeout { .. } => {}
            }
        }
        Ok(outputs)
    }

    pub async fn mark_local_lifecycle(
        &mut self,
        lifecycle: ClusterNodeLifecycle,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let updated = self
            .members
            .mark_lifecycle(self.local_node_id, lifecycle.clone());
        if updated.is_some() {
            let _ = self
                .registry
                .set_member_lifecycle(self.local_node_id, lifecycle.clone())
                .await?;
            if let Some(workflow) = &self.workflow {
                workflow
                    .on_lifecycle_changed(self.local_node_id, lifecycle)
                    .await?;
            }
        }
        Ok(updated)
    }

    pub fn snapshot(&self) -> ClusterSnapshot {
        ClusterSnapshot {
            members: self.members.list(),
        }
    }
}

pub struct ClusterRuntime {
    pub config: ClusterConfig,
    pub engine: ClusterEngine,
}

impl ClusterRuntime {
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            engine: ClusterEngine::Foca,
        }
    }

    pub fn engine_config(&self) -> FocaConfig {
        let cluster_size = NonZeroU32::new((self.config.seeds.len() as u32).saturating_add(1))
            .unwrap_or_else(|| NonZeroU32::new(1).expect("non-zero"));
        FocaConfig::new_lan(cluster_size)
    }
}

#[async_trait]
impl Lifecycle for ClusterRuntime {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{AdvertisedService, ClusterCoordinator, ClusterRuntime, ClusterWorkflowController};
    use crate::failure_detector::{DetectorInput, FailureDetector, FailureDetectorConfig};
    use async_trait::async_trait;
    use greenmqtt_core::{ClusterMembershipRegistry, ClusterNodeLifecycle, ServiceKind};
    use greenmqtt_rpc::StaticServiceEndpointRegistry;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Default)]
    struct TestWorkflow {
        observed: AtomicUsize,
        removed: AtomicUsize,
        lifecycle_changes: AtomicUsize,
    }

    #[async_trait]
    impl ClusterWorkflowController for TestWorkflow {
        async fn on_member_observed(
            &self,
            _member: &greenmqtt_core::ClusterNodeMembership,
        ) -> anyhow::Result<()> {
            self.observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_member_removed(&self, _node_id: greenmqtt_core::NodeId) -> anyhow::Result<()> {
            self.removed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_lifecycle_changed(
            &self,
            _node_id: greenmqtt_core::NodeId,
            _lifecycle: ClusterNodeLifecycle,
        ) -> anyhow::Result<()> {
            self.lifecycle_changes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn coordinator_advertises_local_services_into_registry() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let mut coordinator = ClusterCoordinator::new(
            7,
            FailureDetector::new(FailureDetectorConfig::default()),
            registry.clone(),
        );
        let member = coordinator
            .advertise_local_services(
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    AdvertisedService::new(ServiceKind::Broker, "http://127.0.0.1:1883"),
                    AdvertisedService::new(ServiceKind::Dist, "http://127.0.0.1:50070"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(member.node_id, 7);
        assert_eq!(registry.list_members().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn coordinator_applies_failure_detector_membership_changes_to_registry() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let mut coordinator = ClusterCoordinator::new(
            7,
            FailureDetector::new(FailureDetectorConfig::default()),
            registry.clone(),
        );
        let outputs = coordinator
            .apply_detector_input(DetectorInput::ObserveMember(
                greenmqtt_core::ClusterNodeMembership::new(
                    9,
                    1,
                    ClusterNodeLifecycle::Joining,
                    vec![greenmqtt_core::ServiceEndpoint::new(
                        ServiceKind::Dist,
                        9,
                        "http://127.0.0.1:50090",
                    )],
                ),
            ))
            .await
            .unwrap();
        assert!(!outputs.is_empty());
        assert_eq!(
            registry.resolve_member(9).await.unwrap().unwrap().lifecycle,
            ClusterNodeLifecycle::Joining
        );
    }

    #[tokio::test]
    async fn coordinator_marks_local_lifecycle_and_updates_snapshot() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let mut coordinator = ClusterCoordinator::new(
            7,
            FailureDetector::new(FailureDetectorConfig::default()),
            registry.clone(),
        );
        coordinator
            .advertise_local_services(
                1,
                ClusterNodeLifecycle::Serving,
                vec![AdvertisedService::new(
                    ServiceKind::SessionDict,
                    "http://127.0.0.1:50071",
                )],
            )
            .await
            .unwrap();
        let updated = coordinator
            .mark_local_lifecycle(ClusterNodeLifecycle::Leaving)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.lifecycle, ClusterNodeLifecycle::Leaving);
        assert_eq!(
            coordinator.snapshot().members[0].lifecycle,
            ClusterNodeLifecycle::Leaving
        );
        assert_eq!(
            registry.resolve_member(7).await.unwrap().unwrap().lifecycle,
            ClusterNodeLifecycle::Leaving
        );
    }

    #[tokio::test]
    async fn coordinator_triggers_workflow_hooks_for_observe_remove_and_lifecycle_change() {
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let workflow = Arc::new(TestWorkflow::default());
        let mut coordinator = ClusterCoordinator::with_workflow(
            7,
            FailureDetector::new(FailureDetectorConfig::default()),
            registry,
            Some(workflow.clone()),
        );
        coordinator
            .advertise_local_services(
                1,
                ClusterNodeLifecycle::Serving,
                vec![AdvertisedService::new(
                    ServiceKind::Broker,
                    "http://127.0.0.1:1883",
                )],
            )
            .await
            .unwrap();
        coordinator
            .apply_detector_input(DetectorInput::ObserveMember(
                greenmqtt_core::ClusterNodeMembership::new(
                    9,
                    1,
                    ClusterNodeLifecycle::Joining,
                    vec![greenmqtt_core::ServiceEndpoint::new(
                        ServiceKind::Dist,
                        9,
                        "http://127.0.0.1:50090",
                    )],
                ),
            ))
            .await
            .unwrap();
        coordinator
            .mark_local_lifecycle(ClusterNodeLifecycle::Leaving)
            .await
            .unwrap();
        coordinator
            .apply_detector_input(DetectorInput::Timeout(
                crate::failure_detector::TimeoutEvent::Suspect {
                    target: 9,
                    incarnation: 1,
                },
            ))
            .await
            .unwrap();

        assert_eq!(workflow.observed.load(Ordering::SeqCst), 2);
        assert_eq!(workflow.lifecycle_changes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cluster_runtime_engine_config_uses_seed_count() {
        let runtime = ClusterRuntime::new(super::ClusterConfig {
            node_id: 1,
            bind_addr: "127.0.0.1:7000".into(),
            seeds: vec!["127.0.0.1:7001".into(), "127.0.0.1:7002".into()],
        });
        let config = runtime.engine_config();
        assert_eq!(config.num_indirect_probes.get(), 3);
    }
}
