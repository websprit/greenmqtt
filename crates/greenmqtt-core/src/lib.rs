use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

pub type TenantId = String;
pub type ClientId = String;
pub type TopicFilter = String;
pub type TopicName = String;
pub type SessionId = String;
pub type NodeId = u64;
pub type RangeId = String;
pub type SharedPayload = Bytes;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantQuota {
    pub max_connections: u64,
    pub max_subscriptions: u64,
    pub max_msg_per_sec: u64,
    pub max_memory_bytes: u64,
}

#[derive(Debug, Default)]
pub struct TenantUsage {
    pub connections: AtomicU64,
    pub subscriptions: AtomicU64,
    pub msg_rate: AtomicU64,
    pub memory_bytes: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantUsageSnapshot {
    pub connections: u64,
    pub subscriptions: u64,
    pub msg_rate: u64,
    pub memory_bytes: u64,
}

impl TenantUsage {
    pub fn snapshot(&self) -> TenantUsageSnapshot {
        TenantUsageSnapshot {
            connections: self.connections.load(Ordering::Relaxed),
            subscriptions: self.subscriptions.load(Ordering::Relaxed),
            msg_rate: self.msg_rate.load(Ordering::Relaxed),
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ServiceShardKind {
    SessionDict,
    Inbox,
    Inflight,
    Dist,
    Retain,
}

impl ServiceShardKind {
    pub fn service_kind(&self) -> ServiceKind {
        match self {
            Self::SessionDict => ServiceKind::SessionDict,
            Self::Inbox | Self::Inflight => ServiceKind::Inbox,
            Self::Dist => ServiceKind::Dist,
            Self::Retain => ServiceKind::Retain,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceShardKey {
    pub kind: ServiceShardKind,
    pub tenant_id: TenantId,
    pub scope: String,
}

impl ServiceShardKey {
    pub fn sessiondict(tenant_id: impl Into<TenantId>, session_id: impl Into<String>) -> Self {
        Self {
            kind: ServiceShardKind::SessionDict,
            tenant_id: tenant_id.into(),
            scope: session_id.into(),
        }
    }

    pub fn inbox(tenant_id: impl Into<TenantId>, session_id: impl Into<String>) -> Self {
        Self {
            kind: ServiceShardKind::Inbox,
            tenant_id: tenant_id.into(),
            scope: session_id.into(),
        }
    }

    pub fn inflight(tenant_id: impl Into<TenantId>, session_id: impl Into<String>) -> Self {
        Self {
            kind: ServiceShardKind::Inflight,
            tenant_id: tenant_id.into(),
            scope: session_id.into(),
        }
    }

    pub fn dist(tenant_id: impl Into<TenantId>) -> Self {
        Self {
            kind: ServiceShardKind::Dist,
            tenant_id: tenant_id.into(),
            scope: "*".into(),
        }
    }

    pub fn retain(tenant_id: impl Into<TenantId>) -> Self {
        Self {
            kind: ServiceShardKind::Retain,
            tenant_id: tenant_id.into(),
            scope: "*".into(),
        }
    }

    pub fn service_kind(&self) -> ServiceKind {
        self.kind.service_kind()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceShardLifecycle {
    Bootstrapping,
    Serving,
    Draining,
    Recovering,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceEndpoint {
    pub kind: ServiceKind,
    pub node_id: NodeId,
    pub endpoint: String,
}

impl ServiceEndpoint {
    pub fn new(kind: ServiceKind, node_id: NodeId, endpoint: impl Into<String>) -> Self {
        Self {
            kind,
            node_id,
            endpoint: endpoint.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceShardAssignment {
    pub shard: ServiceShardKey,
    pub endpoint: ServiceEndpoint,
    pub epoch: u64,
    pub fencing_token: u64,
    pub lifecycle: ServiceShardLifecycle,
}

impl ServiceShardAssignment {
    pub fn new(
        shard: ServiceShardKey,
        endpoint: ServiceEndpoint,
        epoch: u64,
        fencing_token: u64,
        lifecycle: ServiceShardLifecycle,
    ) -> Self {
        Self {
            shard,
            endpoint,
            epoch,
            fencing_token,
            lifecycle,
        }
    }

    pub fn owner_node_id(&self) -> NodeId {
        self.endpoint.node_id
    }

    pub fn matches_owner_epoch_fence(&self, other: &ServiceShardAssignment) -> bool {
        self.shard == other.shard
            && self.owner_node_id() == other.owner_node_id()
            && self.epoch == other.epoch
            && self.fencing_token == other.fencing_token
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeBoundary {
    #[serde(default)]
    pub start_key: Option<Vec<u8>>,
    #[serde(default)]
    pub end_key: Option<Vec<u8>>,
}

impl RangeBoundary {
    pub fn full() -> Self {
        Self {
            start_key: None,
            end_key: None,
        }
    }

    pub fn new(start_key: Option<Vec<u8>>, end_key: Option<Vec<u8>>) -> Self {
        Self { start_key, end_key }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let lower_ok = self
            .start_key
            .as_deref()
            .is_none_or(|start_key| key >= start_key);
        let upper_ok = self.end_key.as_deref().is_none_or(|end_key| key < end_key);
        lower_ok && upper_ok
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReplicaRole {
    Voter,
    Learner,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReplicaSyncState {
    Probing,
    Snapshotting,
    Replicating,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeReplica {
    pub node_id: NodeId,
    pub role: ReplicaRole,
    pub sync_state: ReplicaSyncState,
}

impl RangeReplica {
    pub fn new(node_id: NodeId, role: ReplicaRole, sync_state: ReplicaSyncState) -> Self {
        Self {
            node_id,
            role,
            sync_state,
        }
    }

    pub fn is_query_ready(&self) -> bool {
        self.sync_state == ReplicaSyncState::Replicating
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicatedRangeDescriptor {
    pub id: RangeId,
    pub shard: ServiceShardKey,
    pub boundary: RangeBoundary,
    pub epoch: u64,
    pub config_version: u64,
    pub leader_node_id: Option<NodeId>,
    pub replicas: Vec<RangeReplica>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub lifecycle: ServiceShardLifecycle,
}

impl ReplicatedRangeDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<RangeId>,
        shard: ServiceShardKey,
        boundary: RangeBoundary,
        epoch: u64,
        config_version: u64,
        leader_node_id: Option<NodeId>,
        replicas: Vec<RangeReplica>,
        commit_index: u64,
        applied_index: u64,
        lifecycle: ServiceShardLifecycle,
    ) -> Self {
        Self {
            id: id.into(),
            shard,
            boundary,
            epoch,
            config_version,
            leader_node_id,
            replicas,
            commit_index,
            applied_index,
            lifecycle,
        }
    }

    pub fn leader_replica(&self) -> Option<&RangeReplica> {
        let leader_node_id = self.leader_node_id?;
        self.replicas
            .iter()
            .find(|replica| replica.node_id == leader_node_id)
    }

    pub fn voters(&self) -> Vec<&RangeReplica> {
        self.replicas
            .iter()
            .filter(|replica| replica.role == ReplicaRole::Voter)
            .collect()
    }

    pub fn learners(&self) -> Vec<&RangeReplica> {
        self.replicas
            .iter()
            .filter(|replica| replica.role == ReplicaRole::Learner)
            .collect()
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.boundary.contains(key)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BalancerState {
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub load_rules: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconfigurationPhase {
    StagingLearners,
    JointConsensus,
    Finalizing,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeReconfigurationState {
    pub range_id: RangeId,
    #[serde(default)]
    pub old_voters: Vec<NodeId>,
    #[serde(default)]
    pub old_learners: Vec<NodeId>,
    #[serde(default)]
    pub current_voters: Vec<NodeId>,
    #[serde(default)]
    pub current_learners: Vec<NodeId>,
    #[serde(default)]
    pub joint_voters: Vec<NodeId>,
    #[serde(default)]
    pub joint_learners: Vec<NodeId>,
    #[serde(default)]
    pub pending_voters: Vec<NodeId>,
    #[serde(default)]
    pub pending_learners: Vec<NodeId>,
    #[serde(default)]
    pub phase: Option<ReconfigurationPhase>,
    #[serde(default)]
    pub blocked_on_catch_up: bool,
}

#[async_trait]
pub trait ReplicatedRangeRegistry: Send + Sync {
    async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>>;

    async fn resolve_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>>;

    async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>>;

    async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>>;

    async fn list_shard_ranges(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        Ok(self
            .list_ranges(Some(shard.kind.clone()))
            .await?
            .into_iter()
            .filter(|descriptor| descriptor.shard == *shard)
            .collect())
    }

    async fn route_range_for_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .list_shard_ranges(shard)
            .await?
            .into_iter()
            .find(|descriptor| descriptor.contains_key(key)))
    }
}

#[async_trait]
pub trait BalancerStateRegistry: Send + Sync {
    async fn upsert_balancer_state(
        &self,
        balancer_name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>>;

    async fn resolve_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>>;

    async fn remove_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>>;

    async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>>;
}

#[async_trait]
pub trait RangeReconfigurationRegistry: Send + Sync {
    async fn upsert_reconfiguration_state(
        &self,
        state: RangeReconfigurationState,
    ) -> anyhow::Result<Option<RangeReconfigurationState>>;

    async fn resolve_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>>;

    async fn remove_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>>;

    async fn list_reconfiguration_states(
        &self,
    ) -> anyhow::Result<Vec<RangeReconfigurationState>>;
}

pub trait MetadataRegistry:
    ServiceEndpointRegistry
    + ReplicatedRangeRegistry
    + BalancerStateRegistry
    + RangeReconfigurationRegistry
    + ClusterMembershipRegistry
{
}

impl<T> MetadataRegistry for T where
    T: ServiceEndpointRegistry
        + ReplicatedRangeRegistry
        + BalancerStateRegistry
        + RangeReconfigurationRegistry
        + ClusterMembershipRegistry
{
}

pub trait ControlPlaneRegistry: ShardControlRegistry + MetadataRegistry {}

impl<T> ControlPlaneRegistry for T where T: ShardControlRegistry + MetadataRegistry {}

#[async_trait]
pub trait ServiceEndpointRegistry: Send + Sync {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>>;

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>>;

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>>;

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>>;

    async fn resolve_assignments(
        &self,
        shards: &[ServiceShardKey],
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments = Vec::new();
        for shard in shards {
            if let Some(assignment) = self.resolve_assignment(shard).await? {
                assignments.push(assignment);
            }
        }
        Ok(assignments)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterNodeLifecycle {
    Joining,
    Serving,
    Suspect,
    Leaving,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeMembership {
    pub node_id: NodeId,
    pub epoch: u64,
    pub lifecycle: ClusterNodeLifecycle,
    pub endpoints: Vec<ServiceEndpoint>,
}

impl ClusterNodeMembership {
    pub fn new(
        node_id: NodeId,
        epoch: u64,
        lifecycle: ClusterNodeLifecycle,
        endpoints: Vec<ServiceEndpoint>,
    ) -> Self {
        Self {
            node_id,
            epoch,
            lifecycle,
            endpoints,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct HealthScore(pub u8);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Suspect,
    ConfirmedDead,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterMember {
    pub membership: ClusterNodeMembership,
    pub health_score: HealthScore,
    pub status: NodeStatus,
}

#[async_trait]
pub trait ClusterMembershipRegistry: Send + Sync {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>>;

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>>;

    async fn remove_member(&self, node_id: NodeId)
        -> anyhow::Result<Option<ClusterNodeMembership>>;

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>>;

    async fn set_member_lifecycle(
        &self,
        node_id: NodeId,
        lifecycle: ClusterNodeLifecycle,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let Some(mut member) = self.resolve_member(node_id).await? else {
            return Ok(None);
        };
        member.lifecycle = lifecycle;
        self.upsert_member(member).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceShardTransitionKind {
    Rebalance,
    Failover,
    Repair,
    Migration,
    Bootstrap,
    CatchUp,
    AntiEntropy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceShardTransition {
    pub kind: ServiceShardTransitionKind,
    pub shard: ServiceShardKey,
    pub source_node_id: Option<NodeId>,
    pub target_assignment: ServiceShardAssignment,
}

impl ServiceShardTransition {
    pub fn new(
        kind: ServiceShardTransitionKind,
        shard: ServiceShardKey,
        source_node_id: Option<NodeId>,
        target_assignment: ServiceShardAssignment,
    ) -> Self {
        Self {
            kind,
            shard,
            source_node_id,
            target_assignment,
        }
    }
}

#[async_trait]
pub trait ServiceShardRecoveryControl: Send + Sync {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>>;
}

pub trait ShardControlRegistry:
    ServiceEndpointRegistry + ClusterMembershipRegistry + ServiceShardRecoveryControl
{
}

impl<T> ShardControlRegistry for T where
    T: ServiceEndpointRegistry + ClusterMembershipRegistry + ServiceShardRecoveryControl
{
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionKind {
    Transient,
    Persistent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientIdentity {
    pub tenant_id: TenantId,
    pub user_id: String,
    pub client_id: ClientId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRecord {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub kind: SessionKind,
    pub identity: ClientIdentity,
    #[serde(default)]
    pub session_expiry_interval_secs: Option<u32>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Subscription {
    pub session_id: SessionId,
    pub tenant_id: TenantId,
    pub topic_filter: TopicFilter,
    pub qos: u8,
    pub subscription_identifier: Option<u32>,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
    pub shared_group: Option<String>,
    pub kind: SessionKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteRecord {
    pub tenant_id: TenantId,
    pub topic_filter: TopicFilter,
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub subscription_identifier: Option<u32>,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub shared_group: Option<String>,
    pub kind: SessionKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetainedMessage {
    pub tenant_id: TenantId,
    pub topic: TopicName,
    pub payload: SharedPayload,
    pub qos: u8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserProperty {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub content_type: Option<String>,
    pub message_expiry_interval_secs: Option<u32>,
    pub stored_at_ms: Option<u64>,
    pub response_topic: Option<TopicName>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscription_identifiers: Vec<u32>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OfflineMessage {
    pub tenant_id: TenantId,
    pub session_id: SessionId,
    pub topic: TopicName,
    pub payload: SharedPayload,
    pub qos: u8,
    pub retain: bool,
    pub from_session_id: SessionId,
    #[serde(default)]
    pub properties: PublishProperties,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InflightPhase {
    Publish,
    Release,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InflightMessage {
    pub tenant_id: TenantId,
    pub session_id: SessionId,
    pub packet_id: u16,
    pub topic: TopicName,
    pub payload: SharedPayload,
    pub qos: u8,
    pub retain: bool,
    pub from_session_id: SessionId,
    #[serde(default)]
    pub properties: PublishProperties,
    pub phase: InflightPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectRequest {
    pub identity: ClientIdentity,
    pub node_id: NodeId,
    pub kind: SessionKind,
    pub clean_start: bool,
    pub session_expiry_interval_secs: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishRequest {
    pub topic: TopicName,
    pub payload: SharedPayload,
    pub qos: u8,
    pub retain: bool,
    #[serde(default)]
    pub properties: PublishProperties,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Delivery {
    pub tenant_id: TenantId,
    pub session_id: SessionId,
    pub topic: TopicName,
    pub payload: SharedPayload,
    pub qos: u8,
    pub retain: bool,
    pub from_session_id: SessionId,
    #[serde(default)]
    pub properties: PublishProperties,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectReply {
    pub session: SessionRecord,
    pub session_present: bool,
    pub local_session_epoch: u64,
    pub replaced: Option<SessionRecord>,
    pub offline_messages: Vec<OfflineMessage>,
    pub inflight_messages: Vec<InflightMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishOutcome {
    pub matched_routes: usize,
    pub online_deliveries: usize,
    pub offline_enqueues: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceKind {
    Broker,
    SessionDict,
    Dist,
    Inbox,
    Retain,
    HttpApi,
}

#[async_trait]
pub trait Lifecycle: Send + Sync {
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicFilterLevel {
    Literal(String),
    SingleLevelWildcard,
    MultiLevelWildcard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTopicFilter {
    levels: Vec<TopicFilterLevel>,
}

impl CompiledTopicFilter {
    pub fn new(filter: &str) -> Self {
        let levels = filter
            .split('/')
            .map(|level| match level {
                "+" => TopicFilterLevel::SingleLevelWildcard,
                "#" => TopicFilterLevel::MultiLevelWildcard,
                literal => TopicFilterLevel::Literal(literal.to_string()),
            })
            .collect();
        Self { levels }
    }

    pub fn levels(&self) -> &[TopicFilterLevel] {
        &self.levels
    }

    pub fn matches(&self, topic: &str) -> bool {
        let mut topic_levels = topic.split('/');

        for level in &self.levels {
            match level {
                TopicFilterLevel::MultiLevelWildcard => {
                    return matches!(
                        self.levels.last(),
                        Some(TopicFilterLevel::MultiLevelWildcard)
                    );
                }
                TopicFilterLevel::SingleLevelWildcard => {
                    if topic_levels.next().is_none() {
                        return false;
                    }
                }
                TopicFilterLevel::Literal(filter_level) => match topic_levels.next() {
                    Some(topic_level) if topic_level == filter_level => {}
                    _ => return false,
                },
            }
        }

        topic_levels.next().is_none()
    }
}

pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');

    loop {
        match filter_levels.next() {
            Some("#") => return filter_levels.next().is_none(),
            Some("+") => {
                if topic_levels.next().is_none() {
                    return false;
                }
            }
            Some(level) => match topic_levels.next() {
                Some(topic_level) if topic_level == level => {}
                _ => return false,
            },
            None => return topic_levels.next().is_none(),
        }
    }
}

pub fn shared_subscription_key(group: Option<&str>, topic_filter: &str) -> String {
    match group {
        Some(group) => format!("{group}::{topic_filter}"),
        None => topic_filter.to_string(),
    }
}

pub fn dedupe_sessions(routes: &[RouteRecord]) -> BTreeSet<SessionId> {
    routes
        .iter()
        .map(|route| route.session_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        topic_matches, ClientIdentity, CompiledTopicFilter, Delivery, InflightMessage,
        InflightPhase, OfflineMessage, PublishProperties, PublishRequest, RangeBoundary,
        RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, RetainedMessage,
        ServiceEndpoint, ServiceKind, ServiceShardAssignment, ServiceShardLifecycle, SessionKind,
        SharedPayload,
    };
    use bytes::Bytes;
    use std::sync::atomic::Ordering;

    #[test]
    fn matches_single_level_wildcard() {
        assert!(topic_matches("tenant/+/state", "tenant/device1/state"));
        assert!(!topic_matches(
            "tenant/+/state",
            "tenant/device1/meta/state"
        ));
    }

    #[test]
    fn matches_multi_level_wildcard() {
        assert!(topic_matches("tenant/#", "tenant/device1/meta/state"));
        assert!(topic_matches("#", "tenant/device1"));
        assert!(!topic_matches("tenant/#/state", "tenant/device1/state"));
    }

    #[test]
    fn matches_trailing_and_empty_levels_without_allocation_side_effects() {
        assert!(topic_matches("/tenant/+", "/tenant/device1"));
        assert!(topic_matches("tenant/+", "tenant/"));
        assert!(topic_matches("tenant/#", "tenant/"));
        assert!(!topic_matches("tenant/+", "tenant"));
        assert!(!topic_matches("tenant/device", "tenant/device/extra"));
    }

    #[test]
    fn compiled_topic_filter_matches_same_cases_as_runtime_matcher() {
        let filter = CompiledTopicFilter::new("tenant/+/state");
        assert!(filter.matches("tenant/device1/state"));
        assert!(!filter.matches("tenant/device1/meta/state"));

        let hash_filter = CompiledTopicFilter::new("tenant/#");
        assert!(hash_filter.matches("tenant/device1/meta/state"));
        assert!(hash_filter.matches("tenant/"));
        assert!(!CompiledTopicFilter::new("tenant/#/state").matches("tenant/device1/state"));
    }

    #[test]
    fn shared_payload_clones_share_backing_storage_across_message_types() {
        let payload: SharedPayload = Bytes::from(vec![1_u8, 2, 3, 4]);

        let publish = PublishRequest {
            topic: "devices/a/state".into(),
            payload: payload.clone(),
            qos: 1,
            retain: false,
            properties: PublishProperties::default(),
        };
        let offline = OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: publish.topic.clone(),
            payload: publish.payload.clone(),
            qos: publish.qos,
            retain: publish.retain,
            from_session_id: "src".into(),
            properties: publish.properties.clone(),
        };
        let inflight = InflightMessage {
            tenant_id: offline.tenant_id.clone(),
            session_id: offline.session_id.clone(),
            packet_id: 7,
            topic: offline.topic.clone(),
            payload: offline.payload.clone(),
            qos: 2,
            retain: offline.retain,
            from_session_id: offline.from_session_id.clone(),
            properties: offline.properties.clone(),
            phase: InflightPhase::Publish,
        };
        let delivery = Delivery {
            tenant_id: inflight.tenant_id.clone(),
            session_id: inflight.session_id.clone(),
            topic: inflight.topic.clone(),
            payload: inflight.payload.clone(),
            qos: inflight.qos,
            retain: inflight.retain,
            from_session_id: inflight.from_session_id.clone(),
            properties: inflight.properties.clone(),
        };
        let retained = RetainedMessage {
            tenant_id: delivery.tenant_id.clone(),
            topic: delivery.topic.clone(),
            payload: delivery.payload.clone(),
            qos: delivery.qos,
        };

        let ptr = publish.payload.as_ptr();
        assert_eq!(offline.payload.as_ptr(), ptr);
        assert_eq!(inflight.payload.as_ptr(), ptr);
        assert_eq!(delivery.payload.as_ptr(), ptr);
        assert_eq!(retained.payload.as_ptr(), ptr);
        assert_eq!(retained.payload.len(), 4);

        let _ = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let _ = SessionKind::Persistent;
    }

    #[test]
    fn service_shard_keys_define_canonical_boundaries() {
        assert_eq!(
            crate::ServiceShardKey::sessiondict("t1", "s1"),
            crate::ServiceShardKey {
                kind: crate::ServiceShardKind::SessionDict,
                tenant_id: "t1".into(),
                scope: "s1".into(),
            }
        );
        assert_eq!(
            crate::ServiceShardKey::inbox("t1", "s1").scope,
            crate::ServiceShardKey::inflight("t1", "s1").scope
        );
        assert_eq!(crate::ServiceShardKey::dist("t1").scope, "*");
        assert_eq!(crate::ServiceShardKey::retain("t1").scope, "*");
        assert_ne!(
            crate::ServiceShardKey::sessiondict("t1", "s1"),
            crate::ServiceShardKey::inbox("t1", "s1")
        );
        assert_eq!(
            crate::ServiceShardKind::Inflight.service_kind(),
            crate::ServiceKind::Inbox
        );
        assert_eq!(
            crate::ServiceShardKey::retain("t1").service_kind(),
            crate::ServiceKind::Retain
        );
    }

    #[test]
    fn shard_assignments_capture_owner_epoch_fence_and_lifecycle() {
        let shard = crate::ServiceShardKey::dist("tenant-a");
        let endpoint = ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070");
        let assignment = ServiceShardAssignment::new(
            shard.clone(),
            endpoint.clone(),
            11,
            19,
            ServiceShardLifecycle::Serving,
        );
        assert_eq!(assignment.shard, shard);
        assert_eq!(assignment.endpoint, endpoint);
        assert_eq!(assignment.epoch, 11);
        assert_eq!(assignment.fencing_token, 19);
        assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Serving);
        assert_eq!(assignment.owner_node_id(), 7);
    }

    #[test]
    fn cluster_membership_captures_join_suspect_and_leave_states() {
        let endpoint = ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070");
        let member = crate::ClusterNodeMembership::new(
            7,
            3,
            crate::ClusterNodeLifecycle::Joining,
            vec![endpoint.clone()],
        );
        assert_eq!(member.node_id, 7);
        assert_eq!(member.epoch, 3);
        assert_eq!(member.lifecycle, crate::ClusterNodeLifecycle::Joining);
        assert_eq!(member.endpoints, vec![endpoint.clone()]);

        let suspect = crate::ClusterNodeMembership::new(
            7,
            4,
            crate::ClusterNodeLifecycle::Suspect,
            vec![endpoint.clone()],
        );
        assert_eq!(suspect.lifecycle, crate::ClusterNodeLifecycle::Suspect);

        let leaving = crate::ClusterNodeMembership::new(
            7,
            5,
            crate::ClusterNodeLifecycle::Leaving,
            vec![endpoint],
        );
        assert_eq!(leaving.lifecycle, crate::ClusterNodeLifecycle::Leaving);
    }

    #[test]
    fn shard_transitions_capture_rebalance_and_failover_moves() {
        let shard = crate::ServiceShardKey::dist("tenant-a");
        let target = ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 9, "http://127.0.0.1:50090"),
            12,
            21,
            ServiceShardLifecycle::Serving,
        );
        let rebalance = crate::ServiceShardTransition::new(
            crate::ServiceShardTransitionKind::Rebalance,
            shard.clone(),
            Some(7),
            target.clone(),
        );
        assert_eq!(rebalance.kind, crate::ServiceShardTransitionKind::Rebalance);
        assert_eq!(rebalance.source_node_id, Some(7));
        assert_eq!(rebalance.target_assignment.owner_node_id(), 9);

        let failover = crate::ServiceShardTransition::new(
            crate::ServiceShardTransitionKind::Failover,
            shard,
            Some(7),
            target,
        );
        assert_eq!(failover.kind, crate::ServiceShardTransitionKind::Failover);
        assert_eq!(failover.target_assignment.epoch, 12);
        assert_eq!(failover.target_assignment.fencing_token, 21);
    }

    #[test]
    fn range_boundary_contains_keys_with_half_open_semantics() {
        let boundary = RangeBoundary::new(Some(b"b".to_vec()), Some(b"m".to_vec()));
        assert!(!boundary.contains(b"a"));
        assert!(boundary.contains(b"b"));
        assert!(boundary.contains(b"cat"));
        assert!(!boundary.contains(b"m"));

        let full = RangeBoundary::full();
        assert!(full.contains(b""));
        assert!(full.contains(b"tenant/device"));
    }

    #[test]
    fn replicated_range_descriptor_exposes_leader_and_replica_roles() {
        let shard = crate::ServiceShardKey::retain("tenant-a");
        let descriptor = ReplicatedRangeDescriptor::new(
            "retain-range-1",
            shard,
            RangeBoundary::new(Some(b"tenant-a/".to_vec()), Some(b"tenant-b/".to_vec())),
            7,
            3,
            Some(2),
            vec![
                RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(2, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(3, ReplicaRole::Learner, ReplicaSyncState::Snapshotting),
            ],
            42,
            39,
            ServiceShardLifecycle::Serving,
        );

        assert_eq!(descriptor.id, "retain-range-1");
        assert_eq!(descriptor.epoch, 7);
        assert_eq!(descriptor.config_version, 3);
        assert_eq!(descriptor.commit_index, 42);
        assert_eq!(descriptor.applied_index, 39);
        assert_eq!(
            descriptor.leader_replica().map(|replica| replica.node_id),
            Some(2)
        );
        assert_eq!(descriptor.voters().len(), 2);
        assert_eq!(descriptor.learners().len(), 1);
        assert!(descriptor.contains_key(b"tenant-a/devices/a"));
        assert!(!descriptor.contains_key(b"tenant-b/devices/a"));
        assert!(descriptor.replicas[0].is_query_ready());
        assert!(!descriptor.replicas[2].is_query_ready());
    }

    #[test]
    fn tenant_usage_snapshot_reads_all_atomic_counters() {
        let usage = crate::TenantUsage::default();
        usage.connections.store(3, Ordering::Relaxed);
        usage.subscriptions.store(7, Ordering::Relaxed);
        usage.msg_rate.store(11, Ordering::Relaxed);
        usage.memory_bytes.store(13, Ordering::Relaxed);

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.connections, 3);
        assert_eq!(snapshot.subscriptions, 7);
        assert_eq!(snapshot.msg_rate, 11);
        assert_eq!(snapshot.memory_bytes, 13);
    }
}
