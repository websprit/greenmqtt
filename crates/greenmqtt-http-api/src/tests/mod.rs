use super::{
    admin::TenantQuotaResponse, ErrorBody, HttpApi, PeerSummary, PurgeReply,
    SessionDictReassignReply, SessionInboxStateReply, SessionPublishRequest, SubscribeRequest,
};
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use greenmqtt_broker::{
    AdminAuditEntry, BrokerConfig, BrokerRuntime, BrokerStats, DefaultBroker, PeerRegistry,
    SessionSummary,
};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership,
    ConnectReply, ConnectRequest, Delivery, OfflineMessage, PublishProperties, PublishRequest,
    RetainedMessage, RouteRecord, ServiceEndpoint, ServiceEndpointRegistry, ServiceKind,
    ServiceShardAssignment, ServiceShardKey, ServiceShardKind, ServiceShardLifecycle,
    ServiceShardRecoveryControl, ServiceShardTransition, SessionKind, SessionRecord,
    Subscription, TenantQuota,
};
use greenmqtt_dist::{DistHandle, DistRouter};
use greenmqtt_inbox::{InboxHandle, InboxService, PersistentInboxHandle};
use greenmqtt_kv_engine::{KvEngine, KvRangeBootstrap, MemoryKvEngine};
use greenmqtt_kv_raft::{MemoryRaftNode, RaftNode};
use greenmqtt_kv_server::{HostedRange, MemoryKvRangeHost, RangeLifecycleManager, ReplicaRuntime};
use greenmqtt_plugin_api::{
    AllowAllAcl, AllowAllAuth, NoopEventHook, TopicRewriteEventHook, TopicRewriteRule,
};
use greenmqtt_retain::RetainHandle;
use greenmqtt_sessiondict::{SessionDictHandle, SessionDirectory};
use greenmqtt_storage::{
    InboxStore, InflightStore, MemoryInboxStore, MemoryInflightStore, MemorySubscriptionStore,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::util::ServiceExt;

fn test_prometheus_handle() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("prometheus recorder should install once for tests")
        })
        .clone()
}

#[derive(Clone)]
struct TestRangeLifecycleManager {
    engine: MemoryKvEngine,
}

#[async_trait]
impl RangeLifecycleManager for TestRangeLifecycleManager {
    async fn create_range(
        &self,
        descriptor: greenmqtt_core::ReplicatedRangeDescriptor,
    ) -> anyhow::Result<HostedRange> {
        self.engine
            .bootstrap(KvRangeBootstrap {
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
        self.engine.destroy_range(range_id).await
    }
}

#[derive(Default)]
struct TestPeerRegistry {
    nodes: RwLock<Vec<u64>>,
    endpoints: RwLock<BTreeMap<u64, String>>,
}

#[derive(Default)]
struct TestShardRegistry {
    assignments: RwLock<BTreeMap<ServiceShardKey, ServiceShardAssignment>>,
    members: RwLock<BTreeMap<u64, ClusterNodeMembership>>,
}

#[async_trait]
impl ServiceEndpointRegistry for TestShardRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("shard registry poisoned")
            .insert(assignment.shard.clone(), assignment))
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .read()
            .expect("shard registry poisoned")
            .get(shard)
            .cloned())
    }

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("shard registry poisoned")
            .remove(shard))
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments: Vec<_> = self
            .assignments
            .read()
            .expect("shard registry poisoned")
            .values()
            .filter(|assignment| {
                kind.as_ref()
                    .map(|kind| assignment.shard.kind == *kind)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        assignments.sort_by(|left, right| left.shard.cmp(&right.shard));
        Ok(assignments)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for TestShardRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("shard registry poisoned")
            .insert(member.node_id, member))
    }

    async fn resolve_member(&self, node_id: u64) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .read()
            .expect("shard registry poisoned")
            .get(&node_id)
            .cloned())
    }

    async fn remove_member(&self, node_id: u64) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("shard registry poisoned")
            .remove(&node_id))
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        Ok(self
            .members
            .read()
            .expect("shard registry poisoned")
            .values()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for TestShardRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("shard registry poisoned")
            .insert(transition.shard.clone(), transition.target_assignment))
    }
}

impl TestPeerRegistry {
    fn new(nodes: Vec<u64>) -> Self {
        let endpoints = nodes
            .iter()
            .map(|node_id| (*node_id, format!("http://127.0.0.1:{}", 50060 + node_id)))
            .collect();
        Self {
            nodes: RwLock::new(nodes),
            endpoints: RwLock::new(endpoints),
        }
    }
}

#[async_trait]
impl PeerRegistry for TestPeerRegistry {
    fn list_peer_nodes(&self) -> Vec<u64> {
        self.nodes.read().expect("peer registry poisoned").clone()
    }

    fn list_peer_endpoints(&self) -> BTreeMap<u64, String> {
        self.endpoints
            .read()
            .expect("peer registry poisoned")
            .clone()
    }

    fn remove_peer_node(&self, node_id: u64) -> bool {
        let mut nodes = self.nodes.write().expect("peer registry poisoned");
        if let Some(index) = nodes.iter().position(|candidate| *candidate == node_id) {
            nodes.remove(index);
            self.endpoints
                .write()
                .expect("peer registry poisoned")
                .remove(&node_id);
            true
        } else {
            false
        }
    }

    async fn add_peer_node(&self, node_id: u64, endpoint: String) -> anyhow::Result<()> {
        let mut nodes = self.nodes.write().expect("peer registry poisoned");
        if !nodes.contains(&node_id) {
            nodes.push(node_id);
            nodes.sort_unstable();
        }
        self.endpoints
            .write()
            .expect("peer registry poisoned")
            .insert(node_id, endpoint);
        Ok(())
    }
}

static AUDIT_LOG_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct CountingDistRouter {
    inner: DistHandle,
    lookup_session_topic_filter_route_calls: AtomicUsize,
    remove_session_topic_filter_route_calls: AtomicUsize,
    remove_tenant_routes_calls: AtomicUsize,
    remove_tenant_shared_routes_calls: AtomicUsize,
    remove_session_routes_calls: AtomicUsize,
    remove_session_topic_filter_routes_calls: AtomicUsize,
    remove_topic_filter_routes_calls: AtomicUsize,
    remove_topic_filter_shared_routes_calls: AtomicUsize,
    list_session_routes_calls: AtomicUsize,
    list_session_topic_filter_routes_calls: AtomicUsize,
    count_session_routes_calls: AtomicUsize,
    count_session_topic_filter_route_calls: AtomicUsize,
    count_session_topic_filter_routes_calls: AtomicUsize,
    count_tenant_routes_calls: AtomicUsize,
    count_tenant_shared_routes_calls: AtomicUsize,
    list_routes_calls: AtomicUsize,
    list_tenant_shared_routes_calls: AtomicUsize,
    list_topic_filter_routes_calls: AtomicUsize,
    list_topic_filter_shared_routes_calls: AtomicUsize,
    count_topic_filter_routes_calls: AtomicUsize,
    count_topic_filter_shared_routes_calls: AtomicUsize,
    list_exact_routes_calls: AtomicUsize,
}

#[derive(Default)]
struct CountingSessionDirectory {
    inner: SessionDictHandle,
    lookup_session_calls: AtomicUsize,
    unregister_calls: AtomicUsize,
}

impl CountingSessionDirectory {
    fn lookup_session_calls(&self) -> usize {
        self.lookup_session_calls.load(Ordering::SeqCst)
    }

    fn unregister_calls(&self) -> usize {
        self.unregister_calls.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct CountingInboxStore {
    inner: MemoryInboxStore,
    load_messages_calls: AtomicUsize,
    list_tenant_messages_calls: AtomicUsize,
    count_session_messages_calls: AtomicUsize,
    count_tenant_messages_calls: AtomicUsize,
}

impl CountingInboxStore {
    fn load_messages_calls(&self) -> usize {
        self.load_messages_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_messages_calls(&self) -> usize {
        self.list_tenant_messages_calls.load(Ordering::SeqCst)
    }

    fn count_session_messages_calls(&self) -> usize {
        self.count_session_messages_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_messages_calls(&self) -> usize {
        self.count_tenant_messages_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl InboxStore for CountingInboxStore {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        self.inner.append_message(message).await
    }

    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inner.peek_messages(session_id).await
    }

    async fn count_session_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_messages(session_id).await
    }

    async fn list_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.list_tenant_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_messages(tenant_id).await
    }

    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_messages(tenant_id).await
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inner.list_all_messages().await
    }

    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.load_messages_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.load_messages(session_id).await
    }

    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        self.inner.purge_messages(session_id).await
    }

    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.inner.purge_tenant_messages(tenant_id).await
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        self.inner.count_messages().await
    }
}

#[derive(Default)]
struct CountingInflightStore {
    inner: MemoryInflightStore,
    load_inflight_calls: AtomicUsize,
    list_tenant_inflight_calls: AtomicUsize,
    count_session_inflight_calls: AtomicUsize,
    count_tenant_inflight_calls: AtomicUsize,
}

#[derive(Default)]
struct CountingSubscriptionStore {
    inner: MemorySubscriptionStore,
    load_subscription_calls: AtomicUsize,
    list_subscriptions_calls: AtomicUsize,
    list_session_topic_subscriptions_calls: AtomicUsize,
    list_tenant_subscriptions_calls: AtomicUsize,
    list_tenant_shared_subscriptions_calls: AtomicUsize,
    list_tenant_topic_subscriptions_calls: AtomicUsize,
    list_tenant_topic_shared_subscriptions_calls: AtomicUsize,
    count_session_subscriptions_calls: AtomicUsize,
    count_session_topic_subscriptions_calls: AtomicUsize,
    purge_session_topic_subscriptions_calls: AtomicUsize,
    count_tenant_subscriptions_calls: AtomicUsize,
    count_tenant_shared_subscriptions_calls: AtomicUsize,
    count_tenant_topic_subscriptions_calls: AtomicUsize,
    count_tenant_topic_shared_subscriptions_calls: AtomicUsize,
    purge_tenant_topic_shared_subscriptions_calls: AtomicUsize,
}

impl CountingInflightStore {
    fn load_inflight_calls(&self) -> usize {
        self.load_inflight_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_inflight_calls(&self) -> usize {
        self.list_tenant_inflight_calls.load(Ordering::SeqCst)
    }

    fn count_session_inflight_calls(&self) -> usize {
        self.count_session_inflight_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_inflight_calls(&self) -> usize {
        self.count_tenant_inflight_calls.load(Ordering::SeqCst)
    }
}

impl CountingSubscriptionStore {
    fn load_subscription_calls(&self) -> usize {
        self.load_subscription_calls.load(Ordering::SeqCst)
    }

    fn list_subscriptions_calls(&self) -> usize {
        self.list_subscriptions_calls.load(Ordering::SeqCst)
    }

    fn list_session_topic_subscriptions_calls(&self) -> usize {
        self.list_session_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn list_tenant_subscriptions_calls(&self) -> usize {
        self.list_tenant_subscriptions_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_shared_subscriptions_calls(&self) -> usize {
        self.list_tenant_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn list_tenant_topic_subscriptions_calls(&self) -> usize {
        self.list_tenant_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn list_tenant_topic_shared_subscriptions_calls(&self) -> usize {
        self.list_tenant_topic_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn count_session_subscriptions_calls(&self) -> usize {
        self.count_session_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn count_session_topic_subscriptions_calls(&self) -> usize {
        self.count_session_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn purge_session_topic_subscriptions_calls(&self) -> usize {
        self.purge_session_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn count_tenant_subscriptions_calls(&self) -> usize {
        self.count_tenant_subscriptions_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_shared_subscriptions_calls(&self) -> usize {
        self.count_tenant_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn count_tenant_topic_subscriptions_calls(&self) -> usize {
        self.count_tenant_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn count_tenant_topic_shared_subscriptions_calls(&self) -> usize {
        self.count_tenant_topic_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn purge_tenant_topic_shared_subscriptions_calls(&self) -> usize {
        self.purge_tenant_topic_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }
}

#[async_trait]
impl InflightStore for CountingInflightStore {
    async fn save_inflight(&self, message: &greenmqtt_core::InflightMessage) -> anyhow::Result<()> {
        self.inner.save_inflight(message).await
    }

    async fn load_inflight(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::InflightMessage>> {
        self.load_inflight_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.load_inflight(session_id).await
    }

    async fn count_session_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_inflight(session_id).await
    }

    async fn list_tenant_inflight(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::InflightMessage>> {
        self.list_tenant_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_inflight(tenant_id).await
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_inflight(tenant_id).await
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<greenmqtt_core::InflightMessage>> {
        self.inner.list_all_inflight().await
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        self.inner.delete_inflight(session_id, packet_id).await
    }

    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        self.inner.purge_inflight(session_id).await
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.inner.purge_tenant_inflight(tenant_id).await
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        self.inner.count_inflight().await
    }
}

#[async_trait]
impl greenmqtt_storage::SubscriptionStore for CountingSubscriptionStore {
    async fn save_subscription(
        &self,
        subscription: &greenmqtt_core::Subscription,
    ) -> anyhow::Result<()> {
        self.inner.save_subscription(subscription).await
    }

    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<greenmqtt_core::Subscription>> {
        self.load_subscription_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .load_subscription(session_id, topic_filter, shared_group)
            .await
    }

    async fn delete_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.inner
            .delete_subscription(session_id, topic_filter, shared_group)
            .await
    }

    async fn list_subscriptions(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.list_subscriptions_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_subscriptions(session_id).await
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.list_session_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await
    }

    async fn count_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_subscriptions(session_id).await
    }

    async fn count_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.count_session_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_session_topic_subscriptions(session_id, topic_filter)
            .await
    }

    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.purge_session_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .purge_session_topic_subscriptions(session_id, topic_filter)
            .await
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.list_tenant_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_subscriptions(tenant_id).await
    }

    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.list_tenant_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_shared_subscriptions(tenant_id, shared_group)
            .await
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_subscriptions(tenant_id).await
    }

    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_tenant_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_tenant_shared_subscriptions(tenant_id, shared_group)
            .await
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.list_tenant_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await
    }

    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.list_tenant_topic_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.count_tenant_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await
    }

    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_tenant_topic_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.purge_tenant_topic_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .purge_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<greenmqtt_core::Subscription>> {
        self.inner.list_all_subscriptions().await
    }

    async fn count_subscriptions(&self) -> anyhow::Result<usize> {
        self.inner.count_subscriptions().await
    }
}

impl CountingDistRouter {
    fn lookup_session_topic_filter_route_calls(&self) -> usize {
        self.lookup_session_topic_filter_route_calls
            .load(Ordering::SeqCst)
    }

    fn remove_session_topic_filter_route_calls(&self) -> usize {
        self.remove_session_topic_filter_route_calls
            .load(Ordering::SeqCst)
    }

    fn remove_tenant_routes_calls(&self) -> usize {
        self.remove_tenant_routes_calls.load(Ordering::SeqCst)
    }

    fn remove_tenant_shared_routes_calls(&self) -> usize {
        self.remove_tenant_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn remove_session_routes_calls(&self) -> usize {
        self.remove_session_routes_calls.load(Ordering::SeqCst)
    }

    fn remove_session_topic_filter_routes_calls(&self) -> usize {
        self.remove_session_topic_filter_routes_calls
            .load(Ordering::SeqCst)
    }

    fn remove_topic_filter_routes_calls(&self) -> usize {
        self.remove_topic_filter_routes_calls.load(Ordering::SeqCst)
    }

    fn remove_topic_filter_shared_routes_calls(&self) -> usize {
        self.remove_topic_filter_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn list_session_routes_calls(&self) -> usize {
        self.list_session_routes_calls.load(Ordering::SeqCst)
    }

    fn list_session_topic_filter_routes_calls(&self) -> usize {
        self.list_session_topic_filter_routes_calls
            .load(Ordering::SeqCst)
    }

    fn count_session_routes_calls(&self) -> usize {
        self.count_session_routes_calls.load(Ordering::SeqCst)
    }

    fn count_session_topic_filter_route_calls(&self) -> usize {
        self.count_session_topic_filter_route_calls
            .load(Ordering::SeqCst)
    }

    fn count_session_topic_filter_routes_calls(&self) -> usize {
        self.count_session_topic_filter_routes_calls
            .load(Ordering::SeqCst)
    }

    fn count_tenant_routes_calls(&self) -> usize {
        self.count_tenant_routes_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_shared_routes_calls(&self) -> usize {
        self.count_tenant_shared_routes_calls.load(Ordering::SeqCst)
    }

    fn list_routes_calls(&self) -> usize {
        self.list_routes_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_shared_routes_calls(&self) -> usize {
        self.list_tenant_shared_routes_calls.load(Ordering::SeqCst)
    }

    fn list_topic_filter_routes_calls(&self) -> usize {
        self.list_topic_filter_routes_calls.load(Ordering::SeqCst)
    }

    fn list_topic_filter_shared_routes_calls(&self) -> usize {
        self.list_topic_filter_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn count_topic_filter_routes_calls(&self) -> usize {
        self.count_topic_filter_routes_calls.load(Ordering::SeqCst)
    }

    fn count_topic_filter_shared_routes_calls(&self) -> usize {
        self.count_topic_filter_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn list_exact_routes_calls(&self) -> usize {
        self.list_exact_routes_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DistRouter for CountingDistRouter {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        self.inner.add_route(route).await
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        self.inner.remove_route(route).await
    }

    async fn lookup_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        self.lookup_session_topic_filter_route_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .lookup_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn remove_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.remove_session_topic_filter_route_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.remove_tenant_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.remove_tenant_routes(tenant_id).await
    }

    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.remove_tenant_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        self.remove_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.remove_session_routes(session_id).await
    }

    async fn remove_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.remove_session_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn remove_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.remove_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.remove_topic_filter_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_session_routes(session_id).await
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_session_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_routes(session_id).await
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_session_topic_filter_route_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.count_session_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_routes(tenant_id).await
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_tenant_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_tenant_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_topic_filter_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.count_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_topic_filter_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_exact_routes_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_exact_routes(tenant_id, topic_filter).await
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &greenmqtt_core::TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.inner.match_topic(tenant_id, topic).await
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_routes_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_routes(tenant_id).await
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        self.inner.route_count().await
    }
}

#[async_trait]
impl SessionDirectory for CountingSessionDirectory {
    async fn register(
        &self,
        record: greenmqtt_core::SessionRecord,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        self.inner.register(record).await
    }

    async fn unregister(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        self.unregister_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.unregister(session_id).await
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        self.inner.lookup_identity(identity).await
    }

    async fn lookup_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        self.lookup_session_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.lookup_session(session_id).await
    }

    async fn list_sessions(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<greenmqtt_core::SessionRecord>> {
        self.inner.list_sessions(tenant_id).await
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        self.inner.session_count().await
    }
}

fn broker() -> Arc<DefaultBroker> {
    Arc::new(DefaultBroker::new(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        SessionDictHandle::default(),
        DistHandle::default(),
        InboxHandle::default(),
        RetainHandle::default(),
    ))
}

fn broker_with_dist(dist: Arc<dyn DistRouter>) -> Arc<DefaultBroker> {
    Arc::new(DefaultBroker::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        dist,
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    ))
}

fn broker_with_inbox(inbox: Arc<dyn InboxService>) -> Arc<DefaultBroker> {
    Arc::new(DefaultBroker::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        inbox,
        Arc::new(RetainHandle::default()),
    ))
}

fn broker_with_inbox_and_dist(
    inbox: Arc<dyn InboxService>,
    dist: Arc<dyn DistRouter>,
) -> Arc<DefaultBroker> {
    Arc::new(DefaultBroker::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        dist,
        inbox,
        Arc::new(RetainHandle::default()),
    ))
}

fn broker_with_sessiondict(sessiondict: Arc<dyn SessionDirectory>) -> Arc<DefaultBroker> {
    Arc::new(DefaultBroker::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        sessiondict,
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    ))
}

fn broker_with_audit_log(audit_log_path: PathBuf) -> Arc<DefaultBroker> {
    Arc::new(DefaultBroker::new(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: Some(audit_log_path),
        },
        AllowAllAuth,
        SessionDictHandle::default(),
        DistHandle::default(),
        InboxHandle::default(),
        RetainHandle::default(),
    ))
}

fn temp_audit_log_path(name: &str) -> PathBuf {
    let unique = AUDIT_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "greenmqtt-http-api-{name}-{timestamp}-{unique}/admin-audit.jsonl"
    ))
}


mod peer_quota;
mod flow_metrics;
mod subscriptions;
mod routes;
mod offline_inflight;
mod sessiondict;
mod query_audit;
mod range;
mod shard;
