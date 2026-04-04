use super::{
    admin::TenantQuotaResponse, ErrorBody, HttpApi, PeerSummary, PurgeReply,
    SessionDictReassignReply, SessionPublishRequest, SubscribeRequest,
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
    ServiceShardRecoveryControl, ServiceShardTransition, SessionKind, Subscription, TenantQuota,
};
use greenmqtt_dist::{DistHandle, DistRouter};
use greenmqtt_inbox::{InboxHandle, InboxService, PersistentInboxHandle};
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

#[tokio::test]
async fn http_lists_peers_from_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_can_delete_peer_from_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(Request::delete("/v1/peers/5").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![PeerSummary {
            node_id: 2,
            rpc_addr: Some("http://127.0.0.1:50062".into()),
            connected: true,
        }]
    );
}

#[tokio::test]
async fn http_can_upsert_peer_into_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/peers/5?rpc_addr=http://127.0.0.1:50065")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_dry_run_delete_peer_reports_without_removing() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::delete("/v1/peers/5?dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_dry_run_upsert_peer_reports_without_mutating_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/peers/5?rpc_addr=http://127.0.0.1:50065&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![PeerSummary {
            node_id: 2,
            rpc_addr: Some("http://127.0.0.1:50062".into()),
            connected: true,
        }]
    );
}

#[tokio::test]
async fn http_can_set_and_get_tenant_quota() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let quota = TenantQuota {
        max_connections: 3,
        max_subscriptions: 5,
        max_msg_per_sec: 7,
        max_memory_bytes: 1024,
    };

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/tenants/demo/quota")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&quota).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let updated: TenantQuotaResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(updated.tenant_id, "demo");
    assert_eq!(updated.quota, Some(quota.clone()));

    let response = app
        .oneshot(
            Request::get("/v1/tenants/demo/quota")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let fetched: TenantQuotaResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(fetched.tenant_id, "demo");
    assert_eq!(fetched.quota, Some(quota));
}

#[tokio::test]
async fn http_lists_peers_from_registry_in_sorted_order() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![5, 2, 3]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 3,
                rpc_addr: Some("http://127.0.0.1:50063".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_flow_replays_offline_messages() {
    let broker = broker();
    let app = HttpApi::router(broker.clone());

    let connect_req = ConnectRequest {
        identity: ClientIdentity {
            tenant_id: "demo".into(),
            user_id: "alice".into(),
            client_id: "sub".into(),
        },
        node_id: 1,
        kind: SessionKind::Persistent,
        clean_start: true,
        session_expiry_interval_secs: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/connect")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&connect_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_publish_and_drain_deliveries() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());

    let subscribe = SubscribeRequest {
        session_id: subscriber.session.session_id.clone(),
        topic_filter: "devices/+/state".into(),
        qos: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        retain_handling: 0,
        shared_group: None,
    };
    let subscribe_response = app
        .clone()
        .oneshot(
            Request::post("/v1/subscribe")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&subscribe).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(subscribe_response.status(), StatusCode::CREATED);

    let publish = SessionPublishRequest {
        session_id: publisher.session.session_id,
        publish: PublishRequest {
            topic: "devices/d1/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
            retain: false,
            properties: PublishProperties::default(),
        },
    };
    let publish_response = app
        .clone()
        .oneshot(
            Request::post("/v1/publish")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&publish).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(publish_response.status(), StatusCode::OK);

    let deliveries_response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/deliveries",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deliveries_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_publish_respects_topic_rewrite_hook() {
    let broker = Arc::new(BrokerRuntime::with_plugins(
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
        TopicRewriteEventHook::new(vec![TopicRewriteRule {
            tenant_id: Some("demo".into()),
            topic_filter: "devices/+/raw".into(),
            rewrite_to: "devices/{1}/normalized".into(),
        }]),
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    ));
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let subscribe = SubscribeRequest {
        session_id: subscriber.session.session_id.clone(),
        topic_filter: "devices/+/normalized".into(),
        qos: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        retain_handling: 0,
        shared_group: None,
    };
    let subscribe_response = app
        .clone()
        .oneshot(
            Request::post("/v1/subscribe")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&subscribe).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(subscribe_response.status(), StatusCode::CREATED);

    let publish = SessionPublishRequest {
        session_id: publisher.session.session_id,
        publish: PublishRequest {
            topic: "devices/d1/raw".into(),
            payload: b"rewritten".to_vec().into(),
            qos: 1,
            retain: false,
            properties: PublishProperties::default(),
        },
    };
    let publish_response = app
        .clone()
        .oneshot(
            Request::post("/v1/publish")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&publish).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(publish_response.status(), StatusCode::OK);

    let deliveries_response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/deliveries",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deliveries_response.status(), StatusCode::OK);
    let body = to_bytes(deliveries_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let deliveries: Vec<Delivery> = serde_json::from_slice(&body).unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].topic, "devices/d1/normalized");
    assert_eq!(deliveries[0].payload, b"rewritten".to_vec());
}

#[tokio::test]
async fn http_stats_reports_local_session_and_delivery_counts() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/d1/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));
    let response = app
        .oneshot(Request::get("/v1/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let stats: BrokerStats = serde_json::from_slice(&body).unwrap();
    assert_eq!(stats.peer_nodes, 2);
    assert_eq!(stats.local_online_sessions, 2);
    assert_eq!(stats.local_persistent_sessions, 1);
    assert_eq!(stats.local_transient_sessions, 1);
    assert_eq!(stats.local_pending_deliveries, 1);
    assert_eq!(stats.global_session_records, 2);
    assert_eq!(stats.route_records, 1);
    assert_eq!(stats.subscription_records, 1);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
    assert_eq!(stats.retained_messages, 0);
}

#[tokio::test]
async fn http_metrics_returns_prometheus_counters() {
    let metrics = test_prometheus_handle();
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    shards
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50070",
            )],
        ))
        .await
        .unwrap();
    shards
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
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("demo-metrics-http"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo-metrics-http".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    for index in 0..100 {
        broker
            .publish(
                &subscriber.session.session_id,
                PublishRequest {
                    topic: format!("devices/{index}/state"),
                    payload: format!("value-{index}").into_bytes().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
    }
    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        Some(Arc::new(TestPeerRegistry::new(vec![2, 5]))),
        Some(shards.clone()),
        Some(metrics),
    );
    let _ = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/demo-metrics-http/*/move")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/demo-metrics-http/*/failover")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":7}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/demo-metrics-http/*/repair")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("greenmqtt_peer_nodes 2"));
    assert!(text.contains("greenmqtt_local_online_sessions 1"));
    assert!(text.contains("greenmqtt_global_session_records 1"));
    assert!(text.contains("greenmqtt_sessions_count 1"));
    assert!(text.contains("greenmqtt_route_records 1"));
    assert!(text.contains("greenmqtt_subscription_records 1"));
    assert!(text.contains("greenmqtt_subscriptions_count 1"));
    assert!(text.contains("greenmqtt_inflight_count 0"));
    assert!(text.contains("greenmqtt_retained_messages_count 0"));
    assert!(text.contains("greenmqtt_replay_window_entries "));
    assert!(text.contains("greenmqtt_service_session_bytes "));
    assert!(text.contains("greenmqtt_service_route_bytes "));
    assert!(text.contains("greenmqtt_service_subscription_bytes "));
    assert!(text.contains("greenmqtt_service_offline_bytes "));
    assert!(text.contains("greenmqtt_service_inflight_bytes "));
    assert!(text.contains("greenmqtt_service_retained_bytes "));
    assert!(text.contains("greenmqtt_local_hot_state_entries "));
    assert!(text.contains("greenmqtt_local_hot_state_bytes "));
    assert!(text.contains("greenmqtt_pending_delayed_wills "));
    assert!(text.contains("greenmqtt_process_rss_bytes "));
    assert!(text.contains("greenmqtt_broker_rss_bytes "));
    assert!(text.contains("greenmqtt_broker_cpu_usage "));
    assert!(text.contains("greenmqtt_memory_pressure_level "));
    let connect_line = text
        .lines()
        .find(|line| {
            line.starts_with("mqtt_connect_total{")
                && line.contains("tenant_id=\"demo-metrics-http\"")
        })
        .expect("expected connect counter line");
    assert!(connect_line.ends_with(" 1"));
    let publish_count_line = text
        .lines()
        .find(|line| {
            line.starts_with("mqtt_publish_count{")
                && line.contains("tenant_id=\"demo-metrics-http\"")
                && line.contains("qos=\"1\"")
        })
        .expect("expected publish counter line");
    assert!(publish_count_line.ends_with(" 100"));
    assert!(text.lines().any(|line| {
        line.starts_with("mqtt_publish_ingress_bytes{")
            && line.contains("tenant_id=\"demo-metrics-http\"")
            && line.contains("qos=\"1\"")
    }));
    assert!(text.contains("mqtt_shard_move_total"));
    assert!(text.contains("mqtt_shard_failover_total"));
    assert!(text.contains("mqtt_shard_anti_entropy_total"));
}

#[tokio::test]
async fn http_metrics_supports_tenant_scoped_gauges() {
    let broker = broker();
    let demo = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-demo".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &demo.session.session_id,
            "devices/demo/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-other".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &other.session.session_id,
            "devices/other/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .publish(
            &demo.session.session_id,
            PublishRequest {
                topic: "retain/demo/state".into(),
                payload: b"retained".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/metrics?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("greenmqtt_tenant_session_records{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_route_records{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_subscription_records{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_retained_messages{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_session_bytes{tenant_id=\"demo\"} "));
    assert!(text.contains("greenmqtt_tenant_route_bytes{tenant_id=\"demo\"} "));
    assert!(text.contains("greenmqtt_tenant_subscription_bytes{tenant_id=\"demo\"} "));
    assert!(text.contains("greenmqtt_tenant_retained_bytes{tenant_id=\"demo\"} "));
}

#[tokio::test]
async fn http_connect_supports_lazy_replay_query() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "lazy-sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "lazy-pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/demo/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/demo/state".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/connect?hydrate_replay=false")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&ConnectRequest {
                        identity: ClientIdentity {
                            tenant_id: "demo".into(),
                            user_id: "alice".into(),
                            client_id: "lazy-sub".into(),
                        },
                        node_id: 1,
                        kind: SessionKind::Persistent,
                        clean_start: false,
                        session_expiry_interval_secs: None,
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: ConnectReply = serde_json::from_slice(&body).unwrap();
    assert!(reply.offline_messages.is_empty());
    assert!(reply.inflight_messages.is_empty());

    let offline_response = app
        .oneshot(
            Request::get(format!(
                "/v1/offline?tenant_id=demo&session_id={}",
                reply.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(offline_response.status(), StatusCode::OK);
    let body = to_bytes(offline_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let offline: Vec<OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].payload, b"offline".to_vec());
}

#[tokio::test]
async fn http_lists_local_sessions_with_pending_delivery_counts() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(Request::get("/v1/sessions").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sessions: Vec<SessionSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(sessions.len(), 2);
    let subscriber_entry = sessions
        .into_iter()
        .find(|session| session.client_id == "sub")
        .unwrap();
    assert_eq!(subscriber_entry.tenant_id, "demo");
    assert_eq!(subscriber_entry.pending_deliveries, 1);
}

#[tokio::test]
async fn http_lists_retained_messages_for_tenant_query() {
    let broker = broker();
    let demo = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .publish(
            &demo.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"retained".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "pub-other".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .publish(
            &other.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"other".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/retain?tenant_id=demo&topic_filter=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let retained: Vec<greenmqtt_core::RetainedMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].tenant_id, "demo");
    assert_eq!(retained[0].topic, "devices/d1/state");
    assert_eq!(retained[0].payload, b"retained".to_vec());
}

#[tokio::test]
async fn http_lists_all_retained_messages_for_tenant_query() {
    let broker = broker();
    let demo = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (topic, payload) in [
        ("devices/d1/state", b"retained-1".to_vec()),
        ("devices/d2/state", b"retained-2".to_vec()),
    ] {
        broker
            .publish(
                &demo.session.session_id,
                PublishRequest {
                    topic: topic.into(),
                    payload: payload.into(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
    }

    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "pub-other".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .publish(
            &other.session.session_id,
            PublishRequest {
                topic: "devices/other/state".into(),
                payload: b"other".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/retain?tenant_id=demo&topic_filter=%23")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let retained: Vec<greenmqtt_core::RetainedMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(retained.len(), 2);
    assert!(retained.iter().all(|message| message.tenant_id == "demo"));
}

#[tokio::test]
async fn http_can_delete_retained_message() {
    let broker = broker();
    broker
        .retain
        .retain(RetainedMessage {
            tenant_id: "demo".into(),
            topic: "devices/d1/state".into(),
            payload: b"retained".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/retain?tenant_id=demo&topic=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .retain
        .match_topic("demo", "devices/d1/state")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_dry_run_retained_delete_reports_without_removing() {
    let broker = broker();
    broker
        .retain
        .retain(RetainedMessage {
            tenant_id: "demo".into(),
            topic: "devices/d1/state".into(),
            payload: b"retained".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/retain?tenant_id=demo&topic=devices/d1/state&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    let retained = broker
        .retain
        .match_topic("demo", "devices/d1/state")
        .await
        .unwrap();
    assert_eq!(retained.len(), 1);
    let audit = broker.list_admin_audit(None);
    assert!(audit.is_empty());
}

#[tokio::test]
async fn http_lists_session_subscriptions() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(7),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/subscriptions",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
    assert_eq!(subscriptions[0].subscription_identifier, Some(7));
}

#[tokio::test]
async fn http_lists_all_subscriptions_with_filters() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/subscriptions?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].tenant_id, "demo");
    assert_eq!(subscriptions[0].session_id, first.session.session_id);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
}

#[tokio::test]
async fn http_lists_all_subscriptions_by_tenant() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/subscriptions?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].tenant_id, "demo");
    assert_eq!(subscriptions[0].session_id, first.session.session_id);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
}

#[tokio::test]
async fn http_lists_all_subscriptions_by_tenant_and_topic_filter() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let third = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-c".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &third.session.session_id,
            "devices/+/state",
            1,
            Some(3),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].tenant_id, "demo");
    assert_eq!(subscriptions[0].session_id, first.session.session_id);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
}

#[tokio::test]
async fn http_can_purge_subscriptions_by_tenant_and_topic_filter() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let third = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-c".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &third.session.session_id,
            "devices/+/state",
            1,
            Some(3),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let remaining = broker.inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|subscription| {
        !(subscription.tenant_id == "demo" && subscription.topic_filter == "devices/+/state")
    }));

    let demo_routes: Vec<_> = broker
        .dist
        .list_routes(Some("demo"))
        .await
        .unwrap()
        .into_iter()
        .filter(|route| route.topic_filter == "devices/+/state")
        .collect();
    assert!(demo_routes.is_empty());
    let other_routes: Vec<_> = broker
        .dist
        .list_routes(Some("other"))
        .await
        .unwrap()
        .into_iter()
        .filter(|route| route.topic_filter == "devices/+/state")
        .collect();
    assert_eq!(other_routes.len(), 1);
}

#[tokio::test]
async fn http_can_purge_subscriptions_and_routes_by_filter() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let remaining = broker.inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].topic_filter, "alerts/#");

    let routes = broker.dist.list_routes(Some("demo")).await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "alerts/#");
}

#[tokio::test]
async fn http_can_purge_subscriptions_by_session_only() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "alerts/#".into(),
            session_id: subscriber.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .inbox
        .list_subscriptions(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(dist.remove_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_can_purge_subscriptions_by_tenant() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    assert!(broker
        .inbox
        .list_subscriptions(&first.session.session_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        broker
            .inbox
            .list_subscriptions(&second.session.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn http_tenant_subscription_purge_uses_remove_tenant_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_tenant_routes_calls(), 1);
    assert_eq!(dist.remove_session_routes_calls(), 0);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_subscription_purge_by_topic_filter_uses_remove_topic_filter_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
    assert_eq!(dist.remove_tenant_routes_calls(), 0);
    assert_eq!(dist.remove_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_reports_without_removing() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        broker.inbox.list_all_subscriptions().await.unwrap().len(),
        1
    );
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_list_shared_subscription_uses_lookup_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let load_calls_before = subscriptions.load_subscription_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(subscriptions.load_subscription_calls(), load_calls_before);
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
}

#[tokio::test]
async fn http_dry_run_shared_subscription_purge_uses_lookup_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let load_calls_before = subscriptions.load_subscription_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.load_subscription_calls(), load_calls_before);
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
    assert_eq!(
        broker.inbox.list_all_subscriptions().await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn http_shared_subscription_purge_uses_lookup_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let load_calls_before = subscriptions.load_subscription_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.load_subscription_calls(), load_calls_before);
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
    assert!(broker
        .inbox
        .list_all_subscriptions()
        .await
        .unwrap()
        .is_empty());
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "purge_subscriptions");
    assert_eq!(audit[0].details.get("shared_group").unwrap(), "workers");
}

#[tokio::test]
async fn http_list_session_topic_subscriptions_uses_cached_or_direct_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    let list_calls_before = subscriptions.list_subscriptions_calls();
    let session_topic_calls_before = subscriptions.list_session_topic_subscriptions_calls();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        subscriptions.list_session_topic_subscriptions_calls(),
        session_topic_calls_before
    );
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_session_topic_cached_or_direct_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    let count_calls_before = subscriptions.count_session_topic_subscriptions_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        subscriptions.count_session_topic_subscriptions_calls(),
        count_calls_before
    );
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
}

#[tokio::test]
async fn http_subscription_purge_uses_session_topic_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_inbox_and_dist(inbox, dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let purge_calls_before = subscriptions.purge_session_topic_subscriptions_calls();
    let remove_calls_before = dist.remove_session_topic_filter_routes_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(
        subscriptions.purge_session_topic_subscriptions_calls(),
        purge_calls_before + 1
    );
    assert_eq!(
        dist.remove_session_topic_filter_routes_calls(),
        remove_calls_before + 1
    );
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
    assert!(broker
        .inbox
        .list_session_topic_subscriptions(&subscriber.session.session_id, "devices/+/state")
        .await
        .unwrap()
        .is_empty());
    let audit = broker.list_admin_audit(None);
    assert_eq!(
        audit[0].details.get("topic_filter").unwrap(),
        "devices/+/state"
    );
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_session_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_session_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_tenant_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_subscription_query_uses_tenant_shared_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/subscriptions?tenant_id=demo&shared_group=workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(subscriptions.list_tenant_shared_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_shared_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&shared_group=workers&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_tenant_shared_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_shared_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_topic_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_tenant_topic_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_subscription_query_uses_tenant_topic_shared_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(
        subscriptions.list_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_topic_shared_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        subscriptions.count_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(
        subscriptions.list_tenant_topic_shared_subscriptions_calls(),
        0
    );
}

#[tokio::test]
async fn http_subscription_purge_uses_tenant_topic_shared_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let dist = Arc::new(CountingDistRouter::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox_and_dist(inbox, dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        subscriptions.purge_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(dist.remove_topic_filter_shared_routes_calls(), 1);
    let remaining = broker.inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shared_group, None);
}

#[tokio::test]
async fn http_rejects_subscription_purge_without_filter() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_can_purge_routes_by_filter() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "alerts/#".into(),
            session_id: subscriber.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let routes = broker.dist.list_routes(Some("demo")).await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "alerts/#");
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "purge_routes");
    assert_eq!(audit[0].details.get("removed").unwrap(), "1");
}

#[tokio::test]
async fn http_route_query_uses_dist_topic_filter_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/d1/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "devices/+/state".into(),
            session_id: subscriber.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/routes/all?tenant_id=demo&topic_filter=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "devices/d1/state");
    assert_eq!(dist.list_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
    assert_eq!(dist.list_exact_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_topic_filter_shared_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for session_id in [&first.session.session_id, &second.session.session_id] {
        broker
            .subscribe(
                session_id,
                "devices/+/state",
                1,
                Some(2),
                false,
                false,
                0,
                Some("workers".into()),
            )
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(dist.list_topic_filter_shared_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_tenant_shared_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (session_id, topic_filter, shared_group) in [
        (
            &first.session.session_id,
            "devices/+/state",
            Some("workers"),
        ),
        (&second.session.session_id, "alerts/#", Some("workers")),
        (&second.session.session_id, "metrics/#", Some("other")),
    ] {
        broker
            .dist
            .add_route(RouteRecord {
                tenant_id: "demo".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.clone(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/routes/all?tenant_id=demo&shared_group=workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(dist.list_tenant_shared_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_session_topic_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(dist.list_session_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_session_topic_shared_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(dist.lookup_session_topic_filter_route_calls(), 1);
    assert_eq!(dist.list_session_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_session_route_purge_uses_remove_session_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/d1/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "alerts/#".into(),
            session_id: subscriber.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.remove_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        0
    );
}

#[tokio::test]
async fn http_session_topic_route_purge_uses_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.remove_session_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_session_topic_filter_routes_calls(), 0);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert!(broker
        .dist
        .list_session_topic_filter_routes(&subscriber.session.session_id, "devices/+/state")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_session_topic_shared_route_dry_run_uses_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.count_session_topic_filter_route_calls(), 1);
    assert_eq!(dist.lookup_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn http_session_topic_shared_route_purge_uses_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_session_topic_filter_route_calls(), 1);
    assert_eq!(dist.lookup_session_topic_filter_route_calls(), 0);
    let remaining = broker
        .dist
        .list_session_topic_filter_routes(&subscriber.session.session_id, "devices/+/state")
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shared_group, None);
}

#[tokio::test]
async fn http_route_purge_uses_remove_topic_filter_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "alerts/#".into(),
            session_id: subscriber.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
    assert_eq!(dist.list_routes_calls(), 0);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn http_route_purge_uses_remove_topic_filter_shared_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    for session_id in [&first.session.session_id, &second.session.session_id] {
        broker
            .subscribe(
                session_id,
                "devices/+/state",
                1,
                Some(2),
                false,
                false,
                0,
                Some("workers".into()),
            )
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.remove_topic_filter_shared_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
    let remaining = broker.dist.list_routes(Some("demo")).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shared_group, None);
}

#[tokio::test]
async fn http_tenant_route_purge_uses_remove_tenant_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "devices/+/state".into(),
            session_id: first.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "other".into(),
            topic_filter: "alerts/#".into(),
            session_id: second.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: Some(2),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_tenant_routes_calls(), 1);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 0);
    assert_eq!(dist.remove_session_routes_calls(), 0);
    let remaining = broker.dist.list_routes(None).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].tenant_id, "other");
}

#[tokio::test]
async fn http_route_purge_uses_remove_tenant_shared_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (session_id, topic_filter, shared_group) in [
        (
            &first.session.session_id,
            "devices/+/state",
            Some("workers"),
        ),
        (&second.session.session_id, "alerts/#", Some("workers")),
        (&second.session.session_id, "metrics/#", Some("other")),
    ] {
        broker
            .dist
            .add_route(RouteRecord {
                tenant_id: "demo".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.clone(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo&shared_group=workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.remove_tenant_shared_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn http_dry_run_route_purge_reports_without_removing() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_tenant_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.count_tenant_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_session_topic_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.count_session_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_topic_filter_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.count_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_topic_filter_shared_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for session_id in [&first.session.session_id, &second.session.session_id] {
        broker
            .subscribe(
                session_id,
                "devices/+/state",
                1,
                Some(2),
                false,
                false,
                0,
                Some("workers".into()),
            )
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.count_topic_filter_shared_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_tenant_shared_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (session_id, topic_filter, shared_group) in [
        (
            &first.session.session_id,
            "devices/+/state",
            Some("workers"),
        ),
        (&second.session.session_id, "alerts/#", Some("workers")),
        (&second.session.session_id, "metrics/#", Some("other")),
    ] {
        broker
            .dist
            .add_route(RouteRecord {
                tenant_id: "demo".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.clone(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo&shared_group=workers&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.count_tenant_shared_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn http_rejects_route_purge_without_filter() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_lists_session_offline_messages() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/offline",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let offline: Vec<greenmqtt_core::OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].session_id, subscriber.session.session_id);
    assert_eq!(offline[0].payload, b"offline".to_vec());
}

#[tokio::test]
async fn http_lists_all_offline_messages_with_filters() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: first.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "other".into(),
            session_id: second.session.session_id.clone(),
            topic: "alerts/1".into(),
            payload: b"offline-b".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/offline?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let offline: Vec<greenmqtt_core::OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].tenant_id, "demo");
    assert_eq!(offline[0].session_id, first.session.session_id);
    assert_eq!(offline[0].payload, b"offline-a".to_vec());
}

#[tokio::test]
async fn http_lists_all_offline_messages_by_tenant() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (tenant_id, session_id, payload) in [
        (
            "demo",
            first.session.session_id.clone(),
            b"offline-a".to_vec(),
        ),
        (
            "other",
            second.session.session_id.clone(),
            b"offline-b".to_vec(),
        ),
    ] {
        broker
            .inbox
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id,
                topic: "devices/d1/state".into(),
                payload: payload.into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/offline?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let offline: Vec<greenmqtt_core::OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].tenant_id, "demo");
    assert_eq!(offline[0].session_id, first.session.session_id);
}

#[tokio::test]
async fn http_can_purge_offline_messages_by_filter() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?tenant_id=demo&session_id={}",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .inbox
        .peek(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_offline_messages_by_session_only() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?session_id={}",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .inbox
        .peek(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_offline_messages_by_tenant_only() {
    let broker = broker();
    let demo_a = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let demo_b = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "carol".into(),
                client_id: "sub-c".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for session_id in [
        demo_a.session.session_id.clone(),
        demo_b.session.session_id.clone(),
        other.session.session_id.clone(),
    ] {
        let tenant_id = if session_id == other.session.session_id {
            "other"
        } else {
            "demo"
        };
        broker
            .inbox
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id,
                topic: "devices/d1/state".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/offline?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert!(broker
        .inbox
        .list_tenant_offline("demo")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        broker
            .inbox
            .list_tenant_offline("other")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn http_dry_run_offline_purge_reports_without_removing() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?tenant_id=demo&session_id={}&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        broker
            .inbox
            .peek(&subscriber.session.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_offline_purge_uses_session_count_fast_path() {
    let messages = Arc::new(CountingInboxStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        messages.clone(),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?tenant_id=demo&session_id={}&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(messages.count_session_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
    assert_eq!(messages.list_tenant_messages_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_offline_purge_uses_tenant_count_fast_path() {
    let messages = Arc::new(CountingInboxStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        messages.clone(),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    for (tenant_id, user_id, client_id) in [("demo", "alice", "sub-a"), ("other", "bob", "sub-b")] {
        let subscriber = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: tenant_id.into(),
                    user_id: user_id.into(),
                    client_id: client_id.into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        broker
            .inbox
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id: subscriber.session.session_id.clone(),
                topic: "devices/d1/state".into(),
                payload: client_id.as_bytes().to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/offline?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(messages.count_tenant_messages_calls(), 1);
    assert_eq!(messages.list_tenant_messages_calls(), 0);
    assert_eq!(messages.load_messages_calls(), 0);
}

#[tokio::test]
async fn http_rejects_offline_purge_without_filter() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let response = app
        .oneshot(Request::delete("/v1/offline").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_lists_session_inflight_messages() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "pub-session".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/inflight",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let inflight: Vec<greenmqtt_core::InflightMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].session_id, subscriber.session.session_id);
    assert_eq!(inflight[0].packet_id, 7);
    assert_eq!(inflight[0].payload, b"inflight".to_vec());
}

#[tokio::test]
async fn http_lists_all_inflight_messages_with_filters() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (tenant_id, session_id, packet_id) in [
        ("demo", first.session.session_id.clone(), 7u16),
        ("other", second.session.session_id.clone(), 9u16),
    ] {
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id,
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/inflight?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let inflight: Vec<greenmqtt_core::InflightMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].tenant_id, "demo");
    assert_eq!(inflight[0].session_id, first.session.session_id);
    assert_eq!(inflight[0].packet_id, 7);
}

#[tokio::test]
async fn http_lists_all_inflight_messages_by_tenant() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (tenant_id, session_id, packet_id) in [
        ("demo", first.session.session_id.clone(), 7u16),
        ("other", second.session.session_id.clone(), 9u16),
    ] {
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id,
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/inflight?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let inflight: Vec<greenmqtt_core::InflightMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].tenant_id, "demo");
    assert_eq!(inflight[0].session_id, first.session.session_id);
    assert_eq!(inflight[0].packet_id, 7);
}

#[tokio::test]
async fn http_can_purge_inflight_messages_by_filter() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?tenant_id=demo&session_id={}",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .inbox
        .fetch_inflight(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_inflight_messages_by_session_only() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?session_id={}",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .inbox
        .fetch_inflight(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_inflight_messages_by_tenant_only() {
    let broker = broker();
    let demo_a = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let demo_b = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "carol".into(),
                client_id: "sub-c".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (tenant_id, session_id, packet_id) in [
        ("demo", demo_a.session.session_id.clone(), 7u16),
        ("demo", demo_b.session.session_id.clone(), 8u16),
        ("other", other.session.session_id.clone(), 9u16),
    ] {
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id,
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/inflight?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert!(broker
        .inbox
        .list_tenant_inflight("demo")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        broker
            .inbox
            .list_tenant_inflight("other")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn http_dry_run_inflight_purge_reports_without_removing() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?tenant_id=demo&session_id={}&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        broker
            .inbox
            .fetch_inflight(&subscriber.session.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_inflight_purge_uses_session_count_fast_path() {
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        inflight.clone(),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?tenant_id=demo&session_id={}&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(inflight.count_session_inflight_calls(), 1);
    assert_eq!(inflight.load_inflight_calls(), 0);
    assert_eq!(inflight.list_tenant_inflight_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_inflight_purge_uses_tenant_count_fast_path() {
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        inflight.clone(),
    ));
    let broker = broker_with_inbox(inbox);
    for (tenant_id, user_id, client_id, packet_id) in [
        ("demo", "alice", "sub-a", 7u16),
        ("other", "bob", "sub-b", 9u16),
    ] {
        let subscriber = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: tenant_id.into(),
                    user_id: user_id.into(),
                    client_id: client_id.into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: subscriber.session.session_id.clone(),
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/inflight?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(inflight.count_tenant_inflight_calls(), 1);
    assert_eq!(inflight.list_tenant_inflight_calls(), 0);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

#[tokio::test]
async fn http_lookup_sessiondict_by_identity() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/sessiondict/by-identity?tenant_id=demo&user_id=alice&client_id=sub")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let record: Option<greenmqtt_core::SessionRecord> = serde_json::from_slice(&body).unwrap();
    let record = record.expect("session record must exist");
    assert_eq!(record.session_id, session.session_id);
    assert_eq!(record.identity.tenant_id, "demo");
    assert_eq!(record.identity.user_id, "alice");
    assert_eq!(record.identity.client_id, "sub");
}

#[tokio::test]
async fn http_lists_sessiondict_records_with_tenant_filter() {
    let broker = broker();
    for (tenant_id, client_id) in [("demo", "sub-a"), ("demo", "sub-b"), ("other", "sub-c")] {
        broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: tenant_id.into(),
                    user_id: "alice".into(),
                    client_id: client_id.into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/sessiondict?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let records: Vec<greenmqtt_core::SessionRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|record| record.identity.tenant_id == "demo"));
}

#[tokio::test]
async fn http_lookup_sessiondict_by_session_id() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!("/v1/sessiondict/{}", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let record: Option<greenmqtt_core::SessionRecord> = serde_json::from_slice(&body).unwrap();
    let record = record.expect("session record must exist");
    assert_eq!(record.session_id, session.session_id);
    assert_eq!(record.identity.client_id, "sub");
}

#[tokio::test]
async fn http_can_delete_sessiondict_record() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!("/v1/sessiondict/{}", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .is_none());
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "delete_sessiondict");
    assert_eq!(
        audit[0].details.get("session_id").unwrap(),
        &session.session_id
    );
}

#[tokio::test]
async fn http_delete_sessiondict_record_uses_unregister_fast_path() {
    let sessiondict = Arc::new(CountingSessionDirectory::default());
    let broker = broker_with_sessiondict(sessiondict.clone());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!("/v1/sessiondict/{}", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(sessiondict.unregister_calls(), 1);
    assert_eq!(sessiondict.lookup_session_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_sessiondict_delete_reports_without_removing() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/sessiondict/{}?dry_run=true",
                session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .is_some());
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_delete_missing_sessiondict_record_returns_zero_removed() {
    let broker = broker();
    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/sessiondict/missing-session")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 0);
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "delete_sessiondict");
    assert_eq!(audit[0].details.get("removed").unwrap(), "0");
}

#[tokio::test]
async fn http_can_reassign_sessiondict_record_and_routes() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply,
        SessionDictReassignReply {
            updated_sessions: 1,
            updated_routes: 1,
        }
    );
    let updated = broker
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.node_id, 7);
    let route = broker
        .dist
        .list_routes(Some("demo"))
        .await
        .unwrap()
        .into_iter()
        .find(|route| route.session_id == session.session_id)
        .unwrap();
    assert_eq!(route.node_id, 7);
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "reassign_sessiondict");
    assert_eq!(audit[0].details.get("node_id").unwrap(), "7");
}

#[tokio::test]
async fn http_reassign_sessiondict_record_uses_single_session_lookup() {
    let sessiondict = Arc::new(CountingSessionDirectory::default());
    let broker = broker_with_sessiondict(sessiondict.clone());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.updated_sessions, 1);
    assert_eq!(reply.updated_routes, 1);
    assert_eq!(sessiondict.lookup_session_calls(), 1);
}

#[tokio::test]
async fn http_reassign_sessiondict_record_skips_dry_run_route_count_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.updated_sessions, 1);
    assert_eq!(reply.updated_routes, 1);
    assert_eq!(dist.count_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_reassign_sessiondict_record_reports_without_removing() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!(
                "/v1/sessiondict/{}?node_id=7&dry_run=true",
                session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply,
        SessionDictReassignReply {
            updated_sessions: 1,
            updated_routes: 1,
        }
    );
    let unchanged = broker
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.node_id, 2);
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_reassign_sessiondict_uses_session_route_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!(
                "/v1/sessiondict/{}?node_id=7&dry_run=true",
                session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply,
        SessionDictReassignReply {
            updated_sessions: 1,
            updated_routes: 1,
        }
    );
    assert_eq!(dist.count_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert_eq!(dist.remove_session_routes_calls(), 0);
    assert_eq!(dist.remove_tenant_routes_calls(), 0);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_reassign_sessiondict_record_rejects_local_online_session() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: ErrorBody = serde_json::from_slice(&body).unwrap();
    assert!(error
        .error
        .contains("cannot reassign a locally online session"));
}

#[tokio::test]
async fn http_lists_matched_routes_for_topic_query() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/routes?tenant_id=demo&topic=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<greenmqtt_core::RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].tenant_id, "demo");
    assert_eq!(routes[0].topic_filter, "devices/+/state");
    assert_eq!(routes[0].session_id, subscriber.session.session_id);
}

#[tokio::test]
async fn http_lists_all_routes_with_filters() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &first.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/routes/all?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<greenmqtt_core::RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].tenant_id, "demo");
    assert_eq!(routes[0].topic_filter, "devices/+/state");
    assert_eq!(routes[0].session_id, first.session.session_id);
}

#[tokio::test]
async fn http_can_disconnect_local_session_via_delete() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/v1/sessions/{}", subscriber.session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(broker
        .session_record(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn http_disconnect_records_admin_audit_entry() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/v1/sessions/{}", subscriber.session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/audit?limit=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "disconnect_session");
    assert_eq!(
        audit[0].details.get("session_id").unwrap(),
        &subscriber.session.session_id
    );
}

#[tokio::test]
async fn http_subscription_purge_records_admin_audit_entry() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .clone()
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/audit?limit=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    let purge_entry = audit
        .iter()
        .filter(|entry| entry.action == "purge_subscriptions")
        .max_by_key(|entry| entry.seq)
        .expect("missing purge_subscriptions audit entry");
    assert_eq!(purge_entry.details.get("removed").unwrap(), "1");
    assert_eq!(
        purge_entry.details.get("session_id").unwrap(),
        &subscriber.session.session_id
    );
}

#[tokio::test]
async fn http_audit_supports_action_and_since_seq_filters() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), "s1".into())]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("session_id".into(), "s2".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?action=purge_offline&since_seq=1&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "purge_offline");
    assert_eq!(audit[0].target, "offline");
    assert_eq!(audit[0].seq, 2);
    assert!(audit[0].timestamp_ms > 0);
}

#[tokio::test]
async fn http_audit_supports_details_filters() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([
            ("session_id".into(), "s1".into()),
            ("tenant_id".into(), "demo".into()),
        ]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([
            ("session_id".into(), "s2".into()),
            ("tenant_id".into(), "other".into()),
        ]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?details_key=tenant_id&details_value=demo&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "disconnect_session");
    assert_eq!(audit[0].details.get("tenant_id").unwrap(), "demo");
}

#[tokio::test]
async fn http_audit_supports_before_seq_pagination() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), "s1".into())]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("session_id".into(), "s2".into())]),
    );
    broker.record_admin_audit(
        "purge_subscriptions",
        "subscription",
        BTreeMap::from([("session_id".into(), "s3".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?before_seq=3&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[0].seq, 2);
    assert_eq!(audit[1].seq, 1);
}

#[tokio::test]
async fn http_audit_supports_since_timestamp_filters() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), "s1".into())]),
    );
    let first_timestamp = broker.list_admin_audit(None)[0].timestamp_ms;
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("session_id".into(), "s2".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/audit?since_timestamp_ms={first_timestamp}&action=purge_offline&limit=10"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "purge_offline");
    assert!(audit[0].timestamp_ms >= first_timestamp);
}

#[tokio::test]
async fn http_delete_peer_records_admin_audit_entry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));

    let response = app
        .oneshot(Request::delete("/v1/peers/5").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "delete_peer");
    assert_eq!(audit[0].target, "peers");
    assert_eq!(audit[0].details.get("node_id").unwrap(), "5");
    assert_eq!(audit[0].details.get("removed").unwrap(), "1");
}

#[tokio::test]
async fn http_upsert_peer_records_admin_audit_entry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2]));
    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));

    let response = app
        .oneshot(
            Request::put("/v1/peers/5?rpc_addr=http://127.0.0.1:50065")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "upsert_peer");
    assert_eq!(audit[0].target, "peers");
    assert_eq!(audit[0].details.get("node_id").unwrap(), "5");
    assert_eq!(
        audit[0].details.get("rpc_addr").unwrap(),
        "http://127.0.0.1:50065"
    );
}

#[tokio::test]
async fn http_dry_run_peer_management_does_not_write_audit() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/peers/6?rpc_addr=http://127.0.0.1:50066&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::delete("/v1/peers/5?dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_audit_restores_persisted_entries_after_restart() {
    let audit_log_path = temp_audit_log_path("restore");
    let broker = broker_with_audit_log(audit_log_path.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!("/v1/sessions/{}", subscriber.session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    drop(broker);

    let restored = broker_with_audit_log(audit_log_path.clone());
    let restored_app = HttpApi::router(restored);
    let response = restored_app
        .oneshot(
            Request::get("/v1/audit?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    let disconnect_entry = audit
        .iter()
        .find(|entry| entry.action == "disconnect_session")
        .expect("missing disconnect_session audit entry");
    assert_eq!(
        disconnect_entry.details.get("session_id").unwrap(),
        &subscriber.session.session_id
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn http_audit_supports_shard_only_filter() {
    let broker = broker();
    broker.record_admin_audit(
        "shard_move",
        "shards",
        BTreeMap::from([("tenant_id".into(), "t1".into())]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("tenant_id".into(), "t1".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?shard_only=true&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "shard_move");
}

#[tokio::test]
async fn http_lists_shards_with_optional_filters() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::retain("t2"),
            ServiceEndpoint::new(ServiceKind::Retain, 9, "http://127.0.0.1:50090"),
            2,
            11,
            ServiceShardLifecycle::Recovering,
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(broker, None, Some(shards), None);

    let response = app
        .clone()
        .oneshot(Request::get("/v1/shards").body(Body::empty()).unwrap())
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "unexpected status: {} body: {}",
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from_utf8_lossy(&body)
        );
    }
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let assignments: Vec<ServiceShardAssignment> = serde_json::from_slice(&body).unwrap();
    assert_eq!(assignments.len(), 2);

    let response = app
        .oneshot(
            Request::get("/v1/shards?kind=dist&tenant_id=t1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "unexpected status: {} body: {}",
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from_utf8_lossy(&body)
        );
    }
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let assignments: Vec<ServiceShardAssignment> = serde_json::from_slice(&body).unwrap();
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].shard.kind, ServiceShardKind::Dist);
    assert_eq!(assignments[0].shard.tenant_id, "t1");
}

#[tokio::test]
async fn http_gets_single_shard_assignment() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    let assignment = ServiceShardAssignment::new(
        ServiceShardKey::sessiondict("t1", "identity:u1:c1"),
        ServiceEndpoint::new(ServiceKind::SessionDict, 7, "http://127.0.0.1:50070"),
        3,
        12,
        ServiceShardLifecycle::Serving,
    );
    shards.upsert_assignment(assignment.clone()).await.unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(broker, None, Some(shards), None);

    let response = app
        .oneshot(
            Request::get("/v1/shards/sessiondict/t1/identity:u1:c1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "unexpected status: {} body: {}",
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from_utf8_lossy(&body)
        );
    }
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let found: Option<ServiceShardAssignment> = serde_json::from_slice(&body).unwrap();
    assert_eq!(found, Some(assignment));
}

#[tokio::test]
async fn http_can_move_shard_and_record_admin_audit() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    shards
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50070",
            )],
        ))
        .await
        .unwrap();
    shards
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
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        None,
        Some(shards.clone()),
        None,
    );
    let response = app
        .oneshot(
            Request::post("/v1/shards/dist/t1/*/move")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: super::shard::ShardActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.current.as_ref().unwrap().owner_node_id(), 9);
    assert_eq!(
        shards
            .resolve_assignment(&ServiceShardKey::dist("t1"))
            .await
            .unwrap()
            .unwrap()
            .owner_node_id(),
        9
    );
    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| entry.action == "shard_move"));
}

#[tokio::test]
async fn http_dry_run_drain_shard_does_not_mutate_or_audit() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    let assignment = ServiceShardAssignment::new(
        ServiceShardKey::retain("t1"),
        ServiceEndpoint::new(ServiceKind::Retain, 7, "http://127.0.0.1:50070"),
        1,
        10,
        ServiceShardLifecycle::Serving,
    );
    shards.upsert_assignment(assignment.clone()).await.unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        None,
        Some(shards.clone()),
        None,
    );
    let response = app
        .oneshot(
            Request::post("/v1/shards/retain/t1/*/drain")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"dry_run":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: super::shard::ShardActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.previous, Some(assignment.clone()));
    assert_eq!(
        reply.current.as_ref().unwrap().lifecycle,
        ServiceShardLifecycle::Draining
    );
    assert_eq!(
        shards
            .resolve_assignment(&ServiceShardKey::retain("t1"))
            .await
            .unwrap()
            .unwrap(),
        assignment
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_failover_shard_records_audit_and_metrics() {
    let metrics = test_prometheus_handle();
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    shards
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50070",
            )],
        ))
        .await
        .unwrap();
    shards
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
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        None,
        Some(shards.clone()),
        Some(metrics),
    );
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/t1/*/failover")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| entry.action == "shard_failover"));

    let response = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("mqtt_shard_failover_total"));
}
