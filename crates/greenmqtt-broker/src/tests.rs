use super::{
    desired_peer_membership, desired_peer_registry, now_millis, placement_candidates,
    placement_candidates_excluding, placement_node_for_session, shard_index_for_session,
    AdminAuditEntry, BrokerConfig, BrokerRuntime, ClientBalancer, PeerForwarder, PeerRegistry,
    RoundRobinBalancer, ShardedLocalDeliveries, ShardedLocalSessions, ShardedWillGenerations,
};
use crate::broker::pressure::PressureLevel;
use async_trait::async_trait;
use greenmqtt_core::{
    ClientIdentity, ConnectRequest, Delivery, InflightMessage, InflightPhase, NodeId,
    OfflineMessage, PublishProperties, PublishRequest, RetainedMessage, RouteRecord, SessionKind,
    SessionRecord, TenantQuota, TopicName,
};
use greenmqtt_dist::{DistHandle, DistRouter};
use greenmqtt_inbox::{InboxHandle, InboxService, PersistentInboxHandle};
use greenmqtt_plugin_api::{
    AllowAllAcl, AllowAllAuth, EventHook, NoopEventHook, TopicRewriteEventHook, TopicRewriteRule,
};
use greenmqtt_retain::{RetainHandle, RetainService};
use greenmqtt_sessiondict::{PersistentSessionDictHandle, SessionDictHandle, SessionDirectory};
use greenmqtt_storage::{
    InboxStore, MemoryInboxStore, MemoryInflightStore, MemorySessionStore, MemorySubscriptionStore,
};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::time::{sleep, Duration};

fn test_broker() -> BrokerRuntime<AllowAllAuth> {
    BrokerRuntime::new(
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
    )
}

fn temp_audit_log_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("greenmqtt-audit-{name}-{}", now_millis()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("admin-audit.jsonl")
}

#[derive(Clone, Default)]
struct RecordingHook {
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EventHook for RecordingHook {
    async fn on_connect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        self.events
            .lock()
            .expect("hook poisoned")
            .push(format!("connect:{}", session.session_id));
        Ok(())
    }

    async fn on_disconnect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        self.events
            .lock()
            .expect("hook poisoned")
            .push(format!("disconnect:{}", session.session_id));
        Ok(())
    }

    async fn on_subscribe(
        &self,
        _identity: &ClientIdentity,
        subscription: &greenmqtt_core::Subscription,
    ) -> anyhow::Result<()> {
        self.events
            .lock()
            .expect("hook poisoned")
            .push(format!("subscribe:{}", subscription.topic_filter));
        Ok(())
    }

    async fn on_unsubscribe(
        &self,
        _identity: &ClientIdentity,
        subscription: &greenmqtt_core::Subscription,
    ) -> anyhow::Result<()> {
        self.events
            .lock()
            .expect("hook poisoned")
            .push(format!("unsubscribe:{}", subscription.topic_filter));
        Ok(())
    }
}

#[derive(Default)]
struct FailingPeerForwarder;

#[async_trait]
impl PeerForwarder for FailingPeerForwarder {
    async fn forward_delivery(&self, _node_id: u64, _delivery: Delivery) -> anyhow::Result<bool> {
        anyhow::bail!("peer unavailable")
    }
}

#[derive(Default)]
struct CountingBatchPeerForwarder {
    single_calls: AtomicUsize,
    batch_calls: AtomicUsize,
    forwarded: Mutex<HashMap<NodeId, Vec<Delivery>>>,
}

impl CountingBatchPeerForwarder {
    fn single_calls(&self) -> usize {
        self.single_calls.load(Ordering::SeqCst)
    }

    fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::SeqCst)
    }

    fn forwarded_for(&self, node_id: NodeId) -> Vec<Delivery> {
        self.forwarded
            .lock()
            .expect("peer forwarder poisoned")
            .get(&node_id)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl PeerForwarder for CountingBatchPeerForwarder {
    async fn forward_delivery(&self, node_id: u64, delivery: Delivery) -> anyhow::Result<bool> {
        self.single_calls.fetch_add(1, Ordering::SeqCst);
        self.forwarded
            .lock()
            .expect("peer forwarder poisoned")
            .entry(node_id)
            .or_default()
            .push(delivery);
        Ok(true)
    }

    async fn forward_deliveries(
        &self,
        node_id: u64,
        deliveries: Vec<Delivery>,
    ) -> anyhow::Result<Vec<Delivery>> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        self.forwarded
            .lock()
            .expect("peer forwarder poisoned")
            .entry(node_id)
            .or_default()
            .extend(deliveries);
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct CountingBatchInboxStore {
    inner: MemoryInboxStore,
    append_message_calls: AtomicUsize,
    append_messages_calls: AtomicUsize,
}

impl CountingBatchInboxStore {
    fn append_message_calls(&self) -> usize {
        self.append_message_calls.load(Ordering::SeqCst)
    }

    fn append_messages_calls(&self) -> usize {
        self.append_messages_calls.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Default)]
struct CountingSessionDirectory {
    inner: SessionDictHandle,
    lookup_session_calls: Arc<AtomicUsize>,
}

impl CountingSessionDirectory {
    fn lookup_session_calls(&self) -> usize {
        self.lookup_session_calls.load(Ordering::SeqCst)
    }

    fn reset_lookup_session_calls(&self) {
        self.lookup_session_calls.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl SessionDirectory for CountingSessionDirectory {
    async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.register(record).await
    }

    async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.unregister(session_id).await
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.lookup_identity(identity).await
    }

    async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        self.lookup_session_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.lookup_session(session_id).await
    }

    async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>> {
        self.inner.list_sessions(tenant_id).await
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        self.inner.session_count().await
    }
}

#[async_trait]
impl InboxStore for CountingBatchInboxStore {
    async fn append_message(&self, message: &greenmqtt_core::OfflineMessage) -> anyhow::Result<()> {
        self.append_message_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.append_message(message).await
    }

    async fn append_messages(
        &self,
        messages: &[greenmqtt_core::OfflineMessage],
    ) -> anyhow::Result<()> {
        self.append_messages_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.append_messages(messages).await
    }

    async fn peek_messages(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::OfflineMessage>> {
        self.inner.peek_messages(session_id).await
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<greenmqtt_core::OfflineMessage>> {
        self.inner.list_all_messages().await
    }

    async fn load_messages(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::OfflineMessage>> {
        self.inner.load_messages(session_id).await
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        self.inner.count_messages().await
    }
}

#[derive(Default)]
struct CountingDistRouter {
    inner: DistHandle,
    remove_route_calls: AtomicUsize,
    list_routes_calls: AtomicUsize,
    list_session_routes_calls: AtomicUsize,
    count_session_routes_calls: AtomicUsize,
    reassign_session_routes_calls: AtomicUsize,
}

impl CountingDistRouter {
    fn remove_route_calls(&self) -> usize {
        self.remove_route_calls.load(Ordering::SeqCst)
    }

    fn list_routes_calls(&self) -> usize {
        self.list_routes_calls.load(Ordering::SeqCst)
    }

    fn list_session_routes_calls(&self) -> usize {
        self.list_session_routes_calls.load(Ordering::SeqCst)
    }

    fn reassign_session_routes_calls(&self) -> usize {
        self.reassign_session_routes_calls.load(Ordering::SeqCst)
    }

    fn count_session_routes_calls(&self) -> usize {
        self.count_session_routes_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DistRouter for CountingDistRouter {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        self.inner.add_route(route).await
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        self.remove_route_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.remove_route(route).await
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        self.inner.remove_session_routes(session_id).await
    }

    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        self.reassign_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .reassign_session_routes(session_id, node_id)
            .await
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_session_routes(session_id).await
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_routes(session_id).await
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &TopicName,
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

#[derive(Default)]
struct CountingRetainService {
    inner: RetainHandle,
    lookup_topic_calls: AtomicUsize,
    match_topic_calls: AtomicUsize,
}

impl CountingRetainService {
    fn lookup_topic_calls(&self) -> usize {
        self.lookup_topic_calls.load(Ordering::SeqCst)
    }

    fn match_topic_calls(&self) -> usize {
        self.match_topic_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RetainService for CountingRetainService {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        self.inner.retain(message).await
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        self.inner.list_tenant_retained(tenant_id).await
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        self.lookup_topic_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.lookup_topic(tenant_id, topic).await
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        self.match_topic_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.match_topic(tenant_id, topic_filter).await
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        self.inner.retained_count().await
    }
}

#[derive(Default)]
struct TestPeerRegistry {
    endpoints: RwLock<BTreeMap<u64, String>>,
}

#[async_trait]
impl PeerRegistry for TestPeerRegistry {
    fn list_peer_nodes(&self) -> Vec<u64> {
        self.endpoints
            .read()
            .expect("peer registry poisoned")
            .keys()
            .copied()
            .collect()
    }

    fn list_peer_endpoints(&self) -> BTreeMap<u64, String> {
        self.endpoints
            .read()
            .expect("peer registry poisoned")
            .clone()
    }

    fn remove_peer_node(&self, node_id: u64) -> bool {
        self.endpoints
            .write()
            .expect("peer registry poisoned")
            .remove(&node_id)
            .is_some()
    }

    async fn add_peer_node(&self, node_id: u64, endpoint: String) -> anyhow::Result<()> {
        self.endpoints
            .write()
            .expect("peer registry poisoned")
            .insert(node_id, endpoint);
        Ok(())
    }
}

#[test]
fn round_robin_balancer_redirects_to_peer_endpoints() {
    let peers = Arc::new(TestPeerRegistry::default());
    peers
        .endpoints
        .write()
        .expect("peer registry poisoned")
        .extend(BTreeMap::from([
            (2, "http://127.0.0.1:50062".into()),
            (3, "http://127.0.0.1:50063".into()),
        ]));
    let balancer = RoundRobinBalancer::new(1, peers, false);
    let identity = ClientIdentity {
        tenant_id: "demo".into(),
        user_id: "alice".into(),
        client_id: "sub".into(),
    };

    let first = balancer.need_redirect(&identity).unwrap();
    let second = balancer.need_redirect(&identity).unwrap();
    assert_eq!(first.server_reference, "http://127.0.0.1:50062");
    assert_eq!(second.server_reference, "http://127.0.0.1:50063");
    assert!(!first.permanent);
}

#[tokio::test]
async fn persistent_session_replays_offline_messages() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    assert!(!subscriber.session_present);
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

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker
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
    assert_eq!(outcome.offline_enqueues, 1);

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    assert!(resumed.session_present);
    assert_eq!(resumed.offline_messages.len(), 1);
    assert_eq!(resumed.offline_messages[0].payload, b"up".to_vec());
}

#[tokio::test]
async fn connect_without_replay_keeps_replay_in_state_service_until_requested() {
    let subscription_store = Arc::new(MemorySubscriptionStore::default());
    let inbox_store = Arc::new(MemoryInboxStore::default());
    let inflight_store = Arc::new(MemoryInflightStore::default());
    let session_store = Arc::new(MemorySessionStore::default());
    let broker = BrokerRuntime::with_plugins(
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
        Arc::new(
            PersistentSessionDictHandle::open(session_store)
                .await
                .unwrap(),
        ),
        Arc::new(DistHandle::default()),
        Arc::new(PersistentInboxHandle::open(
            subscription_store,
            inbox_store,
            inflight_store,
        )),
        Arc::new(RetainHandle::default()),
    );

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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

    let resumed = broker
        .connect_without_replay(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(resumed.session_present);
    assert!(resumed.offline_messages.is_empty());
    assert!(resumed.inflight_messages.is_empty());

    let (offline_messages, inflight_messages) = broker
        .load_session_replay(&resumed.session.session_id)
        .await
        .unwrap();
    assert_eq!(offline_messages.len(), 1);
    assert_eq!(offline_messages[0].payload, b"up".to_vec());
    assert!(inflight_messages.is_empty());
}

#[tokio::test]
async fn connect_without_replay_keeps_inflight_in_state_service_until_requested() {
    let subscription_store = Arc::new(MemorySubscriptionStore::default());
    let inbox_store = Arc::new(MemoryInboxStore::default());
    let inflight_store = Arc::new(MemoryInflightStore::default());
    let session_store = Arc::new(MemorySessionStore::default());
    let broker = BrokerRuntime::with_plugins(
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
        Arc::new(
            PersistentSessionDictHandle::open(session_store)
                .await
                .unwrap(),
        ),
        Arc::new(DistHandle::default()),
        Arc::new(PersistentInboxHandle::open(
            subscription_store,
            inbox_store,
            inflight_store,
        )),
        Arc::new(RetainHandle::default()),
    );

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    broker
        .inbox
        .stage_inflight(InflightMessage {
            tenant_id: "tenant-a".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 42,
            topic: "devices/d1/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "pub".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();

    let resumed = broker
        .connect_without_replay(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(resumed.session_present);
    assert!(resumed.offline_messages.is_empty());
    assert!(resumed.inflight_messages.is_empty());

    let (offline_messages, inflight_messages) = broker
        .load_session_replay(&resumed.session.session_id)
        .await
        .unwrap();
    assert!(offline_messages.is_empty());
    assert_eq!(inflight_messages.len(), 1);
    assert_eq!(inflight_messages[0].packet_id, 42);
    assert_eq!(inflight_messages[0].phase, InflightPhase::Publish);
    assert_eq!(inflight_messages[0].payload, b"inflight".to_vec());
}

#[tokio::test]
async fn cancel_delayed_will_cleans_local_generation_state() {
    let broker = Arc::new(test_broker());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
        .schedule_delayed_will(
            &session.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"delayed".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            30,
        )
        .await
        .unwrap();
    assert_eq!(broker.pending_delayed_will_count(), 1);

    broker.cancel_delayed_will_for_identity(&session.session.identity);
    assert_eq!(broker.pending_delayed_will_count(), 0);
}

#[tokio::test]
async fn repair_orphan_session_state_cancels_pending_delayed_will() {
    let broker = Arc::new(test_broker());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "orphan-delayed".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    broker
        .schedule_delayed_will(
            &session.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"delayed".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            30,
        )
        .await
        .unwrap();
    assert_eq!(broker.pending_delayed_will_count(), 1);

    broker
        .sessiondict
        .unregister(&session.session.session_id)
        .await
        .unwrap();
    broker
        .repair_orphan_session_state(&session.session.session_id)
        .await
        .unwrap();
    assert_eq!(broker.pending_delayed_will_count(), 0);
}

#[tokio::test]
async fn schedule_delayed_will_replaces_previous_generation_for_same_identity() {
    let broker = Arc::new(test_broker());
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
        .schedule_delayed_will(
            &session.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"first".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            30,
        )
        .await
        .unwrap();
    let key = BrokerRuntime::<AllowAllAuth>::will_identity_key(&session.session.identity);
    let first_generation = broker
        .pending_will_generations
        .get_copied(&key)
        .expect("first delayed will generation");
    assert_eq!(broker.pending_delayed_will_count(), 1);

    broker
        .schedule_delayed_will(
            &session.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"second".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            30,
        )
        .await
        .unwrap();
    let second_generation = broker
        .pending_will_generations
        .get_copied(&key)
        .expect("second delayed will generation");
    assert!(second_generation > first_generation);
    assert_eq!(broker.pending_delayed_will_count(), 1);

    broker.cancel_delayed_will_for_identity(&session.session.identity);
    assert_eq!(broker.pending_delayed_will_count(), 0);
}

#[tokio::test]
async fn publish_uses_batch_offline_enqueue_path_for_multiple_persistent_sessions() {
    let subscription_store = Arc::new(MemorySubscriptionStore::default());
    let inbox_store = Arc::new(CountingBatchInboxStore::default());
    let inflight_store = Arc::new(MemoryInflightStore::default());
    let broker = BrokerRuntime::with_plugins(
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
        Arc::new(PersistentInboxHandle::open(
            subscription_store,
            inbox_store.clone(),
            inflight_store,
        )),
        Arc::new(RetainHandle::default()),
    );

    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub-1".into(),
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
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "sub-2".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    for subscriber in [&first, &second] {
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
    }

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-pub".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker
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

    assert_eq!(outcome.offline_enqueues, 2);
    assert_eq!(inbox_store.append_messages_calls(), 1);
    assert_eq!(inbox_store.append_message_calls(), 0);
    assert_eq!(broker.inbox.offline_count().await.unwrap(), 2);
}

#[tokio::test]
async fn publish_materializes_route_sessions_with_single_lookup_per_session() {
    let sessiondict = Arc::new(CountingSessionDirectory::default());
    let broker = BrokerRuntime::with_plugins(
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
        sessiondict.clone(),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
                tenant_id: "tenant-a".into(),
                user_id: "user-pub".into(),
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
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/d1/state",
            1,
            Some(9),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    sessiondict.reset_lookup_session_calls();

    let outcome = broker
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

    assert_eq!(outcome.matched_routes, 1);
    assert_eq!(sessiondict.lookup_session_calls(), 1);
}

#[test]
fn session_state_and_delivery_queue_share_same_shard_rule() {
    let sessions = ShardedLocalSessions::default();
    let deliveries = ShardedLocalDeliveries::default();
    for session_id in [
        "s1",
        "tenant-a:user-a:client-a:1",
        "remote-42",
        "pubrel-queue",
    ] {
        assert_eq!(
            sessions.shard_index(session_id),
            deliveries.shard_index(session_id)
        );
        assert_eq!(
            sessions.shard_index(session_id),
            shard_index_for_session(session_id, 32)
        );
    }
}

#[tokio::test]
async fn sharded_local_deliveries_keep_sessions_isolated_under_concurrency() {
    let deliveries = Arc::new(ShardedLocalDeliveries::default());
    let mut different_pair = None;
    'outer: for left in 0..128u32 {
        for right in (left + 1)..128u32 {
            let left_id = format!("s-left-{left}");
            let right_id = format!("s-right-{right}");
            if shard_index_for_session(&left_id, 32) != shard_index_for_session(&right_id, 32) {
                different_pair = Some((left_id, right_id));
                break 'outer;
            }
        }
    }
    let (left_session, right_session) = different_pair.expect("expected different shard pair");

    let mut tasks = Vec::new();
    for index in 0..32u16 {
        let deliveries = deliveries.clone();
        let left_session = left_session.clone();
        tasks.push(tokio::spawn(async move {
            let queue_key = left_session.clone();
            deliveries.push(
                &queue_key,
                Delivery {
                    tenant_id: "t1".into(),
                    session_id: left_session,
                    topic: format!("devices/{index}/state"),
                    payload: b"left".to_vec().into(),
                    qos: 1,
                    retain: false,
                    from_session_id: "src".into(),
                    properties: PublishProperties::default(),
                },
            );
        }));
    }
    for index in 0..24u16 {
        let deliveries = deliveries.clone();
        let right_session = right_session.clone();
        tasks.push(tokio::spawn(async move {
            let queue_key = right_session.clone();
            deliveries.push(
                &queue_key,
                Delivery {
                    tenant_id: "t1".into(),
                    session_id: right_session,
                    topic: format!("alerts/{index}"),
                    payload: b"right".to_vec().into(),
                    qos: 1,
                    retain: false,
                    from_session_id: "src".into(),
                    properties: PublishProperties::default(),
                },
            );
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(deliveries.pending_len(&left_session), 32);
    assert_eq!(deliveries.pending_len(&right_session), 24);

    let left = deliveries.drain(&left_session);
    let right = deliveries.drain(&right_session);
    assert_eq!(left.len(), 32);
    assert_eq!(right.len(), 24);
    assert!(left
        .iter()
        .all(|delivery| delivery.session_id == left_session));
    assert!(right
        .iter()
        .all(|delivery| delivery.session_id == right_session));
}

#[test]
fn shard_distribution_regression_spreads_session_hotspots() {
    let mut counts = std::collections::BTreeMap::<usize, usize>::new();
    for index in 0..512usize {
        let session_id = format!(
            "tenant-a:user-{}:client-{}:{}",
            index % 17,
            index % 37,
            index
        );
        let shard = shard_index_for_session(&session_id, 32);
        *counts.entry(shard).or_default() += 1;
    }

    assert!(
        counts.len() >= 24,
        "expected wide shard spread, got {}",
        counts.len()
    );
    let max_load = counts.values().copied().max().unwrap_or(0);
    let min_load = counts.values().copied().min().unwrap_or(0);
    assert!(max_load <= 32, "shard skew too high: {max_load}");
    assert!(min_load >= 8, "shard spread too sparse: {min_load}");
}

#[test]
fn will_generation_shards_follow_same_distribution_rule() {
    let wills = ShardedWillGenerations::default();
    for key in [
        "tenant-a:user-a:client-a",
        "tenant-a:user-b:client-b",
        "tenant-b:user-c:client-c",
        "tenant-z:user-z:client-z",
    ] {
        assert_eq!(wills.shard_index(key), shard_index_for_session(key, 32));
    }
}

#[tokio::test]
async fn lifecycle_hooks_fire_for_connect_subscribe_unsubscribe_disconnect() {
    let hook = RecordingHook::default();
    let events = hook.events.clone();
    let broker = BrokerRuntime::with_plugins(
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
        greenmqtt_plugin_api::AllowAllAcl,
        hook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "hooked".into(),
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
            &reply.session.session_id,
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
    broker
        .unsubscribe(&reply.session.session_id, "devices/#")
        .await
        .unwrap();
    broker.disconnect(&reply.session.session_id).await.unwrap();

    let events = events.lock().expect("hook poisoned").clone();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0], format!("connect:{}", reply.session.session_id));
    assert_eq!(events[1], "subscribe:devices/#");
    assert_eq!(events[2], "unsubscribe:devices/#");
    assert_eq!(
        events[3],
        format!("disconnect:{}", reply.session.session_id)
    );
}

#[tokio::test]
async fn unsubscribing_missing_subscription_returns_false_without_hook() {
    let hook = RecordingHook::default();
    let events = hook.events.clone();
    let broker = BrokerRuntime::with_plugins(
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
        greenmqtt_plugin_api::AllowAllAcl,
        hook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "missing-unsub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(!broker
        .unsubscribe(&reply.session.session_id, "devices/#")
        .await
        .unwrap());

    let events = events.lock().expect("hook poisoned").clone();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], format!("connect:{}", reply.session.session_id));
}

#[tokio::test]
async fn unsubscribing_missing_subscription_skips_route_delete() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = BrokerRuntime::with_plugins(
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
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        dist.clone(),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "missing-unsub-route".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(!broker
        .unsubscribe(&reply.session.session_id, "devices/#")
        .await
        .unwrap());
    assert_eq!(dist.remove_route_calls(), 0);
}

#[test]
fn desired_peer_registry_replays_latest_peer_audit_state() {
    let entries = [
        AdminAuditEntry {
            seq: 1,
            timestamp_ms: 1,
            action: "upsert_peer".into(),
            target: "peers".into(),
            details: BTreeMap::from([
                ("node_id".into(), "2".into()),
                ("rpc_addr".into(), "http://127.0.0.1:50062".into()),
            ]),
        },
        AdminAuditEntry {
            seq: 2,
            timestamp_ms: 2,
            action: "upsert_peer".into(),
            target: "peers".into(),
            details: BTreeMap::from([
                ("node_id".into(), "3".into()),
                ("rpc_addr".into(), "http://127.0.0.1:50063".into()),
            ]),
        },
        AdminAuditEntry {
            seq: 3,
            timestamp_ms: 3,
            action: "delete_peer".into(),
            target: "peers".into(),
            details: BTreeMap::from([("node_id".into(), "2".into())]),
        },
    ];
    assert_eq!(
        desired_peer_registry(entries.iter()),
        BTreeMap::from([(3, "http://127.0.0.1:50063".to_string())])
    );
}

#[test]
fn desired_peer_registry_prefers_latest_endpoint_for_same_node() {
    let entries = [
        AdminAuditEntry {
            seq: 1,
            timestamp_ms: 1,
            action: "upsert_peer".into(),
            target: "peers".into(),
            details: BTreeMap::from([
                ("node_id".into(), "2".into()),
                ("rpc_addr".into(), "http://127.0.0.1:50062".into()),
            ]),
        },
        AdminAuditEntry {
            seq: 2,
            timestamp_ms: 2,
            action: "upsert_peer".into(),
            target: "sessions".into(),
            details: BTreeMap::from([
                ("node_id".into(), "2".into()),
                ("rpc_addr".into(), "http://127.0.0.1:59999".into()),
            ]),
        },
        AdminAuditEntry {
            seq: 3,
            timestamp_ms: 3,
            action: "upsert_peer".into(),
            target: "peers".into(),
            details: BTreeMap::from([
                ("node_id".into(), "2".into()),
                ("rpc_addr".into(), "http://127.0.0.1:50072".into()),
            ]),
        },
        AdminAuditEntry {
            seq: 4,
            timestamp_ms: 4,
            action: "upsert_peer".into(),
            target: "peers".into(),
            details: BTreeMap::from([("node_id".into(), "3".into())]),
        },
    ];

    assert_eq!(
        desired_peer_registry(entries.iter()),
        BTreeMap::from([(2, "http://127.0.0.1:50072".to_string())])
    );
}

#[tokio::test]
async fn restore_peer_registry_replays_persisted_peer_audit_when_registry_is_empty() {
    let audit_log_path = temp_audit_log_path("peer-restore");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    broker.record_admin_audit(
        "upsert_peer",
        "peers",
        BTreeMap::from([
            ("node_id".into(), "2".into()),
            ("rpc_addr".into(), "http://127.0.0.1:50062".into()),
        ]),
    );
    drop(broker);

    let restored = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let peers = TestPeerRegistry::default();
    let restored_count = restored.restore_peer_registry(&peers).await.unwrap();
    assert_eq!(restored_count, 1);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(2, "http://127.0.0.1:50062".to_string())])
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn restore_peer_registry_does_not_override_existing_registry_entries() {
    let audit_log_path = temp_audit_log_path("peer-restore-nonempty");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    broker.record_admin_audit(
        "upsert_peer",
        "peers",
        BTreeMap::from([
            ("node_id".into(), "2".into()),
            ("rpc_addr".into(), "http://127.0.0.1:50062".into()),
        ]),
    );
    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(9, "http://127.0.0.1:50069".into())
        .await
        .unwrap();
    let restored_count = broker.restore_peer_registry(&peers).await.unwrap();
    assert_eq!(restored_count, 0);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(9, "http://127.0.0.1:50069".to_string())])
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn sync_peer_registry_removes_stale_entries_and_replays_latest_audit_state() {
    let audit_log_path = temp_audit_log_path("peer-sync-remove");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    broker.record_admin_audit(
        "upsert_peer",
        "peers",
        BTreeMap::from([
            ("node_id".into(), "2".into()),
            ("rpc_addr".into(), "http://127.0.0.1:50062".into()),
        ]),
    );
    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(9, "http://127.0.0.1:50069".into())
        .await
        .unwrap();

    let changed = broker.sync_peer_registry(&peers).await.unwrap();
    assert_eq!(changed, 2);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(2, "http://127.0.0.1:50062".to_string())])
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn sync_peer_registry_updates_existing_endpoint_when_audit_changes() {
    let audit_log_path = temp_audit_log_path("peer-sync-update");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    broker.record_admin_audit(
        "upsert_peer",
        "peers",
        BTreeMap::from([
            ("node_id".into(), "2".into()),
            ("rpc_addr".into(), "http://127.0.0.1:50062".into()),
        ]),
    );
    broker.record_admin_audit(
        "upsert_peer",
        "peers",
        BTreeMap::from([
            ("node_id".into(), "2".into()),
            ("rpc_addr".into(), "http://127.0.0.1:50072".into()),
        ]),
    );
    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(2, "http://127.0.0.1:50062".into())
        .await
        .unwrap();

    let changed = broker.sync_peer_registry(&peers).await.unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(2, "http://127.0.0.1:50072".to_string())])
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn upsert_peer_registry_entry_updates_registry_and_audit_log() {
    let audit_log_path = temp_audit_log_path("peer-upsert");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let peers = TestPeerRegistry::default();

    let changed = broker
        .upsert_peer_registry_entry(&peers, 7, "http://127.0.0.1:50077".into())
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(7, "http://127.0.0.1:50077".to_string())])
    );
    assert_eq!(
        desired_peer_registry(broker.list_admin_audit(None).iter()),
        BTreeMap::from([(7, "http://127.0.0.1:50077".to_string())])
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn delete_peer_registry_entry_updates_registry_and_audit_log() {
    let audit_log_path = temp_audit_log_path("peer-delete");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(7, "http://127.0.0.1:50077".into())
        .await
        .unwrap();
    broker.record_admin_audit(
        "upsert_peer",
        "peers",
        BTreeMap::from([
            ("node_id".into(), "7".into()),
            ("rpc_addr".into(), "http://127.0.0.1:50077".into()),
        ]),
    );

    let removed = broker.delete_peer_registry_entry(&peers, 7);
    assert!(removed);
    assert!(peers.list_peer_endpoints().is_empty());
    assert!(
        desired_peer_registry(broker.admin_audit.read().expect("broker poisoned").iter())
            .is_empty()
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn suspect_and_confirm_peer_registry_entry_round_trip_membership_state() {
    let audit_log_path = temp_audit_log_path("peer-suspect-confirm");
    let broker = BrokerRuntime::with_plugins(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let peers = TestPeerRegistry::default();
    broker
        .upsert_peer_registry_entry(&peers, 7, "http://127.0.0.1:50077".into())
        .await
        .unwrap();

    let removed = broker.suspect_peer_registry_entry(&peers, 7);
    assert!(removed);
    assert!(peers.list_peer_endpoints().is_empty());
    assert!(
        desired_peer_registry(broker.admin_audit.read().expect("broker poisoned").iter())
            .is_empty()
    );
    assert_eq!(
        desired_peer_membership(broker.admin_audit.read().expect("broker poisoned").iter())
            .get(&7)
            .expect("suspected peer should keep endpoint state")
            .endpoint
            .as_deref(),
        Some("http://127.0.0.1:50077")
    );

    let restored = broker.confirm_peer_registry_entry(&peers, 7).await.unwrap();
    assert!(restored);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(7, "http://127.0.0.1:50077".to_string())])
    );
    assert_eq!(
        desired_peer_registry(broker.admin_audit.read().expect("broker poisoned").iter()),
        BTreeMap::from([(7, "http://127.0.0.1:50077".to_string())])
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn rebalance_sessions_to_registry_reassigns_offline_sessions_from_missing_peer() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "rebalance-sub".into(),
            },
            node_id: 7,
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(8, "http://127.0.0.1:50078".into())
        .await
        .unwrap();
    let expected_target = placement_node_for_session(
        &subscriber.session.session_id,
        &placement_candidates(1, [8]),
    );

    let outcome = broker
        .rebalance_sessions_to_registry(&peers, Some("tenant-a"))
        .await
        .unwrap();
    assert_eq!(outcome.updated_sessions, 1);
    assert_eq!(outcome.updated_routes, 1);

    let session = broker
        .sessiondict
        .lookup_session(&subscriber.session.session_id)
        .await
        .unwrap()
        .expect("session should remain registered");
    assert_eq!(session.node_id, expected_target);
    let route = broker
        .dist
        .list_routes(Some("tenant-a"))
        .await
        .unwrap()
        .into_iter()
        .find(|route| route.session_id == subscriber.session.session_id)
        .expect("route must exist");
    assert_eq!(route.node_id, expected_target);

    let rebalance_audit = broker
        .list_admin_audit_filtered(
            Some("reassign_session"),
            Some("sessions"),
            None,
            None,
            None,
            Some("reason"),
            Some("registry_rebalance"),
            Some(1),
        )
        .into_iter()
        .next()
        .expect("rebalance audit should exist");
    assert_eq!(
        rebalance_audit.details.get("session_id"),
        Some(&subscriber.session.session_id)
    );
    assert_eq!(
        rebalance_audit.details.get("from_node_id"),
        Some(&"7".to_string())
    );
    assert_eq!(
        rebalance_audit.details.get("to_node_id"),
        Some(&expected_target.to_string())
    );
}

#[test]
fn admin_audit_persists_and_restores_from_log() {
    let audit_log_path = temp_audit_log_path("persist");
    let broker = BrokerRuntime::new(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        SessionDictHandle::default(),
        DistHandle::default(),
        InboxHandle::default(),
        RetainHandle::default(),
    );
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
    drop(broker);

    let restored = BrokerRuntime::new(
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
            audit_log_path: Some(audit_log_path.clone()),
        },
        AllowAllAuth,
        SessionDictHandle::default(),
        DistHandle::default(),
        InboxHandle::default(),
        RetainHandle::default(),
    );
    let restored_entries = restored.list_admin_audit(None);
    assert_eq!(restored_entries.len(), 2);
    assert_eq!(restored_entries[0].action, "purge_offline");
    assert_eq!(restored_entries[1].action, "disconnect_session");

    restored.record_admin_audit(
        "delete_retain",
        "retain",
        BTreeMap::from([("topic".into(), "devices/d1/state".into())]),
    );
    let entries = restored.list_admin_audit(None);
    assert_eq!(entries[0].seq, 3);
    assert_eq!(entries[0].action, "delete_retain");

    let _ = std::fs::remove_file(&audit_log_path);
    let _ = std::fs::remove_dir_all(audit_log_path.parent().unwrap());
}

#[tokio::test]
async fn transient_session_disconnect_removes_routes() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
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

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker
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
    assert_eq!(outcome.matched_routes, 0);
}

#[tokio::test]
async fn persistent_connect_over_transient_preserves_new_session_record() {
    let broker = test_broker();
    let transient = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "client-a".into(),
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
            &transient.session.session_id,
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

    let persistent = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "client-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert_eq!(persistent.session.session_id, transient.session.session_id);
    let record = broker
        .sessiondict
        .lookup_session(&persistent.session.session_id)
        .await
        .unwrap()
        .expect("persistent session should remain registered");
    assert!(matches!(record.kind, SessionKind::Persistent));
    assert!(broker
        .inbox
        .list_subscriptions(&persistent.session.session_id)
        .await
        .unwrap()
        .is_empty());
    assert!(broker
        .dist
        .list_routes(Some("tenant-a"))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn reassign_session_node_updates_sessiondict_and_routes_for_offline_session() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let outcome = broker
        .reassign_session_node(&subscriber.session.session_id, 7)
        .await
        .unwrap();
    assert_eq!(outcome.updated_sessions, 1);
    assert_eq!(outcome.updated_routes, 1);

    let session = broker
        .sessiondict
        .lookup_session(&subscriber.session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.node_id, 7);
    let route = broker
        .dist
        .list_routes(Some("tenant-a"))
        .await
        .unwrap()
        .into_iter()
        .find(|route| route.session_id == subscriber.session.session_id)
        .expect("route must exist");
    assert_eq!(route.node_id, 7);
}

#[tokio::test]
async fn reassign_session_node_uses_dist_reassign_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = BrokerRuntime::with_plugins(
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
        dist.clone(),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let outcome = broker
        .reassign_session_node(&subscriber.session.session_id, 7)
        .await
        .unwrap();
    assert_eq!(outcome.updated_sessions, 1);
    assert_eq!(outcome.updated_routes, 1);
    assert_eq!(dist.reassign_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert_eq!(dist.remove_route_calls(), 0);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn reassign_session_node_same_target_uses_session_route_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = BrokerRuntime::with_plugins(
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
        dist.clone(),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let outcome = broker
        .reassign_session_node(&subscriber.session.session_id, 2)
        .await
        .unwrap();
    assert_eq!(outcome.updated_sessions, 0);
    assert_eq!(outcome.updated_routes, 1);
    assert_eq!(dist.count_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert_eq!(dist.reassign_session_routes_calls(), 0);
    assert_eq!(dist.remove_route_calls(), 0);
}

#[tokio::test]
async fn reassign_session_node_rejects_locally_online_session() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let error = broker
        .reassign_session_node(&subscriber.session.session_id, 9)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("cannot reassign a locally online session"));
}

#[tokio::test]
async fn fence_current_session_disconnects_only_matching_epoch() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "fence-sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let stale_fence = broker
        .fence_current_session(
            &subscriber.session.session_id,
            subscriber.local_session_epoch + 1,
            Some(30),
        )
        .await
        .unwrap();
    assert!(!stale_fence);
    assert!(broker.local_session_epoch_matches(
        &subscriber.session.session_id,
        subscriber.local_session_epoch
    ));

    let fenced = broker
        .fence_current_session(
            &subscriber.session.session_id,
            subscriber.local_session_epoch,
            Some(30),
        )
        .await
        .unwrap();
    assert!(fenced);
    assert!(!broker.local_session_epoch_matches(
        &subscriber.session.session_id,
        subscriber.local_session_epoch
    ));
    assert!(broker.list_local_sessions().await.unwrap().is_empty());

    let persisted = broker
        .sessiondict
        .lookup_session(&subscriber.session.session_id)
        .await
        .unwrap()
        .expect("persistent session should remain registered after fencing");
    assert_eq!(persisted.session_expiry_interval_secs, Some(30));
    assert!(persisted.expires_at_ms.is_some());

    let audit = broker
        .list_admin_audit_filtered(
            Some("fence_session"),
            Some("sessions"),
            None,
            None,
            None,
            Some("session_id"),
            Some(&subscriber.session.session_id),
            Some(1),
        )
        .into_iter()
        .next()
        .expect("fence audit should exist");
    assert_eq!(
        audit.details.get("session_epoch"),
        Some(&subscriber.local_session_epoch.to_string())
    );
    assert_eq!(
        audit.details.get("expiry_override_secs"),
        Some(&"30".to_string())
    );
}

#[tokio::test]
async fn failover_sessions_from_node_reassigns_only_failed_peer_sessions() {
    let broker = test_broker();

    let failed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "failed-sub".into(),
            },
            node_id: 7,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &failed.session.session_id,
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
    broker.disconnect(&failed.session.session_id).await.unwrap();

    let healthy = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "healthy-sub".into(),
            },
            node_id: 8,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &healthy.session.session_id,
            "devices/+/healthy",
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
        .disconnect(&healthy.session.session_id)
        .await
        .unwrap();

    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(8, "http://127.0.0.1:50078".into())
        .await
        .unwrap();
    peers
        .add_peer_node(9, "http://127.0.0.1:50079".into())
        .await
        .unwrap();
    let expected_target = placement_node_for_session(
        &failed.session.session_id,
        &placement_candidates_excluding(1, [8, 9], 7),
    );

    let outcome = broker
        .failover_sessions_from_node(&peers, 7, Some("tenant-a"))
        .await
        .unwrap();
    assert_eq!(outcome.updated_sessions, 1);
    assert_eq!(outcome.updated_routes, 1);

    let failed_after = broker
        .sessiondict
        .lookup_session(&failed.session.session_id)
        .await
        .unwrap()
        .expect("failed-node session should remain registered");
    assert_eq!(failed_after.node_id, expected_target);

    let healthy_after = broker
        .sessiondict
        .lookup_session(&healthy.session.session_id)
        .await
        .unwrap()
        .expect("healthy session should remain registered");
    assert_eq!(healthy_after.node_id, 8);

    let routes = broker.dist.list_routes(Some("tenant-a")).await.unwrap();
    let failed_route = routes
        .iter()
        .find(|route| route.session_id == failed.session.session_id)
        .expect("failed route should exist");
    assert_eq!(failed_route.node_id, expected_target);
    let healthy_route = routes
        .iter()
        .find(|route| route.session_id == healthy.session.session_id)
        .expect("healthy route should exist");
    assert_eq!(healthy_route.node_id, 8);

    let audit = broker
        .list_admin_audit_filtered(
            Some("reassign_session"),
            Some("sessions"),
            None,
            None,
            None,
            Some("reason"),
            Some("node_failover"),
            Some(1),
        )
        .into_iter()
        .next()
        .expect("failover audit should exist");
    assert_eq!(
        audit.details.get("session_id"),
        Some(&failed.session.session_id)
    );
    assert_eq!(audit.details.get("from_node_id"), Some(&"7".to_string()));
    assert_eq!(
        audit.details.get("to_node_id"),
        Some(&expected_target.to_string())
    );
}

#[tokio::test]
async fn drain_sessions_from_node_to_target_reassigns_only_source_sessions() {
    let broker = test_broker();

    let draining = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "drain-sub".into(),
            },
            node_id: 7,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &draining.session.session_id,
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
        .disconnect(&draining.session.session_id)
        .await
        .unwrap();

    let healthy = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "healthy-sub".into(),
            },
            node_id: 8,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &healthy.session.session_id,
            "devices/+/healthy",
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
        .disconnect(&healthy.session.session_id)
        .await
        .unwrap();

    let peers = TestPeerRegistry::default();
    peers
        .add_peer_node(8, "http://127.0.0.1:50078".into())
        .await
        .unwrap();
    peers
        .add_peer_node(9, "http://127.0.0.1:50079".into())
        .await
        .unwrap();

    let outcome = broker
        .drain_sessions_from_node_to_target(&peers, 7, 9, Some("tenant-a"))
        .await
        .unwrap();
    assert_eq!(outcome.updated_sessions, 1);
    assert_eq!(outcome.updated_routes, 1);

    let draining_after = broker
        .sessiondict
        .lookup_session(&draining.session.session_id)
        .await
        .unwrap()
        .expect("drained session should remain registered");
    assert_eq!(draining_after.node_id, 9);

    let healthy_after = broker
        .sessiondict
        .lookup_session(&healthy.session.session_id)
        .await
        .unwrap()
        .expect("healthy session should remain registered");
    assert_eq!(healthy_after.node_id, 8);

    let routes = broker.dist.list_routes(Some("tenant-a")).await.unwrap();
    let drained_route = routes
        .iter()
        .find(|route| route.session_id == draining.session.session_id)
        .expect("drained route should exist");
    assert_eq!(drained_route.node_id, 9);
    let healthy_route = routes
        .iter()
        .find(|route| route.session_id == healthy.session.session_id)
        .expect("healthy route should exist");
    assert_eq!(healthy_route.node_id, 8);

    let audit = broker
        .list_admin_audit_filtered(
            Some("reassign_session"),
            Some("sessions"),
            None,
            None,
            None,
            Some("reason"),
            Some("node_drain"),
            Some(1),
        )
        .into_iter()
        .next()
        .expect("drain audit should exist");
    assert_eq!(
        audit.details.get("session_id"),
        Some(&draining.session.session_id)
    );
    assert_eq!(audit.details.get("from_node_id"), Some(&"7".to_string()));
    assert_eq!(audit.details.get("to_node_id"), Some(&"9".to_string()));
}

#[tokio::test]
async fn retained_message_is_returned_on_subscribe() {
    let broker = test_broker();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
            &publisher.session.session_id,
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

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let retained = broker
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
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].payload, b"retained".to_vec());
}

#[tokio::test]
async fn topic_rewrite_hook_rewrites_publish_before_route_match() {
    let broker = BrokerRuntime::with_plugins(
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
            tenant_id: Some("tenant-a".into()),
            topic_filter: "devices/+/raw".into(),
            rewrite_to: "devices/{1}/normalized".into(),
        }]),
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
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
            "devices/+/normalized",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/raw".into(),
                payload: b"rewritten".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(outcome.matched_routes, 1);
    assert_eq!(outcome.online_deliveries, 1);

    let deliveries = broker
        .drain_deliveries(&subscriber.session.session_id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].topic, "devices/d1/normalized");
    assert_eq!(deliveries[0].payload, b"rewritten".to_vec());
}

#[tokio::test]
async fn exact_topic_subscribe_uses_retain_lookup_fast_path() {
    let retain = Arc::new(CountingRetainService::default());
    let broker = BrokerRuntime::with_plugins(
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
        Arc::new(InboxHandle::default()),
        retain.clone(),
    );
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
            &publisher.session.session_id,
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

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let retained = broker
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
    assert_eq!(retained.len(), 1);
    assert_eq!(retain.lookup_topic_calls(), 1);
    assert_eq!(retain.match_topic_calls(), 0);
}

#[tokio::test]
async fn wildcard_subscribe_uses_retain_match_path() {
    let retain = Arc::new(CountingRetainService::default());
    let broker = BrokerRuntime::with_plugins(
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
        Arc::new(InboxHandle::default()),
        retain.clone(),
    );
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
            &publisher.session.session_id,
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

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let retained = broker
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
    assert_eq!(retained.len(), 1);
    assert_eq!(retain.lookup_topic_calls(), 0);
    assert_eq!(retain.match_topic_calls(), 1);
}

#[tokio::test]
async fn tenants_are_isolated_during_match() {
    let broker = test_broker();
    let tenant_a = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
            &tenant_a.session.session_id,
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
        .disconnect(&tenant_a.session.session_id)
        .await
        .unwrap();

    let tenant_b = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-b".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker
        .publish(
            &tenant_b.session.session_id,
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
    assert_eq!(outcome.matched_routes, 0);
}

#[tokio::test]
async fn peer_forward_failure_falls_back_to_offline_for_persistent_route() {
    let sessiondict = Arc::new(SessionDictHandle::default());
    let dist = Arc::new(DistHandle::default());
    let inbox = Arc::new(InboxHandle::default());
    let retain = Arc::new(RetainHandle::default());
    let broker = BrokerRuntime::with_cluster(
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
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(FailingPeerForwarder),
        sessiondict.clone(),
        dist.clone(),
        inbox.clone(),
        retain,
    );

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let remote_session_id = "remote-session".to_string();
    sessiondict
        .register(SessionRecord {
            session_id: remote_session_id.clone(),
            node_id: 2,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-remote".into(),
                client_id: "sub-remote".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "tenant-a".into(),
        topic_filter: "devices/+/state".into(),
        session_id: remote_session_id.clone(),
        node_id: 2,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"failover".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome.matched_routes, 1);
    assert_eq!(outcome.online_deliveries, 0);
    assert_eq!(outcome.offline_enqueues, 1);

    let offline = inbox.fetch(&remote_session_id).await.unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].payload, b"failover".to_vec());
}

#[tokio::test]
async fn peer_forward_batches_remote_deliveries_by_node() {
    let sessiondict = Arc::new(SessionDictHandle::default());
    let dist = Arc::new(DistHandle::default());
    let inbox = Arc::new(InboxHandle::default());
    let retain = Arc::new(RetainHandle::default());
    let peer_forwarder = Arc::new(CountingBatchPeerForwarder::default());
    let broker = BrokerRuntime::with_cluster(
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
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        peer_forwarder.clone(),
        sessiondict.clone(),
        dist.clone(),
        inbox,
        retain,
    );

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    for (session_id, client_id) in [("remote-1", "sub-1"), ("remote-2", "sub-2")] {
        sessiondict
            .register(SessionRecord {
                session_id: session_id.into(),
                node_id: 2,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: format!("user-{client_id}"),
                    client_id: client_id.into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();
        dist.add_route(RouteRecord {
            tenant_id: "tenant-a".into(),
            topic_filter: "devices/+/state".into(),
            session_id: session_id.into(),
            node_id: 2,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    let outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"remote".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(outcome.online_deliveries, 2);
    assert_eq!(outcome.offline_enqueues, 0);
    assert_eq!(peer_forwarder.batch_calls(), 1);
    assert_eq!(peer_forwarder.single_calls(), 0);
    assert_eq!(peer_forwarder.forwarded_for(2).len(), 2);
}

#[tokio::test]
async fn expired_offline_message_is_dropped_on_reconnect() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"expired".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties {
                    message_expiry_interval_secs: Some(1),
                    stored_at_ms: Some(now_millis().saturating_sub(2_000)),
                    ..PublishProperties::default()
                },
            },
        )
        .await
        .unwrap();

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    assert!(resumed.offline_messages.is_empty());
}

#[tokio::test]
async fn expired_inflight_message_is_dropped_on_reconnect() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    broker
        .inbox
        .stage_inflight(InflightMessage {
            tenant_id: "tenant-a".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"expired".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "pub".into(),
            properties: PublishProperties {
                message_expiry_interval_secs: Some(1),
                stored_at_ms: Some(now_millis().saturating_sub(2_000)),
                ..PublishProperties::default()
            },
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(resumed.inflight_messages.is_empty());
    assert!(broker
        .inbox
        .fetch_inflight(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn cold_persistent_sessiondict_reconnect_returns_replaced_session() {
    let store = Arc::new(MemorySessionStore::default());
    let broker1 = BrokerRuntime::with_plugins(
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
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(
            PersistentSessionDictHandle::open(store.clone())
                .await
                .unwrap(),
        ),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let identity = ClientIdentity {
        tenant_id: "tenant-a".into(),
        user_id: "user-a".into(),
        client_id: "persisted".into(),
    };
    let first = broker1
        .connect(ConnectRequest {
            identity: identity.clone(),
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    assert!(!first.session_present);
    assert!(first.replaced.is_none());

    let broker2 = BrokerRuntime::with_plugins(
        BrokerConfig {
            node_id: 2,
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
        greenmqtt_plugin_api::AllowAllAcl,
        greenmqtt_plugin_api::NoopEventHook,
        Arc::new(PersistentSessionDictHandle::open(store).await.unwrap()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    );
    let resumed = broker2
        .connect(ConnectRequest {
            identity,
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    assert!(resumed.session_present);
    assert_eq!(
        resumed
            .replaced
            .as_ref()
            .map(|record| record.session_id.as_str()),
        Some(first.session.session_id.as_str())
    );
    assert_eq!(resumed.session.session_id, first.session.session_id);
}

#[tokio::test]
async fn no_local_subscription_skips_self_publish() {
    let broker = test_broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
            true,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let self_outcome = broker
        .publish(
            &subscriber.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"self".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(self_outcome.online_deliveries, 0);
    assert!(broker
        .drain_deliveries(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let remote_outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"remote".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(remote_outcome.online_deliveries, 1);
    let deliveries = broker
        .drain_deliveries(&subscriber.session.session_id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].payload, b"remote".to_vec());
}

#[tokio::test]
async fn retain_handling_one_skips_replay_on_resubscribe() {
    let broker = test_broker();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
            &publisher.session.session_id,
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

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let first = broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 1);

    let second = broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            1,
            None,
        )
        .await
        .unwrap();
    assert!(second.is_empty());
}

#[tokio::test]
async fn transient_connect_cycles_leave_no_global_state() {
    let broker = test_broker();

    for cycle in 0..10 {
        let subscriber = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: format!("user-sub-{cycle}"),
                    client_id: format!("sub-{cycle}"),
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

        let publisher = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: format!("user-pub-{cycle}"),
                    client_id: format!("pub-{cycle}"),
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
        assert_eq!(
            broker
                .drain_deliveries(&subscriber.session.session_id)
                .await
                .unwrap()
                .len(),
            1
        );

        broker
            .disconnect(&subscriber.session.session_id)
            .await
            .unwrap();
        broker
            .disconnect(&publisher.session.session_id)
            .await
            .unwrap();
    }

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_online_sessions, 0);
    assert_eq!(stats.local_pending_deliveries, 0);
    assert_eq!(stats.global_session_records, 0);
    assert_eq!(stats.route_records, 0);
    assert_eq!(stats.subscription_records, 0);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
}

#[tokio::test]
async fn local_stats_track_publish_drain_requeue_and_disconnect() {
    let broker = test_broker();

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_online_sessions, 2);
    assert_eq!(stats.local_persistent_sessions, 1);
    assert_eq!(stats.local_transient_sessions, 1);
    assert_eq!(stats.local_pending_deliveries, 0);

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

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_pending_deliveries, 1);

    let deliveries = broker
        .drain_deliveries(&subscriber.session.session_id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_pending_deliveries, 0);

    broker
        .requeue_local_deliveries(&subscriber.session.session_id, deliveries)
        .await
        .unwrap();

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_pending_deliveries, 1);

    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_online_sessions, 1);
    assert_eq!(stats.local_persistent_sessions, 0);
    assert_eq!(stats.local_transient_sessions, 1);
    assert_eq!(stats.local_pending_deliveries, 0);

    broker
        .disconnect(&publisher.session.session_id)
        .await
        .unwrap();

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_online_sessions, 0);
    assert_eq!(stats.local_persistent_sessions, 0);
    assert_eq!(stats.local_transient_sessions, 0);
    assert_eq!(stats.local_pending_deliveries, 0);
}

#[tokio::test]
async fn local_hot_state_breakdown_excludes_durable_service_records() {
    let broker = test_broker();
    broker
        .sessiondict
        .register(SessionRecord {
            session_id: "remote-session".into(),
            node_id: 9,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-remote".into(),
                client_id: "remote".into(),
            },
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1234),
        })
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "tenant-a".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "remote-session".into(),
            node_id: 9,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    broker
        .inbox
        .subscribe(greenmqtt_core::Subscription {
            session_id: "remote-session".into(),
            tenant_id: "tenant-a".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "tenant-a".into(),
            session_id: "remote-session".into(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    broker
        .retain
        .retain(RetainedMessage {
            tenant_id: "tenant-a".into(),
            topic: "devices/a/state".into(),
            payload: b"retained".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    let breakdown = broker.local_hot_state_breakdown();
    assert_eq!(breakdown.live_connections, 0);
    assert_eq!(breakdown.transient_send_queue_entries, 0);
    assert_eq!(breakdown.short_lived_tracking_entries, 0);
    assert_eq!(broker.local_hot_state_entries(), 0);

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.global_session_records, 1);
    assert_eq!(stats.route_records, 1);
    assert_eq!(stats.subscription_records, 1);
    assert_eq!(stats.offline_messages, 1);
    assert_eq!(stats.retained_messages, 1);
}

#[tokio::test]
async fn local_hot_state_breakdown_tracks_connections_queues_and_short_lived_markers() {
    let mut broker = test_broker();
    broker.set_connect_debounce_window(Duration::from_secs(30));
    broker.set_tenant_quota(
        "tenant-a",
        TenantQuota {
            max_connections: 10,
            max_subscriptions: 10,
            max_msg_per_sec: 10,
            max_memory_bytes: 1024,
        },
    );
    let broker = Arc::new(broker);

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
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
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
    assert!(!broker.connect_debounce_exceeded(&publisher.session.identity));
    assert!(broker.connect_debounce_exceeded(&publisher.session.identity));
    broker
        .schedule_delayed_will(
            &subscriber.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"later".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            30,
        )
        .await
        .unwrap();

    let breakdown = broker.local_hot_state_breakdown();
    assert_eq!(breakdown.live_connections, 2);
    assert_eq!(breakdown.transient_send_queue_entries, 1);
    assert_eq!(breakdown.short_lived_tracking_entries, 2);
    assert_eq!(broker.local_hot_state_entries(), 5);
}

#[tokio::test]
async fn repair_orphan_session_state_cleans_inbox_routes_and_local_buffers() {
    let broker = test_broker();
    let ghost = "ghost-session".to_string();

    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "tenant-a".into(),
            topic_filter: "devices/#".into(),
            session_id: ghost.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "tenant-a".into(),
            session_id: ghost.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(InflightMessage {
            tenant_id: "tenant-a".into(),
            session_id: ghost.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();
    broker
        .accept_forwarded_delivery(Delivery {
            tenant_id: "tenant-a".into(),
            session_id: ghost.clone(),
            topic: "devices/d1/state".into(),
            payload: b"pending".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap_or(false);

    let repaired = broker.repair_orphan_session_state(&ghost).await.unwrap();
    assert!(repaired);
    assert_eq!(broker.dist.count_session_routes(&ghost).await.unwrap(), 0);
    assert!(broker.inbox.fetch(&ghost).await.unwrap().is_empty());
    assert!(broker
        .inbox
        .fetch_inflight(&ghost)
        .await
        .unwrap()
        .is_empty());
    assert!(broker.drain_deliveries(&ghost).await.is_err());
}

#[tokio::test]
async fn repair_orphan_session_state_ignores_live_session() {
    let broker = test_broker();
    let active = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "live".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let repaired = broker
        .repair_orphan_session_state(&active.session.session_id)
        .await
        .unwrap();
    assert!(!repaired);
    assert!(broker
        .sessiondict
        .lookup_session(&active.session.session_id)
        .await
        .unwrap()
        .is_some());

    broker.disconnect(&active.session.session_id).await.unwrap();
}

#[tokio::test]
async fn connection_slot_limit_rejects_when_full() {
    let mut broker = test_broker();
    broker.set_connection_limit(1);

    let permit = broker
        .try_acquire_connection_slot()
        .expect("limit configured")
        .expect("first permit should be granted");
    assert!(broker.try_acquire_connection_slot().is_err());

    drop(permit);

    let permit = broker
        .try_acquire_connection_slot()
        .expect("limit configured after release")
        .expect("slot should reopen after release");
    drop(permit);
}

#[tokio::test]
async fn max_online_sessions_pressure_tracks_connected_sessions() {
    let mut broker = test_broker();
    broker.set_max_online_sessions(1);
    assert!(!broker.connect_pressure_exceeded());

    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "pressure".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(broker.connect_pressure_exceeded());

    broker.disconnect(&reply.session.session_id).await.unwrap();

    assert!(!broker.connect_pressure_exceeded());
}

#[tokio::test]
async fn memory_pressure_level_tracks_connect_rejection_state() {
    let broker = test_broker();
    assert!(!broker.connect_pressure_exceeded());

    broker.force_memory_pressure_level(PressureLevel::Critical);
    assert!(broker.connect_pressure_exceeded());

    broker.force_memory_pressure_level(PressureLevel::Normal);
    assert!(!broker.connect_pressure_exceeded());
}

#[tokio::test]
async fn connection_slowdown_activates_once_online_sessions_reach_threshold() {
    let mut broker = test_broker();
    broker.set_connection_slowdown(1, Duration::from_millis(25));
    assert_eq!(broker.connect_slowdown_delay(), None);

    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "slowdown".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert_eq!(
        broker.connect_slowdown_delay(),
        Some(Duration::from_millis(25))
    );

    broker.disconnect(&reply.session.session_id).await.unwrap();

    assert_eq!(broker.connect_slowdown_delay(), None);
}

#[tokio::test]
async fn connection_slowdown_is_suppressed_when_pressure_reject_would_fire() {
    let mut broker = test_broker();
    broker.set_connection_slowdown(1, Duration::from_millis(25));
    broker.set_max_online_sessions(1);

    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "busy".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert!(broker.connect_pressure_exceeded());
    assert_eq!(broker.connect_slowdown_delay(), None);

    broker.disconnect(&reply.session.session_id).await.unwrap();
}

#[tokio::test]
async fn connection_shaping_applies_without_pressure() {
    let mut broker = test_broker();
    broker.set_connection_shaping(Duration::from_millis(10));
    assert_eq!(
        broker.connect_effective_delay(),
        Some(Duration::from_millis(10))
    );
}

#[tokio::test]
async fn connection_shaping_and_slowdown_choose_larger_delay() {
    let mut broker = test_broker();
    broker.set_connection_shaping(Duration::from_millis(10));
    broker.set_connection_slowdown(1, Duration::from_millis(25));

    let reply = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "shape-slow".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    assert_eq!(
        broker.connect_effective_delay(),
        Some(Duration::from_millis(25))
    );

    broker.disconnect(&reply.session.session_id).await.unwrap();
}

#[tokio::test]
async fn connection_rate_limit_rejects_after_threshold_within_same_window() {
    let mut broker = test_broker();
    broker.set_connection_rate_limit(1);

    assert!(broker.allow_connection_attempt());
    assert!(!broker.allow_connection_attempt());

    broker.reset_connection_rate_window();
    assert!(broker.allow_connection_attempt());
}

#[tokio::test]
async fn connect_debounce_rejects_repeated_identity_within_window() {
    let mut broker = test_broker();
    broker.set_connect_debounce_window(Duration::from_secs(1));

    let identity = ClientIdentity {
        tenant_id: "tenant-a".into(),
        user_id: "user-a".into(),
        client_id: "debounce".into(),
    };

    assert!(!broker.connect_debounce_exceeded(&identity));
    assert!(broker.connect_debounce_exceeded(&identity));
}

#[tokio::test]
async fn connect_debounce_allows_after_window_elapses() {
    let mut broker = test_broker();
    broker.set_connect_debounce_window(Duration::from_millis(1));

    let identity = ClientIdentity {
        tenant_id: "tenant-a".into(),
        user_id: "user-a".into(),
        client_id: "debounce".into(),
    };

    assert!(!broker.connect_debounce_exceeded(&identity));
    sleep(Duration::from_millis(5)).await;
    assert!(!broker.connect_debounce_exceeded(&identity));
    assert_eq!(
        broker
            .local_hot_state_breakdown()
            .short_lived_tracking_entries,
        1
    );
}

#[tokio::test]
async fn persistent_reconnect_cycles_do_not_duplicate_state() {
    let broker = test_broker();

    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "persistent-client".into(),
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
    broker.disconnect(&first.session.session_id).await.unwrap();

    for _ in 0..5 {
        let resumed = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "user-a".into(),
                    client_id: "persistent-client".into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: false,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        assert_eq!(resumed.session.session_id, first.session.session_id);
        broker
            .disconnect(&resumed.session.session_id)
            .await
            .unwrap();
    }

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_online_sessions, 0);
    assert_eq!(stats.route_records, 1);
    assert_eq!(stats.subscription_records, 1);
    assert_eq!(stats.global_session_records, 1);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
}

#[tokio::test]
async fn persistent_session_takeover_moves_pending_queue_into_replay_store() {
    let broker = test_broker();

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "takeover-pending".into(),
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
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
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
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"pending".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let stats_before = broker.stats().await.unwrap();
    assert_eq!(stats_before.local_pending_deliveries, 1);

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "takeover-pending".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    assert_eq!(resumed.session.session_id, subscriber.session.session_id);
    assert_eq!(resumed.offline_messages.len(), 1);
    assert_eq!(resumed.offline_messages[0].payload, b"pending".to_vec());

    let stats_after = broker.stats().await.unwrap();
    assert_eq!(stats_after.local_online_sessions, 2);
    assert_eq!(stats_after.local_pending_deliveries, 0);
    assert_eq!(broker.inbox.offline_count().await.unwrap(), 0);
}

#[tokio::test]
async fn expired_persistent_session_is_recreated_without_residual_state() {
    let broker = test_broker();

    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "expiring-client".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: Some(1),
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
    broker.disconnect(&first.session.session_id).await.unwrap();

    sleep(Duration::from_millis(1_200)).await;

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "expiring-client".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: Some(1),
        })
        .await
        .unwrap();

    assert_ne!(resumed.session.session_id, first.session.session_id);
    assert!(resumed.offline_messages.is_empty());

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.global_session_records, 1);
    assert_eq!(stats.route_records, 0);
    assert_eq!(stats.subscription_records, 0);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
}

#[tokio::test]
async fn expired_persistent_session_is_purged_when_publish_hits_stale_route() {
    let broker = test_broker();

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "stale-sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: Some(1),
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

    sleep(Duration::from_millis(1_200)).await;

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-b".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"should-drop".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(outcome.online_deliveries, 0);
    assert_eq!(outcome.offline_enqueues, 0);

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.global_session_records, 1);
    assert_eq!(stats.route_records, 0);
    assert_eq!(stats.subscription_records, 0);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
}

#[tokio::test]
async fn disconnect_with_session_expiry_zero_purges_persistent_session_state() {
    let broker = test_broker();

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "zero-expiry".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: Some(60),
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
        .disconnect_with_session_expiry(&subscriber.session.session_id, 0)
        .await
        .unwrap();

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.local_online_sessions, 0);
    assert_eq!(stats.global_session_records, 0);
    assert_eq!(stats.route_records, 0);
    assert_eq!(stats.subscription_records, 0);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
}

#[tokio::test]
async fn disconnect_with_session_expiry_override_updates_persistent_session_expiry() {
    let broker = test_broker();

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "expiry-override".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: Some(60),
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
        .disconnect_with_session_expiry(&subscriber.session.session_id, 1)
        .await
        .unwrap();

    sleep(Duration::from_millis(1_200)).await;

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "expiry-override".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: Some(60),
        })
        .await
        .unwrap();

    assert_ne!(resumed.session.session_id, subscriber.session.session_id);
    assert!(!resumed.session_present);
    assert!(resumed.offline_messages.is_empty());
    assert!(resumed.inflight_messages.is_empty());

    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.global_session_records, 1);
    assert_eq!(stats.route_records, 0);
    assert_eq!(stats.subscription_records, 0);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
}
