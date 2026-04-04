pub mod mqtt;
pub use broker::balancer::{ClientBalancer, Redirection, RoundRobinBalancer};

use async_trait::async_trait;
use broker::admission::AdmissionController;
use broker::balancer::{NoopClientBalancer, SharedClientBalancer};
use broker::tenant::TenantResourceManager;
use broker::throttle::MessageRateLimiter;
use dashmap::DashMap;
use greenmqtt_core::{
    Delivery, NodeId, RetainedMessage, SessionId, SessionKind, SessionRecord, Subscription,
};
use greenmqtt_dist::{DistHandle, DistRouter};
use greenmqtt_inbox::{InboxHandle, InboxService};
use greenmqtt_plugin_api::{AllowAllAcl, AuthProvider, NoopEventHook};
use greenmqtt_retain::{RetainHandle, RetainService};
use greenmqtt_sessiondict::{SessionDictHandle, SessionDirectory};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Semaphore;

pub struct BrokerConfig {
    pub node_id: NodeId,
    pub enable_tcp: bool,
    pub enable_tls: bool,
    pub enable_ws: bool,
    pub enable_wss: bool,
    pub enable_quic: bool,
    pub server_keep_alive_secs: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub audit_log_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerStats {
    pub peer_nodes: usize,
    pub local_online_sessions: usize,
    pub local_persistent_sessions: usize,
    pub local_transient_sessions: usize,
    pub local_pending_deliveries: usize,
    pub global_session_records: usize,
    pub route_records: usize,
    pub subscription_records: usize,
    pub offline_messages: usize,
    pub inflight_messages: usize,
    pub retained_messages: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSummary {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub kind: SessionKind,
    pub tenant_id: String,
    pub user_id: String,
    pub client_id: String,
    pub pending_deliveries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReassignOutcome {
    pub updated_sessions: usize,
    pub updated_routes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminAuditEntry {
    pub seq: u64,
    pub timestamp_ms: u64,
    pub action: String,
    pub target: String,
    pub details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DesiredPeerState {
    endpoint: Option<String>,
    active: bool,
}

#[derive(Debug, Clone)]
struct LocalSessionState {
    record: SessionRecord,
    session_epoch: u64,
}

struct ShardedLocalSessions {
    shards: Vec<RwLock<HashMap<SessionId, LocalSessionState>>>,
}

struct ShardedLocalDeliveries {
    shards: Vec<RwLock<HashMap<SessionId, Vec<Delivery>>>>,
}

fn shard_index_for_session(session_id: &str, shard_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    session_id.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

struct ShardedWillGenerations {
    shards: Vec<RwLock<HashMap<String, u64>>>,
}

impl Default for ShardedWillGenerations {
    fn default() -> Self {
        Self::new(32)
    }
}

impl ShardedWillGenerations {
    fn new(shard_count: usize) -> Self {
        Self {
            shards: (0..shard_count)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard_index(&self, key: &str) -> usize {
        shard_index_for_session(key, self.shards.len())
    }

    fn shard(&self, key: &str) -> &RwLock<HashMap<String, u64>> {
        &self.shards[self.shard_index(key)]
    }

    fn insert(&self, key: String, generation: u64) {
        self.shard(&key)
            .write()
            .expect("broker poisoned")
            .insert(key, generation);
    }

    fn remove(&self, key: &str) -> Option<u64> {
        self.shard(key)
            .write()
            .expect("broker poisoned")
            .remove(key)
    }

    fn get_copied(&self, key: &str) -> Option<u64> {
        self.shard(key)
            .read()
            .expect("broker poisoned")
            .get(key)
            .copied()
    }

    fn total_entries(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().expect("broker poisoned").len())
            .sum()
    }

    fn total_key_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .expect("broker poisoned")
                    .keys()
                    .map(|key| key.len() + std::mem::size_of::<u64>())
                    .sum::<usize>()
            })
            .sum()
    }
}

impl Default for ShardedLocalDeliveries {
    fn default() -> Self {
        Self::new(32)
    }
}

impl ShardedLocalDeliveries {
    fn new(shard_count: usize) -> Self {
        Self {
            shards: (0..shard_count)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard_index(&self, session_id: &str) -> usize {
        shard_index_for_session(session_id, self.shards.len())
    }

    fn shard(&self, session_id: &str) -> &RwLock<HashMap<SessionId, Vec<Delivery>>> {
        &self.shards[self.shard_index(session_id)]
    }

    fn push(&self, session_id: &str, delivery: Delivery) -> usize {
        let mut guard = self.shard(session_id).write().expect("broker poisoned");
        let queue = guard.entry(session_id.to_string()).or_default();
        queue.push(delivery);
        queue.len()
    }

    fn prepend_batch(&self, session_id: &str, mut deliveries: Vec<Delivery>) -> (usize, usize) {
        let mut guard = self.shard(session_id).write().expect("broker poisoned");
        let queue = guard.entry(session_id.to_string()).or_default();
        let previous_len = queue.len();
        deliveries.append(queue);
        let new_len = deliveries.len();
        *queue = deliveries;
        (previous_len, new_len)
    }

    fn drain(&self, session_id: &str) -> Vec<Delivery> {
        self.shard(session_id)
            .write()
            .expect("broker poisoned")
            .remove(session_id)
            .unwrap_or_default()
    }

    fn pending_len(&self, session_id: &str) -> usize {
        self.shard(session_id)
            .read()
            .expect("broker poisoned")
            .get(session_id)
            .map(|queue| queue.len())
            .unwrap_or(0)
    }

    fn values_cloned(&self) -> Vec<Delivery> {
        self.shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .expect("broker poisoned")
                    .values()
                    .flat_map(|queue| queue.iter().cloned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

impl Default for ShardedLocalSessions {
    fn default() -> Self {
        Self::new(32)
    }
}

impl ShardedLocalSessions {
    fn new(shard_count: usize) -> Self {
        Self {
            shards: (0..shard_count)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard_index(&self, session_id: &str) -> usize {
        shard_index_for_session(session_id, self.shards.len())
    }

    fn shard(&self, session_id: &str) -> &RwLock<HashMap<SessionId, LocalSessionState>> {
        &self.shards[self.shard_index(session_id)]
    }

    fn get_cloned(&self, session_id: &str) -> Option<LocalSessionState> {
        self.shard(session_id)
            .read()
            .expect("broker poisoned")
            .get(session_id)
            .cloned()
    }

    fn contains_key(&self, session_id: &str) -> bool {
        self.shard(session_id)
            .read()
            .expect("broker poisoned")
            .contains_key(session_id)
    }

    fn insert(&self, session_id: SessionId, state: LocalSessionState) -> Option<LocalSessionState> {
        self.shard(&session_id)
            .write()
            .expect("broker poisoned")
            .insert(session_id, state)
    }

    fn remove(&self, session_id: &str) -> Option<LocalSessionState> {
        self.shard(session_id)
            .write()
            .expect("broker poisoned")
            .remove(session_id)
    }

    fn get_session_epoch(&self, session_id: &str) -> Option<u64> {
        self.shard(session_id)
            .read()
            .expect("broker poisoned")
            .get(session_id)
            .map(|state| state.session_epoch)
    }

    fn values_cloned(&self) -> Vec<LocalSessionState> {
        self.shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .expect("broker poisoned")
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

#[derive(Default)]
struct LocalStatsState {
    online_sessions: usize,
    persistent_sessions: usize,
    transient_sessions: usize,
    pending_deliveries: usize,
}

#[async_trait]
pub trait PeerForwarder: Send + Sync {
    async fn forward_delivery(&self, node_id: NodeId, delivery: Delivery) -> anyhow::Result<bool>;
    async fn forward_deliveries(
        &self,
        node_id: NodeId,
        deliveries: Vec<Delivery>,
    ) -> anyhow::Result<Vec<Delivery>> {
        let mut undelivered = Vec::new();
        for delivery in deliveries {
            match self.forward_delivery(node_id, delivery.clone()).await {
                Ok(true) => {}
                Ok(false) | Err(_) => undelivered.push(delivery),
            }
        }
        Ok(undelivered)
    }
}

#[async_trait]
pub trait PeerRegistry: Send + Sync {
    fn list_peer_nodes(&self) -> Vec<NodeId>;
    fn list_peer_endpoints(&self) -> BTreeMap<NodeId, String> {
        BTreeMap::new()
    }
    fn remove_peer_node(&self, node_id: NodeId) -> bool;
    async fn add_peer_node(&self, node_id: NodeId, endpoint: String) -> anyhow::Result<()>;
}

#[async_trait]
pub trait DeliverySink: Send + Sync {
    async fn push_delivery(&self, delivery: Delivery) -> anyhow::Result<bool>;
    async fn push_deliveries(&self, deliveries: Vec<Delivery>) -> anyhow::Result<Vec<Delivery>> {
        let mut undelivered = Vec::new();
        for delivery in deliveries {
            match self.push_delivery(delivery.clone()).await {
                Ok(true) => {}
                Ok(false) | Err(_) => undelivered.push(delivery),
            }
        }
        Ok(undelivered)
    }
}

#[derive(Clone, Default)]
pub struct LocalPeerForwarder;

#[async_trait]
impl PeerForwarder for LocalPeerForwarder {
    async fn forward_delivery(
        &self,
        _node_id: NodeId,
        _delivery: Delivery,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[async_trait]
impl PeerRegistry for LocalPeerForwarder {
    fn list_peer_nodes(&self) -> Vec<NodeId> {
        Vec::new()
    }

    fn remove_peer_node(&self, _node_id: NodeId) -> bool {
        false
    }

    async fn add_peer_node(&self, _node_id: NodeId, _endpoint: String) -> anyhow::Result<()> {
        anyhow::bail!("local peer registry does not support dynamic peers")
    }
}

pub struct BrokerRuntime<A, C = AllowAllAcl, H = NoopEventHook> {
    pub config: BrokerConfig,
    pub auth: A,
    pub acl: C,
    pub hooks: H,
    pub sessiondict: Arc<dyn SessionDirectory>,
    pub dist: Arc<dyn DistRouter>,
    pub inbox: Arc<dyn InboxService>,
    pub retain: Arc<dyn RetainService>,
    peer_forwarder: Arc<dyn PeerForwarder>,
    client_balancer: SharedClientBalancer,
    local_sessions: Arc<ShardedLocalSessions>,
    local_deliveries: Arc<ShardedLocalDeliveries>,
    local_stats: Arc<RwLock<LocalStatsState>>,
    pending_will_generations: Arc<ShardedWillGenerations>,
    admin_audit: Arc<RwLock<VecDeque<AdminAuditEntry>>>,
    next_session_seq: AtomicU64,
    next_local_session_epoch: AtomicU64,
    next_will_seq: AtomicU64,
    next_audit_seq: AtomicU64,
    connection_slots: Option<Arc<Semaphore>>,
    admission: AdmissionController,
    publish_rate_limiter: MessageRateLimiter,
    tenant_resources: Arc<TenantResourceManager>,
    inbound_bandwidth_limit: Option<(u64, u64)>,
    outbound_bandwidth_limit: Option<(u64, u64)>,
    inbound_bandwidth_overrides: DashMap<String, (u64, u64)>,
    outbound_bandwidth_overrides: DashMap<String, (u64, u64)>,
    connect_debounce_window_ms: Option<u64>,
    recent_connect_attempts: Arc<ShardedWillGenerations>,
}

pub type DefaultBroker = BrokerRuntime<greenmqtt_plugin_api::AllowAllAuth>;

fn topic_filter_is_exact(topic_filter: &str) -> bool {
    !topic_filter.contains('#') && !topic_filter.contains('+')
}

impl<A> BrokerRuntime<A>
where
    A: AuthProvider,
{
    pub fn new(
        config: BrokerConfig,
        auth: A,
        sessiondict: SessionDictHandle,
        dist: DistHandle,
        inbox: InboxHandle,
        retain: RetainHandle,
    ) -> Self {
        Self::with_plugins(
            config,
            auth,
            AllowAllAcl,
            NoopEventHook,
            Arc::new(sessiondict),
            Arc::new(dist),
            Arc::new(inbox),
            Arc::new(retain),
        )
    }
}

mod broker;

#[cfg(test)]
pub(crate) use broker::session_mgr::{
    desired_peer_membership, desired_peer_registry, now_millis, placement_candidates,
    placement_candidates_excluding, placement_node_for_session,
};

#[cfg(test)]
mod tests;
