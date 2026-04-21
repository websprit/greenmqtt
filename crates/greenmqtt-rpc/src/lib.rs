use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_broker::{DeliverySink, PeerRegistry};
use greenmqtt_core::{
    BalancerState, BalancerStateRegistry, ClientIdentity, ClusterMembershipRegistry,
    ClusterNodeMembership, ControlCommandRecord, ControlCommandRegistry, Delivery, InflightMessage,
    MetadataRegistry, NodeId, OfflineMessage, RangeReconfigurationRegistry,
    RangeReconfigurationState, ReconfigurationPhase, ReplicatedRangeDescriptor,
    ReplicatedRangeRegistry, RetainedMessage, RouteRecord, ServiceEndpoint,
    ServiceEndpointRegistry, ServiceKind, ServiceShardAssignment, ServiceShardKey,
    ServiceShardKind, ServiceShardLifecycle, ServiceShardRecoveryControl, ServiceShardTransition,
    ServiceShardTransitionKind, Subscription,
};
use greenmqtt_dist::{dist_tenant_shard, DistRouter};
use greenmqtt_inbox::{
    inbox_expire_all, inbox_send_lwt, inbox_session_shard, inbox_tenant_gc_preview,
    inbox_tenant_gc_run, inbox_tenant_scan_shard, inflight_tenant_scan_shard, DelayedLwtPublish,
    DelayedLwtSink, InboxService, InboxTenantGcPreview,
};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeExecutorFactory, KvRangeRouter, RangeRoute};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, RocksDbKvEngine,
};
use greenmqtt_kv_raft::RaftMessage;
use greenmqtt_kv_server::{
    KvRangeHost, RangeHealthSnapshot, ReplicaLagSnapshot, ReplicaTransport, ZombieRangeSnapshot,
};
use greenmqtt_proto::internal::{
    broker_peer_service_client::BrokerPeerServiceClient,
    broker_peer_service_server::BrokerPeerServiceServer, dist_service_client::DistServiceClient,
    dist_service_server::DistServiceServer, inbox_service_client::InboxServiceClient,
    inbox_service_server::InboxServiceServer, kv_range_service_client::KvRangeServiceClient,
    kv_range_service_server::KvRangeServiceServer, metadata_service_client::MetadataServiceClient,
    metadata_service_server::MetadataServiceServer,
    raft_transport_service_client::RaftTransportServiceClient,
    raft_transport_service_server::RaftTransportServiceServer,
    range_admin_service_client::RangeAdminServiceClient,
    range_admin_service_server::RangeAdminServiceServer,
    range_control_service_client::RangeControlServiceClient,
    range_control_service_server::RangeControlServiceServer,
    retain_service_client::RetainServiceClient, retain_service_server::RetainServiceServer,
    session_dict_service_client::SessionDictServiceClient,
    session_dict_service_server::SessionDictServiceServer, AddRouteRequest, BalancerStateListReply,
    BalancerStateReply, BalancerStateRequest, BalancerStateUpsertReply, BalancerStateUpsertRequest,
    CountReply, IdentityExistRecord, IdentityExistReply, IdentityExistRequest,
    InboxAckInflightRequest, InboxAttachRequest, InboxDetachRequest, InboxEnqueueRequest,
    InboxExpireAllRequest, InboxFetchInflightReply, InboxFetchInflightRequest, InboxFetchReply,
    InboxFetchRequest, InboxListAllSubscriptionsRequest, InboxListMessagesRequest,
    InboxListSubscriptionsReply, InboxListSubscriptionsRequest, InboxLookupSubscriptionReply,
    InboxLookupSubscriptionRequest, InboxMaintenanceReply, InboxPurgeSessionRequest,
    InboxSendLwtReply, InboxSendLwtRequest, InboxStageInflightRequest, InboxStatsReply,
    InboxSubscribeRequest, InboxTenantGcRequest, InboxUnsubscribeReply, InboxUnsubscribeRequest,
    KvRangeApplyRequest, KvRangeCheckpointReply, KvRangeCheckpointRequest, KvRangeGetReply,
    KvRangeGetRequest, KvRangeScanReply, KvRangeScanRequest, KvRangeSnapshotReply,
    KvRangeSnapshotRequest, ListRoutesReply, ListRoutesRequest, ListSessionRoutesReply,
    ListSessionRoutesRequest, ListSessionsReply, ListSessionsRequest, LookupSessionByIdRequest,
    LookupSessionReply, LookupSessionRequest, MatchTopicReply, MatchTopicRequest, MemberListReply,
    MemberLookupRequest, MemberRecordReply, NamedBalancerStateRecord, PushDeliveriesReply,
    PushDeliveriesRequest, PushDeliveryReply, PushDeliveryRequest, RaftTransportRequest,
    RangeBootstrapReply, RangeBootstrapRequest, RangeChangeReplicasRequest, RangeDebugReply,
    RangeDrainRequest, RangeHealthListReply, RangeHealthRecord, RangeHealthReply,
    RangeHealthRequest, RangeListReply, RangeListRequest, RangeLookupRequest, RangeMergeReply,
    RangeMergeRequest, RangeReconfigurationRecord, RangeRecordReply, RangeRecoverRequest,
    RangeRetireRequest, RangeSplitReply, RangeSplitRequest, RangeTransferLeadershipRequest,
    RangeUpsertRequest, RegisterSessionReply, RegisterSessionRequest, RemoveRouteRequest,
    RemoveSessionRoutesReply, RemoveSessionRoutesRequest, ReplicaLagRecord, RetainExpireAllRequest,
    RetainMaintenanceReply, RetainMatchReply, RetainMatchRequest, RetainTenantGcRequest,
    RetainWriteRequest, RouteRangeRequest, SessionExistRecord, SessionExistReply,
    SessionExistRequest, ShardSnapshotChunk, ShardSnapshotRequest, UnregisterSessionRequest,
    ZombieRangeListReply, ZombieRangeRecord,
};
use greenmqtt_proto::{
    from_proto_balancer_state, from_proto_client_identity, from_proto_cluster_node_membership,
    from_proto_delivery, from_proto_inflight, from_proto_kv_mutation,
    from_proto_kv_range_checkpoint, from_proto_kv_range_snapshot, from_proto_offline,
    from_proto_raft_transport_request, from_proto_replicated_range, from_proto_retain,
    from_proto_route, from_proto_session, from_proto_shard_kind, from_proto_subscription,
    to_proto_balancer_state, to_proto_client_identity, to_proto_cluster_node_membership,
    to_proto_delivery, to_proto_inflight, to_proto_kv_entry, to_proto_kv_range_checkpoint,
    to_proto_kv_range_snapshot, to_proto_offline, to_proto_raft_transport_request,
    to_proto_replicated_range, to_proto_retain, to_proto_route, to_proto_session,
    to_proto_shard_kind, to_proto_subscription,
};
use greenmqtt_retain::{
    retain_expire_all, retain_tenant_gc_preview, retain_tenant_gc_run, retain_tenant_shard,
    RetainService as RetainStoreService,
};
use greenmqtt_sessiondict::SessionOnlineCheckScheduler;
use greenmqtt_sessiondict::{session_identity_shard, SessionDirectory};
use metrics::counter;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};
use tower::Service;

const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_RPC_MAX_IN_FLIGHT: usize = 128;
const DEFAULT_RPC_RETRY_AFTER_MS: u64 = 200;

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_millis() as u64
}

fn env_override_limit(key: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => {
            value.parse().map_err(|_| anyhow::anyhow!("invalid {key}"))
        }
        _ => Ok(default),
    }
}

type BoxGrpcFuture<T> = Pin<Box<dyn Future<Output = Result<T, Infallible>> + Send>>;

#[derive(Clone)]
pub struct RpcGovernedService<S> {
    inner: S,
    governor: Arc<ServiceTrafficGovernor>,
}

impl<S> RpcGovernedService<S> {
    pub fn new(inner: S, governor: Arc<ServiceTrafficGovernor>) -> Self {
        Self { inner, governor }
    }
}

impl<S, ReqBody> Service<tonic::codegen::http::Request<ReqBody>> for RpcGovernedService<S>
where
    S: Service<
            tonic::codegen::http::Request<ReqBody>,
            Response = tonic::codegen::http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxGrpcFuture<Self::Response>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<ReqBody>) -> Self::Future {
        let governor = self.governor.clone();
        if let Some(permit) = governor.try_acquire() {
            let future = self.inner.call(request);
            Box::pin(async move {
                let _permit = permit;
                future.await
            })
        } else {
            governor.record_overload();
            Box::pin(async move { Ok(overload_response(&governor)) })
        }
    }
}

impl<S> tonic::server::NamedService for RpcGovernedService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

fn overload_response(
    governor: &ServiceTrafficGovernor,
) -> tonic::codegen::http::Response<tonic::body::Body> {
    let rules = governor.effective_rules();
    tonic::codegen::http::Response::builder()
        .status(tonic::codegen::http::StatusCode::OK)
        .header(
            tonic::codegen::http::header::CONTENT_TYPE,
            "application/grpc",
        )
        .header("grpc-status", tonic::Code::ResourceExhausted as i32)
        .header(
            "grpc-message",
            format!(
                "rpc/overloaded service={} retry_after_ms={}",
                governor.service.as_str(),
                rules.retry_after_ms
            ),
        )
        .header("x-greenmqtt-service", governor.service.as_str())
        .header(
            "x-greenmqtt-retry-after-ms",
            rules.retry_after_ms.to_string(),
        )
        .body(tonic::body::Body::empty())
        .expect("valid overload response")
}

fn inbox_maintenance_reply(
    subscriptions: usize,
    offline_messages: usize,
    inflight_messages: usize,
) -> InboxMaintenanceReply {
    InboxMaintenanceReply {
        subscriptions: subscriptions as u64,
        offline_messages: offline_messages as u64,
        inflight_messages: inflight_messages as u64,
    }
}

fn inbox_preview_reply(preview: InboxTenantGcPreview) -> InboxMaintenanceReply {
    inbox_maintenance_reply(
        preview.subscriptions,
        preview.offline_messages,
        preview.inflight_messages,
    )
}

fn retain_maintenance_reply(
    result: &greenmqtt_retain::RetainMaintenanceResult,
) -> RetainMaintenanceReply {
    RetainMaintenanceReply {
        tenant_id: result.tenant_id.clone(),
        scanned: result.scanned as u64,
        removed: result.removed as u64,
        refreshed: result.refreshed as u64,
    }
}

#[derive(Clone)]
pub struct RpcRuntime {
    pub sessiondict: Arc<dyn SessionDirectory>,
    pub dist: Arc<dyn DistRouter>,
    pub inbox: Arc<dyn InboxService>,
    pub retain: Arc<dyn RetainStoreService>,
    pub peer_sink: Arc<dyn DeliverySink>,
    pub assignment_registry: Option<Arc<dyn MetadataRegistry>>,
    pub range_host: Option<Arc<dyn KvRangeHost>>,
    pub range_runtime: Option<Arc<greenmqtt_kv_server::ReplicaRuntime>>,
    pub inbox_lwt_sink: Option<Arc<dyn DelayedLwtSink>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcServiceKind {
    SessionDict,
    Dist,
    Inbox,
    Retain,
    Metadata,
    KvRange,
    RangeControl,
}

impl RpcServiceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RpcServiceKind::SessionDict => "sessiondict",
            RpcServiceKind::Dist => "dist",
            RpcServiceKind::Inbox => "inbox",
            RpcServiceKind::Retain => "retain",
            RpcServiceKind::Metadata => "metadata",
            RpcServiceKind::KvRange => "kvrange",
            RpcServiceKind::RangeControl => "rangecontrol",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcServiceLoadSnapshot {
    pub service: &'static str,
    pub current_in_flight: usize,
    pub max_in_flight: usize,
    pub overload_count: u64,
    pub retry_after_ms: u64,
    pub last_overload_ms: Option<u64>,
    pub rules_source: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcTrafficRules {
    pub max_in_flight: usize,
    pub retry_after_ms: u64,
    pub tenant_prefix_limits: BTreeMap<String, usize>,
    pub preferred_endpoints: Vec<String>,
    pub server_group_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcServiceRulesSnapshot {
    pub service: &'static str,
    pub source: &'static str,
    pub effective: RpcTrafficRules,
    pub runtime_override: Option<RpcTrafficRules>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcEndpointSnapshot {
    pub endpoint: String,
    pub last_overload_ms: Option<u64>,
    pub backoff_until_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcTrafficGovernorConfig {
    pub default_max_in_flight: usize,
    pub sessiondict_max_in_flight: usize,
    pub dist_max_in_flight: usize,
    pub inbox_max_in_flight: usize,
    pub retain_max_in_flight: usize,
    pub metadata_max_in_flight: usize,
    pub kvrange_max_in_flight: usize,
    pub rangecontrol_max_in_flight: usize,
    pub retry_after_ms: u64,
}

impl Default for RpcTrafficGovernorConfig {
    fn default() -> Self {
        Self {
            default_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            sessiondict_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            dist_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            inbox_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            retain_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            metadata_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            kvrange_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            rangecontrol_max_in_flight: DEFAULT_RPC_MAX_IN_FLIGHT,
            retry_after_ms: DEFAULT_RPC_RETRY_AFTER_MS,
        }
    }
}

impl RpcTrafficGovernorConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let mut config = Self::default();
        if let Ok(value) = std::env::var("GREENMQTT_RPC_MAX_INFLIGHT_DEFAULT") {
            if !value.trim().is_empty() {
                config.default_max_in_flight = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid GREENMQTT_RPC_MAX_INFLIGHT_DEFAULT"))?;
            }
        }
        config.sessiondict_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_SESSIONDICT_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        config.dist_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_DIST_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        config.inbox_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_INBOX_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        config.retain_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_RETAIN_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        config.metadata_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_METADATA_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        config.kvrange_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_KVRANGE_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        config.rangecontrol_max_in_flight = env_override_limit(
            "GREENMQTT_RPC_RANGECONTROL_MAX_INFLIGHT",
            config.default_max_in_flight,
        )?;
        if let Ok(value) = std::env::var("GREENMQTT_RPC_RETRY_AFTER_MS") {
            if !value.trim().is_empty() {
                config.retry_after_ms = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid GREENMQTT_RPC_RETRY_AFTER_MS"))?;
            }
        }
        Ok(config)
    }
}

#[derive(Clone)]
pub struct RpcTrafficGovernor {
    sessiondict: Arc<ServiceTrafficGovernor>,
    dist: Arc<ServiceTrafficGovernor>,
    inbox: Arc<ServiceTrafficGovernor>,
    retain: Arc<ServiceTrafficGovernor>,
    metadata: Arc<ServiceTrafficGovernor>,
    kvrange: Arc<ServiceTrafficGovernor>,
    rangecontrol: Arc<ServiceTrafficGovernor>,
}

impl Default for RpcTrafficGovernor {
    fn default() -> Self {
        Self::new(RpcTrafficGovernorConfig::default())
    }
}

impl RpcTrafficGovernor {
    pub fn new(config: RpcTrafficGovernorConfig) -> Self {
        Self {
            sessiondict: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::SessionDict,
                config.sessiondict_max_in_flight,
                config.retry_after_ms,
            )),
            dist: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::Dist,
                config.dist_max_in_flight,
                config.retry_after_ms,
            )),
            inbox: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::Inbox,
                config.inbox_max_in_flight,
                config.retry_after_ms,
            )),
            retain: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::Retain,
                config.retain_max_in_flight,
                config.retry_after_ms,
            )),
            metadata: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::Metadata,
                config.metadata_max_in_flight,
                config.retry_after_ms,
            )),
            kvrange: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::KvRange,
                config.kvrange_max_in_flight,
                config.retry_after_ms,
            )),
            rangecontrol: Arc::new(ServiceTrafficGovernor::new(
                RpcServiceKind::RangeControl,
                config.rangecontrol_max_in_flight,
                config.retry_after_ms,
            )),
        }
    }

    pub fn service(&self, kind: RpcServiceKind) -> Arc<ServiceTrafficGovernor> {
        match kind {
            RpcServiceKind::SessionDict => self.sessiondict.clone(),
            RpcServiceKind::Dist => self.dist.clone(),
            RpcServiceKind::Inbox => self.inbox.clone(),
            RpcServiceKind::Retain => self.retain.clone(),
            RpcServiceKind::Metadata => self.metadata.clone(),
            RpcServiceKind::KvRange => self.kvrange.clone(),
            RpcServiceKind::RangeControl => self.rangecontrol.clone(),
        }
    }

    pub fn snapshots(&self) -> Vec<RpcServiceLoadSnapshot> {
        [
            self.sessiondict.snapshot(),
            self.dist.snapshot(),
            self.inbox.snapshot(),
            self.retain.snapshot(),
            self.metadata.snapshot(),
            self.kvrange.snapshot(),
            self.rangecontrol.snapshot(),
        ]
        .to_vec()
    }

    pub fn service_by_name(&self, service: &str) -> Option<Arc<ServiceTrafficGovernor>> {
        match service.to_ascii_lowercase().as_str() {
            "sessiondict" => Some(self.sessiondict.clone()),
            "dist" => Some(self.dist.clone()),
            "inbox" => Some(self.inbox.clone()),
            "retain" => Some(self.retain.clone()),
            "metadata" => Some(self.metadata.clone()),
            "kvrange" => Some(self.kvrange.clone()),
            "rangecontrol" => Some(self.rangecontrol.clone()),
            _ => None,
        }
    }

    pub fn rules_snapshots(&self) -> Vec<RpcServiceRulesSnapshot> {
        [
            self.sessiondict.rules_snapshot(),
            self.dist.rules_snapshot(),
            self.inbox.rules_snapshot(),
            self.retain.rules_snapshot(),
            self.metadata.rules_snapshot(),
            self.kvrange.rules_snapshot(),
            self.rangecontrol.rules_snapshot(),
        ]
        .to_vec()
    }
}

#[derive(Debug)]
pub struct ServiceTrafficGovernor {
    service: RpcServiceKind,
    env_rules: RpcTrafficRules,
    runtime_rules: Arc<RwLock<Option<RpcTrafficRules>>>,
    endpoint_snapshots: Arc<RwLock<BTreeMap<String, RpcEndpointSnapshot>>>,
    current_in_flight: AtomicUsize,
    overload_count: AtomicU64,
    last_overload_ms: AtomicU64,
}

impl ServiceTrafficGovernor {
    fn new(service: RpcServiceKind, max_in_flight: usize, retry_after_ms: u64) -> Self {
        Self {
            service,
            env_rules: RpcTrafficRules {
                max_in_flight,
                retry_after_ms,
                tenant_prefix_limits: BTreeMap::new(),
                preferred_endpoints: Vec::new(),
                server_group_tags: Vec::new(),
            },
            runtime_rules: Arc::new(RwLock::new(None)),
            endpoint_snapshots: Arc::new(RwLock::new(BTreeMap::new())),
            current_in_flight: AtomicUsize::new(0),
            overload_count: AtomicU64::new(0),
            last_overload_ms: AtomicU64::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ServiceTrafficPermit> {
        let max_in_flight = self.effective_rules().max_in_flight;
        loop {
            let current = self.current_in_flight.load(Ordering::SeqCst);
            if current >= max_in_flight {
                return None;
            }
            if self
                .current_in_flight
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(ServiceTrafficPermit {
                    governor: self.clone(),
                });
            }
        }
    }

    fn record_overload(&self) {
        self.overload_count.fetch_add(1, Ordering::SeqCst);
        self.last_overload_ms
            .store(current_time_ms(), Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> RpcServiceLoadSnapshot {
        let last_overload_ms = self.last_overload_ms.load(Ordering::SeqCst);
        let rules_source = if self
            .runtime_rules
            .read()
            .expect("rpc governor poisoned")
            .is_some()
        {
            "runtime"
        } else {
            "env"
        };
        let effective = self.effective_rules();
        RpcServiceLoadSnapshot {
            service: self.service.as_str(),
            current_in_flight: self.current_in_flight.load(Ordering::SeqCst),
            max_in_flight: effective.max_in_flight,
            overload_count: self.overload_count.load(Ordering::SeqCst),
            retry_after_ms: effective.retry_after_ms,
            last_overload_ms: (last_overload_ms != 0).then_some(last_overload_ms),
            rules_source,
        }
    }

    pub fn effective_rules(&self) -> RpcTrafficRules {
        self.runtime_rules
            .read()
            .expect("rpc governor poisoned")
            .clone()
            .unwrap_or_else(|| self.env_rules.clone())
    }

    pub fn runtime_rules(&self) -> Option<RpcTrafficRules> {
        self.runtime_rules
            .read()
            .expect("rpc governor poisoned")
            .clone()
    }

    pub fn set_runtime_rules(&self, rules: RpcTrafficRules) {
        *self.runtime_rules.write().expect("rpc governor poisoned") = Some(rules);
    }

    pub fn clear_runtime_rules(&self) {
        *self.runtime_rules.write().expect("rpc governor poisoned") = None;
    }

    pub fn note_endpoint_overload(&self, endpoint: &str, retry_after_ms: u64) {
        let now = current_time_ms();
        self.endpoint_snapshots
            .write()
            .expect("rpc governor poisoned")
            .insert(
                endpoint.to_string(),
                RpcEndpointSnapshot {
                    endpoint: endpoint.to_string(),
                    last_overload_ms: Some(now),
                    backoff_until_ms: Some(now + retry_after_ms),
                },
            );
    }

    pub fn clear_endpoint_backoff(&self, endpoint: &str) {
        self.endpoint_snapshots
            .write()
            .expect("rpc governor poisoned")
            .remove(endpoint);
    }

    pub fn endpoint_snapshots(&self) -> Vec<RpcEndpointSnapshot> {
        self.endpoint_snapshots
            .read()
            .expect("rpc governor poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn rules_snapshot(&self) -> RpcServiceRulesSnapshot {
        let runtime_override = self.runtime_rules();
        RpcServiceRulesSnapshot {
            service: self.service.as_str(),
            source: if runtime_override.is_some() {
                "runtime"
            } else {
                "env"
            },
            effective: self.effective_rules(),
            runtime_override,
        }
    }
}

struct ServiceTrafficPermit {
    governor: Arc<ServiceTrafficGovernor>,
}

impl Drop for ServiceTrafficPermit {
    fn drop(&mut self) {
        self.governor
            .current_in_flight
            .fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct SessionDictGrpcClient {
    inner: Arc<Mutex<SessionDictServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct DistGrpcClient {
    inner: Arc<Mutex<DistServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct InboxGrpcClient {
    inner: Arc<Mutex<InboxServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct RetainGrpcClient {
    inner: Arc<Mutex<RetainServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct MetadataGrpcClient {
    inner: Arc<Mutex<MetadataServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct KvRangeGrpcClient {
    inner: Arc<Mutex<KvRangeServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct RaftTransportGrpcClient {
    inner: Arc<Mutex<RaftTransportServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct RangeAdminGrpcClient {
    inner: Arc<Mutex<RangeAdminServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct RangeControlGrpcClient {
    inner: Arc<Mutex<RangeControlServiceClient<Channel>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedRangeControlTarget {
    pub range_id: String,
    pub node_id: NodeId,
    pub endpoint: String,
}

#[derive(Clone)]
pub struct RoutedRangeControlGrpcClient {
    metadata: MetadataGrpcClient,
    fallback_endpoint: String,
    endpoint_backoff: Arc<Mutex<HashMap<String, Instant>>>,
    overload_backoff: Duration,
    service_governor: Option<Arc<ServiceTrafficGovernor>>,
}

#[derive(Clone, Default)]
pub struct KvRangeGrpcExecutorFactory;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SessionDictShardSnapshot {
    pub sessions: Vec<greenmqtt_core::SessionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DistShardSnapshot {
    pub routes: Vec<RouteRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct InboxShardSnapshot {
    pub subscriptions: Vec<Subscription>,
    pub offline_messages: Vec<OfflineMessage>,
    pub inflight_messages: Vec<InflightMessage>,
}

#[derive(Clone, Default)]
pub struct StaticPeerForwarder {
    peers: Arc<RwLock<HashMap<NodeId, StaticPeerClient>>>,
    push_delivery_calls: Arc<AtomicUsize>,
    push_deliveries_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
pub struct DynamicServiceEndpointRegistry {
    assignments: Arc<RwLock<BTreeMap<ServiceShardKey, ServiceShardAssignment>>>,
    members: Arc<RwLock<BTreeMap<NodeId, ClusterNodeMembership>>>,
    ranges: Arc<RwLock<BTreeMap<String, ReplicatedRangeDescriptor>>>,
    balancer_states: Arc<RwLock<BTreeMap<String, BalancerState>>>,
    reconfiguration_states: Arc<RwLock<BTreeMap<String, RangeReconfigurationState>>>,
    control_commands: Arc<RwLock<BTreeMap<String, ControlCommandRecord>>>,
}

#[derive(Clone)]
pub struct PersistentMetadataRegistry {
    engine: Arc<RocksDbKvEngine>,
    range_id: String,
}

#[derive(Clone)]
pub struct ReplicatedMetadataRegistry {
    executor: Arc<dyn KvRangeExecutor>,
    layout: MetadataPlaneLayout,
}

#[derive(Clone)]
pub struct PeriodicAntiEntropyReconciler {
    registry: Arc<DynamicServiceEndpointRegistry>,
    interval: std::time::Duration,
}

pub type StaticServiceEndpointRegistry = DynamicServiceEndpointRegistry;

const METADATA_RANGE_ID: &str = "__metadata";
const ASSIGNMENT_PREFIX: &[u8] = b"assignment\0";
const MEMBER_PREFIX: &[u8] = b"member\0";
const RANGE_PREFIX: &[u8] = b"range\0";
const BALANCER_PREFIX: &[u8] = b"balancer\0";
const RECONFIG_PREFIX: &[u8] = b"reconfig\0";
const COMMAND_PREFIX: &[u8] = b"command\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPlaneLayout {
    pub assignments_range_id: String,
    pub members_range_id: String,
    pub ranges_range_id: String,
    pub balancers_range_id: String,
}

impl MetadataPlaneLayout {
    pub fn single(range_id: impl Into<String>) -> Self {
        let range_id = range_id.into();
        Self {
            assignments_range_id: range_id.clone(),
            members_range_id: range_id.clone(),
            ranges_range_id: range_id.clone(),
            balancers_range_id: range_id,
        }
    }

    pub fn sharded(base_range_id: impl Into<String>) -> Self {
        let base = base_range_id.into();
        Self {
            assignments_range_id: format!("{base}.assignments"),
            members_range_id: format!("{base}.members"),
            ranges_range_id: format!("{base}.ranges"),
            balancers_range_id: format!("{base}.balancers"),
        }
    }

    fn range_for_key(&self, key: &[u8]) -> &str {
        if key.starts_with(ASSIGNMENT_PREFIX) {
            &self.assignments_range_id
        } else if key.starts_with(MEMBER_PREFIX) {
            &self.members_range_id
        } else if key.starts_with(RANGE_PREFIX) {
            &self.ranges_range_id
        } else if key.starts_with(BALANCER_PREFIX) || key.starts_with(COMMAND_PREFIX) {
            &self.balancers_range_id
        } else {
            &self.assignments_range_id
        }
    }

    fn range_for_prefix(&self, prefix: &[u8]) -> &str {
        self.range_for_key(prefix)
    }
}

#[derive(Clone)]
struct StaticPeerClient {
    endpoint: String,
    client: Arc<Mutex<BrokerPeerServiceClient<Channel>>>,
}

#[derive(Clone, Default)]
pub struct NoopDeliverySink;

#[derive(Clone, Default)]
pub struct NoopReplicaTransport;

#[derive(Clone)]
struct SessionDictRpc {
    inner: Arc<dyn SessionDirectory>,
    assignment_registry: Option<Arc<dyn MetadataRegistry>>,
}

#[derive(Clone)]
struct DistRpc {
    inner: Arc<dyn DistRouter>,
    assignment_registry: Option<Arc<dyn MetadataRegistry>>,
}

#[derive(Clone)]
struct InboxRpc {
    inner: Arc<dyn InboxService>,
    assignment_registry: Option<Arc<dyn MetadataRegistry>>,
    lwt_sink: Option<Arc<dyn DelayedLwtSink>>,
}

#[derive(Clone)]
struct RetainRpc {
    inner: Arc<dyn RetainStoreService>,
}

#[derive(Clone)]
struct BrokerPeerRpc {
    inner: Arc<dyn DeliverySink>,
}

#[derive(Clone)]
struct MetadataRpc {
    inner: Option<Arc<dyn MetadataRegistry>>,
}

#[derive(Clone)]
struct KvRangeRpc {
    inner: Option<Arc<dyn KvRangeHost>>,
}

#[derive(Clone)]
struct RaftTransportRpc {
    inner: Option<Arc<dyn KvRangeHost>>,
}

#[derive(Clone)]
struct RangeAdminRpc {
    host: Option<Arc<dyn KvRangeHost>>,
    runtime: Option<Arc<greenmqtt_kv_server::ReplicaRuntime>>,
}

#[derive(Clone)]
struct RangeControlRpc {
    inner: Option<Arc<greenmqtt_kv_server::ReplicaRuntime>>,
    registry: Option<Arc<dyn MetadataRegistry>>,
}

async fn apply_committed_entries_for_range(
    hosted: &greenmqtt_kv_server::HostedRange,
) -> anyhow::Result<usize> {
    let entries = hosted.raft.committed_entries().await?;
    if entries.is_empty() {
        return Ok(0);
    }
    let mut applied = 0usize;
    let mut last_index = None;
    for entry in &entries {
        let mutations: Vec<KvMutation> = bincode::deserialize(&entry.command)?;
        hosted.space.writer().apply(mutations).await?;
        applied += 1;
        last_index = Some(entry.index);
    }
    if let Some(index) = last_index {
        hosted.raft.mark_applied(index).await?;
    }
    Ok(applied)
}

fn proto_role(role: greenmqtt_kv_raft::RaftNodeRole) -> String {
    match role {
        greenmqtt_kv_raft::RaftNodeRole::Follower => "follower".to_string(),
        greenmqtt_kv_raft::RaftNodeRole::Candidate => "candidate".to_string(),
        greenmqtt_kv_raft::RaftNodeRole::Leader => "leader".to_string(),
    }
}

fn to_proto_replica_lag(replica: &ReplicaLagSnapshot) -> ReplicaLagRecord {
    ReplicaLagRecord {
        node_id: replica.node_id,
        lag: replica.lag,
        match_index: replica.match_index,
        next_index: replica.next_index,
    }
}

fn to_proto_reconfiguration(state: &RangeReconfigurationState) -> RangeReconfigurationRecord {
    let (phase, has_phase) = match &state.phase {
        Some(ReconfigurationPhase::StagingLearners) => ("staging_learners".to_string(), true),
        Some(ReconfigurationPhase::JointConsensus) => ("joint_consensus".to_string(), true),
        Some(ReconfigurationPhase::Finalizing) => ("finalizing".to_string(), true),
        None => (String::new(), false),
    };
    RangeReconfigurationRecord {
        old_voters: state.old_voters.clone(),
        old_learners: state.old_learners.clone(),
        current_voters: state.current_voters.clone(),
        current_learners: state.current_learners.clone(),
        joint_voters: state.joint_voters.clone(),
        joint_learners: state.joint_learners.clone(),
        pending_voters: state.pending_voters.clone(),
        pending_learners: state.pending_learners.clone(),
        phase,
        has_phase,
        blocked_on_catch_up: state.blocked_on_catch_up,
    }
}

fn to_proto_range_health(health: &RangeHealthSnapshot) -> RangeHealthRecord {
    RangeHealthRecord {
        range_id: health.range_id.clone(),
        role: proto_role(health.role),
        current_term: health.current_term,
        leader_node_id: health.leader_node_id.unwrap_or_default(),
        has_leader_node_id: health.leader_node_id.is_some(),
        commit_index: health.commit_index,
        applied_index: health.applied_index,
        latest_snapshot_index: health.latest_snapshot_index.unwrap_or_default(),
        has_latest_snapshot_index: health.latest_snapshot_index.is_some(),
        replica_lag: health
            .replica_lag
            .iter()
            .map(to_proto_replica_lag)
            .collect(),
        reconfiguration: Some(to_proto_reconfiguration(&health.reconfiguration)),
        lifecycle: match health.lifecycle {
            ServiceShardLifecycle::Bootstrapping => "bootstrapping",
            ServiceShardLifecycle::Serving => "serving",
            ServiceShardLifecycle::Draining => "draining",
            ServiceShardLifecycle::Recovering => "recovering",
            ServiceShardLifecycle::Offline => "offline",
        }
        .to_string(),
    }
}

fn to_proto_zombie_range(snapshot: &ZombieRangeSnapshot) -> ZombieRangeRecord {
    ZombieRangeRecord {
        range_id: snapshot.range_id.clone(),
        lifecycle: match snapshot.lifecycle {
            ServiceShardLifecycle::Bootstrapping => "bootstrapping",
            ServiceShardLifecycle::Serving => "serving",
            ServiceShardLifecycle::Draining => "draining",
            ServiceShardLifecycle::Recovering => "recovering",
            ServiceShardLifecycle::Offline => "offline",
        }
        .to_string(),
        leader_node_id: snapshot.leader_node_id.unwrap_or_default(),
        has_leader_node_id: snapshot.leader_node_id.is_some(),
    }
}

impl SessionDictShardSnapshot {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn checksum(&self) -> anyhow::Result<u64> {
        snapshot_checksum(self)
    }
}

impl DistShardSnapshot {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn checksum(&self) -> anyhow::Result<u64> {
        snapshot_checksum(self)
    }
}

impl InboxShardSnapshot {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn checksum(&self) -> anyhow::Result<u64> {
        snapshot_checksum(self)
    }
}

fn snapshot_checksum<T: Serialize>(value: &T) -> anyhow::Result<u64> {
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(hasher.finish())
}

fn metadata_key(prefix: &[u8], suffix: &[u8]) -> Bytes {
    let mut key = Vec::with_capacity(prefix.len() + suffix.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(suffix);
    Bytes::from(key)
}

fn assignment_key(shard: &ServiceShardKey) -> anyhow::Result<Bytes> {
    Ok(metadata_key(ASSIGNMENT_PREFIX, &bincode::serialize(shard)?))
}

fn member_key(node_id: NodeId) -> Bytes {
    metadata_key(MEMBER_PREFIX, &node_id.to_be_bytes())
}

fn range_key(range_id: &str) -> Bytes {
    metadata_key(RANGE_PREFIX, range_id.as_bytes())
}

fn balancer_key(name: &str) -> Bytes {
    metadata_key(BALANCER_PREFIX, name.as_bytes())
}

fn reconfiguration_key(range_id: &str) -> Bytes {
    metadata_key(RECONFIG_PREFIX, range_id.as_bytes())
}

fn command_key(command_id: &str) -> Bytes {
    metadata_key(COMMAND_PREFIX, command_id.as_bytes())
}

fn decode_metadata_value<T: DeserializeOwned>(value: &[u8]) -> anyhow::Result<T> {
    Ok(bincode::deserialize(value)?)
}

fn encode_metadata_value<T: Serialize>(value: &T) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(value)?))
}

fn sessiondict_scope(identity: &ClientIdentity) -> String {
    format!("identity:{}:{}", identity.user_id, identity.client_id)
}

fn shard_matches_session_scope(shard: &ServiceShardKey, session_id: &str) -> bool {
    shard.scope == "*" || shard.scope == session_id
}

fn shard_matches_sessiondict_record(
    shard: &ServiceShardKey,
    record: &greenmqtt_core::SessionRecord,
) -> bool {
    shard.tenant_id == record.identity.tenant_id
        && (shard.scope == "*"
            || shard.scope == record.session_id
            || shard.scope == sessiondict_scope(&record.identity))
}

fn shard_matches_inbox_session(shard: &ServiceShardKey, tenant_id: &str, session_id: &str) -> bool {
    shard.tenant_id == tenant_id && shard_matches_session_scope(shard, session_id)
}

fn shard_snapshot_request_for_assignment(
    assignment: &ServiceShardAssignment,
) -> ShardSnapshotRequest {
    ShardSnapshotRequest {
        tenant_id: assignment.shard.tenant_id.clone(),
        scope: assignment.shard.scope.clone(),
        owner_node_id: assignment.owner_node_id(),
        epoch: assignment.epoch,
        fencing_token: assignment.fencing_token,
    }
}

fn sessiondict_shard_from_request(request: ShardSnapshotRequest) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::SessionDict,
        tenant_id: request.tenant_id,
        scope: if request.scope.is_empty() {
            "*".into()
        } else {
            request.scope
        },
    }
}

fn inbox_shard_from_request(request: ShardSnapshotRequest) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::Inbox,
        tenant_id: request.tenant_id,
        scope: if request.scope.is_empty() {
            "*".into()
        } else {
            request.scope
        },
    }
}

fn assignment_from_snapshot_request(
    kind: ServiceShardKind,
    request: &ShardSnapshotRequest,
    service_kind: ServiceKind,
) -> Option<ServiceShardAssignment> {
    if request.owner_node_id == 0 && request.epoch == 0 && request.fencing_token == 0 {
        return None;
    }
    Some(ServiceShardAssignment::new(
        ServiceShardKey {
            kind,
            tenant_id: request.tenant_id.clone(),
            scope: if request.scope.is_empty() {
                "*".into()
            } else {
                request.scope.clone()
            },
        },
        ServiceEndpoint::new(service_kind, request.owner_node_id, String::new()),
        request.epoch,
        request.fencing_token,
        ServiceShardLifecycle::Serving,
    ))
}

fn snapshot_chunks(bytes: Vec<u8>, checksum: u64) -> Vec<ShardSnapshotChunk> {
    if bytes.is_empty() {
        return vec![ShardSnapshotChunk {
            data: Vec::new(),
            sequence: 0,
            checksum,
            done: true,
        }];
    }
    let last_index = bytes.chunks(SNAPSHOT_CHUNK_BYTES).count().saturating_sub(1);
    bytes
        .chunks(SNAPSHOT_CHUNK_BYTES)
        .enumerate()
        .map(|(index, chunk)| ShardSnapshotChunk {
            data: chunk.to_vec(),
            sequence: index as u32,
            checksum,
            done: index == last_index,
        })
        .collect()
}

fn same_route_identity(left: &RouteRecord, right: &RouteRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.topic_filter == right.topic_filter
        && left.session_id == right.session_id
        && left.shared_group == right.shared_group
}

fn validate_kv_request_fence(
    hosted: &greenmqtt_kv_server::HostedRange,
    expected_epoch: u64,
) -> Result<(), Status> {
    if expected_epoch != 0 && hosted.descriptor.epoch != expected_epoch {
        return Err(internal_status(anyhow::anyhow!(
            "range epoch mismatch: expected={}, actual={}",
            expected_epoch,
            hosted.descriptor.epoch
        )));
    }
    if hosted.descriptor.lifecycle != ServiceShardLifecycle::Serving {
        return Err(internal_status(anyhow::anyhow!(
            "range lifecycle is not serving: {:?}",
            hosted.descriptor.lifecycle
        )));
    }
    Ok(())
}

async fn validate_snapshot_assignment(
    registry: &Option<Arc<dyn MetadataRegistry>>,
    expected: Option<ServiceShardAssignment>,
) -> Result<(), Status> {
    let (Some(registry), Some(expected)) = (registry.as_ref(), expected) else {
        return Ok(());
    };
    let current = registry
        .resolve_assignment(&expected.shard)
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("missing shard assignment"))?;
    if !current.matches_owner_epoch_fence(&expected) {
        counter!(
            "mqtt_shard_fencing_reject_total",
            "kind" => format!("{:?}", expected.shard.kind).to_lowercase(),
            "tenant_id" => expected.shard.tenant_id.clone()
        )
        .increment(1);
        return Err(Status::failed_precondition("stale shard assignment"));
    }
    Ok(())
}

mod client_traits;
mod clients;
mod registry_impls;
mod runtime;
mod service_impls;

pub(crate) use service_impls::internal_status;

#[cfg(test)]
mod tests;
