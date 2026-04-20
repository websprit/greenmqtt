use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_broker::{DeliverySink, PeerForwarder, PeerRegistry};
use greenmqtt_core::{
    BalancerState, BalancerStateRegistry, ClientIdentity, ClusterMembershipRegistry,
    ClusterNodeMembership, Delivery, InflightMessage, MetadataRegistry, NodeId, OfflineMessage,
    ReplicatedRangeDescriptor, ReplicatedRangeRegistry, RetainedMessage, RouteRecord,
    ServiceEndpoint, ServiceEndpointRegistry, ServiceKind, ServiceShardAssignment, ServiceShardKey,
    ServiceShardKind, ServiceShardLifecycle, ServiceShardRecoveryControl, ServiceShardTransition,
    ServiceShardTransitionKind, Subscription,
};
use greenmqtt_dist::{dist_tenant_shard, DistRouter};
use greenmqtt_inbox::{
    inbox_session_shard, inbox_tenant_scan_shard, inflight_tenant_scan_shard, InboxService,
};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeExecutorFactory, KvRangeRouter, RangeRoute};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, RocksDbKvEngine,
};
use greenmqtt_kv_raft::RaftMessage;
use greenmqtt_kv_server::{KvRangeHost, ReplicaTransport};
use greenmqtt_proto::internal::{
    broker_peer_service_client::BrokerPeerServiceClient,
    broker_peer_service_server::{BrokerPeerService, BrokerPeerServiceServer},
    dist_service_client::DistServiceClient,
    dist_service_server::{DistService, DistServiceServer},
    inbox_service_client::InboxServiceClient,
    inbox_service_server::{InboxService as ProtoInboxService, InboxServiceServer},
    kv_range_service_client::KvRangeServiceClient,
    kv_range_service_server::{KvRangeService, KvRangeServiceServer},
    metadata_service_client::MetadataServiceClient,
    metadata_service_server::{MetadataService, MetadataServiceServer},
    raft_transport_service_client::RaftTransportServiceClient,
    raft_transport_service_server::{RaftTransportService, RaftTransportServiceServer},
    retain_service_client::RetainServiceClient,
    retain_service_server::{RetainService, RetainServiceServer},
    session_dict_service_client::SessionDictServiceClient,
    session_dict_service_server::{SessionDictService, SessionDictServiceServer},
    AddRouteRequest, BalancerStateListReply, BalancerStateReply, BalancerStateRequest,
    BalancerStateUpsertReply, BalancerStateUpsertRequest, CountReply, InboxAckInflightRequest,
    InboxAttachRequest, InboxDetachRequest, InboxEnqueueRequest, InboxFetchInflightReply,
    InboxFetchInflightRequest, InboxFetchReply, InboxFetchRequest,
    InboxListAllSubscriptionsRequest, InboxListMessagesRequest, InboxListSubscriptionsReply,
    InboxListSubscriptionsRequest, InboxLookupSubscriptionReply, InboxLookupSubscriptionRequest,
    InboxPurgeSessionRequest, InboxStageInflightRequest, InboxStatsReply, InboxSubscribeRequest,
    InboxUnsubscribeReply, InboxUnsubscribeRequest, KvRangeApplyRequest, KvRangeCheckpointReply,
    KvRangeCheckpointRequest, KvRangeGetReply, KvRangeGetRequest, KvRangeScanReply,
    KvRangeScanRequest, KvRangeSnapshotReply, KvRangeSnapshotRequest, ListRoutesReply,
    ListRoutesRequest, ListSessionRoutesReply, ListSessionRoutesRequest, ListSessionsReply,
    ListSessionsRequest, LookupSessionByIdRequest, LookupSessionReply, LookupSessionRequest,
    MatchTopicReply, MatchTopicRequest, MemberListReply, MemberLookupRequest, MemberRecordReply,
    NamedBalancerStateRecord, PushDeliveriesReply, PushDeliveriesRequest, PushDeliveryReply,
    PushDeliveryRequest, RaftTransportRequest, RangeListReply, RangeListRequest,
    RangeLookupRequest, RangeRecordReply, RangeUpsertRequest, RegisterSessionReply,
    RegisterSessionRequest, RemoveRouteRequest, RemoveSessionRoutesReply,
    RemoveSessionRoutesRequest, RetainMatchReply, RetainMatchRequest, RetainWriteRequest,
    RouteRangeRequest, ShardSnapshotChunk, ShardSnapshotRequest, UnregisterSessionRequest,
};
use greenmqtt_proto::{
    from_proto_balancer_state, from_proto_client_identity, from_proto_cluster_node_membership,
    from_proto_delivery, from_proto_inflight, from_proto_kv_mutation,
    from_proto_kv_range_checkpoint, from_proto_kv_range_snapshot, from_proto_offline,
    from_proto_raft_transport_request, from_proto_replicated_range, from_proto_retain,
    from_proto_route, from_proto_session, from_proto_shard_kind, from_proto_subscription,
    to_proto_balancer_state, to_proto_cluster_node_membership, to_proto_delivery,
    to_proto_inflight, to_proto_kv_entry, to_proto_kv_range_checkpoint, to_proto_kv_range_snapshot,
    to_proto_offline, to_proto_raft_transport_request, to_proto_replicated_range, to_proto_retain,
    to_proto_route, to_proto_session, to_proto_shard_kind, to_proto_subscription,
};
use greenmqtt_retain::{retain_tenant_shard, RetainService as RetainStoreService};
use greenmqtt_sessiondict::{session_identity_shard, SessionDirectory};
use metrics::counter;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct RpcRuntime {
    pub sessiondict: Arc<dyn SessionDirectory>,
    pub dist: Arc<dyn DistRouter>,
    pub inbox: Arc<dyn InboxService>,
    pub retain: Arc<dyn RetainStoreService>,
    pub peer_sink: Arc<dyn DeliverySink>,
    pub assignment_registry: Option<Arc<dyn MetadataRegistry>>,
    pub range_host: Option<Arc<dyn KvRangeHost>>,
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
}

#[derive(Clone)]
pub struct PersistentMetadataRegistry {
    engine: Arc<RocksDbKvEngine>,
    range_id: String,
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

#[derive(Clone)]
struct StaticPeerClient {
    endpoint: String,
    client: Arc<Mutex<BrokerPeerServiceClient<Channel>>>,
}

#[derive(Clone, Default)]
pub struct NoopDeliverySink;

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

impl SessionDictGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                SessionDictServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Self> {
        let shard = session_identity_shard(identity);
        let assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict endpoint registered for shard {shard:?}")
        })?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn export_shard_snapshot(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<SessionDictShardSnapshot> {
        anyhow::ensure!(
            shard.kind == ServiceShardKind::SessionDict,
            "sessiondict snapshot requires SessionDict shard"
        );
        let sessions = self
            .list_sessions(Some(&shard.tenant_id))
            .await?
            .into_iter()
            .filter(|record| shard_matches_sessiondict_record(shard, record))
            .collect();
        Ok(SessionDictShardSnapshot { sessions })
    }

    pub async fn stream_shard_snapshot(
        &self,
        assignment: &ServiceShardAssignment,
    ) -> anyhow::Result<SessionDictShardSnapshot> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .stream_shard_snapshot(shard_snapshot_request_for_assignment(assignment))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        let mut expected_checksum = None;
        while let Some(chunk) = stream.message().await? {
            if let Some(checksum) = expected_checksum {
                anyhow::ensure!(
                    checksum == chunk.checksum,
                    "sessiondict stream checksum drift"
                );
            } else {
                expected_checksum = Some(chunk.checksum);
            }
            bytes.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        let snapshot = SessionDictShardSnapshot::decode(&bytes)?;
        if let Some(checksum) = expected_checksum {
            anyhow::ensure!(
                snapshot.checksum()? == checksum,
                "sessiondict stream checksum mismatch"
            );
        }
        Ok(snapshot)
    }

    pub async fn import_shard_snapshot(
        &self,
        snapshot: &SessionDictShardSnapshot,
    ) -> anyhow::Result<usize> {
        for session in &snapshot.sessions {
            self.register(session.clone()).await?;
        }
        Ok(snapshot.sessions.len())
    }

    pub async fn move_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict assignment registered for shard {shard:?}")
        })?;
        let source =
            SessionDictGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .move_shard_to_member(shard.clone(), target_node_id)
            .await?;
        let target = SessionDictGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;

        for session in snapshot.sessions {
            source.unregister(&session.session_id).await?;
        }

        Ok(assignment)
    }

    pub async fn catch_up_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict assignment registered for shard {shard:?}")
        })?;
        let source =
            SessionDictGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let expected_checksum = snapshot.checksum()?;

        let assignment = registry
            .catch_up_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = SessionDictGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        let restored = target.export_shard_snapshot(&shard).await?;
        anyhow::ensure!(
            restored.checksum()? == expected_checksum,
            "sessiondict shard catch-up checksum mismatch"
        );
        Ok(assignment)
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        let shard = greenmqtt_sessiondict::session_scan_shard(tenant_id);
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict assignment registered for shard {shard:?}")
        })?;
        let source =
            SessionDictGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::SessionDict)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no sessiondict endpoint")
            })?;
        let replica = SessionDictGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_snapshot = replica.export_shard_snapshot(&shard).await?;
        if source_snapshot.checksum()? != replica_snapshot.checksum()? {
            for record in &replica_snapshot.sessions {
                if !source_snapshot
                    .sessions
                    .iter()
                    .any(|candidate| candidate.session_id == record.session_id)
                {
                    replica.unregister(&record.session_id).await?;
                }
            }
            for record in &source_snapshot.sessions {
                if !replica_snapshot.sessions.iter().any(|candidate| {
                    candidate.session_id == record.session_id && candidate == record
                }) {
                    replica.register(record.clone()).await?;
                }
            }
            let repaired = replica.export_shard_snapshot(&shard).await?;
            anyhow::ensure!(
                repaired.checksum()? == source_snapshot.checksum()?,
                "sessiondict shard replica anti-entropy checksum mismatch"
            );
        }
        source_snapshot.checksum()
    }
}

impl StaticPeerForwarder {
    pub fn push_delivery_calls(&self) -> usize {
        self.push_delivery_calls.load(Ordering::SeqCst)
    }

    pub fn push_deliveries_calls(&self) -> usize {
        self.push_deliveries_calls.load(Ordering::SeqCst)
    }
}

impl DynamicServiceEndpointRegistry {
    async fn validate_current_assignment(
        &self,
        expected: &ServiceShardAssignment,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&expected.shard)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no assignment registered for shard {:?}", expected.shard)
            })?;
        anyhow::ensure!(
            current.matches_owner_epoch_fence(expected),
            "stale shard assignment for {:?}: expected owner={} epoch={} fence={}, current owner={} epoch={} fence={}",
            expected.shard,
            expected.owner_node_id(),
            expected.epoch,
            expected.fencing_token,
            current.owner_node_id(),
            current.epoch,
            current.fencing_token,
        );
        Ok(current)
    }

    pub async fn sync_members(&self, members: Vec<ClusterNodeMembership>) -> anyhow::Result<usize> {
        let mut guard = self.members.write().expect("service registry poisoned");
        let mut next = BTreeMap::new();
        let mut changed = 0usize;
        for member in members {
            if guard.get(&member.node_id) != Some(&member) {
                changed += 1;
            }
            next.insert(member.node_id, member);
        }
        changed += guard
            .keys()
            .filter(|node_id| !next.contains_key(node_id))
            .count();
        *guard = next;
        Ok(changed)
    }

    pub async fn transition_shard_to_member(
        &self,
        kind: ServiceShardTransitionKind,
        shard: ServiceShardKey,
        source_node_id: Option<NodeId>,
        target_node_id: NodeId,
        lifecycle: ServiceShardLifecycle,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let member = self
            .resolve_member(target_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target member {target_node_id} not found"))?;
        let service_kind = shard.service_kind();
        let endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == service_kind)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("target member {target_node_id} has no {service_kind:?} endpoint")
            })?;
        let current = self.resolve_assignment(&shard).await?;
        let next_epoch = current
            .as_ref()
            .map(|assignment| assignment.epoch + 1)
            .unwrap_or(1);
        let next_fencing_token = current
            .as_ref()
            .map(|assignment| assignment.fencing_token + 1)
            .unwrap_or(1);
        let assignment = ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(service_kind, target_node_id, endpoint.endpoint),
            next_epoch,
            next_fencing_token,
            lifecycle,
        );
        self.apply_transition(ServiceShardTransition::new(
            kind,
            shard,
            source_node_id,
            assignment.clone(),
        ))
        .await?;
        Ok(assignment)
    }

    fn select_replacement_member(
        &self,
        service_kind: ServiceKind,
        excluded_node_id: NodeId,
    ) -> Option<NodeId> {
        let members = self.members.read().expect("service registry poisoned");
        members
            .values()
            .filter(|member| {
                member.node_id != excluded_node_id
                    && matches!(
                        member.lifecycle,
                        greenmqtt_core::ClusterNodeLifecycle::Serving
                    )
                    && member
                        .endpoints
                        .iter()
                        .any(|endpoint| endpoint.kind == service_kind)
            })
            .map(|member| member.node_id)
            .min()
    }

    pub async fn move_shard_to_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no assignment registered for shard {:?}", shard))?;
        self.move_shard_to_member_with_fencing(&current, target_node_id)
            .await
    }

    pub async fn move_shard_to_member_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.validate_current_assignment(expected_source).await?;
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Migration,
            expected_source.shard.clone(),
            Some(expected_source.owner_node_id()),
            target_node_id,
            ServiceShardLifecycle::Draining,
        )
        .await
    }

    pub async fn failover_shard_to_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_node_id = self
            .resolve_assignment(&shard)
            .await?
            .map(|assignment| assignment.owner_node_id());
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Failover,
            shard,
            source_node_id,
            target_node_id,
            ServiceShardLifecycle::Recovering,
        )
        .await
    }

    pub async fn bootstrap_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Bootstrap,
            shard,
            None,
            target_node_id,
            ServiceShardLifecycle::Bootstrapping,
        )
        .await
    }

    pub async fn catch_up_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no assignment registered for shard {:?}", shard))?;
        self.catch_up_shard_on_member_with_fencing(&current, target_node_id)
            .await
    }

    pub async fn catch_up_shard_on_member_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.validate_current_assignment(expected_source).await?;
        self.transition_shard_to_member(
            ServiceShardTransitionKind::CatchUp,
            expected_source.shard.clone(),
            Some(expected_source.owner_node_id()),
            target_node_id,
            ServiceShardLifecycle::Recovering,
        )
        .await
    }

    pub async fn repair_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no assignment registered for shard {:?}", shard))?;
        self.repair_shard_on_member_with_fencing(&current, target_node_id)
            .await
    }

    pub async fn repair_shard_on_member_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.validate_current_assignment(expected_source).await?;
        self.transition_shard_to_member(
            ServiceShardTransitionKind::AntiEntropy,
            expected_source.shard.clone(),
            Some(expected_source.owner_node_id()),
            target_node_id,
            ServiceShardLifecycle::Serving,
        )
        .await
    }

    pub async fn drain_shard(
        &self,
        shard: ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let Some(current) = self.resolve_assignment(&shard).await? else {
            return Ok(None);
        };
        self.drain_shard_with_fencing(&current).await
    }

    pub async fn drain_shard_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let current = self.validate_current_assignment(expected_source).await?;
        let drained = ServiceShardAssignment::new(
            current.shard.clone(),
            current.endpoint.clone(),
            current.epoch + 1,
            current.fencing_token + 1,
            ServiceShardLifecycle::Draining,
        );
        self.apply_transition(ServiceShardTransition::new(
            ServiceShardTransitionKind::Migration,
            current.shard.clone(),
            Some(current.owner_node_id()),
            drained.clone(),
        ))
        .await?;
        Ok(Some(drained))
    }

    pub async fn handle_member_lifecycle_transition(
        &self,
        node_id: NodeId,
        lifecycle: greenmqtt_core::ClusterNodeLifecycle,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let _ = self
            .set_member_lifecycle(node_id, lifecycle.clone())
            .await?;
        let owned: Vec<_> = self
            .list_assignments(None)
            .await?
            .into_iter()
            .filter(|assignment| assignment.owner_node_id() == node_id)
            .collect();
        let mut transitioned = Vec::new();
        for assignment in owned {
            let Some(target_node_id) =
                self.select_replacement_member(assignment.shard.service_kind(), node_id)
            else {
                continue;
            };
            let next = match lifecycle {
                greenmqtt_core::ClusterNodeLifecycle::Leaving => {
                    self.move_shard_to_member_with_fencing(&assignment, target_node_id)
                        .await?
                }
                greenmqtt_core::ClusterNodeLifecycle::Offline => {
                    self.failover_shard_to_member(assignment.shard.clone(), target_node_id)
                        .await?
                }
                _ => continue,
            };
            transitioned.push(next);
        }
        Ok(transitioned)
    }
}

impl PersistentMetadataRegistry {
    pub async fn open(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let engine = Arc::new(RocksDbKvEngine::open(path.into())?);
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: METADATA_RANGE_ID.to_string(),
                boundary: greenmqtt_core::RangeBoundary::full(),
            })
            .await?;
        Ok(Self {
            engine,
            range_id: METADATA_RANGE_ID.to_string(),
        })
    }

    async fn read_value<T: DeserializeOwned>(&self, key: Bytes) -> anyhow::Result<Option<T>> {
        let range = self.engine.open_range(&self.range_id).await?;
        range
            .reader()
            .get(&key)
            .await?
            .map(|value| decode_metadata_value(&value))
            .transpose()
    }

    async fn write_value<T: Serialize>(&self, key: Bytes, value: &T) -> anyhow::Result<()> {
        let range = self.engine.open_range(&self.range_id).await?;
        range
            .writer()
            .apply(vec![KvMutation {
                key,
                value: Some(encode_metadata_value(value)?),
            }])
            .await
    }

    async fn delete_value<T: DeserializeOwned>(&self, key: Bytes) -> anyhow::Result<Option<T>> {
        let previous = self.read_value::<T>(key.clone()).await?;
        if previous.is_some() {
            let range = self.engine.open_range(&self.range_id).await?;
            range
                .writer()
                .apply(vec![KvMutation { key, value: None }])
                .await?;
        }
        Ok(previous)
    }

    async fn scan_prefix<T: DeserializeOwned>(&self, prefix: &[u8]) -> anyhow::Result<Vec<T>> {
        let range = self.engine.open_range(&self.range_id).await?;
        Ok(range
            .reader()
            .scan(&greenmqtt_core::RangeBoundary::full(), usize::MAX)
            .await?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(_, value)| decode_metadata_value(&value))
            .collect::<Result<Vec<_>, _>>()?)
    }
}

#[async_trait]
impl ServiceEndpointRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("service registry poisoned")
            .insert(assignment.shard.clone(), assignment))
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .read()
            .expect("service registry poisoned")
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
            .expect("service registry poisoned")
            .remove(shard))
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments: Vec<_> = self
            .assignments
            .read()
            .expect("service registry poisoned")
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
impl ServiceEndpointRegistry for PersistentMetadataRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let key = assignment_key(&assignment.shard)?;
        let previous = self
            .read_value::<ServiceShardAssignment>(key.clone())
            .await?;
        self.write_value(key, &assignment).await?;
        Ok(previous)
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.read_value(assignment_key(shard)?).await
    }

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.delete_value(assignment_key(shard)?).await
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments = self
            .scan_prefix::<ServiceShardAssignment>(ASSIGNMENT_PREFIX)
            .await?;
        if let Some(kind) = kind {
            assignments.retain(|assignment| assignment.shard.kind == kind);
        }
        assignments.sort_by(|left, right| left.shard.cmp(&right.shard));
        Ok(assignments)
    }
}

#[async_trait]
impl ReplicatedRangeRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .ranges
            .write()
            .expect("service registry poisoned")
            .insert(descriptor.id.clone(), descriptor))
    }

    async fn resolve_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .ranges
            .read()
            .expect("service registry poisoned")
            .get(range_id)
            .cloned())
    }

    async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .ranges
            .write()
            .expect("service registry poisoned")
            .remove(range_id))
    }

    async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut ranges: Vec<_> = self
            .ranges
            .read()
            .expect("service registry poisoned")
            .values()
            .filter(|descriptor| {
                shard_kind
                    .as_ref()
                    .map(|kind| descriptor.shard.kind == *kind)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        ranges.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(ranges)
    }
}

#[async_trait]
impl ReplicatedRangeRegistry for PersistentMetadataRegistry {
    async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let key = range_key(&descriptor.id);
        let previous = self
            .read_value::<ReplicatedRangeDescriptor>(key.clone())
            .await?;
        self.write_value(key, &descriptor).await?;
        Ok(previous)
    }

    async fn resolve_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.read_value(range_key(range_id)).await
    }

    async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.delete_value(range_key(range_id)).await
    }

    async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut ranges = self
            .scan_prefix::<ReplicatedRangeDescriptor>(RANGE_PREFIX)
            .await?;
        if let Some(kind) = shard_kind {
            ranges.retain(|descriptor| descriptor.shard.kind == kind);
        }
        ranges.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(ranges)
    }
}

#[async_trait]
impl BalancerStateRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_balancer_state(
        &self,
        balancer_name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        Ok(self
            .balancer_states
            .write()
            .expect("service registry poisoned")
            .insert(balancer_name.to_string(), state))
    }

    async fn resolve_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        Ok(self
            .balancer_states
            .read()
            .expect("service registry poisoned")
            .get(balancer_name)
            .cloned())
    }

    async fn remove_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        Ok(self
            .balancer_states
            .write()
            .expect("service registry poisoned")
            .remove(balancer_name))
    }

    async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        Ok(self
            .balancer_states
            .read()
            .expect("service registry poisoned")
            .clone())
    }
}

#[async_trait]
impl BalancerStateRegistry for PersistentMetadataRegistry {
    async fn upsert_balancer_state(
        &self,
        balancer_name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        let key = balancer_key(balancer_name);
        let previous = self.read_value::<BalancerState>(key.clone()).await?;
        self.write_value(key, &state).await?;
        Ok(previous)
    }

    async fn resolve_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.read_value(balancer_key(balancer_name)).await
    }

    async fn remove_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.delete_value(balancer_key(balancer_name)).await
    }

    async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        let range = self.engine.open_range(&self.range_id).await?;
        let mut states = BTreeMap::new();
        for (key, value) in range
            .reader()
            .scan(&greenmqtt_core::RangeBoundary::full(), usize::MAX)
            .await?
        {
            if !key.starts_with(BALANCER_PREFIX) {
                continue;
            }
            let name = std::str::from_utf8(&key[BALANCER_PREFIX.len()..])?.to_string();
            states.insert(name, decode_metadata_value(&value)?);
        }
        Ok(states)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("service registry poisoned")
            .insert(member.node_id, member))
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .read()
            .expect("service registry poisoned")
            .get(&node_id)
            .cloned())
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("service registry poisoned")
            .remove(&node_id))
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut members: Vec<_> = self
            .members
            .read()
            .expect("service registry poisoned")
            .values()
            .cloned()
            .collect();
        members.sort_by_key(|member| member.node_id);
        Ok(members)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for PersistentMetadataRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let key = member_key(member.node_id);
        let previous = self
            .read_value::<ClusterNodeMembership>(key.clone())
            .await?;
        self.write_value(key, &member).await?;
        Ok(previous)
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.read_value(member_key(node_id)).await
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.delete_value(member_key(node_id)).await
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut members = self
            .scan_prefix::<ClusterNodeMembership>(MEMBER_PREFIX)
            .await?;
        members.sort_by_key(|member| member.node_id);
        Ok(members)
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for DynamicServiceEndpointRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let mut assignments = self.assignments.write().expect("service registry poisoned");
        if let Some(source_node_id) = transition.source_node_id {
            if let Some(current) = assignments.get(&transition.shard) {
                anyhow::ensure!(
                    current.owner_node_id() == source_node_id,
                    "source owner mismatch for shard transition"
                );
            }
        }
        Ok(assignments.insert(transition.shard.clone(), transition.target_assignment))
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for PersistentMetadataRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        if let Some(source_node_id) = transition.source_node_id {
            if let Some(current) = self.resolve_assignment(&transition.shard).await? {
                anyhow::ensure!(
                    current.owner_node_id() == source_node_id,
                    "source owner mismatch for shard transition"
                );
            }
        }
        self.upsert_assignment(transition.target_assignment).await
    }
}

impl DistGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                DistServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = dist_tenant_shard(tenant_id);
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn export_shard_snapshot(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<DistShardSnapshot> {
        Ok(DistShardSnapshot {
            routes: self.list_routes(Some(tenant_id)).await?,
        })
    }

    pub async fn stream_shard_snapshot(
        &self,
        assignment: &ServiceShardAssignment,
    ) -> anyhow::Result<DistShardSnapshot> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .stream_shard_snapshot(shard_snapshot_request_for_assignment(assignment))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        let mut expected_checksum = None;
        while let Some(chunk) = stream.message().await? {
            if let Some(checksum) = expected_checksum {
                anyhow::ensure!(checksum == chunk.checksum, "dist stream checksum drift");
            } else {
                expected_checksum = Some(chunk.checksum);
            }
            bytes.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        let snapshot = DistShardSnapshot::decode(&bytes)?;
        if let Some(checksum) = expected_checksum {
            anyhow::ensure!(
                snapshot.checksum()? == checksum,
                "dist stream checksum mismatch"
            );
        }
        Ok(snapshot)
    }

    pub async fn import_shard_snapshot(
        &self,
        snapshot: &DistShardSnapshot,
    ) -> anyhow::Result<usize> {
        for route in &snapshot.routes {
            self.add_route(route.clone()).await?;
        }
        Ok(snapshot.routes.len())
    }

    pub async fn move_tenant_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .move_shard_to_member(shard.clone(), target_node_id)
            .await?;
        let target = DistGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;

        for route in snapshot.routes {
            let mut migrated = route.clone();
            migrated.node_id = target_node_id;
            target.add_route(migrated.clone()).await?;
            source.remove_route(&route).await?;
        }

        Ok(assignment)
    }

    pub async fn catch_up_tenant_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let expected_checksum = snapshot.checksum()?;

        let assignment = registry
            .catch_up_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = DistGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        let restored = target.export_shard_snapshot(tenant_id).await?;
        anyhow::ensure!(
            restored.checksum()? == expected_checksum,
            "dist shard catch-up checksum mismatch"
        );
        Ok(assignment)
    }

    pub async fn anti_entropy_repair_tenant_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .repair_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = DistGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        let target_snapshot = target.stream_shard_snapshot(&assignment).await?;
        if source_snapshot.checksum()? != target_snapshot.checksum()? {
            for route in &target_snapshot.routes {
                if !source_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    target.remove_route(route).await?;
                }
            }
            for route in &source_snapshot.routes {
                if !target_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    target.add_route(route.clone()).await?;
                }
            }
            let repaired = target.stream_shard_snapshot(&assignment).await?;
            anyhow::ensure!(
                repaired.checksum()? == source_snapshot.checksum()?,
                "dist shard anti-entropy checksum mismatch"
            );
        }
        Ok(assignment)
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::Dist)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no dist endpoint")
            })?;
        let replica = DistGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_snapshot = replica.export_shard_snapshot(tenant_id).await?;
        if source_snapshot.checksum()? != replica_snapshot.checksum()? {
            for route in &replica_snapshot.routes {
                if !source_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    replica.remove_route(route).await?;
                }
            }
            for route in &source_snapshot.routes {
                if !replica_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    replica.add_route(route.clone()).await?;
                }
            }
            let repaired = replica.export_shard_snapshot(tenant_id).await?;
            anyhow::ensure!(
                repaired.checksum()? == source_snapshot.checksum()?,
                "dist shard replica anti-entropy checksum mismatch"
            );
        }
        source_snapshot.checksum()
    }
}

impl PeriodicAntiEntropyReconciler {
    pub fn new(
        registry: Arc<DynamicServiceEndpointRegistry>,
        interval: std::time::Duration,
    ) -> Self {
        Self { registry, interval }
    }

    pub async fn reconcile_dist_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        DistGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn reconcile_sessiondict_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        SessionDictGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn reconcile_inbox_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<(u64, u64)> {
        InboxGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn reconcile_retain_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        RetainGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn run_dist_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_dist_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_sessiondict_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_sessiondict_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_inbox_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_inbox_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_retain_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_retain_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }
}

impl InboxGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                InboxServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
        session_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = inbox_session_shard(tenant_id, &session_id.to_string());
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn export_shard_snapshot(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<InboxShardSnapshot> {
        anyhow::ensure!(
            matches!(
                shard.kind,
                ServiceShardKind::Inbox | ServiceShardKind::Inflight
            ),
            "inbox snapshot requires Inbox or Inflight shard"
        );
        let subscriptions = if shard.kind == ServiceShardKind::Inflight {
            Vec::new()
        } else {
            self.list_all_subscriptions()
                .await?
                .into_iter()
                .filter(|subscription| {
                    shard_matches_inbox_session(
                        shard,
                        &subscription.tenant_id,
                        &subscription.session_id,
                    )
                })
                .collect()
        };
        let offline_messages = if shard.kind == ServiceShardKind::Inflight {
            Vec::new()
        } else {
            self.list_all_offline()
                .await?
                .into_iter()
                .filter(|message| {
                    shard_matches_inbox_session(shard, &message.tenant_id, &message.session_id)
                })
                .collect()
        };
        let inflight_messages = self
            .list_all_inflight()
            .await?
            .into_iter()
            .filter(|message| {
                shard_matches_inbox_session(shard, &message.tenant_id, &message.session_id)
            })
            .collect();
        Ok(InboxShardSnapshot {
            subscriptions,
            offline_messages,
            inflight_messages,
        })
    }

    pub async fn stream_shard_snapshot(
        &self,
        assignment: &ServiceShardAssignment,
    ) -> anyhow::Result<InboxShardSnapshot> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .stream_shard_snapshot(shard_snapshot_request_for_assignment(assignment))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        let mut expected_checksum = None;
        while let Some(chunk) = stream.message().await? {
            if let Some(checksum) = expected_checksum {
                anyhow::ensure!(checksum == chunk.checksum, "inbox stream checksum drift");
            } else {
                expected_checksum = Some(chunk.checksum);
            }
            bytes.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        let snapshot = InboxShardSnapshot::decode(&bytes)?;
        if let Some(checksum) = expected_checksum {
            anyhow::ensure!(
                snapshot.checksum()? == checksum,
                "inbox stream checksum mismatch"
            );
        }
        Ok(snapshot)
    }

    pub async fn import_shard_snapshot(
        &self,
        snapshot: &InboxShardSnapshot,
    ) -> anyhow::Result<usize> {
        for subscription in &snapshot.subscriptions {
            self.subscribe(subscription.clone()).await?;
        }
        for message in &snapshot.offline_messages {
            self.enqueue(message.clone()).await?;
        }
        for message in &snapshot.inflight_messages {
            self.stage_inflight(message.clone()).await?;
        }
        Ok(snapshot.subscriptions.len()
            + snapshot.offline_messages.len()
            + snapshot.inflight_messages.len())
    }

    pub async fn move_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        anyhow::ensure!(
            matches!(
                shard.kind,
                ServiceShardKind::Inbox | ServiceShardKind::Inflight
            ),
            "inbox move requires Inbox or Inflight shard"
        );
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox assignment registered for shard {shard:?}"))?;
        let source = InboxGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .move_shard_to_member(shard.clone(), target_node_id)
            .await?;
        let target = InboxGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        #[cfg(test)]
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let late_snapshot = source.export_shard_snapshot(&shard).await?;
        target.import_shard_snapshot(&late_snapshot).await?;

        let mut session_ids = BTreeMap::<String, ()>::new();
        for subscription in &snapshot.subscriptions {
            session_ids.insert(subscription.session_id.clone(), ());
        }
        for message in &snapshot.offline_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for message in &snapshot.inflight_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for subscription in &late_snapshot.subscriptions {
            session_ids.insert(subscription.session_id.clone(), ());
        }
        for message in &late_snapshot.offline_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for message in &late_snapshot.inflight_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for session_id in session_ids.into_keys() {
            source.purge_session(&session_id).await?;
        }

        Ok(assignment)
    }

    pub async fn catch_up_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        anyhow::ensure!(
            matches!(
                shard.kind,
                ServiceShardKind::Inbox | ServiceShardKind::Inflight
            ),
            "inbox catch-up requires Inbox or Inflight shard"
        );
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox assignment registered for shard {shard:?}"))?;
        let source = InboxGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let expected_checksum = snapshot.checksum()?;

        let assignment = registry
            .catch_up_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = InboxGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        let restored = target.export_shard_snapshot(&shard).await?;
        anyhow::ensure!(
            restored.checksum()? == expected_checksum,
            "inbox shard catch-up checksum mismatch"
        );
        Ok(assignment)
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<(u64, u64)> {
        let source_assignment = registry
            .resolve_assignment(&inbox_tenant_scan_shard(tenant_id))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no inbox assignment registered for tenant {tenant_id}")
            })?;
        let source = InboxGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_inbox_snapshot = source
            .export_shard_snapshot(&inbox_tenant_scan_shard(tenant_id))
            .await?;
        let source_inflight_snapshot = source
            .export_shard_snapshot(&inflight_tenant_scan_shard(tenant_id))
            .await?;

        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::Inbox)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no inbox endpoint")
            })?;
        let replica = InboxGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_inbox_snapshot = replica
            .export_shard_snapshot(&inbox_tenant_scan_shard(tenant_id))
            .await?;
        let replica_inflight_snapshot = replica
            .export_shard_snapshot(&inflight_tenant_scan_shard(tenant_id))
            .await?;

        if source_inbox_snapshot.checksum()? != replica_inbox_snapshot.checksum()? {
            let mut session_ids = BTreeMap::<String, ()>::new();
            for subscription in &source_inbox_snapshot.subscriptions {
                session_ids.insert(subscription.session_id.clone(), ());
            }
            for subscription in &replica_inbox_snapshot.subscriptions {
                session_ids.insert(subscription.session_id.clone(), ());
            }
            for message in &source_inbox_snapshot.offline_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for message in &replica_inbox_snapshot.offline_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for session_id in session_ids.keys() {
                let _ = replica.purge_session_subscriptions_only(session_id).await?;
                let _ = replica.purge_offline(session_id).await?;
            }
            for subscription in &source_inbox_snapshot.subscriptions {
                replica.subscribe(subscription.clone()).await?;
            }
            for message in &source_inbox_snapshot.offline_messages {
                replica.enqueue(message.clone()).await?;
            }
        }

        if source_inflight_snapshot.checksum()? != replica_inflight_snapshot.checksum()? {
            let mut session_ids = BTreeMap::<String, ()>::new();
            for message in &source_inflight_snapshot.inflight_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for message in &replica_inflight_snapshot.inflight_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for session_id in session_ids.keys() {
                let _ = replica.purge_inflight_session(session_id).await?;
            }
            for message in &source_inflight_snapshot.inflight_messages {
                replica.stage_inflight(message.clone()).await?;
            }
        }

        let repaired_inbox_snapshot = replica
            .export_shard_snapshot(&inbox_tenant_scan_shard(tenant_id))
            .await?;
        let repaired_inflight_snapshot = replica
            .export_shard_snapshot(&inflight_tenant_scan_shard(tenant_id))
            .await?;
        anyhow::ensure!(
            repaired_inbox_snapshot.checksum()? == source_inbox_snapshot.checksum()?,
            "inbox tenant replica anti-entropy checksum mismatch"
        );
        anyhow::ensure!(
            repaired_inflight_snapshot.checksum()? == source_inflight_snapshot.checksum()?,
            "inflight tenant replica anti-entropy checksum mismatch"
        );
        Ok((
            source_inbox_snapshot.checksum()?,
            source_inflight_snapshot.checksum()?,
        ))
    }
}

impl RetainGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RetainServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = retain_tenant_shard(tenant_id);
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no retain endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        let shard = retain_tenant_shard(tenant_id);
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no retain assignment registered for shard {shard:?}")
        })?;
        let source = RetainGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.list_tenant_retained(tenant_id).await?;
        let source_checksum = snapshot_checksum(&source_snapshot)?;

        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::Retain)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no retain endpoint")
            })?;
        let replica = RetainGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_snapshot = replica.list_tenant_retained(tenant_id).await?;
        if source_checksum != snapshot_checksum(&replica_snapshot)? {
            for message in &replica_snapshot {
                if !source_snapshot
                    .iter()
                    .any(|candidate| candidate.topic == message.topic)
                {
                    let mut delete = message.clone();
                    delete.payload = Bytes::new();
                    replica.retain(delete).await?;
                }
            }
            for message in &source_snapshot {
                if !replica_snapshot
                    .iter()
                    .any(|candidate| candidate.topic == message.topic && candidate == message)
                {
                    replica.retain(message.clone()).await?;
                }
            }
            let repaired = replica.list_tenant_retained(tenant_id).await?;
            anyhow::ensure!(
                snapshot_checksum(&repaired)? == source_checksum,
                "retain shard replica anti-entropy checksum mismatch"
            );
        }
        Ok(source_checksum)
    }
}

impl MetadataGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                MetadataServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .upsert_member(MemberRecordReply {
                member: Some(to_proto_cluster_node_membership(&member)),
            })
            .await?
            .into_inner();
        Ok(reply.member.map(from_proto_cluster_node_membership))
    }

    pub async fn lookup_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_member(MemberLookupRequest { node_id })
            .await?
            .into_inner();
        Ok(reply.member.map(from_proto_cluster_node_membership))
    }

    pub async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_member(MemberLookupRequest { node_id })
            .await?
            .into_inner();
        Ok(reply.member.map(from_proto_cluster_node_membership))
    }

    pub async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client.list_members(()).await?.into_inner();
        Ok(reply
            .members
            .into_iter()
            .map(from_proto_cluster_node_membership)
            .collect())
    }

    pub async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .upsert_range(RangeUpsertRequest {
                descriptor: Some(to_proto_replicated_range(&descriptor)),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn lookup_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_range(RangeLookupRequest {
                range_id: range_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_range(RangeLookupRequest {
                range_id: range_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
        tenant_id: Option<&str>,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_ranges(RangeListRequest {
                shard_kind: shard_kind
                    .as_ref()
                    .map(to_proto_shard_kind)
                    .unwrap_or_default(),
                tenant_id: tenant_id.unwrap_or_default().to_string(),
                scope: scope.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply
            .descriptors
            .into_iter()
            .map(from_proto_replicated_range)
            .collect())
    }

    pub async fn route_range(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .route_range(RouteRangeRequest {
                shard_kind: to_proto_shard_kind(&shard.kind),
                tenant_id: shard.tenant_id.clone(),
                scope: shard.scope.clone(),
                key: key.to_vec(),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn upsert_balancer_state(
        &self,
        name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .upsert_balancer_state(BalancerStateUpsertRequest {
                name: name.to_string(),
                state: Some(to_proto_balancer_state(&state)),
            })
            .await?
            .into_inner();
        Ok(reply.previous.map(from_proto_balancer_state))
    }

    pub async fn lookup_balancer_state(&self, name: &str) -> anyhow::Result<Option<BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_balancer_state(BalancerStateRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.state.map(from_proto_balancer_state))
    }

    pub async fn remove_balancer_state(&self, name: &str) -> anyhow::Result<Option<BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_balancer_state(BalancerStateRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.state.map(from_proto_balancer_state))
    }

    pub async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client.list_balancer_states(()).await?.into_inner();
        Ok(reply
            .entries
            .into_iter()
            .filter_map(|entry| {
                entry
                    .state
                    .map(|state| (entry.name, from_proto_balancer_state(state)))
            })
            .collect())
    }
}

impl KvRangeGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                KvRangeServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .get(KvRangeGetRequest {
                range_id: range_id.to_string(),
                key: key.to_vec(),
            })
            .await?
            .into_inner();
        Ok(reply.found.then_some(Bytes::from(reply.value)))
    }

    pub async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .scan(KvRangeScanRequest {
                range_id: range_id.to_string(),
                boundary: boundary
                    .as_ref()
                    .map(greenmqtt_proto::to_proto_range_boundary),
                limit: limit as u32,
            })
            .await?
            .into_inner();
        Ok(reply
            .entries
            .into_iter()
            .map(greenmqtt_proto::from_proto_kv_entry)
            .collect())
    }

    pub async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .apply(KvRangeApplyRequest {
                range_id: range_id.to_string(),
                mutations: mutations
                    .iter()
                    .map(greenmqtt_proto::to_proto_kv_mutation)
                    .collect(),
            })
            .await?;
        Ok(())
    }

    pub async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let mut client = self.inner.lock().await;
        let reply = client
            .checkpoint(KvRangeCheckpointRequest {
                range_id: range_id.to_string(),
                checkpoint_id: checkpoint_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(from_proto_kv_range_checkpoint(reply))
    }

    pub async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let mut client = self.inner.lock().await;
        let reply = client
            .snapshot(KvRangeSnapshotRequest {
                range_id: range_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(from_proto_kv_range_snapshot(reply))
    }
}

impl RaftTransportGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RaftTransportServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn send(
        &self,
        range_id: &str,
        from_node_id: NodeId,
        message: &RaftMessage,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .send(to_proto_raft_transport_request(
                range_id,
                from_node_id,
                message,
            ))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ReplicaTransport for RaftTransportGrpcClient {
    async fn send(
        &self,
        from_node_id: NodeId,
        target_node_id: NodeId,
        range_id: &str,
        message: &RaftMessage,
    ) -> anyhow::Result<()> {
        let _ = target_node_id;
        Self::send(self, range_id, from_node_id, message).await
    }
}

#[async_trait]
impl KvRangeExecutor for KvRangeGrpcClient {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        Self::get(self, range_id, key).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        Self::scan(self, range_id, boundary, limit).await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        Self::apply(self, range_id, mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        Self::checkpoint(self, range_id, checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        Self::snapshot(self, range_id).await
    }
}

#[async_trait]
impl KvRangeExecutorFactory for KvRangeGrpcExecutorFactory {
    async fn connect(&self, endpoint: &str) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
        Ok(Arc::new(
            KvRangeGrpcClient::connect(endpoint.to_string()).await?,
        ))
    }
}

#[async_trait]
impl ClusterMembershipRegistry for MetadataGrpcClient {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Self::upsert_member(self, member).await
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Self::lookup_member(self, node_id).await
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Self::remove_member(self, node_id).await
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        Self::list_members(self).await
    }
}

#[async_trait]
impl KvRangeRouter for MetadataGrpcClient {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        let _ = self.upsert_range(descriptor).await?;
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.remove_range(range_id).await
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.list_ranges(None, None, None).await
    }

    async fn by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        Ok(self
            .lookup_range(range_id)
            .await?
            .map(RangeRoute::from_descriptor))
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        Ok(self
            .route_range(shard, key)
            .await?
            .map(RangeRoute::from_descriptor))
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        Ok(self
            .list_ranges(
                Some(shard.kind.clone()),
                Some(&shard.tenant_id),
                Some(&shard.scope),
            )
            .await?
            .into_iter()
            .map(RangeRoute::from_descriptor)
            .collect())
    }
}

impl StaticPeerForwarder {
    pub async fn connect_node(
        &self,
        node_id: NodeId,
        endpoint: impl Into<String>,
    ) -> anyhow::Result<()> {
        let endpoint = endpoint.into();
        let client = BrokerPeerServiceClient::connect(endpoint.clone()).await?;
        self.peers.write().expect("peer forwarder poisoned").insert(
            node_id,
            StaticPeerClient {
                endpoint,
                client: Arc::new(Mutex::new(client)),
            },
        );
        Ok(())
    }

    pub fn configured_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<_> = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .keys()
            .copied()
            .collect();
        nodes.sort_unstable();
        nodes
    }

    pub fn disconnect_node(&self, node_id: NodeId) -> bool {
        self.peers
            .write()
            .expect("peer forwarder poisoned")
            .remove(&node_id)
            .is_some()
    }
}

#[async_trait]
impl PeerRegistry for StaticPeerForwarder {
    fn list_peer_nodes(&self) -> Vec<NodeId> {
        self.configured_nodes()
    }

    fn list_peer_endpoints(&self) -> BTreeMap<NodeId, String> {
        self.peers
            .read()
            .expect("peer forwarder poisoned")
            .iter()
            .map(|(node_id, peer)| (*node_id, peer.endpoint.clone()))
            .collect()
    }

    fn remove_peer_node(&self, node_id: NodeId) -> bool {
        self.disconnect_node(node_id)
    }

    async fn add_peer_node(&self, node_id: NodeId, endpoint: String) -> anyhow::Result<()> {
        self.connect_node(node_id, endpoint).await
    }
}

impl RpcRuntime {
    pub async fn serve(self, bind: SocketAddr) -> anyhow::Result<()> {
        Server::builder()
            .add_service(SessionDictServiceServer::new(SessionDictRpc {
                inner: self.sessiondict,
                assignment_registry: self.assignment_registry.clone(),
            }))
            .add_service(DistServiceServer::new(DistRpc {
                inner: self.dist,
                assignment_registry: self.assignment_registry.clone(),
            }))
            .add_service(InboxServiceServer::new(InboxRpc {
                inner: self.inbox,
                assignment_registry: self.assignment_registry.clone(),
            }))
            .add_service(RetainServiceServer::new(RetainRpc { inner: self.retain }))
            .add_service(BrokerPeerServiceServer::new(BrokerPeerRpc {
                inner: self.peer_sink,
            }))
            .add_service(MetadataServiceServer::new(MetadataRpc {
                inner: self.assignment_registry.clone(),
            }))
            .add_service(KvRangeServiceServer::new(KvRangeRpc {
                inner: self.range_host.clone(),
            }))
            .add_service(RaftTransportServiceServer::new(RaftTransportRpc {
                inner: self.range_host.clone(),
            }))
            .serve(bind)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SessionDirectory for SessionDictGrpcClient {
    async fn register(
        &self,
        record: greenmqtt_core::SessionRecord,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .register_session(RegisterSessionRequest {
                record: Some(to_proto_session(&record)),
            })
            .await?
            .into_inner();
        Ok(reply.replaced.map(from_proto_session))
    }

    async fn unregister(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .unregister_session(UnregisterSessionRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.replaced.map(from_proto_session))
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_session(LookupSessionRequest {
                identity: Some(greenmqtt_proto::to_proto_client_identity(identity)),
            })
            .await?
            .into_inner();
        Ok(reply.record.map(from_proto_session))
    }

    async fn lookup_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_session_by_id(LookupSessionByIdRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.record.map(from_proto_session))
    }

    async fn list_sessions(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_sessions(ListSessionsRequest {
                tenant_id: tenant_id.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.records.into_iter().map(from_proto_session).collect())
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.count_sessions(()).await?.into_inner();
        Ok(reply.count as usize)
    }
}

#[async_trait]
impl DistRouter for DistGrpcClient {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .add_route(AddRouteRequest {
                route: Some(to_proto_route(&route)),
            })
            .await?;
        Ok(())
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .remove_route(RemoveRouteRequest {
                route: Some(to_proto_route(route)),
            })
            .await?;
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_session_routes(RemoveSessionRoutesRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.removed as usize)
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_session_routes(ListSessionRoutesRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.routes.into_iter().map(from_proto_route).collect())
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &greenmqtt_core::TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .match_topic(MatchTopicRequest {
                tenant_id: tenant_id.to_string(),
                topic: topic.clone(),
            })
            .await?
            .into_inner();
        Ok(reply.routes.into_iter().map(from_proto_route).collect())
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_routes(ListRoutesRequest {
                tenant_id: tenant_id.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.routes.into_iter().map(from_proto_route).collect())
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.count_routes(()).await?.into_inner();
        Ok(reply.count as usize)
    }
}

#[async_trait]
impl InboxService for InboxGrpcClient {
    async fn attach(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .attach(InboxAttachRequest {
                session_id: session_id.clone(),
            })
            .await?;
        Ok(())
    }

    async fn detach(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .detach(InboxDetachRequest {
                session_id: session_id.clone(),
            })
            .await?;
        Ok(())
    }

    async fn purge_session(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .purge_session(InboxPurgeSessionRequest {
                session_id: session_id.clone(),
            })
            .await?;
        Ok(())
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .subscribe(InboxSubscribeRequest {
                session_id: subscription.session_id,
                tenant_id: subscription.tenant_id,
                topic_filter: subscription.topic_filter,
                qos: subscription.qos as u32,
                subscription_identifier: subscription.subscription_identifier.unwrap_or_default(),
                no_local: subscription.no_local,
                retain_as_published: subscription.retain_as_published,
                retain_handling: subscription.retain_handling as u32,
                shared_group: subscription.shared_group.unwrap_or_default(),
                kind: greenmqtt_proto::to_proto_session_kind(&subscription.kind),
            })
            .await?;
        Ok(())
    }

    async fn unsubscribe(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<bool> {
        self.unsubscribe_shared(session_id, topic_filter, None)
            .await
    }

    async fn unsubscribe_shared(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let mut client = self.inner.lock().await;
        let reply = client
            .unsubscribe(InboxUnsubscribeRequest {
                session_id: session_id.clone(),
                topic_filter: topic_filter.to_string(),
                shared_group: shared_group.unwrap_or_default().to_string(),
            })
            .await?;
        Ok(reply.into_inner().removed)
    }

    async fn lookup_subscription(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_subscription(InboxLookupSubscriptionRequest {
                session_id: session_id.clone(),
                topic_filter: topic_filter.to_string(),
                shared_group: shared_group.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.subscription.map(from_proto_subscription))
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .enqueue(InboxEnqueueRequest {
                message: Some(to_proto_offline(&message)),
            })
            .await?;
        Ok(())
    }

    async fn list_subscriptions(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_subscriptions(InboxListSubscriptionsRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply
            .subscriptions
            .into_iter()
            .map(from_proto_subscription)
            .collect())
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_all_subscriptions(InboxListAllSubscriptionsRequest {
                tenant_id: String::new(),
                session_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(reply
            .subscriptions
            .into_iter()
            .map(from_proto_subscription)
            .collect())
    }

    async fn peek(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .peek(InboxFetchRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_offline).collect())
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_offline(InboxListMessagesRequest {
                tenant_id: String::new(),
                session_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_offline).collect())
    }

    async fn fetch(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .fetch(InboxFetchRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_offline).collect())
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .stage_inflight(InboxStageInflightRequest {
                message: Some(to_proto_inflight(&message)),
            })
            .await?;
        Ok(())
    }

    async fn ack_inflight(
        &self,
        session_id: &greenmqtt_core::SessionId,
        packet_id: u16,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .ack_inflight(InboxAckInflightRequest {
                session_id: session_id.clone(),
                packet_id: packet_id as u32,
            })
            .await?;
        Ok(())
    }

    async fn fetch_inflight(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<InflightMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .fetch_inflight(InboxFetchInflightRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply
            .messages
            .into_iter()
            .map(from_proto_inflight)
            .collect())
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_inflight(InboxListMessagesRequest {
                tenant_id: String::new(),
                session_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(reply
            .messages
            .into_iter()
            .map(from_proto_inflight)
            .collect())
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.stats(()).await?.into_inner();
        Ok(reply.subscriptions as usize)
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.stats(()).await?.into_inner();
        Ok(reply.offline_messages as usize)
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.stats(()).await?.into_inner();
        Ok(reply.inflight_messages as usize)
    }
}

#[async_trait]
impl RetainStoreService for RetainGrpcClient {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .write(RetainWriteRequest {
                message: Some(to_proto_retain(&message)),
            })
            .await?;
        Ok(())
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        self.match_topic(tenant_id, "#").await
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self.match_topic(tenant_id, topic).await?.into_iter().next())
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .r#match(RetainMatchRequest {
                tenant_id: tenant_id.to_string(),
                topic_filter: topic_filter.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_retain).collect())
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.count_retained(()).await?.into_inner();
        Ok(reply.count as usize)
    }
}

#[async_trait]
impl PeerForwarder for StaticPeerForwarder {
    async fn forward_delivery(&self, node_id: NodeId, delivery: Delivery) -> anyhow::Result<bool> {
        self.push_delivery_calls.fetch_add(1, Ordering::SeqCst);
        let peer = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .get(&node_id)
            .map(|peer| peer.client.clone())
            .ok_or_else(|| anyhow::anyhow!("peer node {node_id} not configured"))?;
        let mut client = peer.lock().await;
        let reply = client
            .push_delivery(PushDeliveryRequest {
                delivery: Some(to_proto_delivery(&delivery)),
            })
            .await?
            .into_inner();
        Ok(reply.delivered)
    }

    async fn forward_deliveries(
        &self,
        node_id: NodeId,
        deliveries: Vec<Delivery>,
    ) -> anyhow::Result<Vec<Delivery>> {
        self.push_deliveries_calls.fetch_add(1, Ordering::SeqCst);
        let peer = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .get(&node_id)
            .map(|peer| peer.client.clone())
            .ok_or_else(|| anyhow::anyhow!("peer node {node_id} not configured"))?;
        let mut client = peer.lock().await;
        let reply = client
            .push_deliveries(PushDeliveriesRequest {
                deliveries: deliveries.iter().map(to_proto_delivery).collect(),
            })
            .await?
            .into_inner();
        let undelivered_indices: std::collections::BTreeSet<_> = reply
            .undelivered_indices
            .into_iter()
            .map(|index| index as usize)
            .collect();
        Ok(deliveries
            .into_iter()
            .enumerate()
            .filter_map(|(index, delivery)| {
                undelivered_indices.contains(&index).then_some(delivery)
            })
            .collect())
    }
}

#[async_trait]
impl DeliverySink for NoopDeliverySink {
    async fn push_delivery(&self, _delivery: Delivery) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[tonic::async_trait]
impl SessionDictService for SessionDictRpc {
    type StreamShardSnapshotStream = tonic::codegen::BoxStream<ShardSnapshotChunk>;

    async fn register_session(
        &self,
        request: Request<RegisterSessionRequest>,
    ) -> Result<Response<RegisterSessionReply>, Status> {
        let record = request
            .into_inner()
            .record
            .ok_or_else(|| Status::invalid_argument("missing session record"))?;
        let replaced = self
            .inner
            .register(from_proto_session(record))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RegisterSessionReply {
            replaced: replaced.as_ref().map(to_proto_session),
        }))
    }

    async fn unregister_session(
        &self,
        request: Request<UnregisterSessionRequest>,
    ) -> Result<Response<RegisterSessionReply>, Status> {
        let replaced = self
            .inner
            .unregister(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RegisterSessionReply {
            replaced: replaced.as_ref().map(to_proto_session),
        }))
    }

    async fn lookup_session(
        &self,
        request: Request<LookupSessionRequest>,
    ) -> Result<Response<LookupSessionReply>, Status> {
        let identity = request
            .into_inner()
            .identity
            .ok_or_else(|| Status::invalid_argument("missing client identity"))?;
        let record = self
            .inner
            .lookup_identity(&from_proto_client_identity(identity))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(LookupSessionReply {
            record: record.as_ref().map(to_proto_session),
        }))
    }

    async fn lookup_session_by_id(
        &self,
        request: Request<LookupSessionByIdRequest>,
    ) -> Result<Response<LookupSessionReply>, Status> {
        let record = self
            .inner
            .lookup_session(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(LookupSessionReply {
            record: record.as_ref().map(to_proto_session),
        }))
    }

    async fn count_sessions(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.session_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsReply>, Status> {
        let request = request.into_inner();
        let tenant_id = if request.tenant_id.is_empty() {
            None
        } else {
            Some(request.tenant_id.as_str())
        };
        let records = self
            .inner
            .list_sessions(tenant_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(ListSessionsReply {
            records: records.iter().map(to_proto_session).collect(),
        }))
    }

    async fn stream_shard_snapshot(
        &self,
        request: Request<ShardSnapshotRequest>,
    ) -> Result<Response<Self::StreamShardSnapshotStream>, Status> {
        let request = request.into_inner();
        validate_snapshot_assignment(
            &self.assignment_registry,
            assignment_from_snapshot_request(
                ServiceShardKind::SessionDict,
                &request,
                ServiceKind::SessionDict,
            ),
        )
        .await?;
        let shard = sessiondict_shard_from_request(request);
        let sessions = self
            .inner
            .list_sessions(Some(&shard.tenant_id))
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|record| shard_matches_sessiondict_record(&shard, record))
            .collect();
        let snapshot = SessionDictShardSnapshot { sessions };
        let bytes = snapshot.encode().map_err(internal_status)?;
        let checksum = snapshot.checksum().map_err(internal_status)?;
        let output = tonic::codegen::tokio_stream::iter(
            snapshot_chunks(bytes, checksum).into_iter().map(Ok),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl DistService for DistRpc {
    type StreamShardSnapshotStream = tonic::codegen::BoxStream<ShardSnapshotChunk>;

    async fn add_route(&self, request: Request<AddRouteRequest>) -> Result<Response<()>, Status> {
        let route = request
            .into_inner()
            .route
            .ok_or_else(|| Status::invalid_argument("missing route"))?;
        self.inner
            .add_route(from_proto_route(route))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn remove_route(
        &self,
        request: Request<RemoveRouteRequest>,
    ) -> Result<Response<()>, Status> {
        let route = request
            .into_inner()
            .route
            .ok_or_else(|| Status::invalid_argument("missing route"))?;
        self.inner
            .remove_route(&from_proto_route(route))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn remove_session_routes(
        &self,
        request: Request<RemoveSessionRoutesRequest>,
    ) -> Result<Response<RemoveSessionRoutesReply>, Status> {
        let removed = self
            .inner
            .remove_session_routes(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RemoveSessionRoutesReply {
            removed: removed as u32,
        }))
    }

    async fn list_session_routes(
        &self,
        request: Request<ListSessionRoutesRequest>,
    ) -> Result<Response<ListSessionRoutesReply>, Status> {
        let routes = self
            .inner
            .list_session_routes(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(ListSessionRoutesReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn match_topic(
        &self,
        request: Request<MatchTopicRequest>,
    ) -> Result<Response<MatchTopicReply>, Status> {
        let request = request.into_inner();
        let routes = self
            .inner
            .match_topic(&request.tenant_id, &request.topic)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(MatchTopicReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn list_routes(
        &self,
        request: Request<ListRoutesRequest>,
    ) -> Result<Response<ListRoutesReply>, Status> {
        let request = request.into_inner();
        let tenant_id = if request.tenant_id.is_empty() {
            None
        } else {
            Some(request.tenant_id.as_str())
        };
        let routes = self
            .inner
            .list_routes(tenant_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(ListRoutesReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn count_routes(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.route_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }

    async fn stream_shard_snapshot(
        &self,
        request: Request<ShardSnapshotRequest>,
    ) -> Result<Response<Self::StreamShardSnapshotStream>, Status> {
        let request = request.into_inner();
        validate_snapshot_assignment(
            &self.assignment_registry,
            assignment_from_snapshot_request(ServiceShardKind::Dist, &request, ServiceKind::Dist),
        )
        .await?;
        let tenant_id = request.tenant_id;
        let snapshot = DistShardSnapshot {
            routes: self
                .inner
                .list_routes(Some(&tenant_id))
                .await
                .map_err(internal_status)?,
        };
        let bytes = snapshot.encode().map_err(internal_status)?;
        let checksum = snapshot.checksum().map_err(internal_status)?;
        let output = tonic::codegen::tokio_stream::iter(
            snapshot_chunks(bytes, checksum).into_iter().map(Ok),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl ProtoInboxService for InboxRpc {
    type StreamShardSnapshotStream = tonic::codegen::BoxStream<ShardSnapshotChunk>;

    async fn attach(&self, request: Request<InboxAttachRequest>) -> Result<Response<()>, Status> {
        self.inner
            .attach(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn detach(&self, request: Request<InboxDetachRequest>) -> Result<Response<()>, Status> {
        self.inner
            .detach(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn purge_session(
        &self,
        request: Request<InboxPurgeSessionRequest>,
    ) -> Result<Response<()>, Status> {
        self.inner
            .purge_session(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn subscribe(
        &self,
        request: Request<InboxSubscribeRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.inner
            .subscribe(Subscription {
                session_id: request.session_id,
                tenant_id: request.tenant_id,
                topic_filter: request.topic_filter,
                qos: request.qos as u8,
                subscription_identifier: if request.subscription_identifier == 0 {
                    None
                } else {
                    Some(request.subscription_identifier)
                },
                no_local: request.no_local,
                retain_as_published: request.retain_as_published,
                retain_handling: request.retain_handling as u8,
                shared_group: if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group)
                },
                kind: greenmqtt_proto::from_proto_session_kind(&request.kind),
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn unsubscribe(
        &self,
        request: Request<InboxUnsubscribeRequest>,
    ) -> Result<Response<InboxUnsubscribeReply>, Status> {
        let request = request.into_inner();
        let removed = self
            .inner
            .unsubscribe_shared(
                &request.session_id,
                &request.topic_filter,
                if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group.as_str())
                },
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxUnsubscribeReply { removed }))
    }

    async fn enqueue(&self, request: Request<InboxEnqueueRequest>) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing offline message"))?;
        self.inner
            .enqueue(from_proto_offline(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn list_subscriptions(
        &self,
        request: Request<InboxListSubscriptionsRequest>,
    ) -> Result<Response<InboxListSubscriptionsReply>, Status> {
        let subscriptions = self
            .inner
            .list_subscriptions(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxListSubscriptionsReply {
            subscriptions: subscriptions.iter().map(to_proto_subscription).collect(),
        }))
    }

    async fn lookup_subscription(
        &self,
        request: Request<InboxLookupSubscriptionRequest>,
    ) -> Result<Response<InboxLookupSubscriptionReply>, Status> {
        let request = request.into_inner();
        let subscription = self
            .inner
            .lookup_subscription(
                &request.session_id,
                &request.topic_filter,
                if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group.as_str())
                },
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxLookupSubscriptionReply {
            subscription: subscription.as_ref().map(to_proto_subscription),
        }))
    }

    async fn list_all_subscriptions(
        &self,
        request: Request<InboxListAllSubscriptionsRequest>,
    ) -> Result<Response<InboxListSubscriptionsReply>, Status> {
        let request = request.into_inner();
        let mut subscriptions = self
            .inner
            .list_all_subscriptions()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            subscriptions.retain(|subscription| subscription.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            subscriptions.retain(|subscription| subscription.session_id == request.session_id);
        }
        Ok(Response::new(InboxListSubscriptionsReply {
            subscriptions: subscriptions.iter().map(to_proto_subscription).collect(),
        }))
    }

    async fn fetch(
        &self,
        request: Request<InboxFetchRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let messages = self
            .inner
            .fetch(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn peek(
        &self,
        request: Request<InboxFetchRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let messages = self
            .inner
            .peek(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn list_offline(
        &self,
        request: Request<InboxListMessagesRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let request = request.into_inner();
        let mut messages = self
            .inner
            .list_all_offline()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            messages.retain(|message| message.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            messages.retain(|message| message.session_id == request.session_id);
        }
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn stage_inflight(
        &self,
        request: Request<InboxStageInflightRequest>,
    ) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing inflight message"))?;
        self.inner
            .stage_inflight(from_proto_inflight(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn ack_inflight(
        &self,
        request: Request<InboxAckInflightRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.inner
            .ack_inflight(&request.session_id, request.packet_id as u16)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn fetch_inflight(
        &self,
        request: Request<InboxFetchInflightRequest>,
    ) -> Result<Response<InboxFetchInflightReply>, Status> {
        let messages = self
            .inner
            .fetch_inflight(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchInflightReply {
            messages: messages.iter().map(to_proto_inflight).collect(),
        }))
    }

    async fn list_inflight(
        &self,
        request: Request<InboxListMessagesRequest>,
    ) -> Result<Response<InboxFetchInflightReply>, Status> {
        let request = request.into_inner();
        let mut messages = self
            .inner
            .list_all_inflight()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            messages.retain(|message| message.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            messages.retain(|message| message.session_id == request.session_id);
        }
        Ok(Response::new(InboxFetchInflightReply {
            messages: messages.iter().map(to_proto_inflight).collect(),
        }))
    }

    async fn stats(&self, _request: Request<()>) -> Result<Response<InboxStatsReply>, Status> {
        let subscriptions = self
            .inner
            .subscription_count()
            .await
            .map_err(internal_status)?;
        let offline_messages = self.inner.offline_count().await.map_err(internal_status)?;
        let inflight_messages = self.inner.inflight_count().await.map_err(internal_status)?;
        Ok(Response::new(InboxStatsReply {
            subscriptions: subscriptions as u64,
            offline_messages: offline_messages as u64,
            inflight_messages: inflight_messages as u64,
        }))
    }

    async fn stream_shard_snapshot(
        &self,
        request: Request<ShardSnapshotRequest>,
    ) -> Result<Response<Self::StreamShardSnapshotStream>, Status> {
        let request = request.into_inner();
        validate_snapshot_assignment(
            &self.assignment_registry,
            assignment_from_snapshot_request(ServiceShardKind::Inbox, &request, ServiceKind::Inbox),
        )
        .await?;
        let shard = inbox_shard_from_request(request);
        let subscriptions = self
            .inner
            .list_all_subscriptions()
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|subscription| {
                shard_matches_inbox_session(
                    &shard,
                    &subscription.tenant_id,
                    &subscription.session_id,
                )
            })
            .collect();
        let offline_messages = self
            .inner
            .list_all_offline()
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|message| {
                shard_matches_inbox_session(&shard, &message.tenant_id, &message.session_id)
            })
            .collect();
        let inflight_messages = self
            .inner
            .list_all_inflight()
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|message| {
                shard_matches_inbox_session(&shard, &message.tenant_id, &message.session_id)
            })
            .collect();
        let snapshot = InboxShardSnapshot {
            subscriptions,
            offline_messages,
            inflight_messages,
        };
        let bytes = snapshot.encode().map_err(internal_status)?;
        let checksum = snapshot.checksum().map_err(internal_status)?;
        let output = tonic::codegen::tokio_stream::iter(
            snapshot_chunks(bytes, checksum).into_iter().map(Ok),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl RetainService for RetainRpc {
    async fn write(&self, request: Request<RetainWriteRequest>) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing retained message"))?;
        self.inner
            .retain(from_proto_retain(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn r#match(
        &self,
        request: Request<RetainMatchRequest>,
    ) -> Result<Response<RetainMatchReply>, Status> {
        let request = request.into_inner();
        let messages = self
            .inner
            .match_topic(&request.tenant_id, &request.topic_filter)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RetainMatchReply {
            messages: messages.iter().map(to_proto_retain).collect(),
        }))
    }

    async fn count_retained(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.retained_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }
}

#[tonic::async_trait]
impl BrokerPeerService for BrokerPeerRpc {
    async fn push_delivery(
        &self,
        request: Request<PushDeliveryRequest>,
    ) -> Result<Response<PushDeliveryReply>, Status> {
        let delivery = request
            .into_inner()
            .delivery
            .ok_or_else(|| Status::invalid_argument("missing delivery"))?;
        let delivered = self
            .inner
            .push_delivery(from_proto_delivery(delivery))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(PushDeliveryReply { delivered }))
    }

    async fn push_deliveries(
        &self,
        request: Request<PushDeliveriesRequest>,
    ) -> Result<Response<PushDeliveriesReply>, Status> {
        let deliveries: Vec<_> = request
            .into_inner()
            .deliveries
            .into_iter()
            .map(from_proto_delivery)
            .collect();
        let undelivered = self
            .inner
            .push_deliveries(deliveries.clone())
            .await
            .map_err(internal_status)?;
        let mut remaining = undelivered;
        let mut undelivered_indices = Vec::new();
        for (index, delivery) in deliveries.into_iter().enumerate() {
            if let Some(position) = remaining.iter().position(|current| *current == delivery) {
                remaining.remove(position);
                undelivered_indices.push(index as u32);
            }
        }
        Ok(Response::new(PushDeliveriesReply {
            undelivered_indices,
        }))
    }
}

#[tonic::async_trait]
impl MetadataService for MetadataRpc {
    async fn upsert_member(
        &self,
        request: Request<MemberRecordReply>,
    ) -> Result<Response<MemberRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let member = request
            .into_inner()
            .member
            .ok_or_else(|| Status::invalid_argument("missing cluster member"))?;
        let previous = registry
            .upsert_member(from_proto_cluster_node_membership(member))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(MemberRecordReply {
            member: previous.as_ref().map(to_proto_cluster_node_membership),
        }))
    }

    async fn lookup_member(
        &self,
        request: Request<MemberLookupRequest>,
    ) -> Result<Response<MemberRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let member = registry
            .resolve_member(request.into_inner().node_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(MemberRecordReply {
            member: member.as_ref().map(to_proto_cluster_node_membership),
        }))
    }

    async fn remove_member(
        &self,
        request: Request<MemberLookupRequest>,
    ) -> Result<Response<MemberRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let member = registry
            .remove_member(request.into_inner().node_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(MemberRecordReply {
            member: member.as_ref().map(to_proto_cluster_node_membership),
        }))
    }

    async fn list_members(
        &self,
        _request: Request<()>,
    ) -> Result<Response<MemberListReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let members = registry.list_members().await.map_err(internal_status)?;
        Ok(Response::new(MemberListReply {
            members: members
                .iter()
                .map(to_proto_cluster_node_membership)
                .collect(),
        }))
    }

    async fn upsert_range(
        &self,
        request: Request<RangeUpsertRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let descriptor = request
            .into_inner()
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing replicated range descriptor"))?;
        let previous = registry
            .upsert_range(from_proto_replicated_range(descriptor))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: previous.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn lookup_range(
        &self,
        request: Request<RangeLookupRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let descriptor = registry
            .resolve_range(&request.into_inner().range_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: descriptor.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn remove_range(
        &self,
        request: Request<RangeLookupRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let descriptor = registry
            .remove_range(&request.into_inner().range_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: descriptor.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn list_ranges(
        &self,
        request: Request<RangeListRequest>,
    ) -> Result<Response<RangeListReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let request = request.into_inner();
        let shard_kind = if request.shard_kind.is_empty() {
            None
        } else {
            Some(from_proto_shard_kind(&request.shard_kind))
        };
        let mut descriptors = registry
            .list_ranges(shard_kind)
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            descriptors.retain(|descriptor| descriptor.shard.tenant_id == request.tenant_id);
        }
        if !request.scope.is_empty() {
            descriptors.retain(|descriptor| descriptor.shard.scope == request.scope);
        }
        Ok(Response::new(RangeListReply {
            descriptors: descriptors.iter().map(to_proto_replicated_range).collect(),
        }))
    }

    async fn route_range(
        &self,
        request: Request<RouteRangeRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let request = request.into_inner();
        let descriptor = registry
            .route_range_for_key(
                &ServiceShardKey {
                    kind: from_proto_shard_kind(&request.shard_kind),
                    tenant_id: request.tenant_id,
                    scope: request.scope,
                },
                &request.key,
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: descriptor.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn upsert_balancer_state(
        &self,
        request: Request<BalancerStateUpsertRequest>,
    ) -> Result<Response<BalancerStateUpsertReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let request = request.into_inner();
        let state = request
            .state
            .ok_or_else(|| Status::invalid_argument("missing balancer state"))?;
        let previous = registry
            .upsert_balancer_state(&request.name, from_proto_balancer_state(state))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(BalancerStateUpsertReply {
            previous: previous.as_ref().map(to_proto_balancer_state),
        }))
    }

    async fn lookup_balancer_state(
        &self,
        request: Request<BalancerStateRequest>,
    ) -> Result<Response<BalancerStateReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let state = registry
            .resolve_balancer_state(&request.into_inner().name)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(BalancerStateReply {
            state: state.as_ref().map(to_proto_balancer_state),
        }))
    }

    async fn remove_balancer_state(
        &self,
        request: Request<BalancerStateRequest>,
    ) -> Result<Response<BalancerStateReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let state = registry
            .remove_balancer_state(&request.into_inner().name)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(BalancerStateReply {
            state: state.as_ref().map(to_proto_balancer_state),
        }))
    }

    async fn list_balancer_states(
        &self,
        _request: Request<()>,
    ) -> Result<Response<BalancerStateListReply>, Status> {
        let registry = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("metadata registry unavailable"))?;
        let states = registry
            .list_balancer_states()
            .await
            .map_err(internal_status)?;
        Ok(Response::new(BalancerStateListReply {
            entries: states
                .into_iter()
                .map(|(name, state)| NamedBalancerStateRecord {
                    name,
                    state: Some(to_proto_balancer_state(&state)),
                })
                .collect(),
        }))
    }
}

#[tonic::async_trait]
impl KvRangeService for KvRangeRpc {
    async fn get(
        &self,
        request: Request<KvRangeGetRequest>,
    ) -> Result<Response<KvRangeGetReply>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let request = request.into_inner();
        let hosted = host
            .open_range(&request.range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        hosted.raft.read_index().await.map_err(internal_status)?;
        let value = hosted
            .space
            .reader()
            .get(&request.key)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(KvRangeGetReply {
            value: value.clone().unwrap_or_default().to_vec(),
            found: value.is_some(),
        }))
    }

    async fn scan(
        &self,
        request: Request<KvRangeScanRequest>,
    ) -> Result<Response<KvRangeScanReply>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let request = request.into_inner();
        let hosted = host
            .open_range(&request.range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        hosted.raft.read_index().await.map_err(internal_status)?;
        let boundary = request
            .boundary
            .map(greenmqtt_proto::from_proto_range_boundary)
            .unwrap_or_else(greenmqtt_core::RangeBoundary::full);
        let entries = hosted
            .space
            .reader()
            .scan(&boundary, request.limit as usize)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(KvRangeScanReply {
            entries: entries.iter().map(to_proto_kv_entry).collect(),
        }))
    }

    async fn apply(&self, request: Request<KvRangeApplyRequest>) -> Result<Response<()>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let request = request.into_inner();
        let hosted = host
            .open_range(&request.range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        let mutations = request
            .mutations
            .into_iter()
            .map(from_proto_kv_mutation)
            .collect::<Vec<_>>();
        let proposed_index = hosted
            .raft
            .propose(Bytes::from(
                bincode::serialize(&mutations)
                    .map_err(|error| internal_status(anyhow::Error::from(error)))?,
            ))
            .await
            .map_err(internal_status)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let _ = apply_committed_entries_for_range(&hosted)
                .await
                .map_err(internal_status)?;
            if hosted
                .raft
                .status()
                .await
                .map_err(internal_status)?
                .applied_index
                >= proposed_index
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "timed out waiting for raft command to apply",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(Response::new(()))
    }

    async fn checkpoint(
        &self,
        request: Request<KvRangeCheckpointRequest>,
    ) -> Result<Response<KvRangeCheckpointReply>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let request = request.into_inner();
        let hosted = host
            .open_range(&request.range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        hosted.raft.read_index().await.map_err(internal_status)?;
        let checkpoint = hosted
            .space
            .checkpoint(&request.checkpoint_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(to_proto_kv_range_checkpoint(&checkpoint)))
    }

    async fn snapshot(
        &self,
        request: Request<KvRangeSnapshotRequest>,
    ) -> Result<Response<KvRangeSnapshotReply>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let request = request.into_inner();
        let hosted = host
            .open_range(&request.range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        hosted.raft.read_index().await.map_err(internal_status)?;
        let snapshot = hosted.space.snapshot().await.map_err(internal_status)?;
        Ok(Response::new(to_proto_kv_range_snapshot(&snapshot)))
    }
}

#[tonic::async_trait]
impl RaftTransportService for RaftTransportRpc {
    async fn send(&self, request: Request<RaftTransportRequest>) -> Result<Response<()>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let (range_id, from_node_id, message) =
            from_proto_raft_transport_request(request.into_inner()).map_err(internal_status)?;
        let hosted = host
            .open_range(&range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        hosted
            .raft
            .receive(from_node_id, message)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests;
