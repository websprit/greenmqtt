use std::convert::TryFrom;

use anyhow::Context;
use bytes::Bytes;
use greenmqtt_core::{
    BalancerState as CoreBalancerState, ClientIdentity as CoreClientIdentity,
    ClusterNodeLifecycle as CoreClusterNodeLifecycle,
    ClusterNodeMembership as CoreClusterNodeMembership, ConnectReply as CoreConnectReply,
    ConnectRequest as CoreConnectRequest, Delivery as CoreDelivery,
    InflightMessage as CoreInflightMessage, InflightPhase as CoreInflightPhase,
    OfflineMessage as CoreOfflineMessage, PublishProperties as CorePublishProperties,
    PublishRequest as CorePublishRequest, RangeBoundary as CoreRangeBoundary,
    RangeReconfigurationState as CoreRangeReconfigurationState, RangeReplica as CoreRangeReplica,
    ReconfigurationPhase as CoreReconfigurationPhase, ReplicaRole as CoreReplicaRole,
    ReplicaSyncState as CoreReplicaSyncState,
    ReplicatedRangeDescriptor as CoreReplicatedRangeDescriptor,
    RetainedMessage as CoreRetainedMessage, RouteRecord as CoreRouteRecord,
    ServiceEndpoint as CoreServiceEndpoint, ServiceEndpointTransport,
    ServiceKind as CoreNodeServiceKind, ServiceShardKey as CoreServiceShardKey,
    ServiceShardKind as CoreServiceShardKind, ServiceShardLifecycle as CoreServiceShardLifecycle,
    SessionKind as CoreSessionKind, SessionRecord as CoreSessionRecord,
    Subscription as CoreSubscription, UserProperty as CoreUserProperty,
};
use greenmqtt_kv_raft::{
    AppendEntriesRequest as CoreAppendEntriesRequest,
    AppendEntriesResponse as CoreAppendEntriesResponse,
    InstallSnapshotRequest as CoreInstallSnapshotRequest,
    InstallSnapshotResponse as CoreInstallSnapshotResponse,
    RaftConfigLogEntry as CoreRaftConfigLogEntry, RaftLogEntry as CoreRaftLogEntry,
    RaftMessage as CoreRaftMessage, RaftSnapshot as CoreRaftSnapshot,
    RequestVoteRequest as CoreRequestVoteRequest, RequestVoteResponse as CoreRequestVoteResponse,
};

pub mod greenmqtt_rpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/greenmqtt_rpc_capnp.rs"));
}

use greenmqtt_rpc_capnp::{
    add_route_request, balancer_state, balancer_state_list_reply, balancer_state_reply,
    balancer_state_request, balancer_state_upsert_reply, balancer_state_upsert_request,
    client_identity, cluster_node_membership, connect_reply, connect_request, delivery,
    inbox_ack_inflight_request, inbox_attach_request, inbox_detach_request,
    inbox_purge_session_request, inflight_message, kv_entry, kv_mutation, kv_range_apply_request,
    kv_range_checkpoint_reply, kv_range_checkpoint_request, kv_range_get_reply,
    kv_range_get_request, kv_range_scan_reply, kv_range_scan_request, kv_range_snapshot_reply,
    kv_range_snapshot_request, list_routes_reply, list_routes_request, list_session_routes_reply,
    list_session_routes_request, list_sessions_reply, list_sessions_request,
    lookup_session_by_id_request, lookup_session_reply, lookup_session_request, match_topic_reply,
    match_topic_request, member_list_reply, member_lookup_request, member_record_reply,
    named_balancer_state, offline_message, publish_properties, publish_request,
    raft_append_entries_request, raft_append_entries_response, raft_install_snapshot_request,
    raft_install_snapshot_response, raft_log_entry, raft_request_vote_request,
    raft_request_vote_response, raft_snapshot, raft_transport_request, range_bootstrap_reply,
    range_bootstrap_request, range_change_replicas_request, range_debug_reply, range_drain_request,
    range_health, range_health_list_reply, range_health_reply, range_health_request,
    range_list_reply, range_list_request, range_lookup_request, range_merge_reply,
    range_merge_request, range_reconfiguration, range_record_reply, range_recover_request,
    range_retire_request, range_split_reply, range_split_request,
    range_transfer_leadership_request, register_session_reply, register_session_request,
    remove_route_request, replica_lag, retain_match_reply, retain_match_request,
    retain_write_request, retained_message, route_range_request, route_record, rpc_frame,
    service_endpoint, session_record, subscription_record, unregister_session_request,
    user_property, zombie_range, zombie_range_list_reply,
    ClusterNodeLifecycle as CapnpClusterNodeLifecycle, FrameKind,
    InflightPhase as CapnpInflightPhase, NodeServiceKind as CapnpNodeServiceKind,
    RaftMessageKind as CapnpRaftMessageKind, RaftRole as CapnpRaftRole,
    ReconfigurationPhase as CapnpReconfigurationPhase, ReplicaRole as CapnpReplicaRole,
    ReplicaSyncState as CapnpReplicaSyncState, ServiceKind,
    ServiceShardKind as CapnpServiceShardKind, ServiceShardLifecycle as CapnpServiceShardLifecycle,
    SessionKind as CapnpSessionKind, StatusCode, TransportKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcServiceKind {
    SessionDict,
    Dist,
    Inbox,
    Retain,
    Metadata,
    KvRange,
    RaftTransport,
    RangeAdmin,
    RangeControl,
    BrokerPeer,
}

impl From<RpcServiceKind> for ServiceKind {
    fn from(value: RpcServiceKind) -> Self {
        match value {
            RpcServiceKind::SessionDict => ServiceKind::SessionDict,
            RpcServiceKind::Dist => ServiceKind::Dist,
            RpcServiceKind::Inbox => ServiceKind::Inbox,
            RpcServiceKind::Retain => ServiceKind::Retain,
            RpcServiceKind::Metadata => ServiceKind::Metadata,
            RpcServiceKind::KvRange => ServiceKind::KvRange,
            RpcServiceKind::RaftTransport => ServiceKind::RaftTransport,
            RpcServiceKind::RangeAdmin => ServiceKind::RangeAdmin,
            RpcServiceKind::RangeControl => ServiceKind::RangeControl,
            RpcServiceKind::BrokerPeer => ServiceKind::BrokerPeer,
        }
    }
}

impl TryFrom<ServiceKind> for RpcServiceKind {
    type Error = anyhow::Error;

    fn try_from(value: ServiceKind) -> anyhow::Result<Self> {
        Ok(match value {
            ServiceKind::SessionDict => Self::SessionDict,
            ServiceKind::Dist => Self::Dist,
            ServiceKind::Inbox => Self::Inbox,
            ServiceKind::Retain => Self::Retain,
            ServiceKind::Metadata => Self::Metadata,
            ServiceKind::KvRange => Self::KvRange,
            ServiceKind::RaftTransport => Self::RaftTransport,
            ServiceKind::RangeAdmin => Self::RangeAdmin,
            ServiceKind::RangeControl => Self::RangeControl,
            ServiceKind::BrokerPeer => Self::BrokerPeer,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcTransportKind {
    GrpcHttp,
    GrpcHttps,
    Quic,
}

impl From<RpcTransportKind> for TransportKind {
    fn from(value: RpcTransportKind) -> Self {
        match value {
            RpcTransportKind::GrpcHttp => TransportKind::GrpcHttp,
            RpcTransportKind::GrpcHttps => TransportKind::GrpcHttps,
            RpcTransportKind::Quic => TransportKind::Quic,
        }
    }
}

impl TryFrom<TransportKind> for RpcTransportKind {
    type Error = anyhow::Error;

    fn try_from(value: TransportKind) -> anyhow::Result<Self> {
        Ok(match value {
            TransportKind::GrpcHttp => Self::GrpcHttp,
            TransportKind::GrpcHttps => Self::GrpcHttps,
            TransportKind::Quic => Self::Quic,
        })
    }
}

impl From<ServiceEndpointTransport> for RpcTransportKind {
    fn from(value: ServiceEndpointTransport) -> Self {
        match value {
            ServiceEndpointTransport::GrpcHttp => Self::GrpcHttp,
            ServiceEndpointTransport::GrpcHttps => Self::GrpcHttps,
            ServiceEndpointTransport::Quic => Self::Quic,
        }
    }
}

impl From<RpcTransportKind> for ServiceEndpointTransport {
    fn from(value: RpcTransportKind) -> Self {
        match value {
            RpcTransportKind::GrpcHttp => Self::GrpcHttp,
            RpcTransportKind::GrpcHttps => Self::GrpcHttps,
            RpcTransportKind::Quic => Self::Quic,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcFrameKind {
    RequestHeader,
    RequestBody,
    ResponseHeader,
    ResponseBody,
    StreamItem,
    StreamEnd,
    Cancel,
    ProtocolError,
}

impl From<RpcFrameKind> for FrameKind {
    fn from(value: RpcFrameKind) -> Self {
        match value {
            RpcFrameKind::RequestHeader => FrameKind::RequestHeader,
            RpcFrameKind::RequestBody => FrameKind::RequestBody,
            RpcFrameKind::ResponseHeader => FrameKind::ResponseHeader,
            RpcFrameKind::ResponseBody => FrameKind::ResponseBody,
            RpcFrameKind::StreamItem => FrameKind::StreamItem,
            RpcFrameKind::StreamEnd => FrameKind::StreamEnd,
            RpcFrameKind::Cancel => FrameKind::Cancel,
            RpcFrameKind::ProtocolError => FrameKind::ProtocolError,
        }
    }
}

impl TryFrom<FrameKind> for RpcFrameKind {
    type Error = anyhow::Error;

    fn try_from(value: FrameKind) -> anyhow::Result<Self> {
        Ok(match value {
            FrameKind::RequestHeader => Self::RequestHeader,
            FrameKind::RequestBody => Self::RequestBody,
            FrameKind::ResponseHeader => Self::ResponseHeader,
            FrameKind::ResponseBody => Self::ResponseBody,
            FrameKind::StreamItem => Self::StreamItem,
            FrameKind::StreamEnd => Self::StreamEnd,
            FrameKind::Cancel => Self::Cancel,
            FrameKind::ProtocolError => Self::ProtocolError,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcStatusCode {
    Ok,
    InvalidArgument,
    NotFound,
    Unavailable,
    ResourceExhausted,
    DeadlineExceeded,
    Internal,
    NotSupported,
}

impl From<RpcStatusCode> for StatusCode {
    fn from(value: RpcStatusCode) -> Self {
        match value {
            RpcStatusCode::Ok => StatusCode::Ok,
            RpcStatusCode::InvalidArgument => StatusCode::InvalidArgument,
            RpcStatusCode::NotFound => StatusCode::NotFound,
            RpcStatusCode::Unavailable => StatusCode::Unavailable,
            RpcStatusCode::ResourceExhausted => StatusCode::ResourceExhausted,
            RpcStatusCode::DeadlineExceeded => StatusCode::DeadlineExceeded,
            RpcStatusCode::Internal => StatusCode::Internal,
            RpcStatusCode::NotSupported => StatusCode::NotSupported,
        }
    }
}

impl TryFrom<StatusCode> for RpcStatusCode {
    type Error = anyhow::Error;

    fn try_from(value: StatusCode) -> anyhow::Result<Self> {
        Ok(match value {
            StatusCode::Ok => Self::Ok,
            StatusCode::InvalidArgument => Self::InvalidArgument,
            StatusCode::NotFound => Self::NotFound,
            StatusCode::Unavailable => Self::Unavailable,
            StatusCode::ResourceExhausted => Self::ResourceExhausted,
            StatusCode::DeadlineExceeded => Self::DeadlineExceeded,
            StatusCode::Internal => Self::Internal,
            StatusCode::NotSupported => Self::NotSupported,
        })
    }
}

impl From<CoreSessionKind> for CapnpSessionKind {
    fn from(value: CoreSessionKind) -> Self {
        match value {
            CoreSessionKind::Transient => CapnpSessionKind::Transient,
            CoreSessionKind::Persistent => CapnpSessionKind::Persistent,
        }
    }
}

impl TryFrom<CapnpSessionKind> for CoreSessionKind {
    type Error = anyhow::Error;

    fn try_from(value: CapnpSessionKind) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpSessionKind::Transient => Self::Transient,
            CapnpSessionKind::Persistent => Self::Persistent,
        })
    }
}

impl From<CoreInflightPhase> for CapnpInflightPhase {
    fn from(value: CoreInflightPhase) -> Self {
        match value {
            CoreInflightPhase::Publish => CapnpInflightPhase::Publish,
            CoreInflightPhase::Release => CapnpInflightPhase::Release,
        }
    }
}

impl TryFrom<CapnpInflightPhase> for CoreInflightPhase {
    type Error = anyhow::Error;

    fn try_from(value: CapnpInflightPhase) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpInflightPhase::Publish => Self::Publish,
            CapnpInflightPhase::Release => Self::Release,
        })
    }
}

impl From<CoreServiceShardKind> for CapnpServiceShardKind {
    fn from(value: CoreServiceShardKind) -> Self {
        match value {
            CoreServiceShardKind::SessionDict => Self::SessionDict,
            CoreServiceShardKind::Inbox => Self::Inbox,
            CoreServiceShardKind::Inflight => Self::Inflight,
            CoreServiceShardKind::Dist => Self::Dist,
            CoreServiceShardKind::Retain => Self::Retain,
        }
    }
}

impl TryFrom<CapnpServiceShardKind> for CoreServiceShardKind {
    type Error = anyhow::Error;

    fn try_from(value: CapnpServiceShardKind) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpServiceShardKind::SessionDict => Self::SessionDict,
            CapnpServiceShardKind::Inbox => Self::Inbox,
            CapnpServiceShardKind::Inflight => Self::Inflight,
            CapnpServiceShardKind::Dist => Self::Dist,
            CapnpServiceShardKind::Retain => Self::Retain,
        })
    }
}

impl From<CoreServiceShardLifecycle> for CapnpServiceShardLifecycle {
    fn from(value: CoreServiceShardLifecycle) -> Self {
        match value {
            CoreServiceShardLifecycle::Bootstrapping => Self::Bootstrapping,
            CoreServiceShardLifecycle::Serving => Self::Serving,
            CoreServiceShardLifecycle::Draining => Self::Draining,
            CoreServiceShardLifecycle::Recovering => Self::Recovering,
            CoreServiceShardLifecycle::Offline => Self::Offline,
        }
    }
}

impl TryFrom<CapnpServiceShardLifecycle> for CoreServiceShardLifecycle {
    type Error = anyhow::Error;

    fn try_from(value: CapnpServiceShardLifecycle) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpServiceShardLifecycle::Bootstrapping => Self::Bootstrapping,
            CapnpServiceShardLifecycle::Serving => Self::Serving,
            CapnpServiceShardLifecycle::Draining => Self::Draining,
            CapnpServiceShardLifecycle::Recovering => Self::Recovering,
            CapnpServiceShardLifecycle::Offline => Self::Offline,
        })
    }
}

impl From<CoreReplicaRole> for CapnpReplicaRole {
    fn from(value: CoreReplicaRole) -> Self {
        match value {
            CoreReplicaRole::Voter => Self::Voter,
            CoreReplicaRole::Learner => Self::Learner,
        }
    }
}

impl TryFrom<CapnpReplicaRole> for CoreReplicaRole {
    type Error = anyhow::Error;

    fn try_from(value: CapnpReplicaRole) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpReplicaRole::Voter => Self::Voter,
            CapnpReplicaRole::Learner => Self::Learner,
        })
    }
}

impl From<CoreReplicaSyncState> for CapnpReplicaSyncState {
    fn from(value: CoreReplicaSyncState) -> Self {
        match value {
            CoreReplicaSyncState::Probing => Self::Probing,
            CoreReplicaSyncState::Snapshotting => Self::Snapshotting,
            CoreReplicaSyncState::Replicating => Self::Replicating,
            CoreReplicaSyncState::Offline => Self::Offline,
        }
    }
}

impl TryFrom<CapnpReplicaSyncState> for CoreReplicaSyncState {
    type Error = anyhow::Error;

    fn try_from(value: CapnpReplicaSyncState) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpReplicaSyncState::Probing => Self::Probing,
            CapnpReplicaSyncState::Snapshotting => Self::Snapshotting,
            CapnpReplicaSyncState::Replicating => Self::Replicating,
            CapnpReplicaSyncState::Offline => Self::Offline,
        })
    }
}

impl From<CoreNodeServiceKind> for CapnpNodeServiceKind {
    fn from(value: CoreNodeServiceKind) -> Self {
        match value {
            CoreNodeServiceKind::Broker => Self::Broker,
            CoreNodeServiceKind::SessionDict => Self::SessionDict,
            CoreNodeServiceKind::Dist => Self::Dist,
            CoreNodeServiceKind::Inbox => Self::Inbox,
            CoreNodeServiceKind::Retain => Self::Retain,
            CoreNodeServiceKind::HttpApi => Self::HttpApi,
        }
    }
}

impl TryFrom<CapnpNodeServiceKind> for CoreNodeServiceKind {
    type Error = anyhow::Error;

    fn try_from(value: CapnpNodeServiceKind) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpNodeServiceKind::Broker => Self::Broker,
            CapnpNodeServiceKind::SessionDict => Self::SessionDict,
            CapnpNodeServiceKind::Dist => Self::Dist,
            CapnpNodeServiceKind::Inbox => Self::Inbox,
            CapnpNodeServiceKind::Retain => Self::Retain,
            CapnpNodeServiceKind::HttpApi => Self::HttpApi,
        })
    }
}

impl From<CoreClusterNodeLifecycle> for CapnpClusterNodeLifecycle {
    fn from(value: CoreClusterNodeLifecycle) -> Self {
        match value {
            CoreClusterNodeLifecycle::Joining => Self::Joining,
            CoreClusterNodeLifecycle::Serving => Self::Serving,
            CoreClusterNodeLifecycle::Suspect => Self::Suspect,
            CoreClusterNodeLifecycle::Leaving => Self::Leaving,
            CoreClusterNodeLifecycle::Offline => Self::Offline,
        }
    }
}

impl TryFrom<CapnpClusterNodeLifecycle> for CoreClusterNodeLifecycle {
    type Error = anyhow::Error;

    fn try_from(value: CapnpClusterNodeLifecycle) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpClusterNodeLifecycle::Joining => Self::Joining,
            CapnpClusterNodeLifecycle::Serving => Self::Serving,
            CapnpClusterNodeLifecycle::Suspect => Self::Suspect,
            CapnpClusterNodeLifecycle::Leaving => Self::Leaving,
            CapnpClusterNodeLifecycle::Offline => Self::Offline,
        })
    }
}

impl From<CoreReconfigurationPhase> for CapnpReconfigurationPhase {
    fn from(value: CoreReconfigurationPhase) -> Self {
        match value {
            CoreReconfigurationPhase::StagingLearners => Self::StagingLearners,
            CoreReconfigurationPhase::JointConsensus => Self::JointConsensus,
            CoreReconfigurationPhase::Finalizing => Self::Finalizing,
        }
    }
}

impl TryFrom<CapnpReconfigurationPhase> for CoreReconfigurationPhase {
    type Error = anyhow::Error;

    fn try_from(value: CapnpReconfigurationPhase) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpReconfigurationPhase::StagingLearners => Self::StagingLearners,
            CapnpReconfigurationPhase::JointConsensus => Self::JointConsensus,
            CapnpReconfigurationPhase::Finalizing => Self::Finalizing,
        })
    }
}

impl From<RaftRoleEnvelope> for CapnpRaftRole {
    fn from(value: RaftRoleEnvelope) -> Self {
        match value {
            RaftRoleEnvelope::Follower => Self::Follower,
            RaftRoleEnvelope::Candidate => Self::Candidate,
            RaftRoleEnvelope::Leader => Self::Leader,
        }
    }
}

impl TryFrom<CapnpRaftRole> for RaftRoleEnvelope {
    type Error = anyhow::Error;

    fn try_from(value: CapnpRaftRole) -> anyhow::Result<Self> {
        Ok(match value {
            CapnpRaftRole::Follower => Self::Follower,
            CapnpRaftRole::Candidate => Self::Candidate,
            CapnpRaftRole::Leader => Self::Leader,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcRequestHeaderEnvelope {
    pub service: RpcServiceKind,
    pub method_id: u16,
    pub protocol_version: u16,
    pub request_id: u64,
    pub timeout_ms: u64,
    pub trace_id: String,
    pub expected_epoch: u64,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponseHeaderEnvelope {
    pub status: RpcStatusCode,
    pub retryable: bool,
    pub retry_after_ms: u64,
    pub error_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupSessionRequestEnvelope {
    pub identity: CoreClientIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupSessionByIdRequestEnvelope {
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupSessionReplyEnvelope {
    pub record: Option<CoreSessionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterSessionRequestEnvelope {
    pub record: CoreSessionRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterSessionReplyEnvelope {
    pub replaced: Option<CoreSessionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterSessionRequestEnvelope {
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSessionsRequestEnvelope {
    pub tenant_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSessionsReplyEnvelope {
    pub records: Vec<CoreSessionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSessionRoutesRequestEnvelope {
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRouteRequestEnvelope {
    pub route: CoreRouteRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveRouteRequestEnvelope {
    pub route: CoreRouteRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSessionRoutesReplyEnvelope {
    pub routes: Vec<CoreRouteRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchTopicRequestEnvelope {
    pub tenant_id: String,
    pub topic: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchTopicReplyEnvelope {
    pub routes: Vec<CoreRouteRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRoutesRequestEnvelope {
    pub tenant_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRoutesReplyEnvelope {
    pub routes: Vec<CoreRouteRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxAttachRequestEnvelope {
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxDetachRequestEnvelope {
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxPurgeSessionRequestEnvelope {
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxAckInflightRequestEnvelope {
    pub session_id: String,
    pub packet_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeGetRequestEnvelope {
    pub range_id: String,
    pub key: Vec<u8>,
    pub expected_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeGetReplyEnvelope {
    pub value: Vec<u8>,
    pub found: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntryEnvelope {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeScanRequestEnvelope {
    pub range_id: String,
    pub boundary: Option<CoreRangeBoundary>,
    pub limit: u32,
    pub expected_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeScanReplyEnvelope {
    pub entries: Vec<KvEntryEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvMutationEnvelope {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeApplyRequestEnvelope {
    pub range_id: String,
    pub mutations: Vec<KvMutationEnvelope>,
    pub expected_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeCheckpointRequestEnvelope {
    pub range_id: String,
    pub checkpoint_id: String,
    pub expected_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeCheckpointReplyEnvelope {
    pub range_id: String,
    pub checkpoint_id: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeSnapshotRequestEnvelope {
    pub range_id: String,
    pub expected_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeSnapshotReplyEnvelope {
    pub range_id: String,
    pub boundary: CoreRangeBoundary,
    pub term: u64,
    pub index: u64,
    pub checksum: u64,
    pub layout_version: u32,
    pub data_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberLookupRequestEnvelope {
    pub node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRecordReplyEnvelope {
    pub member: Option<CoreClusterNodeMembership>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberListReplyEnvelope {
    pub members: Vec<CoreClusterNodeMembership>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancerStateUpsertRequestEnvelope {
    pub name: String,
    pub state: CoreBalancerState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancerStateUpsertReplyEnvelope {
    pub previous: Option<CoreBalancerState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancerStateRequestEnvelope {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancerStateReplyEnvelope {
    pub state: Option<CoreBalancerState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedBalancerStateEnvelope {
    pub name: String,
    pub state: Option<CoreBalancerState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalancerStateListReplyEnvelope {
    pub entries: Vec<NamedBalancerStateEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeLookupRequestEnvelope {
    pub range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRecordReplyEnvelope {
    pub descriptor: Option<CoreReplicatedRangeDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeListRequestEnvelope {
    pub shard_kind: Option<CoreServiceShardKind>,
    pub tenant_id: Option<String>,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeListReplyEnvelope {
    pub descriptors: Vec<CoreReplicatedRangeDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRangeRequestEnvelope {
    pub shard: CoreServiceShardKey,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainWriteRequestEnvelope {
    pub message: CoreRetainedMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainMatchRequestEnvelope {
    pub tenant_id: String,
    pub topic_filter: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainMatchReplyEnvelope {
    pub messages: Vec<CoreRetainedMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRoleEnvelope {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLagEnvelope {
    pub node_id: u64,
    pub lag: u64,
    pub match_index: u64,
    pub next_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeHealthEnvelope {
    pub range_id: String,
    pub lifecycle: CoreServiceShardLifecycle,
    pub role: RaftRoleEnvelope,
    pub current_term: u64,
    pub leader_node_id: Option<u64>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub latest_snapshot_index: Option<u64>,
    pub replica_lag: Vec<ReplicaLagEnvelope>,
    pub reconfiguration: CoreRangeReconfigurationState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeHealthRequestEnvelope {
    pub range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeHealthReplyEnvelope {
    pub health: Option<RangeHealthEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeHealthListReplyEnvelope {
    pub entries: Vec<RangeHealthEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDebugReplyEnvelope {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZombieRangeEnvelope {
    pub range_id: String,
    pub lifecycle: CoreServiceShardLifecycle,
    pub leader_node_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZombieRangeListReplyEnvelope {
    pub entries: Vec<ZombieRangeEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBootstrapRequestEnvelope {
    pub descriptor: CoreReplicatedRangeDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBootstrapReplyEnvelope {
    pub range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeChangeReplicasRequestEnvelope {
    pub range_id: String,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTransferLeadershipRequestEnvelope {
    pub range_id: String,
    pub target_node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRecoverRequestEnvelope {
    pub range_id: String,
    pub new_leader_node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSplitRequestEnvelope {
    pub range_id: String,
    pub split_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSplitReplyEnvelope {
    pub left_range_id: String,
    pub right_range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMergeRequestEnvelope {
    pub left_range_id: String,
    pub right_range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMergeReplyEnvelope {
    pub range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDrainRequestEnvelope {
    pub range_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRetireRequestEnvelope {
    pub range_id: String,
}

const CONFIG_CHANGE_PREFIX: &[u8] = b"__greenmqtt_cfg__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftTransportRequestEnvelope {
    pub range_id: String,
    pub from_node_id: u64,
    pub message: CoreRaftMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcFrameEnvelope {
    pub kind: RpcFrameKind,
    pub request_header: Option<RpcRequestHeaderEnvelope>,
    pub response_header: Option<RpcResponseHeaderEnvelope>,
    pub payload: Vec<u8>,
}

pub fn write_client_identity(
    mut builder: client_identity::Builder<'_>,
    value: &CoreClientIdentity,
) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_user_id(&value.user_id);
    builder.set_client_id(&value.client_id);
}

pub fn read_client_identity(
    reader: client_identity::Reader<'_>,
) -> anyhow::Result<CoreClientIdentity> {
    Ok(CoreClientIdentity {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read client identity tenant id")?
            .to_string()
            .context("failed to decode client identity tenant id as utf-8")?,
        user_id: reader
            .get_user_id()
            .context("failed to read client identity user id")?
            .to_string()
            .context("failed to decode client identity user id as utf-8")?,
        client_id: reader
            .get_client_id()
            .context("failed to read client identity client id")?
            .to_string()
            .context("failed to decode client identity client id as utf-8")?,
    })
}

pub fn write_session_record(mut builder: session_record::Builder<'_>, value: &CoreSessionRecord) {
    builder.set_session_id(&value.session_id);
    builder.set_node_id(value.node_id);
    builder.set_kind(value.kind.clone().into());
    write_client_identity(builder.reborrow().init_identity(), &value.identity);
    builder
        .set_session_expiry_interval_secs(value.session_expiry_interval_secs.unwrap_or_default());
    builder.set_has_session_expiry_interval_secs(value.session_expiry_interval_secs.is_some());
    builder.set_expires_at_ms(value.expires_at_ms.unwrap_or_default());
    builder.set_has_expires_at_ms(value.expires_at_ms.is_some());
}

pub fn read_session_record(
    reader: session_record::Reader<'_>,
) -> anyhow::Result<CoreSessionRecord> {
    Ok(CoreSessionRecord {
        session_id: reader
            .get_session_id()
            .context("failed to read session record session id")?
            .to_string()
            .context("failed to decode session record session id as utf-8")?,
        node_id: reader.get_node_id(),
        kind: CoreSessionKind::try_from(
            reader
                .get_kind()
                .context("failed to read session record kind")?,
        )?,
        identity: read_client_identity(
            reader
                .get_identity()
                .context("failed to read session record identity")?,
        )?,
        session_expiry_interval_secs: reader
            .get_has_session_expiry_interval_secs()
            .then_some(reader.get_session_expiry_interval_secs()),
        expires_at_ms: reader
            .get_has_expires_at_ms()
            .then_some(reader.get_expires_at_ms()),
    })
}

pub fn encode_client_identity(value: &CoreClientIdentity) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_client_identity(message.init_root::<client_identity::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_client_identity(bytes: &[u8]) -> anyhow::Result<CoreClientIdentity> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp client identity")?;
    read_client_identity(
        message
            .get_root::<client_identity::Reader<'_>>()
            .context("failed to read capnp client identity root")?,
    )
}

pub fn encode_session_record(value: &CoreSessionRecord) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_session_record(message.init_root::<session_record::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_session_record(bytes: &[u8]) -> anyhow::Result<CoreSessionRecord> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp session record")?;
    read_session_record(
        message
            .get_root::<session_record::Reader<'_>>()
            .context("failed to read capnp session record root")?,
    )
}

pub fn write_lookup_session_request(
    mut builder: lookup_session_request::Builder<'_>,
    value: &LookupSessionRequestEnvelope,
) {
    write_client_identity(builder.reborrow().init_identity(), &value.identity);
}

pub fn read_lookup_session_request(
    reader: lookup_session_request::Reader<'_>,
) -> anyhow::Result<LookupSessionRequestEnvelope> {
    Ok(LookupSessionRequestEnvelope {
        identity: read_client_identity(
            reader
                .get_identity()
                .context("failed to read lookup session request identity")?,
        )?,
    })
}

pub fn encode_lookup_session_request(
    value: &LookupSessionRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_lookup_session_request(
        message.init_root::<lookup_session_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_lookup_session_request(bytes: &[u8]) -> anyhow::Result<LookupSessionRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp lookup session request")?;
    read_lookup_session_request(
        message
            .get_root::<lookup_session_request::Reader<'_>>()
            .context("failed to read capnp lookup session request root")?,
    )
}

pub fn write_lookup_session_by_id_request(
    mut builder: lookup_session_by_id_request::Builder<'_>,
    value: &LookupSessionByIdRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
}

pub fn read_lookup_session_by_id_request(
    reader: lookup_session_by_id_request::Reader<'_>,
) -> anyhow::Result<LookupSessionByIdRequestEnvelope> {
    Ok(LookupSessionByIdRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read lookup session by id session id")?
            .to_string()
            .context("failed to decode lookup session by id session id as utf-8")?,
    })
}

pub fn encode_lookup_session_by_id_request(
    value: &LookupSessionByIdRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_lookup_session_by_id_request(
        message.init_root::<lookup_session_by_id_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_lookup_session_by_id_request(
    bytes: &[u8],
) -> anyhow::Result<LookupSessionByIdRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp lookup session by id request")?;
    read_lookup_session_by_id_request(
        message
            .get_root::<lookup_session_by_id_request::Reader<'_>>()
            .context("failed to read capnp lookup session by id request root")?,
    )
}

pub fn write_lookup_session_reply(
    mut builder: lookup_session_reply::Builder<'_>,
    value: &LookupSessionReplyEnvelope,
) {
    if let Some(record) = &value.record {
        write_session_record(builder.reborrow().init_record(), record);
    }
    builder.set_has_record(value.record.is_some());
}

pub fn read_lookup_session_reply(
    reader: lookup_session_reply::Reader<'_>,
) -> anyhow::Result<LookupSessionReplyEnvelope> {
    Ok(LookupSessionReplyEnvelope {
        record: if reader.get_has_record() {
            Some(read_session_record(
                reader
                    .get_record()
                    .context("failed to read lookup session reply record")?,
            )?)
        } else {
            None
        },
    })
}

pub fn encode_lookup_session_reply(value: &LookupSessionReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_lookup_session_reply(
        message.init_root::<lookup_session_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_lookup_session_reply(bytes: &[u8]) -> anyhow::Result<LookupSessionReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp lookup session reply")?;
    read_lookup_session_reply(
        message
            .get_root::<lookup_session_reply::Reader<'_>>()
            .context("failed to read capnp lookup session reply root")?,
    )
}

pub fn write_register_session_request(
    mut builder: register_session_request::Builder<'_>,
    value: &RegisterSessionRequestEnvelope,
) {
    write_session_record(builder.reborrow().init_record(), &value.record);
}

pub fn read_register_session_request(
    reader: register_session_request::Reader<'_>,
) -> anyhow::Result<RegisterSessionRequestEnvelope> {
    Ok(RegisterSessionRequestEnvelope {
        record: read_session_record(
            reader
                .get_record()
                .context("failed to read register session request record")?,
        )?,
    })
}

pub fn encode_register_session_request(
    value: &RegisterSessionRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_register_session_request(
        message.init_root::<register_session_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_register_session_request(
    bytes: &[u8],
) -> anyhow::Result<RegisterSessionRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp register session request")?;
    read_register_session_request(
        message
            .get_root::<register_session_request::Reader<'_>>()
            .context("failed to read capnp register session request root")?,
    )
}

pub fn write_register_session_reply(
    mut builder: register_session_reply::Builder<'_>,
    value: &RegisterSessionReplyEnvelope,
) {
    if let Some(replaced) = &value.replaced {
        write_session_record(builder.reborrow().init_replaced(), replaced);
    }
    builder.set_has_replaced(value.replaced.is_some());
}

pub fn read_register_session_reply(
    reader: register_session_reply::Reader<'_>,
) -> anyhow::Result<RegisterSessionReplyEnvelope> {
    Ok(RegisterSessionReplyEnvelope {
        replaced: if reader.get_has_replaced() {
            Some(read_session_record(reader.get_replaced().context(
                "failed to read register session reply replaced",
            )?)?)
        } else {
            None
        },
    })
}

pub fn encode_register_session_reply(
    value: &RegisterSessionReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_register_session_reply(
        message.init_root::<register_session_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_register_session_reply(bytes: &[u8]) -> anyhow::Result<RegisterSessionReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp register session reply")?;
    read_register_session_reply(
        message
            .get_root::<register_session_reply::Reader<'_>>()
            .context("failed to read capnp register session reply root")?,
    )
}

pub fn write_unregister_session_request(
    mut builder: unregister_session_request::Builder<'_>,
    value: &UnregisterSessionRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
}

pub fn read_unregister_session_request(
    reader: unregister_session_request::Reader<'_>,
) -> anyhow::Result<UnregisterSessionRequestEnvelope> {
    Ok(UnregisterSessionRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read unregister session request session id")?
            .to_string()
            .context("failed to decode unregister session request session id as utf-8")?,
    })
}

pub fn encode_unregister_session_request(
    value: &UnregisterSessionRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_unregister_session_request(
        message.init_root::<unregister_session_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_unregister_session_request(
    bytes: &[u8],
) -> anyhow::Result<UnregisterSessionRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp unregister session request")?;
    read_unregister_session_request(
        message
            .get_root::<unregister_session_request::Reader<'_>>()
            .context("failed to read capnp unregister session request root")?,
    )
}

pub fn write_list_sessions_request(
    mut builder: list_sessions_request::Builder<'_>,
    value: &ListSessionsRequestEnvelope,
) {
    builder.set_tenant_id(value.tenant_id.as_deref().unwrap_or_default());
    builder.set_has_tenant_id(value.tenant_id.is_some());
}

pub fn read_list_sessions_request(
    reader: list_sessions_request::Reader<'_>,
) -> anyhow::Result<ListSessionsRequestEnvelope> {
    Ok(ListSessionsRequestEnvelope {
        tenant_id: reader.get_has_tenant_id().then(|| {
            reader
                .get_tenant_id()
                .expect("tenant id read should not fail after has_tenant_id")
                .to_string()
                .expect("tenant id should decode as utf-8")
        }),
    })
}

pub fn encode_list_sessions_request(
    value: &ListSessionsRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_list_sessions_request(
        message.init_root::<list_sessions_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_list_sessions_request(bytes: &[u8]) -> anyhow::Result<ListSessionsRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp list sessions request")?;
    read_list_sessions_request(
        message
            .get_root::<list_sessions_request::Reader<'_>>()
            .context("failed to read capnp list sessions request root")?,
    )
}

pub fn write_list_sessions_reply(
    mut builder: list_sessions_reply::Builder<'_>,
    value: &ListSessionsReplyEnvelope,
) {
    let mut records = builder.reborrow().init_records(value.records.len() as u32);
    for (index, record) in value.records.iter().enumerate() {
        write_session_record(records.reborrow().get(index as u32), record);
    }
}

pub fn read_list_sessions_reply(
    reader: list_sessions_reply::Reader<'_>,
) -> anyhow::Result<ListSessionsReplyEnvelope> {
    Ok(ListSessionsReplyEnvelope {
        records: reader
            .get_records()
            .context("failed to read list sessions reply records")?
            .iter()
            .map(read_session_record)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_list_sessions_reply(value: &ListSessionsReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_list_sessions_reply(
        message.init_root::<list_sessions_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_list_sessions_reply(bytes: &[u8]) -> anyhow::Result<ListSessionsReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp list sessions reply")?;
    read_list_sessions_reply(
        message
            .get_root::<list_sessions_reply::Reader<'_>>()
            .context("failed to read capnp list sessions reply root")?,
    )
}

pub fn write_route_record(mut builder: route_record::Builder<'_>, value: &CoreRouteRecord) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_topic_filter(&value.topic_filter);
    builder.set_session_id(&value.session_id);
    builder.set_node_id(value.node_id);
    builder.set_subscription_identifier(value.subscription_identifier.unwrap_or_default());
    builder.set_has_subscription_identifier(value.subscription_identifier.is_some());
    builder.set_no_local(value.no_local);
    builder.set_retain_as_published(value.retain_as_published);
    builder.set_shared_group(value.shared_group.as_deref().unwrap_or_default());
    builder.set_has_shared_group(value.shared_group.is_some());
    builder.set_kind(value.kind.clone().into());
}

pub fn read_route_record(reader: route_record::Reader<'_>) -> anyhow::Result<CoreRouteRecord> {
    Ok(CoreRouteRecord {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read route tenant id")?
            .to_string()
            .context("failed to decode route tenant id as utf-8")?,
        topic_filter: reader
            .get_topic_filter()
            .context("failed to read route topic filter")?
            .to_string()
            .context("failed to decode route topic filter as utf-8")?,
        session_id: reader
            .get_session_id()
            .context("failed to read route session id")?
            .to_string()
            .context("failed to decode route session id as utf-8")?,
        node_id: reader.get_node_id(),
        subscription_identifier: reader
            .get_has_subscription_identifier()
            .then_some(reader.get_subscription_identifier()),
        no_local: reader.get_no_local(),
        retain_as_published: reader.get_retain_as_published(),
        shared_group: reader.get_has_shared_group().then(|| {
            reader
                .get_shared_group()
                .expect("shared group read should not fail after has_shared_group")
                .to_string()
                .expect("shared group should decode as utf-8")
        }),
        kind: CoreSessionKind::try_from(
            reader
                .get_kind()
                .context("failed to read route session kind")?,
        )?,
    })
}

pub fn encode_route_record(value: &CoreRouteRecord) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_route_record(message.init_root::<route_record::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_route_record(bytes: &[u8]) -> anyhow::Result<CoreRouteRecord> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp route record")?;
    read_route_record(
        message
            .get_root::<route_record::Reader<'_>>()
            .context("failed to read capnp route record root")?,
    )
}

pub fn write_add_route_request(
    mut builder: add_route_request::Builder<'_>,
    value: &AddRouteRequestEnvelope,
) {
    write_route_record(builder.reborrow().init_route(), &value.route);
}

pub fn read_add_route_request(
    reader: add_route_request::Reader<'_>,
) -> anyhow::Result<AddRouteRequestEnvelope> {
    Ok(AddRouteRequestEnvelope {
        route: read_route_record(
            reader
                .get_route()
                .context("failed to read add route request route")?,
        )?,
    })
}

pub fn encode_add_route_request(value: &AddRouteRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_add_route_request(message.init_root::<add_route_request::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_add_route_request(bytes: &[u8]) -> anyhow::Result<AddRouteRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp add route request")?;
    read_add_route_request(
        message
            .get_root::<add_route_request::Reader<'_>>()
            .context("failed to read capnp add route request root")?,
    )
}

pub fn write_remove_route_request(
    mut builder: remove_route_request::Builder<'_>,
    value: &RemoveRouteRequestEnvelope,
) {
    write_route_record(builder.reborrow().init_route(), &value.route);
}

pub fn read_remove_route_request(
    reader: remove_route_request::Reader<'_>,
) -> anyhow::Result<RemoveRouteRequestEnvelope> {
    Ok(RemoveRouteRequestEnvelope {
        route: read_route_record(
            reader
                .get_route()
                .context("failed to read remove route request route")?,
        )?,
    })
}

pub fn encode_remove_route_request(value: &RemoveRouteRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_remove_route_request(
        message.init_root::<remove_route_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_remove_route_request(bytes: &[u8]) -> anyhow::Result<RemoveRouteRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp remove route request")?;
    read_remove_route_request(
        message
            .get_root::<remove_route_request::Reader<'_>>()
            .context("failed to read capnp remove route request root")?,
    )
}

pub fn write_list_session_routes_request(
    mut builder: list_session_routes_request::Builder<'_>,
    value: &ListSessionRoutesRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
}

pub fn read_list_session_routes_request(
    reader: list_session_routes_request::Reader<'_>,
) -> anyhow::Result<ListSessionRoutesRequestEnvelope> {
    Ok(ListSessionRoutesRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read list session routes session id")?
            .to_string()
            .context("failed to decode list session routes session id as utf-8")?,
    })
}

pub fn encode_list_session_routes_request(
    value: &ListSessionRoutesRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_list_session_routes_request(
        message.init_root::<list_session_routes_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_list_session_routes_request(
    bytes: &[u8],
) -> anyhow::Result<ListSessionRoutesRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp list session routes request")?;
    read_list_session_routes_request(
        message
            .get_root::<list_session_routes_request::Reader<'_>>()
            .context("failed to read capnp list session routes request root")?,
    )
}

pub fn write_list_session_routes_reply(
    mut builder: list_session_routes_reply::Builder<'_>,
    value: &ListSessionRoutesReplyEnvelope,
) {
    let mut routes = builder.reborrow().init_routes(value.routes.len() as u32);
    for (index, route) in value.routes.iter().enumerate() {
        write_route_record(routes.reborrow().get(index as u32), route);
    }
}

pub fn read_list_session_routes_reply(
    reader: list_session_routes_reply::Reader<'_>,
) -> anyhow::Result<ListSessionRoutesReplyEnvelope> {
    Ok(ListSessionRoutesReplyEnvelope {
        routes: reader
            .get_routes()
            .context("failed to read list session routes reply routes")?
            .iter()
            .map(read_route_record)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_list_session_routes_reply(
    value: &ListSessionRoutesReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_list_session_routes_reply(
        message.init_root::<list_session_routes_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_list_session_routes_reply(
    bytes: &[u8],
) -> anyhow::Result<ListSessionRoutesReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp list session routes reply")?;
    read_list_session_routes_reply(
        message
            .get_root::<list_session_routes_reply::Reader<'_>>()
            .context("failed to read capnp list session routes reply root")?,
    )
}

pub fn write_match_topic_request(
    mut builder: match_topic_request::Builder<'_>,
    value: &MatchTopicRequestEnvelope,
) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_topic(&value.topic);
}

pub fn read_match_topic_request(
    reader: match_topic_request::Reader<'_>,
) -> anyhow::Result<MatchTopicRequestEnvelope> {
    Ok(MatchTopicRequestEnvelope {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read match topic tenant id")?
            .to_string()
            .context("failed to decode match topic tenant id as utf-8")?,
        topic: reader
            .get_topic()
            .context("failed to read match topic")?
            .to_string()
            .context("failed to decode match topic as utf-8")?,
    })
}

pub fn encode_match_topic_request(value: &MatchTopicRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_match_topic_request(
        message.init_root::<match_topic_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_match_topic_request(bytes: &[u8]) -> anyhow::Result<MatchTopicRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp match topic request")?;
    read_match_topic_request(
        message
            .get_root::<match_topic_request::Reader<'_>>()
            .context("failed to read capnp match topic request root")?,
    )
}

pub fn write_match_topic_reply(
    mut builder: match_topic_reply::Builder<'_>,
    value: &MatchTopicReplyEnvelope,
) {
    let mut routes = builder.reborrow().init_routes(value.routes.len() as u32);
    for (index, route) in value.routes.iter().enumerate() {
        write_route_record(routes.reborrow().get(index as u32), route);
    }
}

pub fn read_match_topic_reply(
    reader: match_topic_reply::Reader<'_>,
) -> anyhow::Result<MatchTopicReplyEnvelope> {
    Ok(MatchTopicReplyEnvelope {
        routes: reader
            .get_routes()
            .context("failed to read match topic reply routes")?
            .iter()
            .map(read_route_record)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_match_topic_reply(value: &MatchTopicReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_match_topic_reply(message.init_root::<match_topic_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_match_topic_reply(bytes: &[u8]) -> anyhow::Result<MatchTopicReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp match topic reply")?;
    read_match_topic_reply(
        message
            .get_root::<match_topic_reply::Reader<'_>>()
            .context("failed to read capnp match topic reply root")?,
    )
}

pub fn write_list_routes_request(
    mut builder: list_routes_request::Builder<'_>,
    value: &ListRoutesRequestEnvelope,
) {
    builder.set_tenant_id(value.tenant_id.as_deref().unwrap_or_default());
    builder.set_has_tenant_id(value.tenant_id.is_some());
}

pub fn read_list_routes_request(
    reader: list_routes_request::Reader<'_>,
) -> anyhow::Result<ListRoutesRequestEnvelope> {
    Ok(ListRoutesRequestEnvelope {
        tenant_id: reader.get_has_tenant_id().then(|| {
            reader
                .get_tenant_id()
                .expect("tenant id read should not fail after has_tenant_id")
                .to_string()
                .expect("tenant id should decode as utf-8")
        }),
    })
}

pub fn encode_list_routes_request(value: &ListRoutesRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_list_routes_request(
        message.init_root::<list_routes_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_list_routes_request(bytes: &[u8]) -> anyhow::Result<ListRoutesRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp list routes request")?;
    read_list_routes_request(
        message
            .get_root::<list_routes_request::Reader<'_>>()
            .context("failed to read capnp list routes request root")?,
    )
}

pub fn write_list_routes_reply(
    mut builder: list_routes_reply::Builder<'_>,
    value: &ListRoutesReplyEnvelope,
) {
    let mut routes = builder.reborrow().init_routes(value.routes.len() as u32);
    for (index, route) in value.routes.iter().enumerate() {
        write_route_record(routes.reborrow().get(index as u32), route);
    }
}

pub fn read_list_routes_reply(
    reader: list_routes_reply::Reader<'_>,
) -> anyhow::Result<ListRoutesReplyEnvelope> {
    Ok(ListRoutesReplyEnvelope {
        routes: reader
            .get_routes()
            .context("failed to read list routes reply routes")?
            .iter()
            .map(read_route_record)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_list_routes_reply(value: &ListRoutesReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_list_routes_reply(message.init_root::<list_routes_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_list_routes_reply(bytes: &[u8]) -> anyhow::Result<ListRoutesReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp list routes reply")?;
    read_list_routes_reply(
        message
            .get_root::<list_routes_reply::Reader<'_>>()
            .context("failed to read capnp list routes reply root")?,
    )
}

pub fn write_inbox_attach_request(
    mut builder: inbox_attach_request::Builder<'_>,
    value: &InboxAttachRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
}

pub fn read_inbox_attach_request(
    reader: inbox_attach_request::Reader<'_>,
) -> anyhow::Result<InboxAttachRequestEnvelope> {
    Ok(InboxAttachRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read inbox attach session id")?
            .to_string()
            .context("failed to decode inbox attach session id as utf-8")?,
    })
}

pub fn encode_inbox_attach_request(value: &InboxAttachRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_inbox_attach_request(
        message.init_root::<inbox_attach_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_inbox_attach_request(bytes: &[u8]) -> anyhow::Result<InboxAttachRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp inbox attach request")?;
    read_inbox_attach_request(
        message
            .get_root::<inbox_attach_request::Reader<'_>>()
            .context("failed to read capnp inbox attach request root")?,
    )
}

pub fn write_inbox_detach_request(
    mut builder: inbox_detach_request::Builder<'_>,
    value: &InboxDetachRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
}

pub fn read_inbox_detach_request(
    reader: inbox_detach_request::Reader<'_>,
) -> anyhow::Result<InboxDetachRequestEnvelope> {
    Ok(InboxDetachRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read inbox detach session id")?
            .to_string()
            .context("failed to decode inbox detach session id as utf-8")?,
    })
}

pub fn encode_inbox_detach_request(value: &InboxDetachRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_inbox_detach_request(
        message.init_root::<inbox_detach_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_inbox_detach_request(bytes: &[u8]) -> anyhow::Result<InboxDetachRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp inbox detach request")?;
    read_inbox_detach_request(
        message
            .get_root::<inbox_detach_request::Reader<'_>>()
            .context("failed to read capnp inbox detach request root")?,
    )
}

pub fn write_inbox_purge_session_request(
    mut builder: inbox_purge_session_request::Builder<'_>,
    value: &InboxPurgeSessionRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
}

pub fn read_inbox_purge_session_request(
    reader: inbox_purge_session_request::Reader<'_>,
) -> anyhow::Result<InboxPurgeSessionRequestEnvelope> {
    Ok(InboxPurgeSessionRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read inbox purge session id")?
            .to_string()
            .context("failed to decode inbox purge session id as utf-8")?,
    })
}

pub fn encode_inbox_purge_session_request(
    value: &InboxPurgeSessionRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_inbox_purge_session_request(
        message.init_root::<inbox_purge_session_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_inbox_purge_session_request(
    bytes: &[u8],
) -> anyhow::Result<InboxPurgeSessionRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp inbox purge session request")?;
    read_inbox_purge_session_request(
        message
            .get_root::<inbox_purge_session_request::Reader<'_>>()
            .context("failed to read capnp inbox purge session request root")?,
    )
}

pub fn write_inbox_ack_inflight_request(
    mut builder: inbox_ack_inflight_request::Builder<'_>,
    value: &InboxAckInflightRequestEnvelope,
) {
    builder.set_session_id(&value.session_id);
    builder.set_packet_id(value.packet_id);
}

pub fn read_inbox_ack_inflight_request(
    reader: inbox_ack_inflight_request::Reader<'_>,
) -> anyhow::Result<InboxAckInflightRequestEnvelope> {
    Ok(InboxAckInflightRequestEnvelope {
        session_id: reader
            .get_session_id()
            .context("failed to read inbox ack session id")?
            .to_string()
            .context("failed to decode inbox ack session id as utf-8")?,
        packet_id: reader.get_packet_id(),
    })
}

pub fn encode_inbox_ack_inflight_request(
    value: &InboxAckInflightRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_inbox_ack_inflight_request(
        message.init_root::<inbox_ack_inflight_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_inbox_ack_inflight_request(
    bytes: &[u8],
) -> anyhow::Result<InboxAckInflightRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp inbox ack inflight request")?;
    read_inbox_ack_inflight_request(
        message
            .get_root::<inbox_ack_inflight_request::Reader<'_>>()
            .context("failed to read capnp inbox ack inflight request root")?,
    )
}

pub fn write_kv_range_get_request(
    mut builder: kv_range_get_request::Builder<'_>,
    value: &KvRangeGetRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_key(&value.key);
    builder.set_expected_epoch(value.expected_epoch);
}

pub fn read_kv_range_get_request(
    reader: kv_range_get_request::Reader<'_>,
) -> anyhow::Result<KvRangeGetRequestEnvelope> {
    Ok(KvRangeGetRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range get range id")?
            .to_string()
            .context("failed to decode kv range get range id as utf-8")?,
        key: reader
            .get_key()
            .context("failed to read kv range get key")?
            .to_vec(),
        expected_epoch: reader.get_expected_epoch(),
    })
}

pub fn encode_kv_range_get_request(value: &KvRangeGetRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_get_request(
        message.init_root::<kv_range_get_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_get_request(bytes: &[u8]) -> anyhow::Result<KvRangeGetRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range get request")?;
    read_kv_range_get_request(
        message
            .get_root::<kv_range_get_request::Reader<'_>>()
            .context("failed to read capnp kv range get request root")?,
    )
}

pub fn write_kv_range_get_reply(
    mut builder: kv_range_get_reply::Builder<'_>,
    value: &KvRangeGetReplyEnvelope,
) {
    builder.set_value(&value.value);
    builder.set_found(value.found);
}

pub fn read_kv_range_get_reply(
    reader: kv_range_get_reply::Reader<'_>,
) -> anyhow::Result<KvRangeGetReplyEnvelope> {
    Ok(KvRangeGetReplyEnvelope {
        value: reader
            .get_value()
            .context("failed to read kv range get reply value")?
            .to_vec(),
        found: reader.get_found(),
    })
}

pub fn encode_kv_range_get_reply(value: &KvRangeGetReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_get_reply(
        message.init_root::<kv_range_get_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_get_reply(bytes: &[u8]) -> anyhow::Result<KvRangeGetReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range get reply")?;
    read_kv_range_get_reply(
        message
            .get_root::<kv_range_get_reply::Reader<'_>>()
            .context("failed to read capnp kv range get reply root")?,
    )
}

pub fn write_kv_entry(mut builder: kv_entry::Builder<'_>, value: &KvEntryEnvelope) {
    builder.set_key(&value.key);
    builder.set_value(&value.value);
}

pub fn read_kv_entry(reader: kv_entry::Reader<'_>) -> anyhow::Result<KvEntryEnvelope> {
    Ok(KvEntryEnvelope {
        key: reader
            .get_key()
            .context("failed to read kv entry key")?
            .to_vec(),
        value: reader
            .get_value()
            .context("failed to read kv entry value")?
            .to_vec(),
    })
}

pub fn write_kv_range_scan_request(
    mut builder: kv_range_scan_request::Builder<'_>,
    value: &KvRangeScanRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    if let Some(boundary) = &value.boundary {
        write_range_boundary(builder.reborrow().init_boundary(), boundary);
    }
    builder.set_has_boundary(value.boundary.is_some());
    builder.set_limit(value.limit);
    builder.set_expected_epoch(value.expected_epoch);
}

pub fn read_kv_range_scan_request(
    reader: kv_range_scan_request::Reader<'_>,
) -> anyhow::Result<KvRangeScanRequestEnvelope> {
    Ok(KvRangeScanRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range scan range id")?
            .to_string()
            .context("failed to decode kv range scan range id as utf-8")?,
        boundary: if reader.get_has_boundary() {
            Some(read_range_boundary(
                reader
                    .get_boundary()
                    .context("failed to read kv range scan boundary")?,
            )?)
        } else {
            None
        },
        limit: reader.get_limit(),
        expected_epoch: reader.get_expected_epoch(),
    })
}

pub fn encode_kv_range_scan_request(value: &KvRangeScanRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_scan_request(
        message.init_root::<kv_range_scan_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_scan_request(bytes: &[u8]) -> anyhow::Result<KvRangeScanRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range scan request")?;
    read_kv_range_scan_request(
        message
            .get_root::<kv_range_scan_request::Reader<'_>>()
            .context("failed to read capnp kv range scan request root")?,
    )
}

pub fn write_kv_range_scan_reply(
    mut builder: kv_range_scan_reply::Builder<'_>,
    value: &KvRangeScanReplyEnvelope,
) {
    let mut entries = builder.reborrow().init_entries(value.entries.len() as u32);
    for (index, entry) in value.entries.iter().enumerate() {
        write_kv_entry(entries.reborrow().get(index as u32), entry);
    }
}

pub fn read_kv_range_scan_reply(
    reader: kv_range_scan_reply::Reader<'_>,
) -> anyhow::Result<KvRangeScanReplyEnvelope> {
    Ok(KvRangeScanReplyEnvelope {
        entries: reader
            .get_entries()
            .context("failed to read kv range scan reply entries")?
            .iter()
            .map(read_kv_entry)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_kv_range_scan_reply(value: &KvRangeScanReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_scan_reply(
        message.init_root::<kv_range_scan_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_scan_reply(bytes: &[u8]) -> anyhow::Result<KvRangeScanReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range scan reply")?;
    read_kv_range_scan_reply(
        message
            .get_root::<kv_range_scan_reply::Reader<'_>>()
            .context("failed to read capnp kv range scan reply root")?,
    )
}

pub fn write_kv_mutation(mut builder: kv_mutation::Builder<'_>, value: &KvMutationEnvelope) {
    builder.set_key(&value.key);
    builder.set_value(value.value.as_deref().unwrap_or_default());
    builder.set_has_value(value.value.is_some());
}

pub fn read_kv_mutation(reader: kv_mutation::Reader<'_>) -> anyhow::Result<KvMutationEnvelope> {
    Ok(KvMutationEnvelope {
        key: reader
            .get_key()
            .context("failed to read kv mutation key")?
            .to_vec(),
        value: reader.get_has_value().then_some(
            reader
                .get_value()
                .context("failed to read kv mutation value")?
                .to_vec(),
        ),
    })
}

pub fn encode_kv_mutation(value: &KvMutationEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_mutation(message.init_root::<kv_mutation::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_mutation(bytes: &[u8]) -> anyhow::Result<KvMutationEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv mutation")?;
    read_kv_mutation(
        message
            .get_root::<kv_mutation::Reader<'_>>()
            .context("failed to read capnp kv mutation root")?,
    )
}

pub fn write_kv_range_apply_request(
    mut builder: kv_range_apply_request::Builder<'_>,
    value: &KvRangeApplyRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    let mut mutations = builder
        .reborrow()
        .init_mutations(value.mutations.len() as u32);
    for (index, mutation) in value.mutations.iter().enumerate() {
        write_kv_mutation(mutations.reborrow().get(index as u32), mutation);
    }
    builder.set_expected_epoch(value.expected_epoch);
}

pub fn read_kv_range_apply_request(
    reader: kv_range_apply_request::Reader<'_>,
) -> anyhow::Result<KvRangeApplyRequestEnvelope> {
    Ok(KvRangeApplyRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range apply range id")?
            .to_string()
            .context("failed to decode kv range apply range id as utf-8")?,
        mutations: reader
            .get_mutations()
            .context("failed to read kv range apply mutations")?
            .iter()
            .map(read_kv_mutation)
            .collect::<anyhow::Result<Vec<_>>>()?,
        expected_epoch: reader.get_expected_epoch(),
    })
}

pub fn encode_kv_range_apply_request(
    value: &KvRangeApplyRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_apply_request(
        message.init_root::<kv_range_apply_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_apply_request(bytes: &[u8]) -> anyhow::Result<KvRangeApplyRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range apply request")?;
    read_kv_range_apply_request(
        message
            .get_root::<kv_range_apply_request::Reader<'_>>()
            .context("failed to read capnp kv range apply request root")?,
    )
}

pub fn write_kv_range_checkpoint_request(
    mut builder: kv_range_checkpoint_request::Builder<'_>,
    value: &KvRangeCheckpointRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_checkpoint_id(&value.checkpoint_id);
    builder.set_expected_epoch(value.expected_epoch);
}

pub fn read_kv_range_checkpoint_request(
    reader: kv_range_checkpoint_request::Reader<'_>,
) -> anyhow::Result<KvRangeCheckpointRequestEnvelope> {
    Ok(KvRangeCheckpointRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range checkpoint range id")?
            .to_string()
            .context("failed to decode kv range checkpoint range id as utf-8")?,
        checkpoint_id: reader
            .get_checkpoint_id()
            .context("failed to read kv range checkpoint id")?
            .to_string()
            .context("failed to decode kv range checkpoint id as utf-8")?,
        expected_epoch: reader.get_expected_epoch(),
    })
}

pub fn encode_kv_range_checkpoint_request(
    value: &KvRangeCheckpointRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_checkpoint_request(
        message.init_root::<kv_range_checkpoint_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_checkpoint_request(
    bytes: &[u8],
) -> anyhow::Result<KvRangeCheckpointRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range checkpoint request")?;
    read_kv_range_checkpoint_request(
        message
            .get_root::<kv_range_checkpoint_request::Reader<'_>>()
            .context("failed to read capnp kv range checkpoint request root")?,
    )
}

pub fn write_kv_range_checkpoint_reply(
    mut builder: kv_range_checkpoint_reply::Builder<'_>,
    value: &KvRangeCheckpointReplyEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_checkpoint_id(&value.checkpoint_id);
    builder.set_path(&value.path);
}

pub fn read_kv_range_checkpoint_reply(
    reader: kv_range_checkpoint_reply::Reader<'_>,
) -> anyhow::Result<KvRangeCheckpointReplyEnvelope> {
    Ok(KvRangeCheckpointReplyEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range checkpoint reply range id")?
            .to_string()
            .context("failed to decode kv range checkpoint reply range id as utf-8")?,
        checkpoint_id: reader
            .get_checkpoint_id()
            .context("failed to read kv range checkpoint reply checkpoint id")?
            .to_string()
            .context("failed to decode kv range checkpoint reply checkpoint id as utf-8")?,
        path: reader
            .get_path()
            .context("failed to read kv range checkpoint reply path")?
            .to_string()
            .context("failed to decode kv range checkpoint reply path as utf-8")?,
    })
}

pub fn encode_kv_range_checkpoint_reply(
    value: &KvRangeCheckpointReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_checkpoint_reply(
        message.init_root::<kv_range_checkpoint_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_checkpoint_reply(
    bytes: &[u8],
) -> anyhow::Result<KvRangeCheckpointReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range checkpoint reply")?;
    read_kv_range_checkpoint_reply(
        message
            .get_root::<kv_range_checkpoint_reply::Reader<'_>>()
            .context("failed to read capnp kv range checkpoint reply root")?,
    )
}

pub fn write_kv_range_snapshot_request(
    mut builder: kv_range_snapshot_request::Builder<'_>,
    value: &KvRangeSnapshotRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_expected_epoch(value.expected_epoch);
}

pub fn read_kv_range_snapshot_request(
    reader: kv_range_snapshot_request::Reader<'_>,
) -> anyhow::Result<KvRangeSnapshotRequestEnvelope> {
    Ok(KvRangeSnapshotRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range snapshot range id")?
            .to_string()
            .context("failed to decode kv range snapshot range id as utf-8")?,
        expected_epoch: reader.get_expected_epoch(),
    })
}

pub fn encode_kv_range_snapshot_request(
    value: &KvRangeSnapshotRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_snapshot_request(
        message.init_root::<kv_range_snapshot_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_snapshot_request(
    bytes: &[u8],
) -> anyhow::Result<KvRangeSnapshotRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range snapshot request")?;
    read_kv_range_snapshot_request(
        message
            .get_root::<kv_range_snapshot_request::Reader<'_>>()
            .context("failed to read capnp kv range snapshot request root")?,
    )
}

pub fn write_kv_range_snapshot_reply(
    mut builder: kv_range_snapshot_reply::Builder<'_>,
    value: &KvRangeSnapshotReplyEnvelope,
) {
    builder.set_range_id(&value.range_id);
    write_range_boundary(builder.reborrow().init_boundary(), &value.boundary);
    builder.set_term(value.term);
    builder.set_index(value.index);
    builder.set_checksum(value.checksum);
    builder.set_layout_version(value.layout_version);
    builder.set_data_path(&value.data_path);
}

pub fn read_kv_range_snapshot_reply(
    reader: kv_range_snapshot_reply::Reader<'_>,
) -> anyhow::Result<KvRangeSnapshotReplyEnvelope> {
    Ok(KvRangeSnapshotReplyEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read kv range snapshot reply range id")?
            .to_string()
            .context("failed to decode kv range snapshot reply range id as utf-8")?,
        boundary: read_range_boundary(
            reader
                .get_boundary()
                .context("failed to read kv range snapshot reply boundary")?,
        )?,
        term: reader.get_term(),
        index: reader.get_index(),
        checksum: reader.get_checksum(),
        layout_version: reader.get_layout_version(),
        data_path: reader
            .get_data_path()
            .context("failed to read kv range snapshot reply data path")?
            .to_string()
            .context("failed to decode kv range snapshot reply data path as utf-8")?,
    })
}

pub fn encode_kv_range_snapshot_reply(
    value: &KvRangeSnapshotReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_kv_range_snapshot_reply(
        message.init_root::<kv_range_snapshot_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_kv_range_snapshot_reply(
    bytes: &[u8],
) -> anyhow::Result<KvRangeSnapshotReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp kv range snapshot reply")?;
    read_kv_range_snapshot_reply(
        message
            .get_root::<kv_range_snapshot_reply::Reader<'_>>()
            .context("failed to read capnp kv range snapshot reply root")?,
    )
}

pub fn write_subscription_record(
    mut builder: subscription_record::Builder<'_>,
    value: &CoreSubscription,
) {
    builder.set_session_id(&value.session_id);
    builder.set_tenant_id(&value.tenant_id);
    builder.set_topic_filter(&value.topic_filter);
    builder.set_qos(value.qos);
    builder.set_subscription_identifier(value.subscription_identifier.unwrap_or_default());
    builder.set_has_subscription_identifier(value.subscription_identifier.is_some());
    builder.set_no_local(value.no_local);
    builder.set_retain_as_published(value.retain_as_published);
    builder.set_retain_handling(value.retain_handling);
    builder.set_shared_group(value.shared_group.as_deref().unwrap_or_default());
    builder.set_has_shared_group(value.shared_group.is_some());
    builder.set_kind(value.kind.clone().into());
}

pub fn read_subscription_record(
    reader: subscription_record::Reader<'_>,
) -> anyhow::Result<CoreSubscription> {
    Ok(CoreSubscription {
        session_id: reader
            .get_session_id()
            .context("failed to read subscription session id")?
            .to_string()
            .context("failed to decode subscription session id as utf-8")?,
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read subscription tenant id")?
            .to_string()
            .context("failed to decode subscription tenant id as utf-8")?,
        topic_filter: reader
            .get_topic_filter()
            .context("failed to read subscription topic filter")?
            .to_string()
            .context("failed to decode subscription topic filter as utf-8")?,
        qos: reader.get_qos(),
        subscription_identifier: reader
            .get_has_subscription_identifier()
            .then_some(reader.get_subscription_identifier()),
        no_local: reader.get_no_local(),
        retain_as_published: reader.get_retain_as_published(),
        retain_handling: reader.get_retain_handling(),
        shared_group: reader.get_has_shared_group().then(|| {
            reader
                .get_shared_group()
                .expect("shared group read should not fail after has_shared_group")
                .to_string()
                .expect("shared group should decode as utf-8")
        }),
        kind: CoreSessionKind::try_from(
            reader
                .get_kind()
                .context("failed to read subscription session kind")?,
        )?,
    })
}

pub fn encode_subscription_record(value: &CoreSubscription) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_subscription_record(
        message.init_root::<subscription_record::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_subscription_record(bytes: &[u8]) -> anyhow::Result<CoreSubscription> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp subscription record")?;
    read_subscription_record(
        message
            .get_root::<subscription_record::Reader<'_>>()
            .context("failed to read capnp subscription record root")?,
    )
}

pub fn write_retained_message(
    mut builder: retained_message::Builder<'_>,
    value: &CoreRetainedMessage,
) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_topic(&value.topic);
    builder.set_payload(&value.payload);
    builder.set_qos(value.qos);
}

pub fn read_retained_message(
    reader: retained_message::Reader<'_>,
) -> anyhow::Result<CoreRetainedMessage> {
    Ok(CoreRetainedMessage {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read retained message tenant id")?
            .to_string()
            .context("failed to decode retained message tenant id as utf-8")?,
        topic: reader
            .get_topic()
            .context("failed to read retained message topic")?
            .to_string()
            .context("failed to decode retained message topic as utf-8")?,
        payload: reader
            .get_payload()
            .context("failed to read retained message payload")?
            .to_vec()
            .into(),
        qos: reader.get_qos(),
    })
}

pub fn encode_retained_message(value: &CoreRetainedMessage) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_retained_message(message.init_root::<retained_message::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_retained_message(bytes: &[u8]) -> anyhow::Result<CoreRetainedMessage> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp retained message")?;
    read_retained_message(
        message
            .get_root::<retained_message::Reader<'_>>()
            .context("failed to read capnp retained message root")?,
    )
}

pub fn write_retain_write_request(
    mut builder: retain_write_request::Builder<'_>,
    value: &RetainWriteRequestEnvelope,
) {
    write_retained_message(builder.reborrow().init_message(), &value.message);
}

pub fn read_retain_write_request(
    reader: retain_write_request::Reader<'_>,
) -> anyhow::Result<RetainWriteRequestEnvelope> {
    Ok(RetainWriteRequestEnvelope {
        message: read_retained_message(
            reader
                .get_message()
                .context("failed to read retain write request message")?,
        )?,
    })
}

pub fn encode_retain_write_request(value: &RetainWriteRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_retain_write_request(
        message.init_root::<retain_write_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_retain_write_request(bytes: &[u8]) -> anyhow::Result<RetainWriteRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp retain write request")?;
    read_retain_write_request(
        message
            .get_root::<retain_write_request::Reader<'_>>()
            .context("failed to read capnp retain write request root")?,
    )
}

pub fn write_retain_match_request(
    mut builder: retain_match_request::Builder<'_>,
    value: &RetainMatchRequestEnvelope,
) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_topic_filter(&value.topic_filter);
}

pub fn read_retain_match_request(
    reader: retain_match_request::Reader<'_>,
) -> anyhow::Result<RetainMatchRequestEnvelope> {
    Ok(RetainMatchRequestEnvelope {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read retain match tenant id")?
            .to_string()
            .context("failed to decode retain match tenant id as utf-8")?,
        topic_filter: reader
            .get_topic_filter()
            .context("failed to read retain match topic filter")?
            .to_string()
            .context("failed to decode retain match topic filter as utf-8")?,
    })
}

pub fn encode_retain_match_request(value: &RetainMatchRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_retain_match_request(
        message.init_root::<retain_match_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_retain_match_request(bytes: &[u8]) -> anyhow::Result<RetainMatchRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp retain match request")?;
    read_retain_match_request(
        message
            .get_root::<retain_match_request::Reader<'_>>()
            .context("failed to read capnp retain match request root")?,
    )
}

pub fn write_retain_match_reply(
    mut builder: retain_match_reply::Builder<'_>,
    value: &RetainMatchReplyEnvelope,
) {
    let mut messages = builder
        .reborrow()
        .init_messages(value.messages.len() as u32);
    for (index, message) in value.messages.iter().enumerate() {
        write_retained_message(messages.reborrow().get(index as u32), message);
    }
}

pub fn read_retain_match_reply(
    reader: retain_match_reply::Reader<'_>,
) -> anyhow::Result<RetainMatchReplyEnvelope> {
    Ok(RetainMatchReplyEnvelope {
        messages: reader
            .get_messages()
            .context("failed to read retain match reply messages")?
            .iter()
            .map(read_retained_message)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_retain_match_reply(value: &RetainMatchReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_retain_match_reply(
        message.init_root::<retain_match_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_retain_match_reply(bytes: &[u8]) -> anyhow::Result<RetainMatchReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp retain match reply")?;
    read_retain_match_reply(
        message
            .get_root::<retain_match_reply::Reader<'_>>()
            .context("failed to read capnp retain match reply root")?,
    )
}

pub fn write_user_property(mut builder: user_property::Builder<'_>, value: &CoreUserProperty) {
    builder.set_key(&value.key);
    builder.set_value(&value.value);
}

pub fn read_user_property(reader: user_property::Reader<'_>) -> anyhow::Result<CoreUserProperty> {
    Ok(CoreUserProperty {
        key: reader
            .get_key()
            .context("failed to read user property key")?
            .to_string()
            .context("failed to decode user property key as utf-8")?,
        value: reader
            .get_value()
            .context("failed to read user property value")?
            .to_string()
            .context("failed to decode user property value as utf-8")?,
    })
}

pub fn encode_user_property(value: &CoreUserProperty) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_user_property(message.init_root::<user_property::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_user_property(bytes: &[u8]) -> anyhow::Result<CoreUserProperty> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp user property")?;
    read_user_property(
        message
            .get_root::<user_property::Reader<'_>>()
            .context("failed to read capnp user property root")?,
    )
}

pub fn write_publish_properties(
    mut builder: publish_properties::Builder<'_>,
    value: &CorePublishProperties,
) {
    builder.set_payload_format_indicator(value.payload_format_indicator.unwrap_or_default());
    builder.set_has_payload_format_indicator(value.payload_format_indicator.is_some());
    builder.set_content_type(value.content_type.as_deref().unwrap_or_default());
    builder.set_has_content_type(value.content_type.is_some());
    builder
        .set_message_expiry_interval_secs(value.message_expiry_interval_secs.unwrap_or_default());
    builder.set_has_message_expiry_interval_secs(value.message_expiry_interval_secs.is_some());
    builder.set_stored_at_ms(value.stored_at_ms.unwrap_or_default());
    builder.set_has_stored_at_ms(value.stored_at_ms.is_some());
    builder.set_response_topic(value.response_topic.as_deref().unwrap_or_default());
    builder.set_has_response_topic(value.response_topic.is_some());
    builder.set_correlation_data(value.correlation_data.as_deref().unwrap_or_default());
    builder.set_has_correlation_data(value.correlation_data.is_some());

    let subscription_identifiers: Vec<u32> = value.subscription_identifiers.clone();
    builder
        .set_subscription_identifiers(&subscription_identifiers[..])
        .expect("subscription identifiers should encode");

    let mut user_properties = builder
        .reborrow()
        .init_user_properties(value.user_properties.len() as u32);
    for (index, property) in value.user_properties.iter().enumerate() {
        write_user_property(user_properties.reborrow().get(index as u32), property);
    }
}

pub fn read_publish_properties(
    reader: publish_properties::Reader<'_>,
) -> anyhow::Result<CorePublishProperties> {
    let subscription_identifiers = reader
        .get_subscription_identifiers()
        .context("failed to read publish properties subscription identifiers")?
        .iter()
        .collect();
    let user_properties = reader
        .get_user_properties()
        .context("failed to read publish properties user properties")?
        .iter()
        .map(read_user_property)
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(CorePublishProperties {
        payload_format_indicator: reader
            .get_has_payload_format_indicator()
            .then_some(reader.get_payload_format_indicator()),
        content_type: reader.get_has_content_type().then(|| {
            reader
                .get_content_type()
                .expect("content type read should not fail after has_content_type")
                .to_string()
                .expect("content type should decode as utf-8")
        }),
        message_expiry_interval_secs: reader
            .get_has_message_expiry_interval_secs()
            .then_some(reader.get_message_expiry_interval_secs()),
        stored_at_ms: reader
            .get_has_stored_at_ms()
            .then_some(reader.get_stored_at_ms()),
        response_topic: reader.get_has_response_topic().then(|| {
            reader
                .get_response_topic()
                .expect("response topic read should not fail after has_response_topic")
                .to_string()
                .expect("response topic should decode as utf-8")
        }),
        correlation_data: reader.get_has_correlation_data().then_some(
            reader
                .get_correlation_data()
                .expect("correlation data")
                .to_vec(),
        ),
        subscription_identifiers,
        user_properties,
    })
}

pub fn encode_publish_properties(value: &CorePublishProperties) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_publish_properties(
        message.init_root::<publish_properties::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_publish_properties(bytes: &[u8]) -> anyhow::Result<CorePublishProperties> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp publish properties")?;
    read_publish_properties(
        message
            .get_root::<publish_properties::Reader<'_>>()
            .context("failed to read capnp publish properties root")?,
    )
}

pub fn write_offline_message(
    mut builder: offline_message::Builder<'_>,
    value: &CoreOfflineMessage,
) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_session_id(&value.session_id);
    builder.set_topic(&value.topic);
    builder.set_payload(&value.payload);
    builder.set_qos(value.qos);
    builder.set_retain(value.retain);
    builder.set_from_session_id(&value.from_session_id);
    write_publish_properties(builder.reborrow().init_properties(), &value.properties);
}

pub fn read_offline_message(
    reader: offline_message::Reader<'_>,
) -> anyhow::Result<CoreOfflineMessage> {
    Ok(CoreOfflineMessage {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read offline message tenant id")?
            .to_string()
            .context("failed to decode offline message tenant id as utf-8")?,
        session_id: reader
            .get_session_id()
            .context("failed to read offline message session id")?
            .to_string()
            .context("failed to decode offline message session id as utf-8")?,
        topic: reader
            .get_topic()
            .context("failed to read offline message topic")?
            .to_string()
            .context("failed to decode offline message topic as utf-8")?,
        payload: reader
            .get_payload()
            .context("failed to read offline message payload")?
            .to_vec()
            .into(),
        qos: reader.get_qos(),
        retain: reader.get_retain(),
        from_session_id: reader
            .get_from_session_id()
            .context("failed to read offline from_session_id")?
            .to_string()
            .context("failed to decode offline from_session_id as utf-8")?,
        properties: read_publish_properties(
            reader
                .get_properties()
                .context("failed to read offline message properties")?,
        )?,
    })
}

pub fn encode_offline_message(value: &CoreOfflineMessage) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_offline_message(message.init_root::<offline_message::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_offline_message(bytes: &[u8]) -> anyhow::Result<CoreOfflineMessage> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp offline message")?;
    read_offline_message(
        message
            .get_root::<offline_message::Reader<'_>>()
            .context("failed to read capnp offline message root")?,
    )
}

pub fn write_inflight_message(
    mut builder: inflight_message::Builder<'_>,
    value: &CoreInflightMessage,
) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_session_id(&value.session_id);
    builder.set_packet_id(value.packet_id);
    builder.set_topic(&value.topic);
    builder.set_payload(&value.payload);
    builder.set_qos(value.qos);
    builder.set_retain(value.retain);
    builder.set_from_session_id(&value.from_session_id);
    write_publish_properties(builder.reborrow().init_properties(), &value.properties);
    builder.set_phase(value.phase.clone().into());
}

pub fn read_inflight_message(
    reader: inflight_message::Reader<'_>,
) -> anyhow::Result<CoreInflightMessage> {
    Ok(CoreInflightMessage {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read inflight message tenant id")?
            .to_string()
            .context("failed to decode inflight message tenant id as utf-8")?,
        session_id: reader
            .get_session_id()
            .context("failed to read inflight message session id")?
            .to_string()
            .context("failed to decode inflight message session id as utf-8")?,
        packet_id: reader.get_packet_id(),
        topic: reader
            .get_topic()
            .context("failed to read inflight message topic")?
            .to_string()
            .context("failed to decode inflight message topic as utf-8")?,
        payload: reader
            .get_payload()
            .context("failed to read inflight message payload")?
            .to_vec()
            .into(),
        qos: reader.get_qos(),
        retain: reader.get_retain(),
        from_session_id: reader
            .get_from_session_id()
            .context("failed to read inflight from_session_id")?
            .to_string()
            .context("failed to decode inflight from_session_id as utf-8")?,
        properties: read_publish_properties(
            reader
                .get_properties()
                .context("failed to read inflight message properties")?,
        )?,
        phase: CoreInflightPhase::try_from(
            reader
                .get_phase()
                .context("failed to read inflight message phase")?,
        )?,
    })
}

pub fn encode_inflight_message(value: &CoreInflightMessage) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_inflight_message(message.init_root::<inflight_message::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_inflight_message(bytes: &[u8]) -> anyhow::Result<CoreInflightMessage> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp inflight message")?;
    read_inflight_message(
        message
            .get_root::<inflight_message::Reader<'_>>()
            .context("failed to read capnp inflight message root")?,
    )
}

pub fn write_delivery(mut builder: delivery::Builder<'_>, value: &CoreDelivery) {
    builder.set_tenant_id(&value.tenant_id);
    builder.set_session_id(&value.session_id);
    builder.set_topic(&value.topic);
    builder.set_payload(&value.payload);
    builder.set_qos(value.qos);
    builder.set_retain(value.retain);
    builder.set_from_session_id(&value.from_session_id);
    write_publish_properties(builder.reborrow().init_properties(), &value.properties);
}

pub fn read_delivery(reader: delivery::Reader<'_>) -> anyhow::Result<CoreDelivery> {
    Ok(CoreDelivery {
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read delivery tenant id")?
            .to_string()
            .context("failed to decode delivery tenant id as utf-8")?,
        session_id: reader
            .get_session_id()
            .context("failed to read delivery session id")?
            .to_string()
            .context("failed to decode delivery session id as utf-8")?,
        topic: reader
            .get_topic()
            .context("failed to read delivery topic")?
            .to_string()
            .context("failed to decode delivery topic as utf-8")?,
        payload: reader
            .get_payload()
            .context("failed to read delivery payload")?
            .to_vec()
            .into(),
        qos: reader.get_qos(),
        retain: reader.get_retain(),
        from_session_id: reader
            .get_from_session_id()
            .context("failed to read delivery from_session_id")?
            .to_string()
            .context("failed to decode delivery from_session_id as utf-8")?,
        properties: read_publish_properties(
            reader
                .get_properties()
                .context("failed to read delivery properties")?,
        )?,
    })
}

pub fn encode_delivery(value: &CoreDelivery) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_delivery(message.init_root::<delivery::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_delivery(bytes: &[u8]) -> anyhow::Result<CoreDelivery> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp delivery")?;
    read_delivery(
        message
            .get_root::<delivery::Reader<'_>>()
            .context("failed to read capnp delivery root")?,
    )
}

pub fn write_connect_request(
    mut builder: connect_request::Builder<'_>,
    value: &CoreConnectRequest,
) {
    write_client_identity(builder.reborrow().init_identity(), &value.identity);
    builder.set_node_id(value.node_id);
    builder.set_kind(value.kind.clone().into());
    builder.set_clean_start(value.clean_start);
    builder
        .set_session_expiry_interval_secs(value.session_expiry_interval_secs.unwrap_or_default());
    builder.set_has_session_expiry_interval_secs(value.session_expiry_interval_secs.is_some());
}

pub fn read_connect_request(
    reader: connect_request::Reader<'_>,
) -> anyhow::Result<CoreConnectRequest> {
    Ok(CoreConnectRequest {
        identity: read_client_identity(
            reader
                .get_identity()
                .context("failed to read connect request identity")?,
        )?,
        node_id: reader.get_node_id(),
        kind: CoreSessionKind::try_from(
            reader
                .get_kind()
                .context("failed to read connect request session kind")?,
        )?,
        clean_start: reader.get_clean_start(),
        session_expiry_interval_secs: reader
            .get_has_session_expiry_interval_secs()
            .then_some(reader.get_session_expiry_interval_secs()),
    })
}

pub fn encode_connect_request(value: &CoreConnectRequest) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_connect_request(message.init_root::<connect_request::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_connect_request(bytes: &[u8]) -> anyhow::Result<CoreConnectRequest> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp connect request")?;
    read_connect_request(
        message
            .get_root::<connect_request::Reader<'_>>()
            .context("failed to read capnp connect request root")?,
    )
}

pub fn write_publish_request(
    mut builder: publish_request::Builder<'_>,
    value: &CorePublishRequest,
) {
    builder.set_topic(&value.topic);
    builder.set_payload(&value.payload);
    builder.set_qos(value.qos);
    builder.set_retain(value.retain);
    write_publish_properties(builder.reborrow().init_properties(), &value.properties);
}

pub fn read_publish_request(
    reader: publish_request::Reader<'_>,
) -> anyhow::Result<CorePublishRequest> {
    Ok(CorePublishRequest {
        topic: reader
            .get_topic()
            .context("failed to read publish request topic")?
            .to_string()
            .context("failed to decode publish request topic as utf-8")?,
        payload: reader
            .get_payload()
            .context("failed to read publish request payload")?
            .to_vec()
            .into(),
        qos: reader.get_qos(),
        retain: reader.get_retain(),
        properties: read_publish_properties(
            reader
                .get_properties()
                .context("failed to read publish request properties")?,
        )?,
    })
}

pub fn encode_publish_request(value: &CorePublishRequest) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_publish_request(message.init_root::<publish_request::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_publish_request(bytes: &[u8]) -> anyhow::Result<CorePublishRequest> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp publish request")?;
    read_publish_request(
        message
            .get_root::<publish_request::Reader<'_>>()
            .context("failed to read capnp publish request root")?,
    )
}

pub fn write_connect_reply(mut builder: connect_reply::Builder<'_>, value: &CoreConnectReply) {
    write_session_record(builder.reborrow().init_session(), &value.session);
    builder.set_session_present(value.session_present);
    builder.set_local_session_epoch(value.local_session_epoch);
    if let Some(replaced) = &value.replaced {
        write_session_record(builder.reborrow().init_replaced(), replaced);
    }
    builder.set_has_replaced(value.replaced.is_some());

    let mut offline_messages = builder
        .reborrow()
        .init_offline_messages(value.offline_messages.len() as u32);
    for (index, message) in value.offline_messages.iter().enumerate() {
        write_offline_message(offline_messages.reborrow().get(index as u32), message);
    }

    let mut inflight_messages = builder
        .reborrow()
        .init_inflight_messages(value.inflight_messages.len() as u32);
    for (index, message) in value.inflight_messages.iter().enumerate() {
        write_inflight_message(inflight_messages.reborrow().get(index as u32), message);
    }
}

pub fn read_connect_reply(reader: connect_reply::Reader<'_>) -> anyhow::Result<CoreConnectReply> {
    let offline_messages = reader
        .get_offline_messages()
        .context("failed to read connect reply offline messages")?
        .iter()
        .map(read_offline_message)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let inflight_messages = reader
        .get_inflight_messages()
        .context("failed to read connect reply inflight messages")?
        .iter()
        .map(read_inflight_message)
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(CoreConnectReply {
        session: read_session_record(
            reader
                .get_session()
                .context("failed to read connect reply session")?,
        )?,
        session_present: reader.get_session_present(),
        local_session_epoch: reader.get_local_session_epoch(),
        replaced: if reader.get_has_replaced() {
            Some(read_session_record(
                reader
                    .get_replaced()
                    .context("failed to read connect reply replaced session")?,
            )?)
        } else {
            None
        },
        offline_messages,
        inflight_messages,
    })
}

pub fn encode_connect_reply(value: &CoreConnectReply) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_connect_reply(message.init_root::<connect_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_connect_reply(bytes: &[u8]) -> anyhow::Result<CoreConnectReply> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp connect reply")?;
    read_connect_reply(
        message
            .get_root::<connect_reply::Reader<'_>>()
            .context("failed to read capnp connect reply root")?,
    )
}

pub fn write_service_shard_key(
    mut builder: greenmqtt_rpc_capnp::service_shard_key::Builder<'_>,
    value: &CoreServiceShardKey,
) {
    builder.set_kind(value.kind.clone().into());
    builder.set_tenant_id(&value.tenant_id);
    builder.set_scope(&value.scope);
}

pub fn read_service_shard_key(
    reader: greenmqtt_rpc_capnp::service_shard_key::Reader<'_>,
) -> anyhow::Result<CoreServiceShardKey> {
    Ok(CoreServiceShardKey {
        kind: CoreServiceShardKind::try_from(
            reader
                .get_kind()
                .context("failed to read service shard kind")?,
        )?,
        tenant_id: reader
            .get_tenant_id()
            .context("failed to read service shard tenant id")?
            .to_string()
            .context("failed to decode service shard tenant id as utf-8")?,
        scope: reader
            .get_scope()
            .context("failed to read service shard scope")?
            .to_string()
            .context("failed to decode service shard scope as utf-8")?,
    })
}

pub fn write_range_boundary(
    mut builder: greenmqtt_rpc_capnp::range_boundary::Builder<'_>,
    value: &CoreRangeBoundary,
) {
    builder.set_start_key(value.start_key.as_deref().unwrap_or_default());
    builder.set_has_start_key(value.start_key.is_some());
    builder.set_end_key(value.end_key.as_deref().unwrap_or_default());
    builder.set_has_end_key(value.end_key.is_some());
}

pub fn read_range_boundary(
    reader: greenmqtt_rpc_capnp::range_boundary::Reader<'_>,
) -> anyhow::Result<CoreRangeBoundary> {
    Ok(CoreRangeBoundary {
        start_key: reader.get_has_start_key().then_some(
            reader
                .get_start_key()
                .context("failed to read range start key")?
                .to_vec(),
        ),
        end_key: reader.get_has_end_key().then_some(
            reader
                .get_end_key()
                .context("failed to read range end key")?
                .to_vec(),
        ),
    })
}

pub fn write_range_replica(
    mut builder: greenmqtt_rpc_capnp::range_replica::Builder<'_>,
    value: &CoreRangeReplica,
) {
    builder.set_node_id(value.node_id);
    builder.set_role(value.role.into());
    builder.set_sync_state(value.sync_state.into());
}

pub fn read_range_replica(
    reader: greenmqtt_rpc_capnp::range_replica::Reader<'_>,
) -> anyhow::Result<CoreRangeReplica> {
    Ok(CoreRangeReplica {
        node_id: reader.get_node_id(),
        role: CoreReplicaRole::try_from(
            reader
                .get_role()
                .context("failed to read range replica role")?,
        )?,
        sync_state: CoreReplicaSyncState::try_from(
            reader
                .get_sync_state()
                .context("failed to read range replica sync state")?,
        )?,
    })
}

pub fn write_replicated_range_descriptor(
    mut builder: greenmqtt_rpc_capnp::replicated_range_descriptor::Builder<'_>,
    value: &CoreReplicatedRangeDescriptor,
) {
    builder.set_id(&value.id);
    write_service_shard_key(builder.reborrow().init_shard(), &value.shard);
    write_range_boundary(builder.reborrow().init_boundary(), &value.boundary);
    builder.set_epoch(value.epoch);
    builder.set_config_version(value.config_version);
    builder.set_leader_node_id(value.leader_node_id.unwrap_or_default());
    builder.set_has_leader_node_id(value.leader_node_id.is_some());
    let mut replicas = builder
        .reborrow()
        .init_replicas(value.replicas.len() as u32);
    for (index, replica) in value.replicas.iter().enumerate() {
        write_range_replica(replicas.reborrow().get(index as u32), replica);
    }
    builder.set_commit_index(value.commit_index);
    builder.set_applied_index(value.applied_index);
    builder.set_lifecycle(value.lifecycle.clone().into());
}

pub fn read_replicated_range_descriptor(
    reader: greenmqtt_rpc_capnp::replicated_range_descriptor::Reader<'_>,
) -> anyhow::Result<CoreReplicatedRangeDescriptor> {
    let replicas = reader
        .get_replicas()
        .context("failed to read replicated range replicas")?
        .iter()
        .map(read_range_replica)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(CoreReplicatedRangeDescriptor {
        id: reader
            .get_id()
            .context("failed to read replicated range id")?
            .to_string()
            .context("failed to decode replicated range id as utf-8")?,
        shard: read_service_shard_key(
            reader
                .get_shard()
                .context("failed to read replicated range shard")?,
        )?,
        boundary: read_range_boundary(
            reader
                .get_boundary()
                .context("failed to read replicated range boundary")?,
        )?,
        epoch: reader.get_epoch(),
        config_version: reader.get_config_version(),
        leader_node_id: reader
            .get_has_leader_node_id()
            .then_some(reader.get_leader_node_id()),
        replicas,
        commit_index: reader.get_commit_index(),
        applied_index: reader.get_applied_index(),
        lifecycle: CoreServiceShardLifecycle::try_from(
            reader
                .get_lifecycle()
                .context("failed to read replicated range lifecycle")?,
        )?,
    })
}

pub fn encode_replicated_range_descriptor(
    value: &CoreReplicatedRangeDescriptor,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_replicated_range_descriptor(
        message.init_root::<greenmqtt_rpc_capnp::replicated_range_descriptor::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_replicated_range_descriptor(
    bytes: &[u8],
) -> anyhow::Result<CoreReplicatedRangeDescriptor> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp replicated range descriptor")?;
    read_replicated_range_descriptor(
        message
            .get_root::<greenmqtt_rpc_capnp::replicated_range_descriptor::Reader<'_>>()
            .context("failed to read capnp replicated range descriptor root")?,
    )
}

pub fn write_service_endpoint(
    mut builder: service_endpoint::Builder<'_>,
    value: &CoreServiceEndpoint,
) {
    builder.set_kind(value.kind.into());
    builder.set_node_id(value.node_id);
    builder.set_endpoint(&value.endpoint);
}

pub fn read_service_endpoint(
    reader: service_endpoint::Reader<'_>,
) -> anyhow::Result<CoreServiceEndpoint> {
    Ok(CoreServiceEndpoint {
        kind: CoreNodeServiceKind::try_from(
            reader
                .get_kind()
                .context("failed to read service endpoint kind")?,
        )?,
        node_id: reader.get_node_id(),
        endpoint: reader
            .get_endpoint()
            .context("failed to read service endpoint endpoint")?
            .to_string()
            .context("failed to decode service endpoint endpoint as utf-8")?,
    })
}

pub fn encode_service_endpoint(value: &CoreServiceEndpoint) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_service_endpoint(message.init_root::<service_endpoint::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_service_endpoint(bytes: &[u8]) -> anyhow::Result<CoreServiceEndpoint> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp service endpoint")?;
    read_service_endpoint(
        message
            .get_root::<service_endpoint::Reader<'_>>()
            .context("failed to read capnp service endpoint root")?,
    )
}

pub fn write_cluster_node_membership(
    mut builder: cluster_node_membership::Builder<'_>,
    value: &CoreClusterNodeMembership,
) {
    builder.set_node_id(value.node_id);
    builder.set_epoch(value.epoch);
    builder.set_lifecycle(value.lifecycle.clone().into());
    let mut endpoints = builder
        .reborrow()
        .init_endpoints(value.endpoints.len() as u32);
    for (index, endpoint) in value.endpoints.iter().enumerate() {
        write_service_endpoint(endpoints.reborrow().get(index as u32), endpoint);
    }
}

pub fn read_cluster_node_membership(
    reader: cluster_node_membership::Reader<'_>,
) -> anyhow::Result<CoreClusterNodeMembership> {
    let endpoints = reader
        .get_endpoints()
        .context("failed to read cluster node membership endpoints")?
        .iter()
        .map(read_service_endpoint)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(CoreClusterNodeMembership {
        node_id: reader.get_node_id(),
        epoch: reader.get_epoch(),
        lifecycle: CoreClusterNodeLifecycle::try_from(
            reader
                .get_lifecycle()
                .context("failed to read cluster node lifecycle")?,
        )?,
        endpoints,
    })
}

pub fn encode_cluster_node_membership(
    value: &CoreClusterNodeMembership,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_cluster_node_membership(
        message.init_root::<cluster_node_membership::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_cluster_node_membership(bytes: &[u8]) -> anyhow::Result<CoreClusterNodeMembership> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp cluster node membership")?;
    read_cluster_node_membership(
        message
            .get_root::<cluster_node_membership::Reader<'_>>()
            .context("failed to read capnp cluster node membership root")?,
    )
}

pub fn write_balancer_state(mut builder: balancer_state::Builder<'_>, value: &CoreBalancerState) {
    builder.set_disabled(value.disabled);
    let mut load_rules = builder
        .reborrow()
        .init_load_rules(value.load_rules.len() as u32);
    for (index, (key, rule)) in value.load_rules.iter().enumerate() {
        write_user_property(
            load_rules.reborrow().get(index as u32),
            &CoreUserProperty {
                key: key.clone(),
                value: rule.clone(),
            },
        );
    }
}

pub fn read_balancer_state(
    reader: balancer_state::Reader<'_>,
) -> anyhow::Result<CoreBalancerState> {
    let load_rules = reader
        .get_load_rules()
        .context("failed to read balancer state load rules")?
        .iter()
        .map(read_user_property)
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .map(|property| (property.key, property.value))
        .collect();
    Ok(CoreBalancerState {
        disabled: reader.get_disabled(),
        load_rules,
    })
}

pub fn encode_balancer_state(value: &CoreBalancerState) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_balancer_state(message.init_root::<balancer_state::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_balancer_state(bytes: &[u8]) -> anyhow::Result<CoreBalancerState> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp balancer state")?;
    read_balancer_state(
        message
            .get_root::<balancer_state::Reader<'_>>()
            .context("failed to read capnp balancer state root")?,
    )
}

pub fn write_member_lookup_request(
    mut builder: member_lookup_request::Builder<'_>,
    value: &MemberLookupRequestEnvelope,
) {
    builder.set_node_id(value.node_id);
}

pub fn read_member_lookup_request(
    reader: member_lookup_request::Reader<'_>,
) -> anyhow::Result<MemberLookupRequestEnvelope> {
    Ok(MemberLookupRequestEnvelope {
        node_id: reader.get_node_id(),
    })
}

pub fn encode_member_lookup_request(
    value: &MemberLookupRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_member_lookup_request(
        message.init_root::<member_lookup_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_member_lookup_request(bytes: &[u8]) -> anyhow::Result<MemberLookupRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp member lookup request")?;
    read_member_lookup_request(
        message
            .get_root::<member_lookup_request::Reader<'_>>()
            .context("failed to read capnp member lookup request root")?,
    )
}

pub fn write_member_record_reply(
    mut builder: member_record_reply::Builder<'_>,
    value: &MemberRecordReplyEnvelope,
) {
    if let Some(member) = &value.member {
        write_cluster_node_membership(builder.reborrow().init_member(), member);
    }
    builder.set_has_member(value.member.is_some());
}

pub fn read_member_record_reply(
    reader: member_record_reply::Reader<'_>,
) -> anyhow::Result<MemberRecordReplyEnvelope> {
    Ok(MemberRecordReplyEnvelope {
        member: if reader.get_has_member() {
            Some(read_cluster_node_membership(
                reader
                    .get_member()
                    .context("failed to read member record reply member")?,
            )?)
        } else {
            None
        },
    })
}

pub fn encode_member_record_reply(value: &MemberRecordReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_member_record_reply(
        message.init_root::<member_record_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_member_record_reply(bytes: &[u8]) -> anyhow::Result<MemberRecordReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp member record reply")?;
    read_member_record_reply(
        message
            .get_root::<member_record_reply::Reader<'_>>()
            .context("failed to read capnp member record reply root")?,
    )
}

pub fn write_member_list_reply(
    mut builder: member_list_reply::Builder<'_>,
    value: &MemberListReplyEnvelope,
) {
    let mut members = builder.reborrow().init_members(value.members.len() as u32);
    for (index, member) in value.members.iter().enumerate() {
        write_cluster_node_membership(members.reborrow().get(index as u32), member);
    }
}

pub fn read_member_list_reply(
    reader: member_list_reply::Reader<'_>,
) -> anyhow::Result<MemberListReplyEnvelope> {
    Ok(MemberListReplyEnvelope {
        members: reader
            .get_members()
            .context("failed to read member list reply members")?
            .iter()
            .map(read_cluster_node_membership)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_member_list_reply(value: &MemberListReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_member_list_reply(message.init_root::<member_list_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_member_list_reply(bytes: &[u8]) -> anyhow::Result<MemberListReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp member list reply")?;
    read_member_list_reply(
        message
            .get_root::<member_list_reply::Reader<'_>>()
            .context("failed to read capnp member list reply root")?,
    )
}

pub fn write_balancer_state_upsert_request(
    mut builder: balancer_state_upsert_request::Builder<'_>,
    value: &BalancerStateUpsertRequestEnvelope,
) {
    builder.set_name(&value.name);
    write_balancer_state(builder.reborrow().init_state(), &value.state);
}

pub fn read_balancer_state_upsert_request(
    reader: balancer_state_upsert_request::Reader<'_>,
) -> anyhow::Result<BalancerStateUpsertRequestEnvelope> {
    Ok(BalancerStateUpsertRequestEnvelope {
        name: reader
            .get_name()
            .context("failed to read balancer state upsert request name")?
            .to_string()
            .context("failed to decode balancer state upsert request name as utf-8")?,
        state: read_balancer_state(
            reader
                .get_state()
                .context("failed to read balancer state upsert request state")?,
        )?,
    })
}

pub fn encode_balancer_state_upsert_request(
    value: &BalancerStateUpsertRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_balancer_state_upsert_request(
        message.init_root::<balancer_state_upsert_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_balancer_state_upsert_request(
    bytes: &[u8],
) -> anyhow::Result<BalancerStateUpsertRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp balancer state upsert request")?;
    read_balancer_state_upsert_request(
        message
            .get_root::<balancer_state_upsert_request::Reader<'_>>()
            .context("failed to read capnp balancer state upsert request root")?,
    )
}

pub fn write_balancer_state_upsert_reply(
    mut builder: balancer_state_upsert_reply::Builder<'_>,
    value: &BalancerStateUpsertReplyEnvelope,
) {
    if let Some(previous) = &value.previous {
        write_balancer_state(builder.reborrow().init_previous(), previous);
    }
    builder.set_has_previous(value.previous.is_some());
}

pub fn read_balancer_state_upsert_reply(
    reader: balancer_state_upsert_reply::Reader<'_>,
) -> anyhow::Result<BalancerStateUpsertReplyEnvelope> {
    Ok(BalancerStateUpsertReplyEnvelope {
        previous: if reader.get_has_previous() {
            Some(read_balancer_state(reader.get_previous().context(
                "failed to read balancer state upsert reply previous",
            )?)?)
        } else {
            None
        },
    })
}

pub fn encode_balancer_state_upsert_reply(
    value: &BalancerStateUpsertReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_balancer_state_upsert_reply(
        message.init_root::<balancer_state_upsert_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_balancer_state_upsert_reply(
    bytes: &[u8],
) -> anyhow::Result<BalancerStateUpsertReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp balancer state upsert reply")?;
    read_balancer_state_upsert_reply(
        message
            .get_root::<balancer_state_upsert_reply::Reader<'_>>()
            .context("failed to read capnp balancer state upsert reply root")?,
    )
}

pub fn write_balancer_state_request(
    mut builder: balancer_state_request::Builder<'_>,
    value: &BalancerStateRequestEnvelope,
) {
    builder.set_name(&value.name);
}

pub fn read_balancer_state_request(
    reader: balancer_state_request::Reader<'_>,
) -> anyhow::Result<BalancerStateRequestEnvelope> {
    Ok(BalancerStateRequestEnvelope {
        name: reader
            .get_name()
            .context("failed to read balancer state request name")?
            .to_string()
            .context("failed to decode balancer state request name as utf-8")?,
    })
}

pub fn encode_balancer_state_request(
    value: &BalancerStateRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_balancer_state_request(
        message.init_root::<balancer_state_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_balancer_state_request(bytes: &[u8]) -> anyhow::Result<BalancerStateRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp balancer state request")?;
    read_balancer_state_request(
        message
            .get_root::<balancer_state_request::Reader<'_>>()
            .context("failed to read capnp balancer state request root")?,
    )
}

pub fn write_balancer_state_reply(
    mut builder: balancer_state_reply::Builder<'_>,
    value: &BalancerStateReplyEnvelope,
) {
    if let Some(state) = &value.state {
        write_balancer_state(builder.reborrow().init_state(), state);
    }
    builder.set_has_state(value.state.is_some());
}

pub fn read_balancer_state_reply(
    reader: balancer_state_reply::Reader<'_>,
) -> anyhow::Result<BalancerStateReplyEnvelope> {
    Ok(BalancerStateReplyEnvelope {
        state: if reader.get_has_state() {
            Some(read_balancer_state(
                reader
                    .get_state()
                    .context("failed to read balancer state reply state")?,
            )?)
        } else {
            None
        },
    })
}

pub fn encode_balancer_state_reply(value: &BalancerStateReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_balancer_state_reply(
        message.init_root::<balancer_state_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_balancer_state_reply(bytes: &[u8]) -> anyhow::Result<BalancerStateReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp balancer state reply")?;
    read_balancer_state_reply(
        message
            .get_root::<balancer_state_reply::Reader<'_>>()
            .context("failed to read capnp balancer state reply root")?,
    )
}

pub fn write_named_balancer_state(
    mut builder: named_balancer_state::Builder<'_>,
    value: &NamedBalancerStateEnvelope,
) {
    builder.set_name(&value.name);
    if let Some(state) = &value.state {
        write_balancer_state(builder.reborrow().init_state(), state);
    }
    builder.set_has_state(value.state.is_some());
}

pub fn read_named_balancer_state(
    reader: named_balancer_state::Reader<'_>,
) -> anyhow::Result<NamedBalancerStateEnvelope> {
    Ok(NamedBalancerStateEnvelope {
        name: reader
            .get_name()
            .context("failed to read named balancer state name")?
            .to_string()
            .context("failed to decode named balancer state name as utf-8")?,
        state: if reader.get_has_state() {
            Some(read_balancer_state(
                reader
                    .get_state()
                    .context("failed to read named balancer state")?,
            )?)
        } else {
            None
        },
    })
}

pub fn write_balancer_state_list_reply(
    mut builder: balancer_state_list_reply::Builder<'_>,
    value: &BalancerStateListReplyEnvelope,
) {
    let mut entries = builder.reborrow().init_entries(value.entries.len() as u32);
    for (index, entry) in value.entries.iter().enumerate() {
        write_named_balancer_state(entries.reborrow().get(index as u32), entry);
    }
}

pub fn read_balancer_state_list_reply(
    reader: balancer_state_list_reply::Reader<'_>,
) -> anyhow::Result<BalancerStateListReplyEnvelope> {
    Ok(BalancerStateListReplyEnvelope {
        entries: reader
            .get_entries()
            .context("failed to read balancer state list entries")?
            .iter()
            .map(read_named_balancer_state)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_balancer_state_list_reply(
    value: &BalancerStateListReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_balancer_state_list_reply(
        message.init_root::<balancer_state_list_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_balancer_state_list_reply(
    bytes: &[u8],
) -> anyhow::Result<BalancerStateListReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp balancer state list reply")?;
    read_balancer_state_list_reply(
        message
            .get_root::<balancer_state_list_reply::Reader<'_>>()
            .context("failed to read capnp balancer state list reply root")?,
    )
}

pub fn write_range_lookup_request(
    mut builder: range_lookup_request::Builder<'_>,
    value: &RangeLookupRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
}

pub fn read_range_lookup_request(
    reader: range_lookup_request::Reader<'_>,
) -> anyhow::Result<RangeLookupRequestEnvelope> {
    Ok(RangeLookupRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range lookup request range id")?
            .to_string()
            .context("failed to decode range lookup request range id as utf-8")?,
    })
}

pub fn encode_range_lookup_request(value: &RangeLookupRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_lookup_request(
        message.init_root::<range_lookup_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_lookup_request(bytes: &[u8]) -> anyhow::Result<RangeLookupRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range lookup request")?;
    read_range_lookup_request(
        message
            .get_root::<range_lookup_request::Reader<'_>>()
            .context("failed to read capnp range lookup request root")?,
    )
}

pub fn write_range_record_reply(
    mut builder: range_record_reply::Builder<'_>,
    value: &RangeRecordReplyEnvelope,
) {
    if let Some(descriptor) = &value.descriptor {
        write_replicated_range_descriptor(builder.reborrow().init_descriptor(), descriptor);
    }
    builder.set_has_descriptor(value.descriptor.is_some());
}

pub fn read_range_record_reply(
    reader: range_record_reply::Reader<'_>,
) -> anyhow::Result<RangeRecordReplyEnvelope> {
    Ok(RangeRecordReplyEnvelope {
        descriptor: if reader.get_has_descriptor() {
            Some(read_replicated_range_descriptor(
                reader
                    .get_descriptor()
                    .context("failed to read range record reply descriptor")?,
            )?)
        } else {
            None
        },
    })
}

pub fn encode_range_record_reply(value: &RangeRecordReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_record_reply(
        message.init_root::<range_record_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_record_reply(bytes: &[u8]) -> anyhow::Result<RangeRecordReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range record reply")?;
    read_range_record_reply(
        message
            .get_root::<range_record_reply::Reader<'_>>()
            .context("failed to read capnp range record reply root")?,
    )
}

pub fn write_range_list_request(
    mut builder: range_list_request::Builder<'_>,
    value: &RangeListRequestEnvelope,
) {
    if let Some(kind) = &value.shard_kind {
        builder.set_shard_kind(kind.clone().into());
    }
    builder.set_has_shard_kind(value.shard_kind.is_some());
    builder.set_tenant_id(value.tenant_id.as_deref().unwrap_or_default());
    builder.set_has_tenant_id(value.tenant_id.is_some());
    builder.set_scope(value.scope.as_deref().unwrap_or_default());
    builder.set_has_scope(value.scope.is_some());
}

pub fn read_range_list_request(
    reader: range_list_request::Reader<'_>,
) -> anyhow::Result<RangeListRequestEnvelope> {
    Ok(RangeListRequestEnvelope {
        shard_kind: if reader.get_has_shard_kind() {
            Some(CoreServiceShardKind::try_from(
                reader
                    .get_shard_kind()
                    .context("failed to read range list request shard kind")?,
            )?)
        } else {
            None
        },
        tenant_id: reader.get_has_tenant_id().then(|| {
            reader
                .get_tenant_id()
                .expect("tenant id read should not fail after has_tenant_id")
                .to_string()
                .expect("tenant id should decode as utf-8")
        }),
        scope: reader.get_has_scope().then(|| {
            reader
                .get_scope()
                .expect("scope read should not fail after has_scope")
                .to_string()
                .expect("scope should decode as utf-8")
        }),
    })
}

pub fn encode_range_list_request(value: &RangeListRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_list_request(
        message.init_root::<range_list_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_list_request(bytes: &[u8]) -> anyhow::Result<RangeListRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range list request")?;
    read_range_list_request(
        message
            .get_root::<range_list_request::Reader<'_>>()
            .context("failed to read capnp range list request root")?,
    )
}

pub fn write_range_list_reply(
    mut builder: range_list_reply::Builder<'_>,
    value: &RangeListReplyEnvelope,
) {
    let mut descriptors = builder
        .reborrow()
        .init_descriptors(value.descriptors.len() as u32);
    for (index, descriptor) in value.descriptors.iter().enumerate() {
        write_replicated_range_descriptor(descriptors.reborrow().get(index as u32), descriptor);
    }
}

pub fn read_range_list_reply(
    reader: range_list_reply::Reader<'_>,
) -> anyhow::Result<RangeListReplyEnvelope> {
    Ok(RangeListReplyEnvelope {
        descriptors: reader
            .get_descriptors()
            .context("failed to read range list reply descriptors")?
            .iter()
            .map(read_replicated_range_descriptor)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_range_list_reply(value: &RangeListReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_list_reply(message.init_root::<range_list_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_list_reply(bytes: &[u8]) -> anyhow::Result<RangeListReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range list reply")?;
    read_range_list_reply(
        message
            .get_root::<range_list_reply::Reader<'_>>()
            .context("failed to read capnp range list reply root")?,
    )
}

pub fn write_route_range_request(
    mut builder: route_range_request::Builder<'_>,
    value: &RouteRangeRequestEnvelope,
) {
    write_service_shard_key(builder.reborrow().init_shard(), &value.shard);
    builder.set_key(&value.key);
}

pub fn read_route_range_request(
    reader: route_range_request::Reader<'_>,
) -> anyhow::Result<RouteRangeRequestEnvelope> {
    Ok(RouteRangeRequestEnvelope {
        shard: read_service_shard_key(
            reader
                .get_shard()
                .context("failed to read route range request shard")?,
        )?,
        key: reader
            .get_key()
            .context("failed to read route range request key")?
            .to_vec(),
    })
}

pub fn encode_route_range_request(value: &RouteRangeRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_route_range_request(
        message.init_root::<route_range_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_route_range_request(bytes: &[u8]) -> anyhow::Result<RouteRangeRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp route range request")?;
    read_route_range_request(
        message
            .get_root::<route_range_request::Reader<'_>>()
            .context("failed to read capnp route range request root")?,
    )
}

pub fn write_replica_lag(mut builder: replica_lag::Builder<'_>, value: &ReplicaLagEnvelope) {
    builder.set_node_id(value.node_id);
    builder.set_lag(value.lag);
    builder.set_match_index(value.match_index);
    builder.set_next_index(value.next_index);
}

pub fn read_replica_lag(reader: replica_lag::Reader<'_>) -> anyhow::Result<ReplicaLagEnvelope> {
    Ok(ReplicaLagEnvelope {
        node_id: reader.get_node_id(),
        lag: reader.get_lag(),
        match_index: reader.get_match_index(),
        next_index: reader.get_next_index(),
    })
}

pub fn write_range_reconfiguration(
    mut builder: range_reconfiguration::Builder<'_>,
    value: &CoreRangeReconfigurationState,
) {
    builder
        .set_old_voters(&value.old_voters[..])
        .expect("old_voters should encode");
    builder
        .set_old_learners(&value.old_learners[..])
        .expect("old_learners should encode");
    builder
        .set_current_voters(&value.current_voters[..])
        .expect("current_voters should encode");
    builder
        .set_current_learners(&value.current_learners[..])
        .expect("current_learners should encode");
    builder
        .set_joint_voters(&value.joint_voters[..])
        .expect("joint_voters should encode");
    builder
        .set_joint_learners(&value.joint_learners[..])
        .expect("joint_learners should encode");
    builder
        .set_pending_voters(&value.pending_voters[..])
        .expect("pending_voters should encode");
    builder
        .set_pending_learners(&value.pending_learners[..])
        .expect("pending_learners should encode");
    if let Some(phase) = &value.phase {
        builder.set_phase((*phase).clone().into());
    }
    builder.set_has_phase(value.phase.is_some());
    builder.set_blocked_on_catch_up(value.blocked_on_catch_up);
}

pub fn read_range_reconfiguration(
    reader: range_reconfiguration::Reader<'_>,
    range_id: impl Into<String>,
) -> anyhow::Result<CoreRangeReconfigurationState> {
    Ok(CoreRangeReconfigurationState {
        range_id: range_id.into(),
        old_voters: reader
            .get_old_voters()
            .context("failed to read reconfiguration old_voters")?
            .iter()
            .collect(),
        old_learners: reader
            .get_old_learners()
            .context("failed to read reconfiguration old_learners")?
            .iter()
            .collect(),
        current_voters: reader
            .get_current_voters()
            .context("failed to read reconfiguration current_voters")?
            .iter()
            .collect(),
        current_learners: reader
            .get_current_learners()
            .context("failed to read reconfiguration current_learners")?
            .iter()
            .collect(),
        joint_voters: reader
            .get_joint_voters()
            .context("failed to read reconfiguration joint_voters")?
            .iter()
            .collect(),
        joint_learners: reader
            .get_joint_learners()
            .context("failed to read reconfiguration joint_learners")?
            .iter()
            .collect(),
        pending_voters: reader
            .get_pending_voters()
            .context("failed to read reconfiguration pending_voters")?
            .iter()
            .collect(),
        pending_learners: reader
            .get_pending_learners()
            .context("failed to read reconfiguration pending_learners")?
            .iter()
            .collect(),
        phase: if reader.get_has_phase() {
            Some(CoreReconfigurationPhase::try_from(
                reader
                    .get_phase()
                    .context("failed to read reconfiguration phase")?,
            )?)
        } else {
            None
        },
        blocked_on_catch_up: reader.get_blocked_on_catch_up(),
    })
}

pub fn write_range_health(mut builder: range_health::Builder<'_>, value: &RangeHealthEnvelope) {
    builder.set_range_id(&value.range_id);
    builder.set_lifecycle(value.lifecycle.clone().into());
    builder.set_role(value.role.into());
    builder.set_current_term(value.current_term);
    builder.set_leader_node_id(value.leader_node_id.unwrap_or_default());
    builder.set_has_leader_node_id(value.leader_node_id.is_some());
    builder.set_commit_index(value.commit_index);
    builder.set_applied_index(value.applied_index);
    builder.set_latest_snapshot_index(value.latest_snapshot_index.unwrap_or_default());
    builder.set_has_latest_snapshot_index(value.latest_snapshot_index.is_some());
    let mut replica_lag = builder
        .reborrow()
        .init_replica_lag(value.replica_lag.len() as u32);
    for (index, lag) in value.replica_lag.iter().enumerate() {
        write_replica_lag(replica_lag.reborrow().get(index as u32), lag);
    }
    write_range_reconfiguration(
        builder.reborrow().init_reconfiguration(),
        &value.reconfiguration,
    );
}

pub fn read_range_health(reader: range_health::Reader<'_>) -> anyhow::Result<RangeHealthEnvelope> {
    let range_id = reader
        .get_range_id()
        .context("failed to read range health range id")?
        .to_string()
        .context("failed to decode range health range id as utf-8")?;
    Ok(RangeHealthEnvelope {
        range_id: range_id.clone(),
        lifecycle: CoreServiceShardLifecycle::try_from(
            reader
                .get_lifecycle()
                .context("failed to read range health lifecycle")?,
        )?,
        role: RaftRoleEnvelope::try_from(
            reader
                .get_role()
                .context("failed to read range health role")?,
        )?,
        current_term: reader.get_current_term(),
        leader_node_id: reader
            .get_has_leader_node_id()
            .then_some(reader.get_leader_node_id()),
        commit_index: reader.get_commit_index(),
        applied_index: reader.get_applied_index(),
        latest_snapshot_index: reader
            .get_has_latest_snapshot_index()
            .then_some(reader.get_latest_snapshot_index()),
        replica_lag: reader
            .get_replica_lag()
            .context("failed to read range health replica_lag")?
            .iter()
            .map(read_replica_lag)
            .collect::<anyhow::Result<Vec<_>>>()?,
        reconfiguration: read_range_reconfiguration(
            reader
                .get_reconfiguration()
                .context("failed to read range health reconfiguration")?,
            range_id,
        )?,
    })
}

pub fn encode_range_health(value: &RangeHealthEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_health(message.init_root::<range_health::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_health(bytes: &[u8]) -> anyhow::Result<RangeHealthEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range health")?;
    read_range_health(
        message
            .get_root::<range_health::Reader<'_>>()
            .context("failed to read capnp range health root")?,
    )
}

pub fn write_range_health_request(
    mut builder: range_health_request::Builder<'_>,
    value: &RangeHealthRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
}

pub fn read_range_health_request(
    reader: range_health_request::Reader<'_>,
) -> anyhow::Result<RangeHealthRequestEnvelope> {
    Ok(RangeHealthRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range health request range id")?
            .to_string()
            .context("failed to decode range health request range id as utf-8")?,
    })
}

pub fn encode_range_health_request(value: &RangeHealthRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_health_request(
        message.init_root::<range_health_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_health_request(bytes: &[u8]) -> anyhow::Result<RangeHealthRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range health request")?;
    read_range_health_request(
        message
            .get_root::<range_health_request::Reader<'_>>()
            .context("failed to read capnp range health request root")?,
    )
}

pub fn write_range_health_reply(
    mut builder: range_health_reply::Builder<'_>,
    value: &RangeHealthReplyEnvelope,
) {
    if let Some(health) = &value.health {
        write_range_health(builder.reborrow().init_health(), health);
    }
    builder.set_has_health(value.health.is_some());
}

pub fn read_range_health_reply(
    reader: range_health_reply::Reader<'_>,
) -> anyhow::Result<RangeHealthReplyEnvelope> {
    Ok(RangeHealthReplyEnvelope {
        health: if reader.get_has_health() {
            Some(read_range_health(
                reader
                    .get_health()
                    .context("failed to read range health reply health")?,
            )?)
        } else {
            None
        },
    })
}

pub fn encode_range_health_reply(value: &RangeHealthReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_health_reply(
        message.init_root::<range_health_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_health_reply(bytes: &[u8]) -> anyhow::Result<RangeHealthReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range health reply")?;
    read_range_health_reply(
        message
            .get_root::<range_health_reply::Reader<'_>>()
            .context("failed to read capnp range health reply root")?,
    )
}

pub fn write_range_health_list_reply(
    mut builder: range_health_list_reply::Builder<'_>,
    value: &RangeHealthListReplyEnvelope,
) {
    let mut entries = builder.reborrow().init_entries(value.entries.len() as u32);
    for (index, entry) in value.entries.iter().enumerate() {
        write_range_health(entries.reborrow().get(index as u32), entry);
    }
}

pub fn read_range_health_list_reply(
    reader: range_health_list_reply::Reader<'_>,
) -> anyhow::Result<RangeHealthListReplyEnvelope> {
    Ok(RangeHealthListReplyEnvelope {
        entries: reader
            .get_entries()
            .context("failed to read range health list entries")?
            .iter()
            .map(read_range_health)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_range_health_list_reply(
    value: &RangeHealthListReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_health_list_reply(
        message.init_root::<range_health_list_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_health_list_reply(
    bytes: &[u8],
) -> anyhow::Result<RangeHealthListReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range health list reply")?;
    read_range_health_list_reply(
        message
            .get_root::<range_health_list_reply::Reader<'_>>()
            .context("failed to read capnp range health list reply root")?,
    )
}

pub fn write_range_debug_reply(
    mut builder: range_debug_reply::Builder<'_>,
    value: &RangeDebugReplyEnvelope,
) {
    builder.set_text(&value.text);
}

pub fn read_range_debug_reply(
    reader: range_debug_reply::Reader<'_>,
) -> anyhow::Result<RangeDebugReplyEnvelope> {
    Ok(RangeDebugReplyEnvelope {
        text: reader
            .get_text()
            .context("failed to read range debug text")?
            .to_string()
            .context("failed to decode range debug text as utf-8")?,
    })
}

pub fn encode_range_debug_reply(value: &RangeDebugReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_debug_reply(message.init_root::<range_debug_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_debug_reply(bytes: &[u8]) -> anyhow::Result<RangeDebugReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range debug reply")?;
    read_range_debug_reply(
        message
            .get_root::<range_debug_reply::Reader<'_>>()
            .context("failed to read capnp range debug reply root")?,
    )
}

pub fn write_zombie_range(mut builder: zombie_range::Builder<'_>, value: &ZombieRangeEnvelope) {
    builder.set_range_id(&value.range_id);
    builder.set_lifecycle(value.lifecycle.clone().into());
    builder.set_leader_node_id(value.leader_node_id.unwrap_or_default());
    builder.set_has_leader_node_id(value.leader_node_id.is_some());
}

pub fn read_zombie_range(reader: zombie_range::Reader<'_>) -> anyhow::Result<ZombieRangeEnvelope> {
    Ok(ZombieRangeEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read zombie range id")?
            .to_string()
            .context("failed to decode zombie range id as utf-8")?,
        lifecycle: CoreServiceShardLifecycle::try_from(
            reader
                .get_lifecycle()
                .context("failed to read zombie range lifecycle")?,
        )?,
        leader_node_id: reader
            .get_has_leader_node_id()
            .then_some(reader.get_leader_node_id()),
    })
}

pub fn write_zombie_range_list_reply(
    mut builder: zombie_range_list_reply::Builder<'_>,
    value: &ZombieRangeListReplyEnvelope,
) {
    let mut entries = builder.reborrow().init_entries(value.entries.len() as u32);
    for (index, entry) in value.entries.iter().enumerate() {
        write_zombie_range(entries.reborrow().get(index as u32), entry);
    }
}

pub fn read_zombie_range_list_reply(
    reader: zombie_range_list_reply::Reader<'_>,
) -> anyhow::Result<ZombieRangeListReplyEnvelope> {
    Ok(ZombieRangeListReplyEnvelope {
        entries: reader
            .get_entries()
            .context("failed to read zombie range list entries")?
            .iter()
            .map(read_zombie_range)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

pub fn encode_zombie_range_list_reply(
    value: &ZombieRangeListReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_zombie_range_list_reply(
        message.init_root::<zombie_range_list_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_zombie_range_list_reply(
    bytes: &[u8],
) -> anyhow::Result<ZombieRangeListReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp zombie range list reply")?;
    read_zombie_range_list_reply(
        message
            .get_root::<zombie_range_list_reply::Reader<'_>>()
            .context("failed to read capnp zombie range list reply root")?,
    )
}

pub fn write_range_bootstrap_request(
    mut builder: range_bootstrap_request::Builder<'_>,
    value: &RangeBootstrapRequestEnvelope,
) {
    write_replicated_range_descriptor(builder.reborrow().init_descriptor(), &value.descriptor);
}

pub fn read_range_bootstrap_request(
    reader: range_bootstrap_request::Reader<'_>,
) -> anyhow::Result<RangeBootstrapRequestEnvelope> {
    Ok(RangeBootstrapRequestEnvelope {
        descriptor: read_replicated_range_descriptor(
            reader
                .get_descriptor()
                .context("failed to read range bootstrap descriptor")?,
        )?,
    })
}

pub fn encode_range_bootstrap_request(
    value: &RangeBootstrapRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_bootstrap_request(
        message.init_root::<range_bootstrap_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_bootstrap_request(
    bytes: &[u8],
) -> anyhow::Result<RangeBootstrapRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range bootstrap request")?;
    read_range_bootstrap_request(
        message
            .get_root::<range_bootstrap_request::Reader<'_>>()
            .context("failed to read capnp range bootstrap request root")?,
    )
}

pub fn write_range_bootstrap_reply(
    mut builder: range_bootstrap_reply::Builder<'_>,
    value: &RangeBootstrapReplyEnvelope,
) {
    builder.set_range_id(&value.range_id);
}

pub fn read_range_bootstrap_reply(
    reader: range_bootstrap_reply::Reader<'_>,
) -> anyhow::Result<RangeBootstrapReplyEnvelope> {
    Ok(RangeBootstrapReplyEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range bootstrap reply range id")?
            .to_string()
            .context("failed to decode range bootstrap reply range id as utf-8")?,
    })
}

pub fn encode_range_bootstrap_reply(
    value: &RangeBootstrapReplyEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_bootstrap_reply(
        message.init_root::<range_bootstrap_reply::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_bootstrap_reply(bytes: &[u8]) -> anyhow::Result<RangeBootstrapReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range bootstrap reply")?;
    read_range_bootstrap_reply(
        message
            .get_root::<range_bootstrap_reply::Reader<'_>>()
            .context("failed to read capnp range bootstrap reply root")?,
    )
}

pub fn write_range_change_replicas_request(
    mut builder: range_change_replicas_request::Builder<'_>,
    value: &RangeChangeReplicasRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder
        .set_voters(&value.voters[..])
        .expect("voters should encode");
    builder
        .set_learners(&value.learners[..])
        .expect("learners should encode");
}

pub fn read_range_change_replicas_request(
    reader: range_change_replicas_request::Reader<'_>,
) -> anyhow::Result<RangeChangeReplicasRequestEnvelope> {
    Ok(RangeChangeReplicasRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range change replicas range id")?
            .to_string()
            .context("failed to decode range change replicas range id as utf-8")?,
        voters: reader
            .get_voters()
            .context("failed to read range change replicas voters")?
            .iter()
            .collect(),
        learners: reader
            .get_learners()
            .context("failed to read range change replicas learners")?
            .iter()
            .collect(),
    })
}

pub fn encode_range_change_replicas_request(
    value: &RangeChangeReplicasRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_change_replicas_request(
        message.init_root::<range_change_replicas_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_change_replicas_request(
    bytes: &[u8],
) -> anyhow::Result<RangeChangeReplicasRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range change replicas request")?;
    read_range_change_replicas_request(
        message
            .get_root::<range_change_replicas_request::Reader<'_>>()
            .context("failed to read capnp range change replicas request root")?,
    )
}

pub fn write_range_transfer_leadership_request(
    mut builder: range_transfer_leadership_request::Builder<'_>,
    value: &RangeTransferLeadershipRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_target_node_id(value.target_node_id);
}

pub fn read_range_transfer_leadership_request(
    reader: range_transfer_leadership_request::Reader<'_>,
) -> anyhow::Result<RangeTransferLeadershipRequestEnvelope> {
    Ok(RangeTransferLeadershipRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range transfer leadership range id")?
            .to_string()
            .context("failed to decode range transfer leadership range id as utf-8")?,
        target_node_id: reader.get_target_node_id(),
    })
}

pub fn encode_range_transfer_leadership_request(
    value: &RangeTransferLeadershipRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_transfer_leadership_request(
        message.init_root::<range_transfer_leadership_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_transfer_leadership_request(
    bytes: &[u8],
) -> anyhow::Result<RangeTransferLeadershipRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range transfer leadership request")?;
    read_range_transfer_leadership_request(
        message
            .get_root::<range_transfer_leadership_request::Reader<'_>>()
            .context("failed to read capnp range transfer leadership request root")?,
    )
}

pub fn write_range_recover_request(
    mut builder: range_recover_request::Builder<'_>,
    value: &RangeRecoverRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_new_leader_node_id(value.new_leader_node_id);
}

pub fn read_range_recover_request(
    reader: range_recover_request::Reader<'_>,
) -> anyhow::Result<RangeRecoverRequestEnvelope> {
    Ok(RangeRecoverRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range recover request range id")?
            .to_string()
            .context("failed to decode range recover request range id as utf-8")?,
        new_leader_node_id: reader.get_new_leader_node_id(),
    })
}

pub fn encode_range_recover_request(
    value: &RangeRecoverRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_recover_request(
        message.init_root::<range_recover_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_recover_request(bytes: &[u8]) -> anyhow::Result<RangeRecoverRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range recover request")?;
    read_range_recover_request(
        message
            .get_root::<range_recover_request::Reader<'_>>()
            .context("failed to read capnp range recover request root")?,
    )
}

pub fn write_range_split_request(
    mut builder: range_split_request::Builder<'_>,
    value: &RangeSplitRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
    builder.set_split_key(&value.split_key);
}

pub fn read_range_split_request(
    reader: range_split_request::Reader<'_>,
) -> anyhow::Result<RangeSplitRequestEnvelope> {
    Ok(RangeSplitRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range split request range id")?
            .to_string()
            .context("failed to decode range split request range id as utf-8")?,
        split_key: reader
            .get_split_key()
            .context("failed to read range split request split key")?
            .to_vec(),
    })
}

pub fn encode_range_split_request(value: &RangeSplitRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_split_request(
        message.init_root::<range_split_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_split_request(bytes: &[u8]) -> anyhow::Result<RangeSplitRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range split request")?;
    read_range_split_request(
        message
            .get_root::<range_split_request::Reader<'_>>()
            .context("failed to read capnp range split request root")?,
    )
}

pub fn write_range_split_reply(
    mut builder: range_split_reply::Builder<'_>,
    value: &RangeSplitReplyEnvelope,
) {
    builder.set_left_range_id(&value.left_range_id);
    builder.set_right_range_id(&value.right_range_id);
}

pub fn read_range_split_reply(
    reader: range_split_reply::Reader<'_>,
) -> anyhow::Result<RangeSplitReplyEnvelope> {
    Ok(RangeSplitReplyEnvelope {
        left_range_id: reader
            .get_left_range_id()
            .context("failed to read range split reply left_range_id")?
            .to_string()
            .context("failed to decode left_range_id as utf-8")?,
        right_range_id: reader
            .get_right_range_id()
            .context("failed to read range split reply right_range_id")?
            .to_string()
            .context("failed to decode right_range_id as utf-8")?,
    })
}

pub fn encode_range_split_reply(value: &RangeSplitReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_split_reply(message.init_root::<range_split_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_split_reply(bytes: &[u8]) -> anyhow::Result<RangeSplitReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range split reply")?;
    read_range_split_reply(
        message
            .get_root::<range_split_reply::Reader<'_>>()
            .context("failed to read capnp range split reply root")?,
    )
}

pub fn write_range_merge_request(
    mut builder: range_merge_request::Builder<'_>,
    value: &RangeMergeRequestEnvelope,
) {
    builder.set_left_range_id(&value.left_range_id);
    builder.set_right_range_id(&value.right_range_id);
}

pub fn read_range_merge_request(
    reader: range_merge_request::Reader<'_>,
) -> anyhow::Result<RangeMergeRequestEnvelope> {
    Ok(RangeMergeRequestEnvelope {
        left_range_id: reader
            .get_left_range_id()
            .context("failed to read range merge request left_range_id")?
            .to_string()
            .context("failed to decode left_range_id as utf-8")?,
        right_range_id: reader
            .get_right_range_id()
            .context("failed to read range merge request right_range_id")?
            .to_string()
            .context("failed to decode right_range_id as utf-8")?,
    })
}

pub fn encode_range_merge_request(value: &RangeMergeRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_merge_request(
        message.init_root::<range_merge_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_merge_request(bytes: &[u8]) -> anyhow::Result<RangeMergeRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range merge request")?;
    read_range_merge_request(
        message
            .get_root::<range_merge_request::Reader<'_>>()
            .context("failed to read capnp range merge request root")?,
    )
}

pub fn write_range_merge_reply(
    mut builder: range_merge_reply::Builder<'_>,
    value: &RangeMergeReplyEnvelope,
) {
    builder.set_range_id(&value.range_id);
}

pub fn read_range_merge_reply(
    reader: range_merge_reply::Reader<'_>,
) -> anyhow::Result<RangeMergeReplyEnvelope> {
    Ok(RangeMergeReplyEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range merge reply range id")?
            .to_string()
            .context("failed to decode range merge reply range id as utf-8")?,
    })
}

pub fn encode_range_merge_reply(value: &RangeMergeReplyEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_merge_reply(message.init_root::<range_merge_reply::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_merge_reply(bytes: &[u8]) -> anyhow::Result<RangeMergeReplyEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range merge reply")?;
    read_range_merge_reply(
        message
            .get_root::<range_merge_reply::Reader<'_>>()
            .context("failed to read capnp range merge reply root")?,
    )
}

pub fn write_range_drain_request(
    mut builder: range_drain_request::Builder<'_>,
    value: &RangeDrainRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
}

pub fn read_range_drain_request(
    reader: range_drain_request::Reader<'_>,
) -> anyhow::Result<RangeDrainRequestEnvelope> {
    Ok(RangeDrainRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range drain request range id")?
            .to_string()
            .context("failed to decode range drain request range id as utf-8")?,
    })
}

pub fn encode_range_drain_request(value: &RangeDrainRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_drain_request(
        message.init_root::<range_drain_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_drain_request(bytes: &[u8]) -> anyhow::Result<RangeDrainRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range drain request")?;
    read_range_drain_request(
        message
            .get_root::<range_drain_request::Reader<'_>>()
            .context("failed to read capnp range drain request root")?,
    )
}

pub fn write_range_retire_request(
    mut builder: range_retire_request::Builder<'_>,
    value: &RangeRetireRequestEnvelope,
) {
    builder.set_range_id(&value.range_id);
}

pub fn read_range_retire_request(
    reader: range_retire_request::Reader<'_>,
) -> anyhow::Result<RangeRetireRequestEnvelope> {
    Ok(RangeRetireRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read range retire request range id")?
            .to_string()
            .context("failed to decode range retire request range id as utf-8")?,
    })
}

pub fn encode_range_retire_request(value: &RangeRetireRequestEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_range_retire_request(
        message.init_root::<range_retire_request::Builder<'_>>(),
        value,
    );
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_range_retire_request(bytes: &[u8]) -> anyhow::Result<RangeRetireRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp range retire request")?;
    read_range_retire_request(
        message
            .get_root::<range_retire_request::Reader<'_>>()
            .context("failed to read capnp range retire request root")?,
    )
}

fn encode_raft_command(entry: &CoreRaftLogEntry) -> anyhow::Result<Vec<u8>> {
    if let Some(config_change) = &entry.config_change {
        let mut encoded = CONFIG_CHANGE_PREFIX.to_vec();
        encoded.extend(
            bincode::serialize(config_change).context("failed to serialize raft config change")?,
        );
        Ok(encoded)
    } else {
        Ok(entry.command.to_vec())
    }
}

fn decode_raft_command(bytes: &[u8]) -> anyhow::Result<(Option<CoreRaftConfigLogEntry>, Bytes)> {
    if bytes.starts_with(CONFIG_CHANGE_PREFIX) {
        let payload = &bytes[CONFIG_CHANGE_PREFIX.len()..];
        let config_change = bincode::deserialize::<CoreRaftConfigLogEntry>(payload)
            .context("failed to deserialize raft config change")?;
        Ok((Some(config_change), Bytes::new()))
    } else {
        Ok((None, Bytes::from(bytes.to_vec())))
    }
}

pub fn write_raft_log_entry(
    mut builder: raft_log_entry::Builder<'_>,
    value: &CoreRaftLogEntry,
) -> anyhow::Result<()> {
    builder.set_term(value.term);
    builder.set_index(value.index);
    builder.set_command(&encode_raft_command(value)?);
    Ok(())
}

pub fn read_raft_log_entry(reader: raft_log_entry::Reader<'_>) -> anyhow::Result<CoreRaftLogEntry> {
    let (config_change, command) = decode_raft_command(
        reader
            .get_command()
            .context("failed to read raft log entry command")?,
    )?;
    Ok(CoreRaftLogEntry {
        term: reader.get_term(),
        index: reader.get_index(),
        config_change,
        command,
    })
}

pub fn encode_raft_log_entry(value: &CoreRaftLogEntry) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_raft_log_entry(message.init_root::<raft_log_entry::Builder<'_>>(), value)?;
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_raft_log_entry(bytes: &[u8]) -> anyhow::Result<CoreRaftLogEntry> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp raft log entry")?;
    read_raft_log_entry(
        message
            .get_root::<raft_log_entry::Reader<'_>>()
            .context("failed to read capnp raft log entry root")?,
    )
}

pub fn write_raft_snapshot(mut builder: raft_snapshot::Builder<'_>, value: &CoreRaftSnapshot) {
    builder.set_range_id(&value.range_id);
    builder.set_term(value.term);
    builder.set_index(value.index);
    builder.set_payload(&value.payload);
}

pub fn read_raft_snapshot(reader: raft_snapshot::Reader<'_>) -> anyhow::Result<CoreRaftSnapshot> {
    Ok(CoreRaftSnapshot {
        range_id: reader
            .get_range_id()
            .context("failed to read raft snapshot range id")?
            .to_string()
            .context("failed to decode raft snapshot range id as utf-8")?,
        term: reader.get_term(),
        index: reader.get_index(),
        payload: reader
            .get_payload()
            .context("failed to read raft snapshot payload")?
            .to_vec()
            .into(),
    })
}

pub fn encode_raft_snapshot(value: &CoreRaftSnapshot) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_raft_snapshot(message.init_root::<raft_snapshot::Builder<'_>>(), value);
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_raft_snapshot(bytes: &[u8]) -> anyhow::Result<CoreRaftSnapshot> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp raft snapshot")?;
    read_raft_snapshot(
        message
            .get_root::<raft_snapshot::Reader<'_>>()
            .context("failed to read capnp raft snapshot root")?,
    )
}

pub fn write_raft_append_entries_request(
    mut builder: raft_append_entries_request::Builder<'_>,
    value: &CoreAppendEntriesRequest,
) -> anyhow::Result<()> {
    builder.set_term(value.term);
    builder.set_leader_id(value.leader_id);
    builder.set_prev_log_index(value.prev_log_index);
    builder.set_prev_log_term(value.prev_log_term);
    let mut entries = builder.reborrow().init_entries(value.entries.len() as u32);
    for (index, entry) in value.entries.iter().enumerate() {
        write_raft_log_entry(entries.reborrow().get(index as u32), entry)?;
    }
    builder.set_leader_commit(value.leader_commit);
    Ok(())
}

pub fn read_raft_append_entries_request(
    reader: raft_append_entries_request::Reader<'_>,
) -> anyhow::Result<CoreAppendEntriesRequest> {
    Ok(CoreAppendEntriesRequest {
        term: reader.get_term(),
        leader_id: reader.get_leader_id(),
        prev_log_index: reader.get_prev_log_index(),
        prev_log_term: reader.get_prev_log_term(),
        entries: reader
            .get_entries()
            .context("failed to read raft append entries list")?
            .iter()
            .map(read_raft_log_entry)
            .collect::<anyhow::Result<Vec<_>>>()?,
        leader_commit: reader.get_leader_commit(),
    })
}

pub fn write_raft_append_entries_response(
    mut builder: raft_append_entries_response::Builder<'_>,
    value: &CoreAppendEntriesResponse,
) {
    builder.set_term(value.term);
    builder.set_success(value.success);
    builder.set_match_index(value.match_index);
}

pub fn read_raft_append_entries_response(
    reader: raft_append_entries_response::Reader<'_>,
) -> anyhow::Result<CoreAppendEntriesResponse> {
    Ok(CoreAppendEntriesResponse {
        term: reader.get_term(),
        success: reader.get_success(),
        match_index: reader.get_match_index(),
    })
}

pub fn write_raft_request_vote_request(
    mut builder: raft_request_vote_request::Builder<'_>,
    value: &CoreRequestVoteRequest,
) {
    builder.set_term(value.term);
    builder.set_candidate_id(value.candidate_id);
    builder.set_last_log_index(value.last_log_index);
    builder.set_last_log_term(value.last_log_term);
}

pub fn read_raft_request_vote_request(
    reader: raft_request_vote_request::Reader<'_>,
) -> anyhow::Result<CoreRequestVoteRequest> {
    Ok(CoreRequestVoteRequest {
        term: reader.get_term(),
        candidate_id: reader.get_candidate_id(),
        last_log_index: reader.get_last_log_index(),
        last_log_term: reader.get_last_log_term(),
    })
}

pub fn write_raft_request_vote_response(
    mut builder: raft_request_vote_response::Builder<'_>,
    value: &CoreRequestVoteResponse,
) {
    builder.set_term(value.term);
    builder.set_vote_granted(value.vote_granted);
}

pub fn read_raft_request_vote_response(
    reader: raft_request_vote_response::Reader<'_>,
) -> anyhow::Result<CoreRequestVoteResponse> {
    Ok(CoreRequestVoteResponse {
        term: reader.get_term(),
        vote_granted: reader.get_vote_granted(),
    })
}

pub fn write_raft_install_snapshot_request(
    mut builder: raft_install_snapshot_request::Builder<'_>,
    value: &CoreInstallSnapshotRequest,
) {
    builder.set_term(value.term);
    builder.set_leader_id(value.leader_id);
    write_raft_snapshot(builder.reborrow().init_snapshot(), &value.snapshot);
}

pub fn read_raft_install_snapshot_request(
    reader: raft_install_snapshot_request::Reader<'_>,
) -> anyhow::Result<CoreInstallSnapshotRequest> {
    Ok(CoreInstallSnapshotRequest {
        term: reader.get_term(),
        leader_id: reader.get_leader_id(),
        snapshot: read_raft_snapshot(
            reader
                .get_snapshot()
                .context("failed to read install snapshot request snapshot")?,
        )?,
    })
}

pub fn write_raft_install_snapshot_response(
    mut builder: raft_install_snapshot_response::Builder<'_>,
    value: &CoreInstallSnapshotResponse,
) {
    builder.set_term(value.term);
    builder.set_accepted(value.accepted);
}

pub fn read_raft_install_snapshot_response(
    reader: raft_install_snapshot_response::Reader<'_>,
) -> anyhow::Result<CoreInstallSnapshotResponse> {
    Ok(CoreInstallSnapshotResponse {
        term: reader.get_term(),
        accepted: reader.get_accepted(),
    })
}

pub fn write_raft_transport_request(
    mut builder: raft_transport_request::Builder<'_>,
    value: &RaftTransportRequestEnvelope,
) -> anyhow::Result<()> {
    builder.set_range_id(&value.range_id);
    builder.set_from_node_id(value.from_node_id);
    match &value.message {
        CoreRaftMessage::AppendEntries(request) => {
            builder.set_kind(CapnpRaftMessageKind::AppendEntries);
            write_raft_append_entries_request(builder.reborrow().init_append_entries(), request)?;
        }
        CoreRaftMessage::AppendEntriesResponse(response) => {
            builder.set_kind(CapnpRaftMessageKind::AppendEntriesResponse);
            write_raft_append_entries_response(
                builder.reborrow().init_append_entries_response(),
                response,
            );
        }
        CoreRaftMessage::RequestVote(request) => {
            builder.set_kind(CapnpRaftMessageKind::RequestVote);
            write_raft_request_vote_request(builder.reborrow().init_request_vote(), request);
        }
        CoreRaftMessage::RequestVoteResponse(response) => {
            builder.set_kind(CapnpRaftMessageKind::RequestVoteResponse);
            write_raft_request_vote_response(
                builder.reborrow().init_request_vote_response(),
                response,
            );
        }
        CoreRaftMessage::InstallSnapshot(request) => {
            builder.set_kind(CapnpRaftMessageKind::InstallSnapshot);
            write_raft_install_snapshot_request(
                builder.reborrow().init_install_snapshot(),
                request,
            );
        }
        CoreRaftMessage::InstallSnapshotResponse(response) => {
            builder.set_kind(CapnpRaftMessageKind::InstallSnapshotResponse);
            write_raft_install_snapshot_response(
                builder.reborrow().init_install_snapshot_response(),
                response,
            );
        }
    }
    Ok(())
}

pub fn read_raft_transport_request(
    reader: raft_transport_request::Reader<'_>,
) -> anyhow::Result<RaftTransportRequestEnvelope> {
    let kind = reader
        .get_kind()
        .context("failed to read raft transport kind")?;
    let message = match kind {
        CapnpRaftMessageKind::AppendEntries => {
            CoreRaftMessage::AppendEntries(read_raft_append_entries_request(
                reader
                    .get_append_entries()
                    .context("failed to read append entries payload")?,
            )?)
        }
        CapnpRaftMessageKind::AppendEntriesResponse => {
            CoreRaftMessage::AppendEntriesResponse(read_raft_append_entries_response(
                reader
                    .get_append_entries_response()
                    .context("failed to read append entries response payload")?,
            )?)
        }
        CapnpRaftMessageKind::RequestVote => {
            CoreRaftMessage::RequestVote(read_raft_request_vote_request(
                reader
                    .get_request_vote()
                    .context("failed to read request vote payload")?,
            )?)
        }
        CapnpRaftMessageKind::RequestVoteResponse => {
            CoreRaftMessage::RequestVoteResponse(read_raft_request_vote_response(
                reader
                    .get_request_vote_response()
                    .context("failed to read request vote response payload")?,
            )?)
        }
        CapnpRaftMessageKind::InstallSnapshot => {
            CoreRaftMessage::InstallSnapshot(read_raft_install_snapshot_request(
                reader
                    .get_install_snapshot()
                    .context("failed to read install snapshot payload")?,
            )?)
        }
        CapnpRaftMessageKind::InstallSnapshotResponse => {
            CoreRaftMessage::InstallSnapshotResponse(read_raft_install_snapshot_response(
                reader
                    .get_install_snapshot_response()
                    .context("failed to read install snapshot response payload")?,
            )?)
        }
    };
    Ok(RaftTransportRequestEnvelope {
        range_id: reader
            .get_range_id()
            .context("failed to read raft transport range id")?
            .to_string()
            .context("failed to decode raft transport range id as utf-8")?,
        from_node_id: reader.get_from_node_id(),
        message,
    })
}

pub fn encode_raft_transport_request(
    value: &RaftTransportRequestEnvelope,
) -> anyhow::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    write_raft_transport_request(
        message.init_root::<raft_transport_request::Builder<'_>>(),
        value,
    )?;
    Ok(capnp::serialize::write_message_to_words(&message))
}

pub fn decode_raft_transport_request(bytes: &[u8]) -> anyhow::Result<RaftTransportRequestEnvelope> {
    let mut slice = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .context("failed to decode capnp raft transport request")?;
    read_raft_transport_request(
        message
            .get_root::<raft_transport_request::Reader<'_>>()
            .context("failed to read capnp raft transport request root")?,
    )
}

impl RpcFrameEnvelope {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut frame = message.init_root::<rpc_frame::Builder<'_>>();
            frame.set_kind(self.kind.into());
            frame.set_payload(&self.payload);

            if let Some(request) = &self.request_header {
                let mut builder = frame.reborrow().init_request_header();
                builder.set_service(request.service.into());
                builder.set_method_id(request.method_id);
                builder.set_protocol_version(request.protocol_version);
                builder.set_request_id(request.request_id);
                builder.set_timeout_ms(request.timeout_ms);
                builder.set_trace_id(&request.trace_id);
                builder.set_expected_epoch(request.expected_epoch);
                builder.set_fencing_token(request.fencing_token);
            }

            if let Some(response) = &self.response_header {
                let mut builder = frame.reborrow().init_response_header();
                builder.set_status(response.status.into());
                builder.set_retryable(response.retryable);
                builder.set_retry_after_ms(response.retry_after_ms);
                builder.set_error_message(&response.error_message);
            }
        }

        Ok(capnp::serialize::write_message_to_words(&message))
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut slice = bytes;
        let message = capnp::serialize::read_message_from_flat_slice(
            &mut slice,
            capnp::message::ReaderOptions::new(),
        )
        .context("failed to decode capnp rpc frame")?;
        let frame = message
            .get_root::<rpc_frame::Reader<'_>>()
            .context("failed to read capnp rpc frame root")?;

        let request_header = if frame.has_request_header() {
            let request = frame
                .get_request_header()
                .context("failed to read rpc request header")?;
            Some(RpcRequestHeaderEnvelope {
                service: RpcServiceKind::try_from(
                    request
                        .get_service()
                        .context("failed to read rpc service kind")?,
                )?,
                method_id: request.get_method_id(),
                protocol_version: request.get_protocol_version(),
                request_id: request.get_request_id(),
                timeout_ms: request.get_timeout_ms(),
                trace_id: request
                    .get_trace_id()
                    .context("failed to read rpc trace id")?
                    .to_string()
                    .context("failed to decode rpc trace id as utf-8")?,
                expected_epoch: request.get_expected_epoch(),
                fencing_token: request.get_fencing_token(),
            })
        } else {
            None
        };

        let response_header = if frame.has_response_header() {
            let response = frame
                .get_response_header()
                .context("failed to read rpc response header")?;
            Some(RpcResponseHeaderEnvelope {
                status: RpcStatusCode::try_from(
                    response
                        .get_status()
                        .context("failed to read rpc status code")?,
                )?,
                retryable: response.get_retryable(),
                retry_after_ms: response.get_retry_after_ms(),
                error_message: response
                    .get_error_message()
                    .context("failed to read rpc error message")?
                    .to_string()
                    .context("failed to decode rpc error message as utf-8")?,
            })
        } else {
            None
        };

        Ok(Self {
            kind: RpcFrameKind::try_from(
                frame.get_kind().context("failed to read rpc frame kind")?,
            )?,
            request_header,
            response_header,
            payload: frame
                .get_payload()
                .context("failed to read rpc payload")?
                .to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_add_route_request, decode_balancer_state, decode_balancer_state_list_reply,
        decode_balancer_state_reply, decode_balancer_state_request,
        decode_balancer_state_upsert_reply, decode_balancer_state_upsert_request,
        decode_client_identity, decode_cluster_node_membership, decode_connect_reply,
        decode_connect_request, decode_delivery, decode_inbox_ack_inflight_request,
        decode_inbox_attach_request, decode_inbox_detach_request,
        decode_inbox_purge_session_request, decode_inflight_message, decode_kv_mutation,
        decode_kv_range_apply_request, decode_kv_range_checkpoint_reply,
        decode_kv_range_checkpoint_request, decode_kv_range_get_reply, decode_kv_range_get_request,
        decode_kv_range_scan_reply, decode_kv_range_scan_request, decode_kv_range_snapshot_reply,
        decode_kv_range_snapshot_request, decode_list_routes_reply, decode_list_routes_request,
        decode_list_session_routes_reply, decode_list_session_routes_request,
        decode_list_sessions_reply, decode_list_sessions_request,
        decode_lookup_session_by_id_request, decode_lookup_session_reply,
        decode_lookup_session_request, decode_match_topic_reply, decode_match_topic_request,
        decode_member_list_reply, decode_member_lookup_request, decode_member_record_reply,
        decode_offline_message, decode_publish_properties, decode_publish_request,
        decode_raft_log_entry, decode_raft_snapshot, decode_raft_transport_request,
        decode_range_bootstrap_reply, decode_range_bootstrap_request,
        decode_range_change_replicas_request, decode_range_debug_reply, decode_range_drain_request,
        decode_range_health_list_reply, decode_range_health_reply, decode_range_health_request,
        decode_range_list_reply, decode_range_list_request, decode_range_lookup_request,
        decode_range_merge_reply, decode_range_merge_request, decode_range_record_reply,
        decode_range_recover_request, decode_range_retire_request, decode_range_split_reply,
        decode_range_split_request, decode_range_transfer_leadership_request,
        decode_register_session_reply, decode_register_session_request,
        decode_remove_route_request, decode_replicated_range_descriptor, decode_retain_match_reply,
        decode_retain_match_request, decode_retain_write_request, decode_retained_message,
        decode_route_range_request, decode_route_record, decode_service_endpoint,
        decode_session_record, decode_subscription_record, decode_unregister_session_request,
        decode_user_property, decode_zombie_range_list_reply, encode_add_route_request,
        encode_balancer_state, encode_balancer_state_list_reply, encode_balancer_state_reply,
        encode_balancer_state_request, encode_balancer_state_upsert_reply,
        encode_balancer_state_upsert_request, encode_client_identity,
        encode_cluster_node_membership, encode_connect_reply, encode_connect_request,
        encode_delivery, encode_inbox_ack_inflight_request, encode_inbox_attach_request,
        encode_inbox_detach_request, encode_inbox_purge_session_request, encode_inflight_message,
        encode_kv_mutation, encode_kv_range_apply_request, encode_kv_range_checkpoint_reply,
        encode_kv_range_checkpoint_request, encode_kv_range_get_reply, encode_kv_range_get_request,
        encode_kv_range_scan_reply, encode_kv_range_scan_request, encode_kv_range_snapshot_reply,
        encode_kv_range_snapshot_request, encode_list_routes_reply, encode_list_routes_request,
        encode_list_session_routes_reply, encode_list_session_routes_request,
        encode_list_sessions_reply, encode_list_sessions_request,
        encode_lookup_session_by_id_request, encode_lookup_session_reply,
        encode_lookup_session_request, encode_match_topic_reply, encode_match_topic_request,
        encode_member_list_reply, encode_member_lookup_request, encode_member_record_reply,
        encode_offline_message, encode_publish_properties, encode_publish_request,
        encode_raft_log_entry, encode_raft_snapshot, encode_raft_transport_request,
        encode_range_bootstrap_reply, encode_range_bootstrap_request,
        encode_range_change_replicas_request, encode_range_debug_reply, encode_range_drain_request,
        encode_range_health_list_reply, encode_range_health_reply, encode_range_health_request,
        encode_range_list_reply, encode_range_list_request, encode_range_lookup_request,
        encode_range_merge_reply, encode_range_merge_request, encode_range_record_reply,
        encode_range_recover_request, encode_range_retire_request, encode_range_split_reply,
        encode_range_split_request, encode_range_transfer_leadership_request,
        encode_register_session_reply, encode_register_session_request,
        encode_remove_route_request, encode_replicated_range_descriptor, encode_retain_match_reply,
        encode_retain_match_request, encode_retain_write_request, encode_retained_message,
        encode_route_range_request, encode_route_record, encode_service_endpoint,
        encode_session_record, encode_subscription_record, encode_unregister_session_request,
        encode_user_property, encode_zombie_range_list_reply, AddRouteRequestEnvelope,
        BalancerStateListReplyEnvelope, BalancerStateReplyEnvelope, BalancerStateRequestEnvelope,
        BalancerStateUpsertReplyEnvelope, BalancerStateUpsertRequestEnvelope,
        InboxAckInflightRequestEnvelope, InboxAttachRequestEnvelope, InboxDetachRequestEnvelope,
        InboxPurgeSessionRequestEnvelope, KvEntryEnvelope, KvMutationEnvelope,
        KvRangeApplyRequestEnvelope, KvRangeCheckpointReplyEnvelope,
        KvRangeCheckpointRequestEnvelope, KvRangeGetReplyEnvelope, KvRangeGetRequestEnvelope,
        KvRangeScanReplyEnvelope, KvRangeScanRequestEnvelope, KvRangeSnapshotReplyEnvelope,
        KvRangeSnapshotRequestEnvelope, ListRoutesReplyEnvelope, ListRoutesRequestEnvelope,
        ListSessionRoutesReplyEnvelope, ListSessionRoutesRequestEnvelope,
        ListSessionsReplyEnvelope, ListSessionsRequestEnvelope, LookupSessionByIdRequestEnvelope,
        LookupSessionReplyEnvelope, LookupSessionRequestEnvelope, MatchTopicReplyEnvelope,
        MatchTopicRequestEnvelope, MemberListReplyEnvelope, MemberLookupRequestEnvelope,
        MemberRecordReplyEnvelope, NamedBalancerStateEnvelope, RaftRoleEnvelope,
        RaftTransportRequestEnvelope, RangeBootstrapReplyEnvelope, RangeBootstrapRequestEnvelope,
        RangeChangeReplicasRequestEnvelope, RangeDebugReplyEnvelope, RangeDrainRequestEnvelope,
        RangeHealthEnvelope, RangeHealthListReplyEnvelope, RangeHealthReplyEnvelope,
        RangeHealthRequestEnvelope, RangeListReplyEnvelope, RangeListRequestEnvelope,
        RangeLookupRequestEnvelope, RangeMergeReplyEnvelope, RangeMergeRequestEnvelope,
        RangeRecordReplyEnvelope, RangeRecoverRequestEnvelope, RangeRetireRequestEnvelope,
        RangeSplitReplyEnvelope, RangeSplitRequestEnvelope, RangeTransferLeadershipRequestEnvelope,
        RegisterSessionReplyEnvelope, RegisterSessionRequestEnvelope, RemoveRouteRequestEnvelope,
        ReplicaLagEnvelope, RetainMatchReplyEnvelope, RetainMatchRequestEnvelope,
        RetainWriteRequestEnvelope, RouteRangeRequestEnvelope, RpcFrameEnvelope, RpcFrameKind,
        RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope, RpcServiceKind, RpcStatusCode,
        RpcTransportKind, UnregisterSessionRequestEnvelope, ZombieRangeEnvelope,
        ZombieRangeListReplyEnvelope,
    };
    use greenmqtt_core::{
        BalancerState, ClientIdentity, ClusterNodeLifecycle, ClusterNodeMembership, ConnectReply,
        ConnectRequest, Delivery, InflightMessage, InflightPhase, OfflineMessage,
        PublishProperties, PublishRequest, RangeBoundary, RangeReconfigurationState, RangeReplica,
        ReconfigurationPhase, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        RetainedMessage, RouteRecord, ServiceEndpoint, ServiceEndpointTransport, ServiceKind,
        ServiceShardKey, ServiceShardKind, ServiceShardLifecycle, SessionKind, SessionRecord,
        Subscription, UserProperty,
    };
    use greenmqtt_kv_raft::{
        AppendEntriesRequest, InstallSnapshotRequest, RaftClusterConfig, RaftConfigLogEntry,
        RaftLogEntry, RaftMessage, RaftSnapshot, RequestVoteResponse,
    };

    #[test]
    fn rpc_transport_kind_maps_to_core_transport_kind() {
        assert_eq!(
            RpcTransportKind::from(ServiceEndpointTransport::GrpcHttp),
            RpcTransportKind::GrpcHttp
        );
        assert_eq!(
            ServiceEndpointTransport::from(RpcTransportKind::GrpcHttps),
            ServiceEndpointTransport::GrpcHttps
        );
        assert_eq!(
            ServiceEndpointTransport::from(RpcTransportKind::Quic),
            ServiceEndpointTransport::Quic
        );
    }

    #[test]
    fn rpc_frame_envelope_round_trip_works() {
        let envelope = RpcFrameEnvelope {
            kind: RpcFrameKind::RequestHeader,
            request_header: Some(RpcRequestHeaderEnvelope {
                service: RpcServiceKind::Metadata,
                method_id: 7,
                protocol_version: 1,
                request_id: 42,
                timeout_ms: 5000,
                trace_id: "trace-1".to_string(),
                expected_epoch: 11,
                fencing_token: 29,
            }),
            response_header: Some(RpcResponseHeaderEnvelope {
                status: RpcStatusCode::Ok,
                retryable: false,
                retry_after_ms: 0,
                error_message: String::new(),
            }),
            payload: b"abc".to_vec(),
        };

        let bytes = envelope.encode().expect("frame should encode");
        let decoded = RpcFrameEnvelope::decode(&bytes).expect("frame should decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn client_identity_round_trip_works() {
        let identity = ClientIdentity {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-a".to_string(),
            client_id: "client-a".to_string(),
        };

        let bytes = encode_client_identity(&identity).expect("identity should encode");
        let decoded = decode_client_identity(&bytes).expect("identity should decode");
        assert_eq!(decoded, identity);
    }

    #[test]
    fn session_record_round_trip_preserves_optional_fields() {
        let record = SessionRecord {
            session_id: "session-a".to_string(),
            node_id: 7,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "tenant-a".to_string(),
                user_id: "user-a".to_string(),
                client_id: "client-a".to_string(),
            },
            session_expiry_interval_secs: Some(3600),
            expires_at_ms: Some(123_456),
        };

        let bytes = encode_session_record(&record).expect("session record should encode");
        let decoded = decode_session_record(&bytes).expect("session record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn lookup_session_request_round_trip_preserves_identity() {
        let request = LookupSessionRequestEnvelope {
            identity: ClientIdentity {
                tenant_id: "tenant-a".to_string(),
                user_id: "user-a".to_string(),
                client_id: "client-a".to_string(),
            },
        };

        let bytes =
            encode_lookup_session_request(&request).expect("lookup session request should encode");
        let decoded =
            decode_lookup_session_request(&bytes).expect("lookup session request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn lookup_session_by_id_request_round_trip_preserves_session_id() {
        let request = LookupSessionByIdRequestEnvelope {
            session_id: "session-a".to_string(),
        };

        let bytes = encode_lookup_session_by_id_request(&request)
            .expect("lookup session by id request should encode");
        let decoded = decode_lookup_session_by_id_request(&bytes)
            .expect("lookup session by id request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn lookup_session_reply_round_trip_preserves_optional_record() {
        let reply = LookupSessionReplyEnvelope {
            record: Some(SessionRecord {
                session_id: "session-a".to_string(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "tenant-a".to_string(),
                    user_id: "user-a".to_string(),
                    client_id: "client-a".to_string(),
                },
                session_expiry_interval_secs: Some(3600),
                expires_at_ms: Some(123_456),
            }),
        };

        let bytes =
            encode_lookup_session_reply(&reply).expect("lookup session reply should encode");
        let decoded =
            decode_lookup_session_reply(&bytes).expect("lookup session reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn register_session_request_round_trip_preserves_record() {
        let request = RegisterSessionRequestEnvelope {
            record: SessionRecord {
                session_id: "session-a".to_string(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "tenant-a".to_string(),
                    user_id: "user-a".to_string(),
                    client_id: "client-a".to_string(),
                },
                session_expiry_interval_secs: Some(3600),
                expires_at_ms: Some(123_456),
            },
        };

        let bytes = encode_register_session_request(&request)
            .expect("register session request should encode");
        let decoded = decode_register_session_request(&bytes)
            .expect("register session request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn register_session_reply_round_trip_preserves_optional_replaced() {
        let reply = RegisterSessionReplyEnvelope {
            replaced: Some(SessionRecord {
                session_id: "session-a".to_string(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "tenant-a".to_string(),
                    user_id: "user-a".to_string(),
                    client_id: "client-a".to_string(),
                },
                session_expiry_interval_secs: Some(3600),
                expires_at_ms: Some(123_456),
            }),
        };

        let bytes =
            encode_register_session_reply(&reply).expect("register session reply should encode");
        let decoded =
            decode_register_session_reply(&bytes).expect("register session reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn unregister_session_request_round_trip_preserves_session_id() {
        let request = UnregisterSessionRequestEnvelope {
            session_id: "session-a".to_string(),
        };

        let bytes = encode_unregister_session_request(&request)
            .expect("unregister session request should encode");
        let decoded = decode_unregister_session_request(&bytes)
            .expect("unregister session request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn list_sessions_request_round_trip_preserves_optional_tenant() {
        let request = ListSessionsRequestEnvelope {
            tenant_id: Some("tenant-a".to_string()),
        };

        let bytes =
            encode_list_sessions_request(&request).expect("list sessions request should encode");
        let decoded =
            decode_list_sessions_request(&bytes).expect("list sessions request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn list_sessions_reply_round_trip_preserves_records() {
        let reply = ListSessionsReplyEnvelope {
            records: vec![
                SessionRecord {
                    session_id: "session-a".to_string(),
                    node_id: 7,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: "tenant-a".to_string(),
                        user_id: "user-a".to_string(),
                        client_id: "client-a".to_string(),
                    },
                    session_expiry_interval_secs: Some(3600),
                    expires_at_ms: Some(123_456),
                },
                SessionRecord {
                    session_id: "session-b".to_string(),
                    node_id: 9,
                    kind: SessionKind::Transient,
                    identity: ClientIdentity {
                        tenant_id: "tenant-a".to_string(),
                        user_id: "user-b".to_string(),
                        client_id: "client-b".to_string(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                },
            ],
        };

        let bytes = encode_list_sessions_reply(&reply).expect("list sessions reply should encode");
        let decoded =
            decode_list_sessions_reply(&bytes).expect("list sessions reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn route_record_round_trip_preserves_optional_fields() {
        let route = RouteRecord {
            tenant_id: "tenant-a".to_string(),
            topic_filter: "devices/+/state".to_string(),
            session_id: "session-a".to_string(),
            node_id: 7,
            subscription_identifier: Some(99),
            no_local: true,
            retain_as_published: false,
            shared_group: Some("group-a".to_string()),
            kind: SessionKind::Persistent,
        };

        let bytes = encode_route_record(&route).expect("route record should encode");
        let decoded = decode_route_record(&bytes).expect("route record should decode");
        assert_eq!(decoded, route);
    }

    #[test]
    fn add_route_request_round_trip_preserves_route() {
        let request = AddRouteRequestEnvelope {
            route: RouteRecord {
                tenant_id: "tenant-a".to_string(),
                topic_filter: "devices/+/state".to_string(),
                session_id: "session-a".to_string(),
                node_id: 7,
                subscription_identifier: Some(99),
                no_local: true,
                retain_as_published: false,
                shared_group: Some("group-a".to_string()),
                kind: SessionKind::Persistent,
            },
        };

        let bytes = encode_add_route_request(&request).expect("add route request should encode");
        let decoded = decode_add_route_request(&bytes).expect("add route request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn remove_route_request_round_trip_preserves_route() {
        let request = RemoveRouteRequestEnvelope {
            route: RouteRecord {
                tenant_id: "tenant-a".to_string(),
                topic_filter: "devices/+/state".to_string(),
                session_id: "session-a".to_string(),
                node_id: 7,
                subscription_identifier: Some(99),
                no_local: true,
                retain_as_published: false,
                shared_group: Some("group-a".to_string()),
                kind: SessionKind::Persistent,
            },
        };

        let bytes =
            encode_remove_route_request(&request).expect("remove route request should encode");
        let decoded =
            decode_remove_route_request(&bytes).expect("remove route request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn list_session_routes_request_round_trip_preserves_session_id() {
        let request = ListSessionRoutesRequestEnvelope {
            session_id: "session-a".to_string(),
        };

        let bytes = encode_list_session_routes_request(&request)
            .expect("list session routes request should encode");
        let decoded = decode_list_session_routes_request(&bytes)
            .expect("list session routes request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn list_session_routes_reply_round_trip_preserves_routes() {
        let reply = ListSessionRoutesReplyEnvelope {
            routes: vec![RouteRecord {
                tenant_id: "tenant-a".to_string(),
                topic_filter: "devices/+/state".to_string(),
                session_id: "session-a".to_string(),
                node_id: 7,
                subscription_identifier: Some(99),
                no_local: true,
                retain_as_published: false,
                shared_group: Some("group-a".to_string()),
                kind: SessionKind::Persistent,
            }],
        };

        let bytes = encode_list_session_routes_reply(&reply)
            .expect("list session routes reply should encode");
        let decoded = decode_list_session_routes_reply(&bytes)
            .expect("list session routes reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn match_topic_request_round_trip_preserves_topic() {
        let request = MatchTopicRequestEnvelope {
            tenant_id: "tenant-a".to_string(),
            topic: "devices/d1/state".to_string(),
        };

        let bytes =
            encode_match_topic_request(&request).expect("match topic request should encode");
        let decoded =
            decode_match_topic_request(&bytes).expect("match topic request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn match_topic_reply_round_trip_preserves_routes() {
        let reply = MatchTopicReplyEnvelope {
            routes: vec![RouteRecord {
                tenant_id: "tenant-a".to_string(),
                topic_filter: "devices/+/state".to_string(),
                session_id: "session-a".to_string(),
                node_id: 7,
                subscription_identifier: Some(99),
                no_local: true,
                retain_as_published: false,
                shared_group: Some("group-a".to_string()),
                kind: SessionKind::Persistent,
            }],
        };

        let bytes = encode_match_topic_reply(&reply).expect("match topic reply should encode");
        let decoded = decode_match_topic_reply(&bytes).expect("match topic reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn list_routes_request_round_trip_preserves_optional_tenant() {
        let request = ListRoutesRequestEnvelope {
            tenant_id: Some("tenant-a".to_string()),
        };

        let bytes =
            encode_list_routes_request(&request).expect("list routes request should encode");
        let decoded =
            decode_list_routes_request(&bytes).expect("list routes request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn list_routes_reply_round_trip_preserves_routes() {
        let reply = ListRoutesReplyEnvelope {
            routes: vec![RouteRecord {
                tenant_id: "tenant-a".to_string(),
                topic_filter: "devices/+/state".to_string(),
                session_id: "session-a".to_string(),
                node_id: 7,
                subscription_identifier: Some(99),
                no_local: true,
                retain_as_published: false,
                shared_group: Some("group-a".to_string()),
                kind: SessionKind::Persistent,
            }],
        };

        let bytes = encode_list_routes_reply(&reply).expect("list routes reply should encode");
        let decoded = decode_list_routes_reply(&bytes).expect("list routes reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn inbox_attach_request_round_trip_preserves_session_id() {
        let request = InboxAttachRequestEnvelope {
            session_id: "session-a".to_string(),
        };

        let bytes = encode_inbox_attach_request(&request).expect("inbox attach should encode");
        let decoded = decode_inbox_attach_request(&bytes).expect("inbox attach should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn inbox_detach_request_round_trip_preserves_session_id() {
        let request = InboxDetachRequestEnvelope {
            session_id: "session-a".to_string(),
        };

        let bytes = encode_inbox_detach_request(&request).expect("inbox detach should encode");
        let decoded = decode_inbox_detach_request(&bytes).expect("inbox detach should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn inbox_purge_session_request_round_trip_preserves_session_id() {
        let request = InboxPurgeSessionRequestEnvelope {
            session_id: "session-a".to_string(),
        };

        let bytes =
            encode_inbox_purge_session_request(&request).expect("inbox purge should encode");
        let decoded =
            decode_inbox_purge_session_request(&bytes).expect("inbox purge should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn inbox_ack_inflight_request_round_trip_preserves_fields() {
        let request = InboxAckInflightRequestEnvelope {
            session_id: "session-a".to_string(),
            packet_id: 7,
        };

        let bytes = encode_inbox_ack_inflight_request(&request).expect("inbox ack should encode");
        let decoded = decode_inbox_ack_inflight_request(&bytes).expect("inbox ack should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn kv_range_get_request_round_trip_preserves_fields() {
        let request = KvRangeGetRequestEnvelope {
            range_id: "range-a".to_string(),
            key: b"mango".to_vec(),
            expected_epoch: 7,
        };

        let bytes = encode_kv_range_get_request(&request).expect("kv range get should encode");
        let decoded = decode_kv_range_get_request(&bytes).expect("kv range get should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn kv_range_get_reply_round_trip_preserves_value_and_found() {
        let reply = KvRangeGetReplyEnvelope {
            value: b"yellow".to_vec(),
            found: true,
        };

        let bytes = encode_kv_range_get_reply(&reply).expect("kv range get reply should encode");
        let decoded = decode_kv_range_get_reply(&bytes).expect("kv range get reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn kv_range_scan_request_round_trip_preserves_boundary_and_limit() {
        let request = KvRangeScanRequestEnvelope {
            range_id: "range-a".to_string(),
            boundary: Some(RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec()))),
            limit: 10,
            expected_epoch: 7,
        };

        let bytes = encode_kv_range_scan_request(&request).expect("kv range scan should encode");
        let decoded = decode_kv_range_scan_request(&bytes).expect("kv range scan should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn kv_range_scan_reply_round_trip_preserves_entries() {
        let reply = KvRangeScanReplyEnvelope {
            entries: vec![
                KvEntryEnvelope {
                    key: b"mango".to_vec(),
                    value: b"yellow".to_vec(),
                },
                KvEntryEnvelope {
                    key: b"pear".to_vec(),
                    value: b"green".to_vec(),
                },
            ],
        };

        let bytes = encode_kv_range_scan_reply(&reply).expect("kv range scan reply should encode");
        let decoded =
            decode_kv_range_scan_reply(&bytes).expect("kv range scan reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn kv_mutation_round_trip_preserves_optional_value() {
        let mutation = KvMutationEnvelope {
            key: b"mango".to_vec(),
            value: Some(b"yellow".to_vec()),
        };

        let bytes = encode_kv_mutation(&mutation).expect("kv mutation should encode");
        let decoded = decode_kv_mutation(&bytes).expect("kv mutation should decode");
        assert_eq!(decoded, mutation);
    }

    #[test]
    fn kv_range_apply_request_round_trip_preserves_mutations() {
        let request = KvRangeApplyRequestEnvelope {
            range_id: "range-a".to_string(),
            mutations: vec![
                KvMutationEnvelope {
                    key: b"mango".to_vec(),
                    value: Some(b"yellow".to_vec()),
                },
                KvMutationEnvelope {
                    key: b"pear".to_vec(),
                    value: None,
                },
            ],
            expected_epoch: 7,
        };

        let bytes = encode_kv_range_apply_request(&request).expect("kv apply should encode");
        let decoded = decode_kv_range_apply_request(&bytes).expect("kv apply should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn kv_range_checkpoint_request_round_trip_preserves_fields() {
        let request = KvRangeCheckpointRequestEnvelope {
            range_id: "range-a".to_string(),
            checkpoint_id: "cp-1".to_string(),
            expected_epoch: 7,
        };

        let bytes =
            encode_kv_range_checkpoint_request(&request).expect("kv checkpoint should encode");
        let decoded =
            decode_kv_range_checkpoint_request(&bytes).expect("kv checkpoint should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn kv_range_checkpoint_reply_round_trip_preserves_fields() {
        let reply = KvRangeCheckpointReplyEnvelope {
            range_id: "range-a".to_string(),
            checkpoint_id: "cp-1".to_string(),
            path: "memory://range-a/checkpoints/cp-1".to_string(),
        };

        let bytes =
            encode_kv_range_checkpoint_reply(&reply).expect("kv checkpoint reply should encode");
        let decoded =
            decode_kv_range_checkpoint_reply(&bytes).expect("kv checkpoint reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn kv_range_snapshot_request_round_trip_preserves_fields() {
        let request = KvRangeSnapshotRequestEnvelope {
            range_id: "range-a".to_string(),
            expected_epoch: 7,
        };

        let bytes = encode_kv_range_snapshot_request(&request).expect("kv snapshot should encode");
        let decoded = decode_kv_range_snapshot_request(&bytes).expect("kv snapshot should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn kv_range_snapshot_reply_round_trip_preserves_metadata() {
        let reply = KvRangeSnapshotReplyEnvelope {
            range_id: "range-a".to_string(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            term: 3,
            index: 11,
            checksum: 42,
            layout_version: 2,
            data_path: "/tmp/range-a/snapshot".to_string(),
        };

        let bytes =
            encode_kv_range_snapshot_reply(&reply).expect("kv snapshot reply should encode");
        let decoded =
            decode_kv_range_snapshot_reply(&bytes).expect("kv snapshot reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn subscription_record_round_trip_preserves_optional_fields() {
        let subscription = Subscription {
            session_id: "session-a".to_string(),
            tenant_id: "tenant-a".to_string(),
            topic_filter: "devices/+/state".to_string(),
            qos: 1,
            subscription_identifier: Some(88),
            no_local: false,
            retain_as_published: true,
            retain_handling: 2,
            shared_group: Some("group-a".to_string()),
            kind: SessionKind::Persistent,
        };

        let bytes =
            encode_subscription_record(&subscription).expect("subscription record should encode");
        let decoded =
            decode_subscription_record(&bytes).expect("subscription record should decode");
        assert_eq!(decoded, subscription);
    }

    #[test]
    fn retained_message_round_trip_preserves_payload_bytes() {
        let retained = RetainedMessage {
            tenant_id: "tenant-a".to_string(),
            topic: "devices/a/state".to_string(),
            payload: b"payload-1".to_vec().into(),
            qos: 1,
        };

        let bytes = encode_retained_message(&retained).expect("retained message should encode");
        let decoded = decode_retained_message(&bytes).expect("retained message should decode");
        assert_eq!(decoded, retained);
    }

    #[test]
    fn retain_write_request_round_trip_works() {
        let request = RetainWriteRequestEnvelope {
            message: RetainedMessage {
                tenant_id: "tenant-a".to_string(),
                topic: "devices/a/state".to_string(),
                payload: b"payload-1".to_vec().into(),
                qos: 1,
            },
        };
        let bytes =
            encode_retain_write_request(&request).expect("retain write request should encode");
        let decoded =
            decode_retain_write_request(&bytes).expect("retain write request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn retain_match_request_round_trip_works() {
        let request = RetainMatchRequestEnvelope {
            tenant_id: "tenant-a".to_string(),
            topic_filter: "devices/#".to_string(),
        };
        let bytes =
            encode_retain_match_request(&request).expect("retain match request should encode");
        let decoded =
            decode_retain_match_request(&bytes).expect("retain match request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn retain_match_reply_round_trip_preserves_messages() {
        let reply = RetainMatchReplyEnvelope {
            messages: vec![
                RetainedMessage {
                    tenant_id: "tenant-a".to_string(),
                    topic: "devices/a/state".to_string(),
                    payload: b"payload-1".to_vec().into(),
                    qos: 1,
                },
                RetainedMessage {
                    tenant_id: "tenant-a".to_string(),
                    topic: "devices/b/state".to_string(),
                    payload: b"payload-2".to_vec().into(),
                    qos: 0,
                },
            ],
        };
        let bytes = encode_retain_match_reply(&reply).expect("retain match reply should encode");
        let decoded = decode_retain_match_reply(&bytes).expect("retain match reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn offline_message_round_trip_preserves_payload_and_properties() {
        let message = OfflineMessage {
            tenant_id: "tenant-a".to_string(),
            session_id: "session-a".to_string(),
            topic: "devices/a/state".to_string(),
            payload: b"payload-1".to_vec().into(),
            qos: 1,
            retain: true,
            from_session_id: "from-a".to_string(),
            properties: sample_publish_properties(),
        };

        let bytes = encode_offline_message(&message).expect("offline message should encode");
        let decoded = decode_offline_message(&bytes).expect("offline message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn inflight_message_round_trip_preserves_phase_and_properties() {
        let message = InflightMessage {
            tenant_id: "tenant-a".to_string(),
            session_id: "session-a".to_string(),
            packet_id: 42,
            topic: "devices/a/state".to_string(),
            payload: b"payload-2".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "from-a".to_string(),
            properties: sample_publish_properties(),
            phase: InflightPhase::Release,
        };

        let bytes = encode_inflight_message(&message).expect("inflight message should encode");
        let decoded = decode_inflight_message(&bytes).expect("inflight message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn delivery_round_trip_preserves_payload_and_properties() {
        let delivery = Delivery {
            tenant_id: "tenant-a".to_string(),
            session_id: "session-a".to_string(),
            topic: "devices/a/state".to_string(),
            payload: b"payload-3".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "from-a".to_string(),
            properties: sample_publish_properties(),
        };

        let bytes = encode_delivery(&delivery).expect("delivery should encode");
        let decoded = decode_delivery(&bytes).expect("delivery should decode");
        assert_eq!(decoded, delivery);
    }

    #[test]
    fn connect_request_round_trip_preserves_optional_fields() {
        let request = ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".to_string(),
                user_id: "user-a".to_string(),
                client_id: "client-a".to_string(),
            },
            node_id: 7,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: Some(3600),
        };

        let bytes = encode_connect_request(&request).expect("connect request should encode");
        let decoded = decode_connect_request(&bytes).expect("connect request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn publish_request_round_trip_preserves_payload_and_properties() {
        let request = PublishRequest {
            topic: "devices/a/state".to_string(),
            payload: b"payload-pub".to_vec().into(),
            qos: 1,
            retain: true,
            properties: sample_publish_properties(),
        };

        let bytes = encode_publish_request(&request).expect("publish request should encode");
        let decoded = decode_publish_request(&bytes).expect("publish request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn connect_reply_round_trip_preserves_nested_messages() {
        let reply = ConnectReply {
            session: SessionRecord {
                session_id: "session-a".to_string(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "tenant-a".to_string(),
                    user_id: "user-a".to_string(),
                    client_id: "client-a".to_string(),
                },
                session_expiry_interval_secs: Some(3600),
                expires_at_ms: Some(123_456),
            },
            session_present: true,
            local_session_epoch: 88,
            replaced: Some(SessionRecord {
                session_id: "session-old".to_string(),
                node_id: 8,
                kind: SessionKind::Transient,
                identity: ClientIdentity {
                    tenant_id: "tenant-a".to_string(),
                    user_id: "user-a".to_string(),
                    client_id: "client-a".to_string(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            }),
            offline_messages: vec![OfflineMessage {
                tenant_id: "tenant-a".to_string(),
                session_id: "session-a".to_string(),
                topic: "devices/a/state".to_string(),
                payload: b"payload-1".to_vec().into(),
                qos: 1,
                retain: true,
                from_session_id: "from-a".to_string(),
                properties: sample_publish_properties(),
            }],
            inflight_messages: vec![InflightMessage {
                tenant_id: "tenant-a".to_string(),
                session_id: "session-a".to_string(),
                packet_id: 42,
                topic: "devices/a/state".to_string(),
                payload: b"payload-2".to_vec().into(),
                qos: 2,
                retain: false,
                from_session_id: "from-a".to_string(),
                properties: sample_publish_properties(),
                phase: InflightPhase::Release,
            }],
        };

        let bytes = encode_connect_reply(&reply).expect("connect reply should encode");
        let decoded = decode_connect_reply(&bytes).expect("connect reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn replicated_range_descriptor_round_trip_preserves_nested_metadata() {
        let descriptor = ReplicatedRangeDescriptor {
            id: "range-a".to_string(),
            shard: ServiceShardKey {
                kind: ServiceShardKind::Dist,
                tenant_id: "tenant-a".to_string(),
                scope: "*".to_string(),
            },
            boundary: RangeBoundary {
                start_key: Some(b"a".to_vec()),
                end_key: Some(b"z".to_vec()),
            },
            epoch: 11,
            config_version: 3,
            leader_node_id: Some(7),
            replicas: vec![
                RangeReplica {
                    node_id: 7,
                    role: ReplicaRole::Voter,
                    sync_state: ReplicaSyncState::Replicating,
                },
                RangeReplica {
                    node_id: 8,
                    role: ReplicaRole::Learner,
                    sync_state: ReplicaSyncState::Snapshotting,
                },
            ],
            commit_index: 101,
            applied_index: 99,
            lifecycle: ServiceShardLifecycle::Serving,
        };

        let bytes = encode_replicated_range_descriptor(&descriptor)
            .expect("replicated range descriptor should encode");
        let decoded = decode_replicated_range_descriptor(&bytes)
            .expect("replicated range descriptor should decode");
        assert_eq!(decoded, descriptor);
    }

    #[test]
    fn service_endpoint_round_trip_preserves_kind_and_endpoint() {
        let endpoint = ServiceEndpoint {
            kind: ServiceKind::HttpApi,
            node_id: 7,
            endpoint: "quic://node-1.internal:60051".to_string(),
        };

        let bytes = encode_service_endpoint(&endpoint).expect("service endpoint should encode");
        let decoded = decode_service_endpoint(&bytes).expect("service endpoint should decode");
        assert_eq!(decoded, endpoint);
    }

    #[test]
    fn cluster_node_membership_round_trip_preserves_endpoints() {
        let membership = ClusterNodeMembership {
            node_id: 7,
            epoch: 3,
            lifecycle: ClusterNodeLifecycle::Serving,
            endpoints: vec![
                ServiceEndpoint {
                    kind: ServiceKind::Broker,
                    node_id: 7,
                    endpoint: "http://127.0.0.1:1883".to_string(),
                },
                ServiceEndpoint {
                    kind: ServiceKind::HttpApi,
                    node_id: 7,
                    endpoint: "quic://node-1.internal:60051".to_string(),
                },
            ],
        };

        let bytes =
            encode_cluster_node_membership(&membership).expect("cluster membership should encode");
        let decoded =
            decode_cluster_node_membership(&bytes).expect("cluster membership should decode");
        assert_eq!(decoded, membership);
    }

    #[test]
    fn balancer_state_round_trip_preserves_load_rules() {
        let mut state = BalancerState {
            disabled: true,
            ..Default::default()
        };
        state
            .load_rules
            .insert("max_qps".to_string(), "5000".to_string());
        state
            .load_rules
            .insert("rebalance".to_string(), "enabled".to_string());

        let bytes = encode_balancer_state(&state).expect("balancer state should encode");
        let decoded = decode_balancer_state(&bytes).expect("balancer state should decode");
        assert_eq!(decoded, state);
    }

    #[test]
    fn member_lookup_request_round_trip_works() {
        let request = MemberLookupRequestEnvelope { node_id: 7 };
        let bytes =
            encode_member_lookup_request(&request).expect("member lookup request should encode");
        let decoded =
            decode_member_lookup_request(&bytes).expect("member lookup request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn member_record_reply_round_trip_preserves_optional_member() {
        let reply = MemberRecordReplyEnvelope {
            member: Some(ClusterNodeMembership {
                node_id: 7,
                epoch: 3,
                lifecycle: ClusterNodeLifecycle::Serving,
                endpoints: vec![ServiceEndpoint {
                    kind: ServiceKind::HttpApi,
                    node_id: 7,
                    endpoint: "quic://node-1.internal:60051".to_string(),
                }],
            }),
        };
        let bytes = encode_member_record_reply(&reply).expect("member record reply should encode");
        let decoded =
            decode_member_record_reply(&bytes).expect("member record reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn member_list_reply_round_trip_preserves_members() {
        let reply = MemberListReplyEnvelope {
            members: vec![ClusterNodeMembership {
                node_id: 7,
                epoch: 3,
                lifecycle: ClusterNodeLifecycle::Serving,
                endpoints: vec![ServiceEndpoint {
                    kind: ServiceKind::Broker,
                    node_id: 7,
                    endpoint: "http://127.0.0.1:1883".to_string(),
                }],
            }],
        };
        let bytes = encode_member_list_reply(&reply).expect("member list reply should encode");
        let decoded = decode_member_list_reply(&bytes).expect("member list reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn balancer_state_request_round_trip_works() {
        let request = BalancerStateRequestEnvelope {
            name: "dist-default".to_string(),
        };
        let bytes =
            encode_balancer_state_request(&request).expect("balancer state request should encode");
        let decoded =
            decode_balancer_state_request(&bytes).expect("balancer state request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn balancer_state_upsert_request_round_trip_preserves_state() {
        let mut state = BalancerState::default();
        state
            .load_rules
            .insert("max_qps".to_string(), "5000".to_string());
        let request = BalancerStateUpsertRequestEnvelope {
            name: "dist-default".to_string(),
            state,
        };
        let bytes = encode_balancer_state_upsert_request(&request)
            .expect("balancer state upsert request should encode");
        let decoded = decode_balancer_state_upsert_request(&bytes)
            .expect("balancer state upsert request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn balancer_state_upsert_reply_round_trip_preserves_optional_previous() {
        let mut previous = BalancerState::default();
        previous
            .load_rules
            .insert("rebalance".to_string(), "enabled".to_string());
        let reply = BalancerStateUpsertReplyEnvelope {
            previous: Some(previous),
        };
        let bytes = encode_balancer_state_upsert_reply(&reply)
            .expect("balancer state upsert reply should encode");
        let decoded = decode_balancer_state_upsert_reply(&bytes)
            .expect("balancer state upsert reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn balancer_state_reply_round_trip_preserves_optional_state() {
        let mut state = BalancerState::default();
        state
            .load_rules
            .insert("max_qps".to_string(), "5000".to_string());
        let reply = BalancerStateReplyEnvelope { state: Some(state) };
        let bytes =
            encode_balancer_state_reply(&reply).expect("balancer state reply should encode");
        let decoded =
            decode_balancer_state_reply(&bytes).expect("balancer state reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn balancer_state_list_reply_round_trip_preserves_entries() {
        let mut state = BalancerState::default();
        state
            .load_rules
            .insert("max_qps".to_string(), "5000".to_string());
        let reply = BalancerStateListReplyEnvelope {
            entries: vec![NamedBalancerStateEnvelope {
                name: "dist-default".to_string(),
                state: Some(state),
            }],
        };
        let bytes = encode_balancer_state_list_reply(&reply)
            .expect("balancer state list reply should encode");
        let decoded = decode_balancer_state_list_reply(&bytes)
            .expect("balancer state list reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn range_lookup_request_round_trip_works() {
        let request = RangeLookupRequestEnvelope {
            range_id: "range-a".to_string(),
        };
        let bytes =
            encode_range_lookup_request(&request).expect("range lookup request should encode");
        let decoded =
            decode_range_lookup_request(&bytes).expect("range lookup request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_record_reply_round_trip_preserves_optional_descriptor() {
        let reply = RangeRecordReplyEnvelope {
            descriptor: Some(sample_replicated_range_descriptor()),
        };
        let bytes = encode_range_record_reply(&reply).expect("range record reply should encode");
        let decoded = decode_range_record_reply(&bytes).expect("range record reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn range_list_request_round_trip_preserves_optional_filters() {
        let request = RangeListRequestEnvelope {
            shard_kind: Some(ServiceShardKind::Retain),
            tenant_id: Some("t1".to_string()),
            scope: Some("*".to_string()),
        };
        let bytes = encode_range_list_request(&request).expect("range list request should encode");
        let decoded = decode_range_list_request(&bytes).expect("range list request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_list_reply_round_trip_preserves_descriptors() {
        let reply = RangeListReplyEnvelope {
            descriptors: vec![sample_replicated_range_descriptor()],
        };
        let bytes = encode_range_list_reply(&reply).expect("range list reply should encode");
        let decoded = decode_range_list_reply(&bytes).expect("range list reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn route_range_request_round_trip_preserves_shard_and_key() {
        let request = RouteRangeRequestEnvelope {
            shard: ServiceShardKey::retain("t1"),
            key: b"pear".to_vec(),
        };
        let bytes =
            encode_route_range_request(&request).expect("route range request should encode");
        let decoded =
            decode_route_range_request(&bytes).expect("route range request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_health_request_round_trip_works() {
        let request = RangeHealthRequestEnvelope {
            range_id: "range-a".to_string(),
        };
        let bytes =
            encode_range_health_request(&request).expect("range health request should encode");
        let decoded =
            decode_range_health_request(&bytes).expect("range health request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_health_reply_round_trip_preserves_nested_state() {
        let reply = RangeHealthReplyEnvelope {
            health: Some(sample_range_health()),
        };
        let bytes = encode_range_health_reply(&reply).expect("range health reply should encode");
        let decoded = decode_range_health_reply(&bytes).expect("range health reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn range_health_list_reply_round_trip_preserves_entries() {
        let reply = RangeHealthListReplyEnvelope {
            entries: vec![sample_range_health()],
        };
        let bytes =
            encode_range_health_list_reply(&reply).expect("range health list reply should encode");
        let decoded =
            decode_range_health_list_reply(&bytes).expect("range health list reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn range_debug_reply_round_trip_works() {
        let reply = RangeDebugReplyEnvelope {
            text: "debug dump".to_string(),
        };
        let bytes = encode_range_debug_reply(&reply).expect("range debug reply should encode");
        let decoded = decode_range_debug_reply(&bytes).expect("range debug reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn zombie_range_list_reply_round_trip_preserves_entries() {
        let reply = ZombieRangeListReplyEnvelope {
            entries: vec![ZombieRangeEnvelope {
                range_id: "range-z".to_string(),
                lifecycle: ServiceShardLifecycle::Offline,
                leader_node_id: Some(9),
            }],
        };
        let bytes =
            encode_zombie_range_list_reply(&reply).expect("zombie range list reply should encode");
        let decoded =
            decode_zombie_range_list_reply(&bytes).expect("zombie range list reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn range_bootstrap_request_round_trip_preserves_descriptor() {
        let request = RangeBootstrapRequestEnvelope {
            descriptor: sample_replicated_range_descriptor(),
        };
        let bytes = encode_range_bootstrap_request(&request)
            .expect("range bootstrap request should encode");
        let decoded =
            decode_range_bootstrap_request(&bytes).expect("range bootstrap request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_bootstrap_reply_round_trip_works() {
        let reply = RangeBootstrapReplyEnvelope {
            range_id: "range-new".to_string(),
        };
        let bytes =
            encode_range_bootstrap_reply(&reply).expect("range bootstrap reply should encode");
        let decoded =
            decode_range_bootstrap_reply(&bytes).expect("range bootstrap reply should decode");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn range_change_replicas_request_round_trip_preserves_lists() {
        let request = RangeChangeReplicasRequestEnvelope {
            range_id: "range-a".to_string(),
            voters: vec![7, 8],
            learners: vec![9],
        };
        let bytes = encode_range_change_replicas_request(&request)
            .expect("range change replicas request should encode");
        let decoded = decode_range_change_replicas_request(&bytes)
            .expect("range change replicas request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_transfer_leadership_request_round_trip_works() {
        let request = RangeTransferLeadershipRequestEnvelope {
            range_id: "range-a".to_string(),
            target_node_id: 8,
        };
        let bytes = encode_range_transfer_leadership_request(&request)
            .expect("range transfer leadership request should encode");
        let decoded = decode_range_transfer_leadership_request(&bytes)
            .expect("range transfer leadership request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_recover_request_round_trip_works() {
        let request = RangeRecoverRequestEnvelope {
            range_id: "range-a".to_string(),
            new_leader_node_id: 9,
        };
        let bytes =
            encode_range_recover_request(&request).expect("range recover request should encode");
        let decoded =
            decode_range_recover_request(&bytes).expect("range recover request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn range_split_request_and_reply_round_trip() {
        let request = RangeSplitRequestEnvelope {
            range_id: "range-a".to_string(),
            split_key: b"mid".to_vec(),
        };
        let request_bytes =
            encode_range_split_request(&request).expect("range split request should encode");
        let decoded_request =
            decode_range_split_request(&request_bytes).expect("range split request should decode");
        assert_eq!(decoded_request, request);

        let reply = RangeSplitReplyEnvelope {
            left_range_id: "range-left".to_string(),
            right_range_id: "range-right".to_string(),
        };
        let reply_bytes =
            encode_range_split_reply(&reply).expect("range split reply should encode");
        let decoded_reply =
            decode_range_split_reply(&reply_bytes).expect("range split reply should decode");
        assert_eq!(decoded_reply, reply);
    }

    #[test]
    fn range_merge_request_and_reply_round_trip() {
        let request = RangeMergeRequestEnvelope {
            left_range_id: "range-left".to_string(),
            right_range_id: "range-right".to_string(),
        };
        let request_bytes =
            encode_range_merge_request(&request).expect("range merge request should encode");
        let decoded_request =
            decode_range_merge_request(&request_bytes).expect("range merge request should decode");
        assert_eq!(decoded_request, request);

        let reply = RangeMergeReplyEnvelope {
            range_id: "range-merged".to_string(),
        };
        let reply_bytes =
            encode_range_merge_reply(&reply).expect("range merge reply should encode");
        let decoded_reply =
            decode_range_merge_reply(&reply_bytes).expect("range merge reply should decode");
        assert_eq!(decoded_reply, reply);
    }

    #[test]
    fn range_drain_and_retire_request_round_trip() {
        let drain = RangeDrainRequestEnvelope {
            range_id: "range-a".to_string(),
        };
        let drain_bytes =
            encode_range_drain_request(&drain).expect("range drain request should encode");
        let decoded_drain =
            decode_range_drain_request(&drain_bytes).expect("range drain request should decode");
        assert_eq!(decoded_drain, drain);

        let retire = RangeRetireRequestEnvelope {
            range_id: "range-a".to_string(),
        };
        let retire_bytes =
            encode_range_retire_request(&retire).expect("range retire request should encode");
        let decoded_retire =
            decode_range_retire_request(&retire_bytes).expect("range retire request should decode");
        assert_eq!(decoded_retire, retire);
    }

    #[test]
    fn raft_log_entry_round_trip_preserves_config_change_encoding() {
        let entry = RaftLogEntry {
            term: 3,
            index: 11,
            config_change: Some(RaftConfigLogEntry::JointConfig {
                old_config: RaftClusterConfig {
                    voters: vec![7],
                    learners: vec![],
                },
                joint_config: RaftClusterConfig {
                    voters: vec![7, 8],
                    learners: vec![9],
                },
                final_config: RaftClusterConfig {
                    voters: vec![7, 8],
                    learners: vec![9],
                },
            }),
            command: Vec::<u8>::new().into(),
        };
        let bytes = encode_raft_log_entry(&entry).expect("raft log entry should encode");
        let decoded = decode_raft_log_entry(&bytes).expect("raft log entry should decode");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn raft_snapshot_round_trip_preserves_payload() {
        let snapshot = RaftSnapshot {
            range_id: "range-a".to_string(),
            term: 4,
            index: 12,
            payload: b"snapshot-payload".to_vec().into(),
        };
        let bytes = encode_raft_snapshot(&snapshot).expect("raft snapshot should encode");
        let decoded = decode_raft_snapshot(&bytes).expect("raft snapshot should decode");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn raft_transport_request_round_trip_preserves_append_entries() {
        let request = RaftTransportRequestEnvelope {
            range_id: "range-a".to_string(),
            from_node_id: 7,
            message: RaftMessage::AppendEntries(AppendEntriesRequest {
                term: 5,
                leader_id: 7,
                prev_log_index: 10,
                prev_log_term: 4,
                entries: vec![RaftLogEntry {
                    term: 5,
                    index: 11,
                    config_change: None,
                    command: b"cmd-1".to_vec().into(),
                }],
                leader_commit: 11,
            }),
        };
        let bytes =
            encode_raft_transport_request(&request).expect("raft transport request should encode");
        let decoded =
            decode_raft_transport_request(&bytes).expect("raft transport request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn raft_transport_request_round_trip_preserves_vote_response() {
        let request = RaftTransportRequestEnvelope {
            range_id: "range-a".to_string(),
            from_node_id: 8,
            message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: 6,
                vote_granted: true,
            }),
        };
        let bytes =
            encode_raft_transport_request(&request).expect("raft transport request should encode");
        let decoded =
            decode_raft_transport_request(&bytes).expect("raft transport request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn raft_transport_request_round_trip_preserves_install_snapshot() {
        let request = RaftTransportRequestEnvelope {
            range_id: "range-a".to_string(),
            from_node_id: 9,
            message: RaftMessage::InstallSnapshot(InstallSnapshotRequest {
                term: 7,
                leader_id: 9,
                snapshot: RaftSnapshot {
                    range_id: "range-a".to_string(),
                    term: 7,
                    index: 20,
                    payload: b"snapshot-2".to_vec().into(),
                },
            }),
        };
        let bytes =
            encode_raft_transport_request(&request).expect("raft transport request should encode");
        let decoded =
            decode_raft_transport_request(&bytes).expect("raft transport request should decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn user_property_round_trip_works() {
        let property = UserProperty {
            key: "k1".to_string(),
            value: "v1".to_string(),
        };

        let bytes = encode_user_property(&property).expect("user property should encode");
        let decoded = decode_user_property(&bytes).expect("user property should decode");
        assert_eq!(decoded, property);
    }

    #[test]
    fn publish_properties_round_trip_preserves_optional_fields_and_lists() {
        let properties = sample_publish_properties();

        let bytes =
            encode_publish_properties(&properties).expect("publish properties should encode");
        let decoded = decode_publish_properties(&bytes).expect("publish properties should decode");
        assert_eq!(decoded, properties);
    }

    fn sample_publish_properties() -> PublishProperties {
        PublishProperties {
            payload_format_indicator: Some(1),
            content_type: Some("application/json".to_string()),
            message_expiry_interval_secs: Some(60),
            stored_at_ms: Some(123_456),
            response_topic: Some("devices/a/reply".to_string()),
            correlation_data: Some(b"corr-1".to_vec()),
            subscription_identifiers: vec![7, 8, 9],
            user_properties: vec![
                UserProperty {
                    key: "k1".to_string(),
                    value: "v1".to_string(),
                },
                UserProperty {
                    key: "k2".to_string(),
                    value: "v2".to_string(),
                },
            ],
        }
    }

    fn sample_replicated_range_descriptor() -> ReplicatedRangeDescriptor {
        ReplicatedRangeDescriptor {
            id: "range-a".to_string(),
            shard: ServiceShardKey {
                kind: ServiceShardKind::Dist,
                tenant_id: "tenant-a".to_string(),
                scope: "*".to_string(),
            },
            boundary: RangeBoundary {
                start_key: Some(b"a".to_vec()),
                end_key: Some(b"z".to_vec()),
            },
            epoch: 11,
            config_version: 3,
            leader_node_id: Some(7),
            replicas: vec![
                RangeReplica {
                    node_id: 7,
                    role: ReplicaRole::Voter,
                    sync_state: ReplicaSyncState::Replicating,
                },
                RangeReplica {
                    node_id: 8,
                    role: ReplicaRole::Learner,
                    sync_state: ReplicaSyncState::Snapshotting,
                },
            ],
            commit_index: 101,
            applied_index: 99,
            lifecycle: ServiceShardLifecycle::Serving,
        }
    }

    fn sample_range_health() -> RangeHealthEnvelope {
        RangeHealthEnvelope {
            range_id: "range-a".to_string(),
            lifecycle: ServiceShardLifecycle::Serving,
            role: RaftRoleEnvelope::Leader,
            current_term: 12,
            leader_node_id: Some(7),
            commit_index: 101,
            applied_index: 99,
            latest_snapshot_index: Some(80),
            replica_lag: vec![
                ReplicaLagEnvelope {
                    node_id: 7,
                    lag: 0,
                    match_index: 101,
                    next_index: 102,
                },
                ReplicaLagEnvelope {
                    node_id: 8,
                    lag: 2,
                    match_index: 99,
                    next_index: 100,
                },
            ],
            reconfiguration: RangeReconfigurationState {
                range_id: "range-a".to_string(),
                old_voters: vec![7, 8],
                old_learners: vec![9],
                current_voters: vec![7, 8],
                current_learners: vec![9],
                joint_voters: vec![7, 8],
                joint_learners: vec![9],
                pending_voters: vec![7, 8],
                pending_learners: vec![9],
                phase: Some(ReconfigurationPhase::JointConsensus),
                blocked_on_catch_up: true,
            },
        }
    }
}
