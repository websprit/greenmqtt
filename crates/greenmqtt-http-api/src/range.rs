use axum::{
    extract::{Extension, Path, Query, State},
    Json,
};
use greenmqtt_broker::BrokerRuntime;
use greenmqtt_core::{
    ClusterNodeMembership, ControlCommandExecutionState, ControlCommandRecord,
    ControlCommandReflectionState, ControlPlaneRegistry, NodeId, RangeReconfigurationState,
    ReplicatedRangeDescriptor, ServiceKind,
};
use greenmqtt_kv_server::{RangeHealthSnapshot, ReplicaRuntime, ZombieRangeSnapshot};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use greenmqtt_range_client::{DirectRangeControlClient, RangeControlClient};
use greenmqtt_rpc::RangeAdminGrpcClient;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{ApiError, RangeListQuery};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RangeActionReply {
    pub operation: String,
    pub ok: bool,
    pub status: String,
    pub reason: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub forwarded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_action: Option<String>,
    pub range_id: Option<String>,
    pub left_range_id: Option<String>,
    pub right_range_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RangeLookupReply {
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<RangeHealthSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<ReplicatedRangeDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconfiguration: Option<RangeReconfigurationState>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RangeBootstrapBody {
    pub descriptor: ReplicatedRangeDescriptor,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RangeChangeReplicasBody {
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RangeTransferLeadershipBody {
    pub target_node_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RangeRecoverBody {
    pub new_leader_node_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RangeSplitBody {
    pub split_key: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RangeMergeBody {
    pub left_range_id: String,
    pub right_range_id: String,
}

struct RemoteRangeControlTarget {
    node_id: NodeId,
    endpoint: String,
    client: Arc<dyn RangeControlClient>,
}

static MANUAL_COMMAND_SEQ: AtomicU64 = AtomicU64::new(1);

fn require_range_runtime(
    runtime: Option<Arc<ReplicaRuntime>>,
) -> Result<Arc<ReplicaRuntime>, ApiError> {
    runtime.ok_or_else(|| ApiError::from(anyhow::anyhow!("range runtime unavailable")))
}

fn operator_endpoint_for_member(
    member: &ClusterNodeMembership,
    preferred_kind: Option<ServiceKind>,
) -> anyhow::Result<String> {
    if let Some(kind) = preferred_kind {
        if let Some(endpoint) = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == kind)
        {
            return Ok(endpoint.endpoint.clone());
        }
    }
    member
        .endpoints
        .first()
        .map(|endpoint| endpoint.endpoint.clone())
        .ok_or_else(|| anyhow::anyhow!("member {} has no registered RPC endpoints", member.node_id))
}

fn target_node_for_descriptor(descriptor: &ReplicatedRangeDescriptor) -> Option<NodeId> {
    descriptor
        .leader_node_id
        .or_else(|| descriptor.voters().first().map(|replica| replica.node_id))
        .or_else(|| descriptor.replicas.first().map(|replica| replica.node_id))
}

fn prefer_range_health(current: &RangeHealthSnapshot, candidate: &RangeHealthSnapshot) -> bool {
    candidate.current_term > current.current_term
        || (candidate.current_term == current.current_term
            && candidate.applied_index > current.applied_index)
}

async fn aggregate_range_health(
    range_runtime: Option<Arc<ReplicaRuntime>>,
    range_routing: Option<Arc<dyn ControlPlaneRegistry>>,
) -> Result<Vec<RangeHealthSnapshot>, ApiError> {
    if let Some(range_routing) = range_routing {
        let mut entries: BTreeMap<String, RangeHealthSnapshot> = BTreeMap::new();
        for member in range_routing.list_members().await.map_err(ApiError::from)? {
            let endpoint = operator_endpoint_for_member(&member, None).map_err(ApiError::from)?;
            let client = RangeAdminGrpcClient::connect(endpoint)
                .await
                .map_err(ApiError::from)?;
            for health in client
                .list_range_health_snapshots()
                .await
                .map_err(ApiError::from)?
            {
                match entries.get(&health.range_id) {
                    Some(current) if !prefer_range_health(current, &health) => {}
                    _ => {
                        entries.insert(health.range_id.clone(), health);
                    }
                }
            }
        }
        return Ok(entries.into_values().collect());
    }

    let runtime = require_range_runtime(range_runtime)?;
    runtime.health_snapshot().await.map_err(ApiError::from)
}

async fn remote_range_control_client<A, C, H>(
    broker: &Arc<BrokerRuntime<A, C, H>>,
    range_runtime: Option<Arc<ReplicaRuntime>>,
    range_routing: Option<Arc<dyn ControlPlaneRegistry>>,
    range_id: &str,
) -> Result<Option<RemoteRangeControlTarget>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(range_routing) = range_routing else {
        return Ok(None);
    };
    let Some(descriptor) = range_routing
        .resolve_range(range_id)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(None);
    };
    let Some(target_node_id) = target_node_for_descriptor(&descriptor) else {
        return Ok(None);
    };
    if target_node_id == broker.config.node_id && range_runtime.is_some() {
        return Ok(None);
    }
    let member = range_routing
        .resolve_member(target_node_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::from(anyhow::anyhow!("target node {target_node_id} not found")))?;
    let endpoint = operator_endpoint_for_member(&member, Some(descriptor.shard.service_kind()))
        .map_err(ApiError::from)?;
    Ok(Some(RemoteRangeControlTarget {
        node_id: target_node_id,
        endpoint: endpoint.clone(),
        client: Arc::new(
            DirectRangeControlClient::connect(endpoint)
                .await
                .map_err(ApiError::from)?,
        ),
    }))
}

async fn remote_bootstrap_client<A, C, H>(
    broker: &Arc<BrokerRuntime<A, C, H>>,
    range_runtime: Option<Arc<ReplicaRuntime>>,
    range_routing: Option<Arc<dyn ControlPlaneRegistry>>,
    descriptor: &ReplicatedRangeDescriptor,
) -> Result<Option<RemoteRangeControlTarget>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(range_routing) = range_routing else {
        return Ok(None);
    };
    let Some(target_node_id) = target_node_for_descriptor(descriptor) else {
        return Ok(None);
    };
    if target_node_id == broker.config.node_id && range_runtime.is_some() {
        return Ok(None);
    }
    let member = range_routing
        .resolve_member(target_node_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::from(anyhow::anyhow!("target node {target_node_id} not found")))?;
    let endpoint = operator_endpoint_for_member(&member, Some(descriptor.shard.service_kind()))
        .map_err(ApiError::from)?;
    Ok(Some(RemoteRangeControlTarget {
        node_id: target_node_id,
        endpoint: endpoint.clone(),
        client: Arc::new(
            DirectRangeControlClient::connect(endpoint)
                .await
                .map_err(ApiError::from)?,
        ),
    }))
}

fn range_action_reply(
    operation: &str,
    mode: &str,
    status: impl Into<String>,
    reason: impl Into<String>,
    target: Option<(u64, String)>,
    command_id: Option<String>,
    audit_action: Option<String>,
    range_id: Option<String>,
    left_range_id: Option<String>,
    right_range_id: Option<String>,
) -> RangeActionReply {
    RangeActionReply {
        operation: operation.to_string(),
        ok: true,
        status: status.into(),
        reason: reason.into(),
        mode: mode.to_string(),
        forwarded: target.is_some(),
        target_node_id: target.as_ref().map(|(node_id, _)| *node_id),
        target_endpoint: target.map(|(_, endpoint)| endpoint),
        command_id,
        audit_action,
        range_id,
        left_range_id,
        right_range_id,
    }
}

fn matches_range_filters(health: &RangeHealthSnapshot, query: &RangeListQuery) -> bool {
    if let Some(prefix) = query.range_id_prefix.as_deref() {
        if !health.range_id.starts_with(prefix) {
            return false;
        }
    }
    if let Some(lifecycle) = query.lifecycle.as_deref() {
        let lifecycle = lifecycle.to_ascii_lowercase();
        let matches = if lifecycle == "zombie" {
            health.leader_node_id.is_none()
        } else {
            format!("{:?}", health.lifecycle).eq_ignore_ascii_case(&lifecycle)
        };
        if !matches {
            return false;
        }
    }
    if query.zombie_only && health.leader_node_id.is_some() {
        return false;
    }
    if query.blocked_on_catch_up && !health.reconfiguration.blocked_on_catch_up {
        return false;
    }
    if query.pending_only && health.reconfiguration.phase.is_none() {
        return false;
    }
    if let Some(leader_node_id) = query.leader_node_id {
        if health.leader_node_id != Some(leader_node_id) {
            return false;
        }
    }
    true
}

fn parse_command_target_shard(target: &str) -> Option<greenmqtt_core::ServiceShardKey> {
    let mut parts = target.splitn(3, ':');
    let kind = match parts.next()? {
        "SessionDict" => greenmqtt_core::ServiceShardKind::SessionDict,
        "Inbox" => greenmqtt_core::ServiceShardKind::Inbox,
        "Inflight" => greenmqtt_core::ServiceShardKind::Inflight,
        "Dist" => greenmqtt_core::ServiceShardKind::Dist,
        "Retain" => greenmqtt_core::ServiceShardKind::Retain,
        _ => return None,
    };
    Some(greenmqtt_core::ServiceShardKey {
        kind,
        tenant_id: parts.next()?.to_string(),
        scope: parts.next()?.to_string(),
    })
}

async fn range_matches_structured_filters(
    registry: Option<&Arc<dyn ControlPlaneRegistry>>,
    health: &RangeHealthSnapshot,
    query: &RangeListQuery,
) -> Result<bool, ApiError> {
    let Some(registry) = registry else {
        return Ok(query.owner_node_id.is_none()
            && query.tenant_id.is_none()
            && query.service_kind.is_none());
    };
    let Some(descriptor) = registry
        .resolve_range(&health.range_id)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(false);
    };
    if let Some(owner_node_id) = query.owner_node_id {
        let assignment = registry
            .resolve_assignment(&descriptor.shard)
            .await
            .map_err(ApiError::from)?;
        if assignment
            .as_ref()
            .map(|assignment| assignment.owner_node_id())
            != Some(owner_node_id)
        {
            return Ok(false);
        }
    }
    if let Some(tenant_id) = query.tenant_id.as_deref() {
        if descriptor.shard.tenant_id != tenant_id {
            return Ok(false);
        }
    }
    if let Some(service_kind) = query.service_kind.as_deref() {
        if !format!("{:?}", descriptor.shard.kind).eq_ignore_ascii_case(service_kind) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn metadata_fallback_health(
    registry: &Arc<dyn ControlPlaneRegistry>,
) -> Result<Vec<RangeHealthSnapshot>, ApiError> {
    let mut ranges = Vec::new();
    for descriptor in registry.list_ranges(None).await.map_err(ApiError::from)? {
        ranges.push(RangeHealthSnapshot {
            range_id: descriptor.id.clone(),
            lifecycle: descriptor.lifecycle.clone(),
            role: greenmqtt_kv_raft::RaftNodeRole::Follower,
            current_term: 0,
            leader_node_id: descriptor.leader_node_id,
            commit_index: descriptor.commit_index,
            applied_index: descriptor.applied_index,
            latest_snapshot_index: None,
            replica_lag: Vec::new(),
            reconfiguration: registry
                .resolve_reconfiguration_state(&descriptor.id)
                .await
                .map_err(ApiError::from)?
                .unwrap_or_default(),
        });
    }
    Ok(ranges)
}

async fn record_manual_range_command(
    registry: Option<&Arc<dyn ControlPlaneRegistry>>,
    operation: &str,
    range_id: &str,
    issued_by: &str,
    mode: &str,
    target: Option<(u64, String)>,
) -> Result<Option<String>, ApiError> {
    let Some(registry) = registry else {
        return Ok(None);
    };
    let correlation_id = format!("manual:{operation}:{range_id}");
    let command_id = format!(
        "{correlation_id}:{}:{}",
        crate::admin::current_time_ms(),
        MANUAL_COMMAND_SEQ.fetch_add(1, Ordering::SeqCst)
    );
    let mut payload = BTreeMap::from([
        ("correlation_id".into(), correlation_id),
        ("mode".into(), mode.to_string()),
    ]);
    if let Some((target_node_id, target_endpoint)) = target {
        payload.insert("target_node_id".into(), target_node_id.to_string());
        payload.insert("target_endpoint".into(), target_endpoint);
    }
    registry
        .upsert_control_command(ControlCommandRecord {
            command_id: command_id.clone(),
            command_type: operation.to_string(),
            target_range_or_shard: range_id.to_string(),
            issued_at_ms: crate::admin::current_time_ms(),
            issued_by: issued_by.to_string(),
            attempt_count: 0,
            payload,
            execution_state: ControlCommandExecutionState::Applied,
            reflection_state: ControlCommandReflectionState::Pending,
            last_error: None,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Some(command_id))
}

pub(crate) async fn list_ranges<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RangeListQuery>,
) -> Result<Json<Vec<RangeHealthSnapshot>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let command_target = if let (Some(command_id), Some(registry)) =
        (query.command_id.as_deref(), range_routing.as_ref())
    {
        registry
            .resolve_control_command(command_id)
            .await
            .map_err(ApiError::from)?
            .map(|record| record.target_range_or_shard)
    } else {
        None
    };
    let command_target_shard = command_target
        .as_deref()
        .and_then(parse_command_target_shard);
    let mut entries = Vec::new();
    let live_entries = aggregate_range_health(range_runtime, range_routing.clone())
        .await
        .unwrap_or_default();
    let source_entries = if live_entries.is_empty() {
        if let Some(registry) = range_routing.as_ref() {
            metadata_fallback_health(registry).await?
        } else {
            Vec::new()
        }
    } else {
        live_entries
    };
    for health in source_entries.into_iter() {
        if let Some(target) = command_target.as_deref() {
            if health.range_id != target {
                if let Some(shard) = command_target_shard.as_ref() {
                    let Some(registry) = range_routing.as_ref() else {
                        continue;
                    };
                    let Some(descriptor) = registry
                        .resolve_range(&health.range_id)
                        .await
                        .map_err(ApiError::from)?
                    else {
                        continue;
                    };
                    if descriptor.shard != *shard {
                        continue;
                    }
                } else {
                    continue;
                }
            }
        }
        if !matches_range_filters(&health, &query) {
            continue;
        }
        if !range_matches_structured_filters(range_routing.as_ref(), &health, &query).await? {
            continue;
        }
        entries.push(health);
    }
    Ok(Json(entries))
}

pub(crate) async fn get_range<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Path(range_id): Path<String>,
) -> Result<Json<Option<RangeLookupReply>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let snapshot = aggregate_range_health(range_runtime, range_routing.clone())
        .await?
        .into_iter()
        .find(|entry| entry.range_id == range_id);
    if snapshot.is_some() {
        return Ok(Json(Some(RangeLookupReply {
            source: "live".into(),
            health: snapshot,
            descriptor: None,
            reconfiguration: None,
        })));
    }
    let fallback = if let Some(registry) = range_routing {
        if let Some(descriptor) = registry
            .resolve_range(&range_id)
            .await
            .map_err(ApiError::from)?
        {
            Some(RangeLookupReply {
                source: "metadata_fallback".into(),
                health: None,
                descriptor: Some(descriptor),
                reconfiguration: registry
                    .resolve_reconfiguration_state(&range_id)
                    .await
                    .map_err(ApiError::from)?,
            })
        } else {
            None
        }
    } else {
        None
    };
    Ok(Json(fallback))
}

pub(crate) async fn bootstrap_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Json(request): Json<RangeBootstrapBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let (range_id, target) = if let Some(remote) = remote_bootstrap_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &request.descriptor,
    )
    .await?
    {
        (
            remote
                .client
                .bootstrap_range(request.descriptor)
                .await
                .map_err(ApiError::from)?,
            Some((remote.node_id, remote.endpoint)),
        )
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        (
            runtime
                .bootstrap_range(request.descriptor)
                .await
                .map_err(ApiError::from)?,
            None,
        )
    };
    broker.record_admin_audit(
        "range_bootstrap",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let mode = if target.is_some() {
        "forwarded"
    } else {
        "local"
    };
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "bootstrap",
        &range_id,
        "http",
        mode,
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "bootstrap",
        mode,
        "bootstrapped",
        "Range bootstrapped and marked serving.",
        target,
        command_id,
        Some("range_bootstrap".into()),
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn change_replicas<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeChangeReplicasBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let (status, reason, target) = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &range_id,
    )
    .await?
    {
        remote
            .client
            .change_replicas(&range_id, request.voters, request.learners)
            .await
            .map_err(ApiError::from)?;
        (
            "reconfiguring",
            "Replica change forwarded to the range owner; consult range health for staging or catch-up progress.",
            Some((remote.node_id, remote.endpoint)),
        )
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        runtime
            .change_replicas(&range_id, request.voters, request.learners)
            .await
            .map_err(ApiError::from)?;
        let (status, reason) = match runtime
            .reconfiguration_state(&range_id)
            .await
            .map_err(ApiError::from)?
            .and_then(|state| state.phase)
        {
            Some(greenmqtt_core::ReconfigurationPhase::StagingLearners) => (
                "staging-learners",
                "Replica change accepted; new targets are being staged as learners first.",
            ),
            Some(greenmqtt_core::ReconfigurationPhase::JointConsensus) => (
                "joint-consensus",
                "Replica change is in joint configuration; both the old and target voter sets must catch up before finalize.",
            ),
            Some(greenmqtt_core::ReconfigurationPhase::Finalizing) => (
                "finalizing",
                "Replica change accepted; waiting for catch-up before finalizing the target config.",
            ),
            None => (
                "replicas-updated",
                "Replica configuration already matches the requested target.",
            ),
        };
        (status, reason, None)
    };
    broker.record_admin_audit(
        "range_change_replicas",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "change_replicas",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "change-replicas",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        status,
        reason,
        target,
        command_id,
        Some("range_change_replicas".into()),
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn transfer_leadership<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeTransferLeadershipBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let target = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &range_id,
    )
    .await?
    {
        remote
            .client
            .transfer_leadership(&range_id, request.target_node_id)
            .await
            .map_err(ApiError::from)?;
        Some((remote.node_id, remote.endpoint))
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        runtime
            .transfer_leadership(&range_id, request.target_node_id)
            .await
            .map_err(ApiError::from)?;
        None
    };
    broker.record_admin_audit(
        "range_transfer_leadership",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "transfer_leadership",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "transfer-leadership",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        "leadership-updated",
        "Leadership transfer accepted; if the target already led the range this was a no-op.",
        target,
        command_id,
        Some("range_transfer_leadership".into()),
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn recover_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeRecoverBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let target = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &range_id,
    )
    .await?
    {
        remote
            .client
            .recover_range(&range_id, request.new_leader_node_id)
            .await
            .map_err(ApiError::from)?;
        Some((remote.node_id, remote.endpoint))
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        runtime
            .recover_range(&range_id, request.new_leader_node_id)
            .await
            .map_err(ApiError::from)?;
        None
    };
    broker.record_admin_audit(
        "range_recover",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "recover",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "recover",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        "recovered",
        "Recovery command accepted; the target leader is now authoritative or already was.",
        target,
        command_id,
        Some("range_recover".into()),
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn split_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeSplitBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let (left_range_id, right_range_id, target) = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &range_id,
    )
    .await?
    {
        let result = remote
            .client
            .split_range(&range_id, request.split_key)
            .await
            .map_err(ApiError::from)?;
        (
            result.left_range_id,
            result.right_range_id,
            Some((remote.node_id, remote.endpoint)),
        )
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        let (left, right) = runtime
            .split_range(&range_id, request.split_key)
            .await
            .map_err(ApiError::from)?;
        (left, right, None)
    };
    broker.record_admin_audit(
        "range_split",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "split",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "split",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        "split",
        "Range split completed; child ranges are now available.",
        target,
        command_id,
        Some("range_split".into()),
        Some(range_id),
        Some(left_range_id),
        Some(right_range_id),
    )))
}

pub(crate) async fn merge_ranges<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Json(request): Json<RangeMergeBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let (range_id, target) = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &request.left_range_id,
    )
    .await?
    {
        (
            remote
                .client
                .merge_ranges(&request.left_range_id, &request.right_range_id)
                .await
                .map_err(ApiError::from)?,
            Some((remote.node_id, remote.endpoint)),
        )
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        (
            runtime
                .merge_ranges(&request.left_range_id, &request.right_range_id)
                .await
                .map_err(ApiError::from)?,
            None,
        )
    };
    broker.record_admin_audit(
        "range_merge",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "merge",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "merge",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        "merged",
        "Sibling ranges were merged into a single serving range.",
        target,
        command_id,
        Some("range_merge".into()),
        Some(range_id),
        Some(request.left_range_id),
        Some(request.right_range_id),
    )))
}

pub(crate) async fn drain_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let target = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &range_id,
    )
    .await?
    {
        remote
            .client
            .drain_range(&range_id)
            .await
            .map_err(ApiError::from)?;
        Some((remote.node_id, remote.endpoint))
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        runtime
            .drain_range(&range_id)
            .await
            .map_err(ApiError::from)?;
        None
    };
    broker.record_admin_audit(
        "range_drain",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "drain",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "drain",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        "draining",
        "Range marked draining; writes should migrate away before retirement.",
        target,
        command_id,
        Some("range_drain".into()),
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn retire_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let target = if let Some(remote) = remote_range_control_client(
        &broker,
        range_runtime.clone(),
        range_routing.clone(),
        &range_id,
    )
    .await?
    {
        remote
            .client
            .retire_range(&range_id)
            .await
            .map_err(ApiError::from)?;
        Some((remote.node_id, remote.endpoint))
    } else {
        let runtime = require_range_runtime(range_runtime)?;
        runtime
            .retire_range(&range_id)
            .await
            .map_err(ApiError::from)?;
        None
    };
    broker.record_admin_audit(
        "range_retire",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    let command_id = record_manual_range_command(
        range_routing.as_ref(),
        "retire",
        &range_id,
        "http",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        target.clone(),
    )
    .await?;
    Ok(Json(range_action_reply(
        "retire",
        if target.is_some() {
            "forwarded"
        } else {
            "local"
        },
        "retired",
        "Range retired; repeated retire requests are treated as already applied.",
        target,
        command_id,
        Some("range_retire".into()),
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn list_zombie_ranges<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn ControlPlaneRegistry>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(mut query): Query<RangeListQuery>,
) -> Result<Json<Vec<ZombieRangeSnapshot>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    query.zombie_only = true;
    let entries = aggregate_range_health(range_runtime, range_routing)
        .await?
        .into_iter()
        .filter(|health| matches_range_filters(health, &query))
        .map(|health| ZombieRangeSnapshot {
            range_id: health.range_id,
            lifecycle: health.lifecycle,
            leader_node_id: health.leader_node_id,
        })
        .collect();
    Ok(Json(entries))
}
