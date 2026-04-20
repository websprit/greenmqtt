use axum::{
    extract::{Extension, Path, Query, State},
    Json,
};
use greenmqtt_broker::BrokerRuntime;
use greenmqtt_core::ReplicatedRangeDescriptor;
use greenmqtt_kv_server::{RangeHealthSnapshot, ReplicaRuntime, ZombieRangeSnapshot};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{ApiError, RangeListQuery};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RangeActionReply {
    pub operation: String,
    pub ok: bool,
    pub status: String,
    pub reason: String,
    pub range_id: Option<String>,
    pub left_range_id: Option<String>,
    pub right_range_id: Option<String>,
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

fn require_range_runtime(
    runtime: Option<Arc<ReplicaRuntime>>,
) -> Result<Arc<ReplicaRuntime>, ApiError> {
    runtime.ok_or_else(|| ApiError::from(anyhow::anyhow!("range runtime unavailable")))
}

fn range_action_reply(
    operation: &str,
    status: impl Into<String>,
    reason: impl Into<String>,
    range_id: Option<String>,
    left_range_id: Option<String>,
    right_range_id: Option<String>,
) -> RangeActionReply {
    RangeActionReply {
        operation: operation.to_string(),
        ok: true,
        status: status.into(),
        reason: reason.into(),
        range_id,
        left_range_id,
        right_range_id,
    }
}

fn matches_range_filters(
    health: &RangeHealthSnapshot,
    query: &RangeListQuery,
) -> bool {
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
    true
}

pub(crate) async fn list_ranges<A, C, H>(
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RangeListQuery>,
) -> Result<Json<Vec<RangeHealthSnapshot>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    let entries = runtime
        .health_snapshot()
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .filter(|health| matches_range_filters(health, &query))
        .collect();
    Ok(Json(entries))
}

pub(crate) async fn bootstrap_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Json(request): Json<RangeBootstrapBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    let range_id = runtime
        .bootstrap_range(request.descriptor)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_bootstrap",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "bootstrap",
        "bootstrapped",
        "Range bootstrapped and marked serving.",
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn change_replicas<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeChangeReplicasBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    runtime
        .change_replicas(&range_id, request.voters, request.learners)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_change_replicas",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
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
    Ok(Json(range_action_reply(
        "change-replicas",
        status,
        reason,
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn transfer_leadership<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeTransferLeadershipBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    runtime
        .transfer_leadership(&range_id, request.target_node_id)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_transfer_leadership",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "transfer-leadership",
        "leadership-updated",
        "Leadership transfer accepted; if the target already led the range this was a no-op.",
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn recover_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeRecoverBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    runtime
        .recover_range(&range_id, request.new_leader_node_id)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_recover",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "recover",
        "recovered",
        "Recovery command accepted; the target leader is now authoritative or already was.",
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn split_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
    Json(request): Json<RangeSplitBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    let (left_range_id, right_range_id) = runtime
        .split_range(&range_id, request.split_key)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_split",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "split",
        "split",
        "Range split completed; child ranges are now available.",
        Some(range_id),
        Some(left_range_id),
        Some(right_range_id),
    )))
}

pub(crate) async fn merge_ranges<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Json(request): Json<RangeMergeBody>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    let range_id = runtime
        .merge_ranges(&request.left_range_id, &request.right_range_id)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_merge",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "merge",
        "merged",
        "Sibling ranges were merged into a single serving range.",
        Some(range_id),
        Some(request.left_range_id),
        Some(request.right_range_id),
    )))
}

pub(crate) async fn drain_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    runtime.drain_range(&range_id).await.map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_drain",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "drain",
        "draining",
        "Range marked draining; writes should migrate away before retirement.",
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn retire_range<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    Path(range_id): Path<String>,
) -> Result<Json<RangeActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    runtime.retire_range(&range_id).await.map_err(ApiError::from)?;
    broker.record_admin_audit(
        "range_retire",
        "ranges",
        BTreeMap::from([("range_id".into(), range_id.clone())]),
    );
    Ok(Json(range_action_reply(
        "retire",
        "retired",
        "Range retired; repeated retire requests are treated as already applied.",
        Some(range_id),
        None,
        None,
    )))
}

pub(crate) async fn list_zombie_ranges<A, C, H>(
    Extension(range_runtime): Extension<Option<Arc<ReplicaRuntime>>>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(mut query): Query<RangeListQuery>,
) -> Result<Json<Vec<ZombieRangeSnapshot>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let runtime = require_range_runtime(range_runtime)?;
    query.zombie_only = true;
    let entries = runtime
        .health_snapshot()
        .await
        .map_err(ApiError::from)?
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
