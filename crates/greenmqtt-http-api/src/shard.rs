use axum::{
    extract::{Extension, Path, State},
    Json,
};
use greenmqtt_broker::BrokerRuntime;
use greenmqtt_core::{
    ServiceEndpoint, ServiceShardAssignment, ServiceShardKey, ServiceShardLifecycle,
    ServiceShardTransition, ServiceShardTransitionKind, ShardControlRegistry,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use metrics::counter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ApiError;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ShardActionRequest {
    pub target_node_id: Option<u64>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShardActionReply {
    pub previous: Option<ServiceShardAssignment>,
    pub current: Option<ServiceShardAssignment>,
}

pub(crate) async fn drain_shard<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(shards): Extension<Option<Arc<dyn ShardControlRegistry>>>,
    Path((kind, tenant_id, scope)): Path<(String, String, String)>,
    Json(request): Json<ShardActionRequest>,
) -> Result<Json<ShardActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let registry =
        shards.ok_or_else(|| ApiError::from(anyhow::anyhow!("shard registry unavailable")))?;
    let shard = shard_key(&kind, tenant_id, scope)?;
    let previous = registry
        .resolve_assignment(&shard)
        .await
        .map_err(ApiError::from)?;
    let current = previous.as_ref().map(|assignment| {
        ServiceShardAssignment::new(
            assignment.shard.clone(),
            assignment.endpoint.clone(),
            assignment.epoch + 1,
            assignment.fencing_token + 1,
            ServiceShardLifecycle::Draining,
        )
    });
    if !request.dry_run {
        if let Some(next) = current.clone() {
            registry
                .apply_transition(ServiceShardTransition::new(
                    ServiceShardTransitionKind::Migration,
                    shard,
                    previous
                        .as_ref()
                        .map(|assignment| assignment.owner_node_id()),
                    next.clone(),
                ))
                .await
                .map_err(ApiError::from)?;
            record_shard_metric("mqtt_shard_move_total", &next);
            broker.record_admin_audit("shard_drain", "shards", shard_audit_details(&next, None));
        }
    }
    Ok(Json(ShardActionReply { previous, current }))
}

pub(crate) async fn move_shard<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(shards): Extension<Option<Arc<dyn ShardControlRegistry>>>,
    Path((kind, tenant_id, scope)): Path<(String, String, String)>,
    Json(request): Json<ShardActionRequest>,
) -> Result<Json<ShardActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    transition_shard(
        broker,
        shards,
        kind,
        tenant_id,
        scope,
        request,
        ServiceShardTransitionKind::Migration,
        ServiceShardLifecycle::Draining,
        "shard_move",
    )
    .await
}

pub(crate) async fn catch_up_shard<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(shards): Extension<Option<Arc<dyn ShardControlRegistry>>>,
    Path((kind, tenant_id, scope)): Path<(String, String, String)>,
    Json(request): Json<ShardActionRequest>,
) -> Result<Json<ShardActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    transition_shard(
        broker,
        shards,
        kind,
        tenant_id,
        scope,
        request,
        ServiceShardTransitionKind::CatchUp,
        ServiceShardLifecycle::Recovering,
        "shard_catch_up",
    )
    .await
}

pub(crate) async fn repair_shard<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(shards): Extension<Option<Arc<dyn ShardControlRegistry>>>,
    Path((kind, tenant_id, scope)): Path<(String, String, String)>,
    Json(request): Json<ShardActionRequest>,
) -> Result<Json<ShardActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    transition_shard(
        broker,
        shards,
        kind,
        tenant_id,
        scope,
        request,
        ServiceShardTransitionKind::AntiEntropy,
        ServiceShardLifecycle::Serving,
        "shard_repair",
    )
    .await
}

pub(crate) async fn failover_shard<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(shards): Extension<Option<Arc<dyn ShardControlRegistry>>>,
    Path((kind, tenant_id, scope)): Path<(String, String, String)>,
    Json(request): Json<ShardActionRequest>,
) -> Result<Json<ShardActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    transition_shard(
        broker,
        shards,
        kind,
        tenant_id,
        scope,
        request,
        ServiceShardTransitionKind::Failover,
        ServiceShardLifecycle::Recovering,
        "shard_failover",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn transition_shard<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    shards: Option<Arc<dyn ShardControlRegistry>>,
    kind: String,
    tenant_id: String,
    scope: String,
    request: ShardActionRequest,
    transition_kind: ServiceShardTransitionKind,
    lifecycle: ServiceShardLifecycle,
    audit_action: &str,
) -> Result<Json<ShardActionReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let registry =
        shards.ok_or_else(|| ApiError::from(anyhow::anyhow!("shard registry unavailable")))?;
    let target_node_id = request
        .target_node_id
        .ok_or_else(|| ApiError::from(anyhow::anyhow!("target_node_id is required")))?;
    let shard = shard_key(&kind, tenant_id, scope)?;
    let previous = registry
        .resolve_assignment(&shard)
        .await
        .map_err(ApiError::from)?;
    let target_endpoint = target_service_endpoint(&*registry, &shard, target_node_id).await?;
    let current = previous.as_ref().map(|assignment| {
        ServiceShardAssignment::new(
            shard.clone(),
            target_endpoint,
            assignment.epoch + 1,
            assignment.fencing_token + 1,
            lifecycle,
        )
    });
    if !request.dry_run {
        if let Some(next) = current.clone() {
            let metric_name = match transition_kind {
                ServiceShardTransitionKind::Migration => Some("mqtt_shard_move_total"),
                ServiceShardTransitionKind::Failover => Some("mqtt_shard_failover_total"),
                ServiceShardTransitionKind::CatchUp | ServiceShardTransitionKind::AntiEntropy => {
                    Some("mqtt_shard_anti_entropy_total")
                }
                _ => None,
            };
            registry
                .apply_transition(ServiceShardTransition::new(
                    transition_kind,
                    shard,
                    previous
                        .as_ref()
                        .map(|assignment| assignment.owner_node_id()),
                    next.clone(),
                ))
                .await
                .map_err(ApiError::from)?;
            if let Some(metric_name) = metric_name {
                record_shard_metric(metric_name, &next);
            }
            broker.record_admin_audit(
                audit_action,
                "shards",
                shard_audit_details(&next, Some(target_node_id)),
            );
        }
    }
    Ok(Json(ShardActionReply { previous, current }))
}

async fn target_service_endpoint(
    registry: &dyn ShardControlRegistry,
    shard: &ServiceShardKey,
    target_node_id: u64,
) -> Result<ServiceEndpoint, ApiError> {
    let member = registry
        .resolve_member(target_node_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::from(anyhow::anyhow!("target node not found")))?;
    let service_kind = shard.service_kind();
    member
        .endpoints
        .iter()
        .find(|endpoint| endpoint.kind == service_kind)
        .cloned()
        .ok_or_else(|| {
            ApiError::from(anyhow::anyhow!(
                "target node has no matching service endpoint"
            ))
        })
}

fn shard_key(kind: &str, tenant_id: String, scope: String) -> Result<ServiceShardKey, ApiError> {
    let kind = match kind {
        "sessiondict" => greenmqtt_core::ServiceShardKind::SessionDict,
        "inbox" => greenmqtt_core::ServiceShardKind::Inbox,
        "inflight" => greenmqtt_core::ServiceShardKind::Inflight,
        "dist" => greenmqtt_core::ServiceShardKind::Dist,
        "retain" => greenmqtt_core::ServiceShardKind::Retain,
        _ => return Err(ApiError::from(anyhow::anyhow!("invalid shard kind"))),
    };
    Ok(ServiceShardKey {
        kind,
        tenant_id,
        scope,
    })
}

fn shard_audit_details(
    assignment: &ServiceShardAssignment,
    target_node_id: Option<u64>,
) -> BTreeMap<String, String> {
    let mut details = BTreeMap::from([
        (
            "kind".into(),
            format!("{:?}", assignment.shard.kind).to_lowercase(),
        ),
        ("tenant_id".into(), assignment.shard.tenant_id.clone()),
        ("scope".into(), assignment.shard.scope.clone()),
        (
            "owner_node_id".into(),
            assignment.owner_node_id().to_string(),
        ),
        ("epoch".into(), assignment.epoch.to_string()),
        ("fencing_token".into(), assignment.fencing_token.to_string()),
        (
            "lifecycle".into(),
            format!("{:?}", assignment.lifecycle).to_lowercase(),
        ),
    ]);
    if let Some(target_node_id) = target_node_id {
        details.insert("target_node_id".into(), target_node_id.to_string());
    }
    details
}

fn record_shard_metric(name: &'static str, assignment: &ServiceShardAssignment) {
    counter!(
        name,
        "kind" => format!("{:?}", assignment.shard.kind).to_lowercase(),
        "tenant_id" => assignment.shard.tenant_id.clone()
    )
    .increment(1);
}
