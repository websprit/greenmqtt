use axum::{
    extract::{Path, Query, State},
    Extension,
    Json,
};
use greenmqtt_broker::{AdminAuditEntry, BrokerRuntime};
use greenmqtt_core::{
    ControlCommandExecutionState, ControlCommandRecord, ControlCommandReflectionState,
    InflightMessage, OfflineMessage, PublishRequest, RetainedMessage, RouteRecord, TenantQuota,
    TenantUsageSnapshot,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use metrics::counter;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{ApiError, MessageListQuery, PurgeReply, RouteListQuery};

pub(crate) fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_millis() as u64
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetainDeleteQuery {
    pub tenant_id: String,
    pub topic: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TenantOnlyQuery {
    pub tenant_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AuditQuery {
    pub limit: Option<usize>,
    pub action: Option<String>,
    pub target: Option<String>,
    pub since_seq: Option<u64>,
    pub before_seq: Option<u64>,
    pub since_timestamp_ms: Option<u64>,
    pub details_key: Option<String>,
    pub details_value: Option<String>,
    #[serde(default)]
    pub shard_only: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TenantQuotaResponse {
    pub tenant_id: String,
    pub quota: Option<TenantQuota>,
    pub usage: Option<TenantUsageSnapshot>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InboxSendLwtBody {
    pub tenant_id: String,
    pub session_id: String,
    pub publish: PublishRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InboxTenantOperationBody {
    pub tenant_id: String,
    pub now_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InboxMaintenanceResponse {
    pub subscriptions: usize,
    pub offline_messages: usize,
    pub inflight_messages: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ControlCommandQuery {
    pub execution_state: Option<String>,
    pub reflection_state: Option<String>,
    pub issued_by: Option<String>,
    pub target_range_or_shard: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ControlCommandFailBody {
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ControlCommandPruneQuery {
    pub older_than_ms: u64,
}

pub(crate) async fn list_admin_audit<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<Vec<AdminAuditEntry>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut entries = broker.list_admin_audit_filtered(
        query.action.as_deref(),
        query.target.as_deref(),
        query.since_seq,
        query.before_seq,
        query.since_timestamp_ms,
        query.details_key.as_deref(),
        query.details_value.as_deref(),
        query.limit,
    );
    if query.shard_only {
        entries.retain(|entry| entry.action.starts_with("shard_"));
    }
    Ok(Json(entries))
}

fn matches_control_command_query(
    record: &ControlCommandRecord,
    query: &ControlCommandQuery,
) -> bool {
    if let Some(execution_state) = query.execution_state.as_deref() {
        if !format!("{:?}", record.execution_state).eq_ignore_ascii_case(execution_state) {
            return false;
        }
    }
    if let Some(reflection_state) = query.reflection_state.as_deref() {
        if !format!("{:?}", record.reflection_state).eq_ignore_ascii_case(reflection_state) {
            return false;
        }
    }
    if let Some(issued_by) = query.issued_by.as_deref() {
        if record.issued_by != issued_by {
            return false;
        }
    }
    if let Some(target) = query.target_range_or_shard.as_deref() {
        if record.target_range_or_shard != target {
            return false;
        }
    }
    true
}

pub(crate) async fn list_control_commands<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn greenmqtt_core::ControlPlaneRegistry>>>,
    Query(query): Query<ControlCommandQuery>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Vec<ControlCommandRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(registry) = range_routing else {
        return Ok(Json(Vec::new()));
    };
    let mut commands = registry.list_control_commands().await.map_err(ApiError::from)?;
    commands.retain(|record| matches_control_command_query(record, &query));
    Ok(Json(commands))
}

pub(crate) async fn get_control_command<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn greenmqtt_core::ControlPlaneRegistry>>>,
    Path(command_id): Path<String>,
    State(_broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Option<ControlCommandRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(registry) = range_routing else {
        return Ok(Json(None));
    };
    Ok(Json(
        registry
            .resolve_control_command(&command_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn retry_control_command<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn greenmqtt_core::ControlPlaneRegistry>>>,
    Path(command_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Option<ControlCommandRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(registry) = range_routing else {
        return Ok(Json(None));
    };
    let Some(mut record) = registry
        .resolve_control_command(&command_id)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(Json(None));
    };
    record.execution_state = ControlCommandExecutionState::Issued;
    record.reflection_state = ControlCommandReflectionState::Pending;
    record.last_error = None;
    registry
        .upsert_control_command(record.clone())
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "control_command_retry",
        "control_commands",
        BTreeMap::from([("command_id".into(), command_id)]),
    );
    Ok(Json(Some(record)))
}

pub(crate) async fn fail_control_command<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn greenmqtt_core::ControlPlaneRegistry>>>,
    Path(command_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(body): Json<ControlCommandFailBody>,
) -> Result<Json<Option<ControlCommandRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(registry) = range_routing else {
        return Ok(Json(None));
    };
    let Some(mut record) = registry
        .resolve_control_command(&command_id)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(Json(None));
    };
    record.execution_state = ControlCommandExecutionState::TerminalFailed;
    record.reflection_state = ControlCommandReflectionState::Unreflected;
    record.last_error = body.last_error.or_else(|| Some("manually marked terminal".into()));
    registry
        .upsert_control_command(record.clone())
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "control_command_fail",
        "control_commands",
        BTreeMap::from([("command_id".into(), command_id)]),
    );
    Ok(Json(Some(record)))
}

pub(crate) async fn prune_control_commands<A, C, H>(
    Extension(range_routing): Extension<Option<Arc<dyn greenmqtt_core::ControlPlaneRegistry>>>,
    Query(query): Query<ControlCommandPruneQuery>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(registry) = range_routing else {
        return Ok(Json(PurgeReply { removed: 0 }));
    };
    let cutoff = current_time_ms().saturating_sub(query.older_than_ms);
    let commands = registry.list_control_commands().await.map_err(ApiError::from)?;
    let mut removed = 0usize;
    for record in commands {
        if record.issued_at_ms < cutoff {
            let _ = registry
                .remove_control_command(&record.command_id)
                .await
                .map_err(ApiError::from)?;
            removed += 1;
        }
    }
    broker.record_admin_audit(
        "control_command_prune",
        "control_commands",
        BTreeMap::from([
            ("removed".into(), removed.to_string()),
            ("older_than_ms".into(), query.older_than_ms.to_string()),
        ]),
    );
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn get_tenant_quota<A, C, H>(
    Path(tenant_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<TenantQuotaResponse>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(TenantQuotaResponse {
        tenant_id: tenant_id.clone(),
        quota: broker.tenant_quota(&tenant_id),
        usage: broker.tenant_usage(&tenant_id),
    }))
}

pub(crate) async fn put_tenant_quota<A, C, H>(
    Path(tenant_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(quota): Json<TenantQuota>,
) -> Result<Json<TenantQuotaResponse>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    broker.set_tenant_quota(tenant_id.clone(), quota.clone());
    Ok(Json(TenantQuotaResponse {
        tenant_id: tenant_id.clone(),
        quota: Some(quota),
        usage: broker.tenant_usage(&tenant_id),
    }))
}

pub(crate) async fn delete_retained<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RetainDeleteQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let tenant_id = query.tenant_id.clone();
    let topic = query.topic.clone();
    let existed = broker
        .retain
        .lookup_topic(&tenant_id, &topic)
        .await
        .map_err(ApiError::from)?
        .is_some();
    if !query.dry_run {
        broker
            .retain
            .retain(RetainedMessage {
                tenant_id: tenant_id.clone(),
                topic: topic.clone(),
                payload: Vec::new().into(),
                qos: 0,
            })
            .await
            .map_err(ApiError::from)?;
        broker.record_admin_audit(
            "delete_retain",
            "retain",
            BTreeMap::from([
                ("tenant_id".into(), tenant_id),
                ("topic".into(), topic),
                ("removed".into(), usize::from(existed).to_string()),
            ]),
        );
    }
    Ok(Json(PurgeReply {
        removed: usize::from(existed),
    }))
}

pub(crate) async fn send_inbox_lwt<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(body): Json<InboxSendLwtBody>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    broker
        .publish(&body.session_id, body.publish)
        .await
        .map_err(ApiError::from)?;
    counter!("greenmqtt_inbox_delayed_lwt_dispatch_total").increment(1);
    broker.record_admin_audit(
        "inbox_send_lwt",
        "inbox",
        BTreeMap::from([
            ("tenant_id".into(), body.tenant_id),
            ("session_id".into(), body.session_id),
        ]),
    );
    Ok(Json(PurgeReply { removed: 1 }))
}

pub(crate) async fn expire_inbox_tenant<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(body): Json<InboxTenantOperationBody>,
) -> Result<Json<InboxMaintenanceResponse>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let stats = broker
        .inbox
        .expire_tenant_messages(&body.tenant_id, body.now_ms)
        .await
        .map_err(ApiError::from)?;
    counter!("greenmqtt_inbox_tenant_gc_total", "action" => "expire_all").increment(1);
    broker.record_admin_audit(
        "inbox_expire_all",
        "inbox",
        BTreeMap::from([
            ("tenant_id".into(), body.tenant_id),
            (
                "offline_messages".into(),
                stats.offline_messages.to_string(),
            ),
            (
                "inflight_messages".into(),
                stats.inflight_messages.to_string(),
            ),
        ]),
    );
    Ok(Json(InboxMaintenanceResponse {
        subscriptions: 0,
        offline_messages: stats.offline_messages,
        inflight_messages: stats.inflight_messages,
    }))
}

pub(crate) async fn preview_inbox_tenant_gc<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(body): Json<InboxTenantOperationBody>,
) -> Result<Json<InboxMaintenanceResponse>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(InboxMaintenanceResponse {
        subscriptions: broker
            .inbox
            .count_tenant_subscriptions(&body.tenant_id)
            .await
            .map_err(ApiError::from)?,
        offline_messages: broker
            .inbox
            .count_tenant_offline(&body.tenant_id)
            .await
            .map_err(ApiError::from)?,
        inflight_messages: broker
            .inbox
            .count_tenant_inflight(&body.tenant_id)
            .await
            .map_err(ApiError::from)?,
    }))
}

pub(crate) async fn run_inbox_tenant_gc<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(body): Json<InboxTenantOperationBody>,
) -> Result<Json<InboxMaintenanceResponse>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let stats = broker
        .inbox
        .expire_tenant_messages(&body.tenant_id, body.now_ms)
        .await
        .map_err(ApiError::from)?;
    counter!("greenmqtt_inbox_tenant_gc_total", "action" => "run").increment(1);
    broker.record_admin_audit(
        "inbox_tenant_gc",
        "inbox",
        BTreeMap::from([
            ("tenant_id".into(), body.tenant_id),
            (
                "offline_messages".into(),
                stats.offline_messages.to_string(),
            ),
            (
                "inflight_messages".into(),
                stats.inflight_messages.to_string(),
            ),
        ]),
    );
    Ok(Json(InboxMaintenanceResponse {
        subscriptions: 0,
        offline_messages: stats.offline_messages,
        inflight_messages: stats.inflight_messages,
    }))
}

pub(crate) async fn expire_retain_tenant<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<TenantOnlyQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let removed = greenmqtt_retain::retain_expire_all(broker.retain.as_ref(), &query.tenant_id)
        .await
        .map_err(ApiError::from)?;
    counter!("greenmqtt_retain_expire_sweep_total", "action" => "expire_all").increment(1);
    broker.record_admin_audit(
        "retain_expire_all",
        "retain",
        BTreeMap::from([
            ("tenant_id".into(), query.tenant_id),
            ("removed".into(), removed.to_string()),
        ]),
    );
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn preview_retain_tenant_gc<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<TenantOnlyQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let removed =
        greenmqtt_retain::retain_tenant_gc_preview(broker.retain.as_ref(), &query.tenant_id)
            .await
            .map_err(ApiError::from)?;
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn run_retain_tenant_gc<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<TenantOnlyQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let removed = greenmqtt_retain::retain_tenant_gc_run(broker.retain.as_ref(), &query.tenant_id)
        .await
        .map_err(ApiError::from)?;
    counter!("greenmqtt_retain_expire_sweep_total", "action" => "tenant_gc").increment(1);
    broker.record_admin_audit(
        "retain_tenant_gc",
        "retain",
        BTreeMap::from([
            ("tenant_id".into(), query.tenant_id),
            ("removed".into(), removed.to_string()),
        ]),
    );
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn purge_routes<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RouteListQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    if query.tenant_id.is_none()
        && query.session_id.is_none()
        && query.topic_filter.is_none()
        && query.shared_group.is_none()
    {
        return Err(ApiError(anyhow::anyhow!(
            "tenant_id, session_id, topic_filter, or shared_group is required to purge routes"
        )));
    }
    if let (Some(session_id), Some(topic_filter), Some(shared_group)) = (
        query.session_id.as_deref(),
        query.topic_filter.as_deref(),
        query.shared_group.as_deref(),
    ) {
        if query.dry_run {
            let removed = if query.tenant_id.is_none() {
                broker
                    .dist
                    .count_session_topic_filter_route(session_id, topic_filter, Some(shared_group))
                    .await
                    .map_err(ApiError::from)?
            } else {
                usize::from(
                    broker
                        .dist
                        .lookup_session_topic_filter_route(
                            session_id,
                            topic_filter,
                            Some(shared_group),
                        )
                        .await
                        .map_err(ApiError::from)?
                        .is_some_and(|route| {
                            route.tenant_id == query.tenant_id.as_deref().unwrap()
                        }),
                )
            };
            return Ok(Json(PurgeReply { removed }));
        }
        let matches_tenant = query.tenant_id.is_none()
            || broker
                .dist
                .lookup_session_topic_filter_route(session_id, topic_filter, Some(shared_group))
                .await
                .map_err(ApiError::from)?
                .is_some_and(|route| route.tenant_id == query.tenant_id.as_deref().unwrap());
        let removed = if matches_tenant {
            broker
                .dist
                .remove_session_topic_filter_route(session_id, topic_filter, Some(shared_group))
                .await
                .map_err(ApiError::from)?
        } else {
            0
        };
        let mut details = BTreeMap::new();
        if let Some(tenant_id) = query.tenant_id.clone() {
            details.insert("tenant_id".into(), tenant_id);
        }
        details.insert("session_id".into(), session_id.to_string());
        details.insert("topic_filter".into(), topic_filter.to_string());
        details.insert("shared_group".into(), shared_group.to_string());
        details.insert("removed".into(), removed.to_string());
        broker.record_admin_audit("purge_routes", "routes", details);
        return Ok(Json(PurgeReply { removed }));
    }
    if query.dry_run && query.shared_group.is_none() {
        match (
            query.tenant_id.as_deref(),
            query.session_id.as_deref(),
            query.topic_filter.as_deref(),
        ) {
            (None, Some(session_id), None) => {
                let removed = broker
                    .dist
                    .count_session_routes(session_id)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            (None, Some(session_id), Some(topic_filter)) => {
                let removed = broker
                    .dist
                    .count_session_topic_filter_routes(session_id, topic_filter)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            (Some(tenant_id), None, None) => {
                let removed = broker
                    .dist
                    .count_tenant_routes(tenant_id)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            (Some(tenant_id), None, Some(topic_filter)) => {
                let removed = broker
                    .dist
                    .count_topic_filter_routes(tenant_id, topic_filter)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            _ => {}
        }
    }
    if query.dry_run && query.session_id.is_none() {
        if let (Some(tenant_id), Some(topic_filter), Some(shared_group)) = (
            query.tenant_id.as_deref(),
            query.topic_filter.as_deref(),
            query.shared_group.as_deref(),
        ) {
            let removed = broker
                .dist
                .count_topic_filter_shared_routes(tenant_id, topic_filter, Some(shared_group))
                .await
                .map_err(ApiError::from)?;
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if query.dry_run
        && query.session_id.is_none()
        && query.topic_filter.is_none()
        && query.shared_group.is_some()
    {
        if let (Some(tenant_id), Some(shared_group)) =
            (query.tenant_id.as_deref(), query.shared_group.as_deref())
        {
            let removed = broker
                .dist
                .count_tenant_shared_routes(tenant_id, Some(shared_group))
                .await
                .map_err(ApiError::from)?;
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if !query.dry_run && query.tenant_id.is_none() && query.shared_group.is_none() {
        if let (Some(session_id), Some(topic_filter)) =
            (query.session_id.as_deref(), query.topic_filter.as_deref())
        {
            let removed = broker
                .dist
                .remove_session_topic_filter_routes(session_id, topic_filter)
                .await
                .map_err(ApiError::from)?;
            let mut details = BTreeMap::new();
            details.insert("session_id".into(), session_id.to_string());
            details.insert("topic_filter".into(), topic_filter.to_string());
            details.insert("removed".into(), removed.to_string());
            broker.record_admin_audit("purge_routes", "routes", details);
            return Ok(Json(PurgeReply { removed }));
        }
        if query.topic_filter.is_none() {
            if let Some(session_id) = query.session_id.as_deref() {
                let removed = broker
                    .dist
                    .remove_session_routes(session_id)
                    .await
                    .map_err(ApiError::from)?;
                let mut details = BTreeMap::new();
                details.insert("session_id".into(), session_id.to_string());
                details.insert("removed".into(), removed.to_string());
                broker.record_admin_audit("purge_routes", "routes", details);
                return Ok(Json(PurgeReply { removed }));
            }
        }
    }
    if !query.dry_run
        && query.session_id.is_none()
        && query.topic_filter.is_none()
        && query.shared_group.is_none()
    {
        if let Some(tenant_id) = query.tenant_id.as_deref() {
            let removed = broker
                .dist
                .remove_tenant_routes(tenant_id)
                .await
                .map_err(ApiError::from)?;
            broker.record_admin_audit(
                "purge_routes",
                "routes",
                BTreeMap::from([
                    ("tenant_id".into(), tenant_id.to_string()),
                    ("removed".into(), removed.to_string()),
                ]),
            );
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if !query.dry_run && query.session_id.is_none() && query.topic_filter.is_none() {
        if let (Some(tenant_id), Some(shared_group)) =
            (query.tenant_id.as_deref(), query.shared_group.as_deref())
        {
            let removed = broker
                .dist
                .remove_tenant_shared_routes(tenant_id, Some(shared_group))
                .await
                .map_err(ApiError::from)?;
            broker.record_admin_audit(
                "purge_routes",
                "routes",
                BTreeMap::from([
                    ("tenant_id".into(), tenant_id.to_string()),
                    ("shared_group".into(), shared_group.to_string()),
                    ("removed".into(), removed.to_string()),
                ]),
            );
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if !query.dry_run && query.session_id.is_none() && query.shared_group.is_none() {
        if let (Some(tenant_id), Some(topic_filter)) =
            (query.tenant_id.as_deref(), query.topic_filter.as_deref())
        {
            let removed = broker
                .dist
                .remove_topic_filter_routes(tenant_id, topic_filter)
                .await
                .map_err(ApiError::from)?;
            let mut details = BTreeMap::new();
            details.insert("tenant_id".into(), tenant_id.to_string());
            details.insert("topic_filter".into(), topic_filter.to_string());
            details.insert("removed".into(), removed.to_string());
            broker.record_admin_audit("purge_routes", "routes", details);
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if !query.dry_run && query.session_id.is_none() {
        if let (Some(tenant_id), Some(topic_filter), Some(shared_group)) = (
            query.tenant_id.as_deref(),
            query.topic_filter.as_deref(),
            query.shared_group.as_deref(),
        ) {
            let removed = broker
                .dist
                .remove_topic_filter_shared_routes(tenant_id, topic_filter, Some(shared_group))
                .await
                .map_err(ApiError::from)?;
            let mut details = BTreeMap::new();
            details.insert("tenant_id".into(), tenant_id.to_string());
            details.insert("topic_filter".into(), topic_filter.to_string());
            details.insert("shared_group".into(), shared_group.to_string());
            details.insert("removed".into(), removed.to_string());
            broker.record_admin_audit("purge_routes", "routes", details);
            return Ok(Json(PurgeReply { removed }));
        }
    }
    let matched = list_filtered_routes(broker.clone(), &query).await?;
    let removed = matched.len();
    if !query.dry_run {
        for route in &matched {
            broker
                .dist
                .remove_route(route)
                .await
                .map_err(ApiError::from)?;
        }
        let mut details = BTreeMap::new();
        if let Some(tenant_id) = query.tenant_id.clone() {
            details.insert("tenant_id".into(), tenant_id);
        }
        if let Some(session_id) = query.session_id.clone() {
            details.insert("session_id".into(), session_id);
        }
        if let Some(topic_filter) = query.topic_filter.clone() {
            details.insert("topic_filter".into(), topic_filter);
        }
        if let Some(shared_group) = query.shared_group.clone() {
            details.insert("shared_group".into(), shared_group);
        }
        details.insert("removed".into(), removed.to_string());
        broker.record_admin_audit("purge_routes", "routes", details);
    }
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn list_filtered_routes<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    query: &RouteListQuery,
) -> Result<Vec<RouteRecord>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut routes = if let (Some(session_id), Some(topic_filter), Some(shared_group)) = (
        query.session_id.as_deref(),
        query.topic_filter.as_deref(),
        query.shared_group.as_deref(),
    ) {
        broker
            .dist
            .lookup_session_topic_filter_route(session_id, topic_filter, Some(shared_group))
            .await
            .map_err(ApiError::from)?
            .into_iter()
            .collect()
    } else if let (Some(session_id), Some(topic_filter)) =
        (query.session_id.as_deref(), query.topic_filter.as_deref())
    {
        broker
            .dist
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await
            .map_err(ApiError::from)?
    } else if let Some(session_id) = query.session_id.as_deref() {
        broker
            .dist
            .list_session_routes(session_id)
            .await
            .map_err(ApiError::from)?
    } else if let (Some(tenant_id), Some(topic_filter), Some(shared_group)) = (
        query.tenant_id.as_deref(),
        query.topic_filter.as_deref(),
        query.shared_group.as_deref(),
    ) {
        broker
            .dist
            .list_topic_filter_shared_routes(tenant_id, topic_filter, Some(shared_group))
            .await
            .map_err(ApiError::from)?
    } else if let (Some(tenant_id), Some(shared_group)) =
        (query.tenant_id.as_deref(), query.shared_group.as_deref())
    {
        broker
            .dist
            .list_tenant_shared_routes(tenant_id, Some(shared_group))
            .await
            .map_err(ApiError::from)?
    } else if let (Some(tenant_id), Some(topic_filter)) =
        (query.tenant_id.as_deref(), query.topic_filter.as_deref())
    {
        broker
            .dist
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await
            .map_err(ApiError::from)?
    } else {
        broker
            .dist
            .list_routes(query.tenant_id.as_deref())
            .await
            .map_err(ApiError::from)?
    };
    if let Some(tenant_id) = query.tenant_id.as_deref() {
        routes.retain(|route| route.tenant_id == tenant_id);
    }
    if let Some(topic_filter) = query.topic_filter.as_deref() {
        routes.retain(|route| route.topic_filter == topic_filter);
    }
    if let Some(shared_group) = query.shared_group.as_deref() {
        routes.retain(|route| route.shared_group.as_deref() == Some(shared_group));
    }
    Ok(routes)
}

pub(crate) async fn purge_offline_messages<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<MessageListQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    if query.tenant_id.is_none() && query.session_id.is_none() {
        return Err(ApiError(anyhow::anyhow!(
            "tenant_id or session_id is required to purge offline messages"
        )));
    }
    match (query.tenant_id.as_deref(), query.session_id.as_deref()) {
        (Some(tenant_id), None) => {
            let removed = if query.dry_run {
                broker
                    .inbox
                    .count_tenant_offline(tenant_id)
                    .await
                    .map_err(ApiError::from)?
            } else {
                broker
                    .inbox
                    .purge_tenant_offline(tenant_id)
                    .await
                    .map_err(ApiError::from)?
            };
            if !query.dry_run {
                broker.record_admin_audit(
                    "purge_offline",
                    "offline",
                    BTreeMap::from([
                        ("tenant_id".into(), tenant_id.to_string()),
                        ("removed".into(), removed.to_string()),
                    ]),
                );
            }
            return Ok(Json(PurgeReply { removed }));
        }
        (None, Some(session_id)) => {
            let session_id = session_id.to_string();
            let removed = if query.dry_run {
                broker
                    .inbox
                    .count_session_offline(&session_id)
                    .await
                    .map_err(ApiError::from)?
            } else {
                broker
                    .inbox
                    .purge_offline(&session_id)
                    .await
                    .map_err(ApiError::from)?
            };
            if !query.dry_run {
                broker.record_admin_audit(
                    "purge_offline",
                    "offline",
                    BTreeMap::from([
                        ("session_id".into(), session_id.to_string()),
                        ("removed".into(), removed.to_string()),
                    ]),
                );
            }
            return Ok(Json(PurgeReply { removed }));
        }
        (Some(tenant_id), Some(session_id)) => {
            if let Some(record) = broker
                .sessiondict
                .lookup_session(session_id)
                .await
                .map_err(ApiError::from)?
            {
                if record.identity.tenant_id == tenant_id {
                    let session_id = session_id.to_string();
                    let removed = if query.dry_run {
                        broker
                            .inbox
                            .count_session_offline(&session_id)
                            .await
                            .map_err(ApiError::from)?
                    } else {
                        broker
                            .inbox
                            .purge_offline(&session_id)
                            .await
                            .map_err(ApiError::from)?
                    };
                    if !query.dry_run {
                        broker.record_admin_audit(
                            "purge_offline",
                            "offline",
                            BTreeMap::from([
                                ("tenant_id".into(), tenant_id.to_string()),
                                ("session_id".into(), session_id.to_string()),
                                ("removed".into(), removed.to_string()),
                            ]),
                        );
                    }
                    return Ok(Json(PurgeReply { removed }));
                }
                if !query.dry_run {
                    broker.record_admin_audit(
                        "purge_offline",
                        "offline",
                        BTreeMap::from([
                            ("tenant_id".into(), tenant_id.to_string()),
                            ("session_id".into(), session_id.to_string()),
                            ("removed".into(), "0".into()),
                        ]),
                    );
                }
                return Ok(Json(PurgeReply { removed: 0 }));
            }
        }
        (None, None) => {}
    }
    let matched = list_filtered_offline(broker.clone(), &query).await?;
    let removed = matched.len();
    if !query.dry_run {
        let session_ids: BTreeSet<_> = matched
            .into_iter()
            .map(|message| message.session_id)
            .collect();
        let mut actually_removed = 0usize;
        for session_id in session_ids {
            actually_removed += broker
                .inbox
                .fetch(&session_id)
                .await
                .map_err(ApiError::from)?
                .len();
        }
        let mut details = BTreeMap::new();
        if let Some(tenant_id) = query.tenant_id.clone() {
            details.insert("tenant_id".into(), tenant_id);
        }
        if let Some(session_id) = query.session_id.clone() {
            details.insert("session_id".into(), session_id);
        }
        details.insert("removed".into(), actually_removed.to_string());
        broker.record_admin_audit("purge_offline", "offline", details);
        return Ok(Json(PurgeReply {
            removed: actually_removed,
        }));
    }
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn list_filtered_offline<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    query: &MessageListQuery,
) -> Result<Vec<OfflineMessage>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut messages = if let Some(session_id) = query.session_id.as_ref() {
        broker
            .inbox
            .peek(session_id)
            .await
            .map_err(ApiError::from)?
    } else if let Some(tenant_id) = query.tenant_id.as_deref() {
        broker
            .inbox
            .list_tenant_offline(tenant_id)
            .await
            .map_err(ApiError::from)?
    } else {
        broker
            .inbox
            .list_all_offline()
            .await
            .map_err(ApiError::from)?
    };
    if let Some(tenant_id) = query.tenant_id.as_deref() {
        messages.retain(|message| message.tenant_id == tenant_id);
    }
    Ok(messages)
}

pub(crate) async fn purge_inflight_messages<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<MessageListQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    if query.tenant_id.is_none() && query.session_id.is_none() {
        return Err(ApiError(anyhow::anyhow!(
            "tenant_id or session_id is required to purge inflight messages"
        )));
    }
    match (query.tenant_id.as_deref(), query.session_id.as_deref()) {
        (Some(tenant_id), None) => {
            let removed = if query.dry_run {
                broker
                    .inbox
                    .count_tenant_inflight(tenant_id)
                    .await
                    .map_err(ApiError::from)?
            } else {
                broker
                    .inbox
                    .purge_tenant_inflight(tenant_id)
                    .await
                    .map_err(ApiError::from)?
            };
            if !query.dry_run {
                broker.record_admin_audit(
                    "purge_inflight",
                    "inflight",
                    BTreeMap::from([
                        ("tenant_id".into(), tenant_id.to_string()),
                        ("removed".into(), removed.to_string()),
                    ]),
                );
            }
            return Ok(Json(PurgeReply { removed }));
        }
        (None, Some(session_id)) => {
            let session_id = session_id.to_string();
            let removed = if query.dry_run {
                broker
                    .inbox
                    .count_session_inflight(&session_id)
                    .await
                    .map_err(ApiError::from)?
            } else {
                broker
                    .inbox
                    .purge_inflight_session(&session_id)
                    .await
                    .map_err(ApiError::from)?
            };
            if !query.dry_run {
                broker.record_admin_audit(
                    "purge_inflight",
                    "inflight",
                    BTreeMap::from([
                        ("session_id".into(), session_id.to_string()),
                        ("removed".into(), removed.to_string()),
                    ]),
                );
            }
            return Ok(Json(PurgeReply { removed }));
        }
        (Some(tenant_id), Some(session_id)) => {
            if let Some(record) = broker
                .sessiondict
                .lookup_session(session_id)
                .await
                .map_err(ApiError::from)?
            {
                if record.identity.tenant_id == tenant_id {
                    let session_id = session_id.to_string();
                    let removed = if query.dry_run {
                        broker
                            .inbox
                            .count_session_inflight(&session_id)
                            .await
                            .map_err(ApiError::from)?
                    } else {
                        broker
                            .inbox
                            .purge_inflight_session(&session_id)
                            .await
                            .map_err(ApiError::from)?
                    };
                    if !query.dry_run {
                        broker.record_admin_audit(
                            "purge_inflight",
                            "inflight",
                            BTreeMap::from([
                                ("tenant_id".into(), tenant_id.to_string()),
                                ("session_id".into(), session_id.to_string()),
                                ("removed".into(), removed.to_string()),
                            ]),
                        );
                    }
                    return Ok(Json(PurgeReply { removed }));
                }
                if !query.dry_run {
                    broker.record_admin_audit(
                        "purge_inflight",
                        "inflight",
                        BTreeMap::from([
                            ("tenant_id".into(), tenant_id.to_string()),
                            ("session_id".into(), session_id.to_string()),
                            ("removed".into(), "0".into()),
                        ]),
                    );
                }
                return Ok(Json(PurgeReply { removed: 0 }));
            }
        }
        (None, None) => {}
    }
    let matched = list_filtered_inflight(broker.clone(), &query).await?;
    let removed = matched.len();
    if !query.dry_run {
        for message in matched {
            broker
                .inbox
                .ack_inflight(&message.session_id, message.packet_id)
                .await
                .map_err(ApiError::from)?;
        }
        let mut details = BTreeMap::new();
        if let Some(tenant_id) = query.tenant_id.clone() {
            details.insert("tenant_id".into(), tenant_id);
        }
        if let Some(session_id) = query.session_id.clone() {
            details.insert("session_id".into(), session_id);
        }
        details.insert("removed".into(), removed.to_string());
        broker.record_admin_audit("purge_inflight", "inflight", details);
    }
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn list_filtered_inflight<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    query: &MessageListQuery,
) -> Result<Vec<InflightMessage>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut messages = if let Some(session_id) = query.session_id.as_ref() {
        broker
            .inbox
            .fetch_inflight(session_id)
            .await
            .map_err(ApiError::from)?
    } else if let Some(tenant_id) = query.tenant_id.as_deref() {
        broker
            .inbox
            .list_tenant_inflight(tenant_id)
            .await
            .map_err(ApiError::from)?
    } else {
        broker
            .inbox
            .list_all_inflight()
            .await
            .map_err(ApiError::from)?
    };
    if let Some(tenant_id) = query.tenant_id.as_deref() {
        messages.retain(|message| message.tenant_id == tenant_id);
    }
    Ok(messages)
}
