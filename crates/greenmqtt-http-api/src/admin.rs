use axum::{
    extract::{Path, Query, State},
    Json,
};
use greenmqtt_broker::{AdminAuditEntry, BrokerRuntime};
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, TenantQuota, TenantUsageSnapshot,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{ApiError, MessageListQuery, PurgeReply, RouteListQuery};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetainDeleteQuery {
    pub tenant_id: String,
    pub topic: String,
    #[serde(default)]
    pub dry_run: bool,
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
