use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use greenmqtt_broker::BrokerRuntime;
use greenmqtt_core::RouteRecord;
use greenmqtt_core::Subscription;
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{ApiError, PurgeReply};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubscribeRequest {
    pub session_id: String,
    pub topic_filter: String,
    pub qos: u8,
    #[serde(default)]
    pub subscription_identifier: Option<u32>,
    #[serde(default)]
    pub no_local: bool,
    #[serde(default)]
    pub retain_as_published: bool,
    #[serde(default)]
    pub retain_handling: u8,
    pub shared_group: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnsubscribeRequest {
    pub session_id: String,
    pub topic_filter: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SubscriptionListQuery {
    pub tenant_id: Option<String>,
    pub session_id: Option<String>,
    pub topic_filter: Option<String>,
    pub shared_group: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

pub(crate) async fn list_all_subscriptions<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SubscriptionListQuery>,
) -> Result<Json<Vec<Subscription>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(list_filtered_subscriptions(broker, &query).await?))
}

pub(crate) async fn purge_subscriptions<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SubscriptionListQuery>,
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
            "tenant_id, session_id, topic_filter, or shared_group is required to purge subscriptions"
        )));
    }
    if let (Some(session_id), Some(topic_filter), Some(shared_group)) = (
        query.session_id.as_deref(),
        query.topic_filter.as_deref(),
        query.shared_group.as_deref(),
    ) {
        let matched = broker
            .inbox
            .lookup_subscription(&session_id.to_string(), topic_filter, Some(shared_group))
            .await
            .map_err(ApiError::from)?;
        let matched = matched.filter(|subscription| {
            query
                .tenant_id
                .as_deref()
                .is_none_or(|tenant_id| subscription.tenant_id == tenant_id)
        });
        let removed = usize::from(matched.is_some());
        if query.dry_run {
            return Ok(Json(PurgeReply { removed }));
        }
        if let Some(subscription) = matched {
            broker
                .inbox
                .unsubscribe_shared(
                    &subscription.session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await
                .map_err(ApiError::from)?;
            broker
                .dist
                .remove_route(&RouteRecord {
                    tenant_id: subscription.tenant_id.clone(),
                    topic_filter: subscription.topic_filter.clone(),
                    session_id: subscription.session_id.clone(),
                    node_id: 0,
                    subscription_identifier: subscription.subscription_identifier,
                    no_local: subscription.no_local,
                    retain_as_published: subscription.retain_as_published,
                    shared_group: subscription.shared_group.clone(),
                    kind: subscription.kind,
                })
                .await
                .map_err(ApiError::from)?;
        }
        let mut details = BTreeMap::new();
        if let Some(tenant_id) = query.tenant_id.clone() {
            details.insert("tenant_id".into(), tenant_id);
        }
        details.insert("session_id".into(), session_id.to_string());
        details.insert("topic_filter".into(), topic_filter.to_string());
        details.insert("shared_group".into(), shared_group.to_string());
        details.insert("removed".into(), removed.to_string());
        broker.record_admin_audit("purge_subscriptions", "subscriptions", details);
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
                    .inbox
                    .count_session_subscriptions(&session_id.to_string())
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            (None, Some(session_id), Some(topic_filter)) => {
                let removed = broker
                    .inbox
                    .count_session_topic_subscriptions(&session_id.to_string(), topic_filter)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            (Some(tenant_id), None, None) => {
                let removed = broker
                    .inbox
                    .count_tenant_subscriptions(tenant_id)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(Json(PurgeReply { removed }));
            }
            (Some(tenant_id), None, Some(topic_filter)) => {
                let removed = broker
                    .inbox
                    .count_tenant_topic_subscriptions(tenant_id, topic_filter)
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
                .inbox
                .count_tenant_topic_shared_subscriptions(
                    tenant_id,
                    topic_filter,
                    Some(shared_group),
                )
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
                .inbox
                .count_tenant_shared_subscriptions(tenant_id, Some(shared_group))
                .await
                .map_err(ApiError::from)?;
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if !query.dry_run && query.tenant_id.is_none() && query.shared_group.is_none() {
        if let Some(session_id) = query.session_id.as_deref() {
            let session_id = session_id.to_string();
            if let Some(topic_filter) = query.topic_filter.as_deref() {
                let removed = broker
                    .inbox
                    .purge_session_topic_subscriptions(&session_id, topic_filter)
                    .await
                    .map_err(ApiError::from)?;
                broker
                    .dist
                    .remove_session_topic_filter_routes(&session_id, topic_filter)
                    .await
                    .map_err(ApiError::from)?;
                broker.record_admin_audit(
                    "purge_subscriptions",
                    "subscriptions",
                    BTreeMap::from([
                        ("session_id".into(), session_id),
                        ("topic_filter".into(), topic_filter.to_string()),
                        ("removed".into(), removed.to_string()),
                    ]),
                );
                return Ok(Json(PurgeReply { removed }));
            }
            let removed = broker
                .inbox
                .purge_session_subscriptions_only(&session_id)
                .await
                .map_err(ApiError::from)?;
            broker
                .dist
                .remove_session_routes(&session_id)
                .await
                .map_err(ApiError::from)?;
            broker.record_admin_audit(
                "purge_subscriptions",
                "subscriptions",
                BTreeMap::from([
                    ("session_id".into(), session_id),
                    ("removed".into(), removed.to_string()),
                ]),
            );
            return Ok(Json(PurgeReply { removed }));
        }
    }
    if !query.dry_run
        && query.session_id.is_none()
        && query.topic_filter.is_none()
        && query.shared_group.is_none()
    {
        if let Some(tenant_id) = query.tenant_id.as_deref() {
            let removed = broker
                .inbox
                .purge_tenant_subscriptions(tenant_id)
                .await
                .map_err(ApiError::from)?;
            broker
                .dist
                .remove_tenant_routes(tenant_id)
                .await
                .map_err(ApiError::from)?;
            broker.record_admin_audit(
                "purge_subscriptions",
                "subscriptions",
                BTreeMap::from([
                    ("tenant_id".into(), tenant_id.to_string()),
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
                .inbox
                .purge_tenant_topic_subscriptions(tenant_id, topic_filter)
                .await
                .map_err(ApiError::from)?;
            broker
                .dist
                .remove_topic_filter_routes(tenant_id, topic_filter)
                .await
                .map_err(ApiError::from)?;
            let mut details = BTreeMap::new();
            details.insert("tenant_id".into(), tenant_id.to_string());
            details.insert("topic_filter".into(), topic_filter.to_string());
            details.insert("removed".into(), removed.to_string());
            broker.record_admin_audit("purge_subscriptions", "subscriptions", details);
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
                .inbox
                .purge_tenant_topic_shared_subscriptions(
                    tenant_id,
                    topic_filter,
                    Some(shared_group),
                )
                .await
                .map_err(ApiError::from)?;
            broker
                .dist
                .remove_topic_filter_shared_routes(tenant_id, topic_filter, Some(shared_group))
                .await
                .map_err(ApiError::from)?;
            let mut details = BTreeMap::new();
            details.insert("tenant_id".into(), tenant_id.to_string());
            details.insert("topic_filter".into(), topic_filter.to_string());
            details.insert("shared_group".into(), shared_group.to_string());
            details.insert("removed".into(), removed.to_string());
            broker.record_admin_audit("purge_subscriptions", "subscriptions", details);
            return Ok(Json(PurgeReply { removed }));
        }
    }
    let matched = list_filtered_subscriptions(broker.clone(), &query).await?;
    let removed = matched.len();
    if !query.dry_run {
        for subscription in matched {
            broker
                .inbox
                .unsubscribe_shared(
                    &subscription.session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await
                .map_err(ApiError::from)?;
            broker
                .dist
                .remove_route(&RouteRecord {
                    tenant_id: subscription.tenant_id,
                    topic_filter: subscription.topic_filter,
                    session_id: subscription.session_id,
                    node_id: 0,
                    subscription_identifier: subscription.subscription_identifier,
                    no_local: subscription.no_local,
                    retain_as_published: subscription.retain_as_published,
                    shared_group: subscription.shared_group,
                    kind: subscription.kind,
                })
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
        broker.record_admin_audit("purge_subscriptions", "subscriptions", details);
    }
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn list_filtered_subscriptions<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    query: &SubscriptionListQuery,
) -> Result<Vec<Subscription>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut subscriptions = if let (Some(session_id), Some(topic_filter), Some(shared_group)) = (
        query.session_id.as_deref(),
        query.topic_filter.as_deref(),
        query.shared_group.as_deref(),
    ) {
        broker
            .inbox
            .lookup_subscription(&session_id.to_string(), topic_filter, Some(shared_group))
            .await
            .map_err(ApiError::from)?
            .into_iter()
            .collect()
    } else if let (Some(session_id), Some(topic_filter)) =
        (query.session_id.as_deref(), query.topic_filter.as_deref())
    {
        broker
            .inbox
            .list_session_topic_subscriptions(&session_id.to_string(), topic_filter)
            .await
            .map_err(ApiError::from)?
    } else if let Some(session_id) = query.session_id.as_ref() {
        broker
            .inbox
            .list_subscriptions(session_id)
            .await
            .map_err(ApiError::from)?
    } else if let (Some(tenant_id), Some(topic_filter), Some(shared_group)) = (
        query.tenant_id.as_deref(),
        query.topic_filter.as_deref(),
        query.shared_group.as_deref(),
    ) {
        broker
            .inbox
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, Some(shared_group))
            .await
            .map_err(ApiError::from)?
    } else if let (Some(tenant_id), Some(shared_group)) =
        (query.tenant_id.as_deref(), query.shared_group.as_deref())
    {
        broker
            .inbox
            .list_tenant_shared_subscriptions(tenant_id, Some(shared_group))
            .await
            .map_err(ApiError::from)?
    } else if let (Some(tenant_id), Some(topic_filter)) =
        (query.tenant_id.as_deref(), query.topic_filter.as_deref())
    {
        broker
            .inbox
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await
            .map_err(ApiError::from)?
    } else if let Some(tenant_id) = query.tenant_id.as_deref() {
        broker
            .inbox
            .list_tenant_subscriptions(tenant_id)
            .await
            .map_err(ApiError::from)?
    } else {
        broker
            .inbox
            .list_all_subscriptions()
            .await
            .map_err(ApiError::from)?
    };
    if let Some(tenant_id) = query.tenant_id.as_deref() {
        subscriptions.retain(|subscription| subscription.tenant_id == tenant_id);
    }
    if let Some(topic_filter) = query.topic_filter.as_deref() {
        subscriptions.retain(|subscription| subscription.topic_filter == topic_filter);
    }
    if let Some(shared_group) = query.shared_group.as_deref() {
        subscriptions
            .retain(|subscription| subscription.shared_group.as_deref() == Some(shared_group));
    }
    Ok(subscriptions)
}

pub(crate) async fn subscribe<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(request): Json<SubscribeRequest>,
) -> Result<StatusCode, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    broker
        .subscribe(
            &request.session_id,
            &request.topic_filter,
            request.qos,
            request.subscription_identifier,
            request.no_local,
            request.retain_as_published,
            request.retain_handling,
            request.shared_group,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

pub(crate) async fn unsubscribe<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(request): Json<UnsubscribeRequest>,
) -> Result<StatusCode, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    broker
        .unsubscribe(&request.session_id, &request.topic_filter)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}
