use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use greenmqtt_broker::{BrokerRuntime, SessionSummary};
use greenmqtt_core::{
    ClientIdentity, ConnectReply, ConnectRequest, Delivery, InflightMessage, OfflineMessage,
    SessionRecord, Subscription,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{ApiError, DryRunQuery, PurgeReply};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ConnectQuery {
    pub hydrate_replay: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DisconnectRequest {
    pub session_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionIdentityQuery {
    pub tenant_id: String,
    pub user_id: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionDictReassignQuery {
    pub node_id: u64,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SessionDictQuery {
    pub tenant_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionDictReassignReply {
    pub updated_sessions: usize,
    pub updated_routes: usize,
}

pub(crate) async fn list_sessions<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Vec<SessionSummary>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker.list_local_sessions().await.map_err(ApiError::from)?,
    ))
}

pub(crate) async fn list_session_subscriptions<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Vec<Subscription>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .inbox
            .list_subscriptions(&session_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn list_sessiondict<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SessionDictQuery>,
) -> Result<Json<Vec<SessionRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .sessiondict
            .list_sessions(query.tenant_id.as_deref())
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn lookup_session_by_identity<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SessionIdentityQuery>,
) -> Result<Json<Option<SessionRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .sessiondict
            .lookup_identity(&ClientIdentity {
                tenant_id: query.tenant_id,
                user_id: query.user_id,
                client_id: query.client_id,
            })
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn lookup_session_by_id<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Option<SessionRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .sessiondict
            .lookup_session(&session_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn delete_sessiondict_record<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<DryRunQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let existed = if query.dry_run {
        broker
            .sessiondict
            .lookup_session(&session_id)
            .await
            .map_err(ApiError::from)?
            .is_some()
    } else {
        broker
            .sessiondict
            .unregister(&session_id)
            .await
            .map_err(ApiError::from)?
            .is_some()
    };
    if !query.dry_run {
        broker.record_admin_audit(
            "delete_sessiondict",
            "sessiondict",
            BTreeMap::from([
                ("session_id".into(), session_id),
                ("removed".into(), usize::from(existed).to_string()),
            ]),
        );
    }
    Ok(Json(PurgeReply {
        removed: usize::from(existed),
    }))
}

pub(crate) async fn reassign_sessiondict_record<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SessionDictReassignQuery>,
) -> Result<Json<SessionDictReassignReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let session = broker
        .sessiondict
        .lookup_session(&session_id)
        .await
        .map_err(ApiError::from)?;
    let Some(session) = session else {
        return Ok(Json(SessionDictReassignReply {
            updated_sessions: 0,
            updated_routes: 0,
        }));
    };

    if !query.dry_run {
        let outcome = broker
            .reassign_known_session_node(session.clone(), query.node_id)
            .await
            .map_err(ApiError::from)?;
        broker.record_admin_audit(
            "reassign_sessiondict",
            "sessiondict",
            BTreeMap::from([
                ("session_id".into(), session_id),
                ("node_id".into(), query.node_id.to_string()),
                (
                    "updated_sessions".into(),
                    outcome.updated_sessions.to_string(),
                ),
                ("updated_routes".into(), outcome.updated_routes.to_string()),
            ]),
        );
        return Ok(Json(SessionDictReassignReply {
            updated_sessions: outcome.updated_sessions,
            updated_routes: outcome.updated_routes,
        }));
    }

    let updated_routes = broker
        .dist
        .count_session_routes(&session_id)
        .await
        .map_err(ApiError::from)?;
    let updated_sessions = usize::from(session.node_id != query.node_id);

    Ok(Json(SessionDictReassignReply {
        updated_sessions,
        updated_routes,
    }))
}

pub(crate) async fn list_offline_messages<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Vec<OfflineMessage>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .inbox
            .peek(&session_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn list_inflight_messages<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Vec<InflightMessage>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .inbox
            .fetch_inflight(&session_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn connect<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<ConnectQuery>,
    Json(request): Json<ConnectRequest>,
) -> Result<Json<ConnectReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let reply = if query.hydrate_replay == Some(false) {
        broker
            .connect_without_replay(request)
            .await
            .map_err(ApiError::from)?
    } else {
        broker.connect(request).await.map_err(ApiError::from)?
    };
    Ok(Json(reply))
}

pub(crate) async fn disconnect<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(request): Json<DisconnectRequest>,
) -> Result<StatusCode, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    broker
        .disconnect(&request.session_id)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), request.session_id)]),
    );
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn disconnect_session<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<StatusCode, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    broker
        .disconnect(&session_id)
        .await
        .map_err(ApiError::from)?;
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), session_id)]),
    );
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn drain_deliveries<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<Vec<Delivery>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .drain_deliveries(&session_id)
            .await
            .map_err(ApiError::from)?,
    ))
}
