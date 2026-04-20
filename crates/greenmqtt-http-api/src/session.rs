use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use greenmqtt_broker::{BrokerRuntime, PeerRegistry, SessionSummary};
use greenmqtt_core::{
    ClientIdentity, ConnectReply, ConnectRequest, Delivery, InflightMessage, OfflineMessage,
    SessionRecord, Subscription,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{ApiError, DryRunQuery, ErrorBody, PurgeReply};

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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SessionKillAllQuery {
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub client_id: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionDictReassignReply {
    pub updated_sessions: usize,
    pub updated_routes: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionInboxStateReply {
    pub session_id: String,
    pub session_present: bool,
    pub subscriptions: usize,
    pub offline_messages: usize,
    pub inflight_messages: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SessionTakeoverQuery {
    #[serde(default)]
    pub dry_run: bool,
    pub expiry_override_secs: Option<u32>,
}

fn redirect_response(
    error: impl Into<String>,
    server_reference: String,
    node_id: u64,
) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        Json(ErrorBody {
            error: error.into(),
            server_reference: Some(server_reference),
            node_id: Some(node_id),
        }),
    )
        .into_response()
}

fn remote_session_redirect(
    record: &SessionRecord,
    peers: Option<&Arc<dyn PeerRegistry>>,
    action: &str,
) -> Option<axum::response::Response> {
    let peers = peers?;
    let server_reference = peers.list_peer_endpoints().get(&record.node_id).cloned()?;
    Some(redirect_response(
        format!("{action} must be issued on the owning node"),
        server_reference,
        record.node_id,
    ))
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

pub(crate) async fn kill_sessiondict_record<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<DryRunQuery>,
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
) -> Result<axum::response::Response, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    if let Some(record) = broker
        .sessiondict
        .lookup_session(&session_id)
        .await
        .map_err(ApiError::from)?
    {
        if record.node_id != broker.config.node_id {
            if let Some(response) = remote_session_redirect(&record, peers.as_ref(), "kill") {
                return Ok(response);
            }
        }
    }

    let removed = if query.dry_run {
        usize::from(
            broker
                .sessiondict
                .lookup_session(&session_id)
                .await
                .map_err(ApiError::from)?
                .is_some(),
        )
    } else {
        usize::from(
            broker
                .kill_session_admin(&session_id)
                .await
                .map_err(ApiError::from)?,
        )
    };
    if !query.dry_run {
        broker.record_admin_audit(
            "kill_session",
            "sessiondict",
            BTreeMap::from([
                ("session_id".into(), session_id),
                ("removed".into(), removed.to_string()),
            ]),
        );
    }
    Ok(Json(PurgeReply { removed }).into_response())
}

pub(crate) async fn kill_all_sessiondict_records<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SessionKillAllQuery>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let matched = broker
        .sessiondict
        .list_sessions(query.tenant_id.as_deref())
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .filter(|record| {
            query
                .user_id
                .as_deref()
                .map(|value| record.identity.user_id == value)
                .unwrap_or(true)
        })
        .filter(|record| {
            query
                .client_id
                .as_deref()
                .map(|value| record.identity.client_id == value)
                .unwrap_or(true)
        })
        .count();

    let removed = if query.dry_run {
        matched
    } else {
        broker
            .kill_sessions_admin(
                query.tenant_id.as_deref(),
                query.user_id.as_deref(),
                query.client_id.as_deref(),
            )
            .await
            .map_err(ApiError::from)?
    };

    if !query.dry_run {
        let mut details = BTreeMap::from([("removed".into(), removed.to_string())]);
        if let Some(tenant_id) = query.tenant_id {
            details.insert("tenant_id".into(), tenant_id);
        }
        if let Some(user_id) = query.user_id {
            details.insert("user_id".into(), user_id);
        }
        if let Some(client_id) = query.client_id {
            details.insert("client_id".into(), client_id);
        }
        broker.record_admin_audit("kill_sessions", "sessiondict", details);
    }

    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn takeover_sessiondict_record<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<SessionTakeoverQuery>,
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
) -> Result<axum::response::Response, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let record = broker
        .sessiondict
        .lookup_session(&session_id)
        .await
        .map_err(ApiError::from)?;
    let Some(record) = record else {
        return Ok(Json(PurgeReply { removed: 0 }).into_response());
    };
    if record.node_id != broker.config.node_id {
        if let Some(response) = remote_session_redirect(&record, peers.as_ref(), "takeover") {
            return Ok(response);
        }
    }

    let removed = if query.dry_run {
        usize::from(broker.session_record(&session_id).await.map_err(ApiError::from)?.is_some())
    } else {
        usize::from(
            broker
                .takeover_session_admin(&session_id, query.expiry_override_secs)
                .await
                .map_err(ApiError::from)?,
        )
    };
    if !query.dry_run {
        let mut details = BTreeMap::from([
            ("session_id".into(), session_id.clone()),
            ("removed".into(), removed.to_string()),
        ]);
        if let Some(expiry_override_secs) = query.expiry_override_secs {
            details.insert(
                "expiry_override_secs".into(),
                expiry_override_secs.to_string(),
            );
        }
        broker.record_admin_audit("takeover_session", "sessiondict", details);
    }
    Ok(Json(PurgeReply { removed }).into_response())
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

pub(crate) async fn get_session_inbox_state<A, C, H>(
    Path(session_id): Path<String>,
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
) -> Result<Json<SessionInboxStateReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let session_present = broker
        .sessiondict
        .lookup_session(&session_id)
        .await
        .map_err(ApiError::from)?
        .is_some()
        || broker.session_record(&session_id).await.map_err(ApiError::from)?.is_some();
    let subscriptions = broker
        .inbox
        .list_subscriptions(&session_id)
        .await
        .map_err(ApiError::from)?
        .len();
    let offline_messages = broker
        .inbox
        .peek(&session_id)
        .await
        .map_err(ApiError::from)?
        .len();
    let inflight_messages = broker
        .inbox
        .fetch_inflight(&session_id)
        .await
        .map_err(ApiError::from)?
        .len();
    Ok(Json(SessionInboxStateReply {
        session_id,
        session_present,
        subscriptions,
        offline_messages,
        inflight_messages,
    }))
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
