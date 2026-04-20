mod admin;
mod cluster;
mod http_api;
mod publish;
mod query;
mod range;
mod session;
mod shard;
mod subscription;

pub use admin::{AuditQuery, RetainDeleteQuery};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
pub use cluster::{PeerSummary, PeerUpsertQuery};
use greenmqtt_broker::BrokerRuntime;
use greenmqtt_core::{InflightMessage, OfflineMessage};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
pub use http_api::HttpApi;
pub use publish::SessionPublishRequest;
pub use query::{MetricsQuery, RetainQuery};
use serde::{Deserialize, Serialize};
pub use session::{
    ConnectQuery, DisconnectRequest, SessionDictQuery, SessionDictReassignQuery,
    SessionDictReassignReply, SessionIdentityQuery, SessionInboxStateReply,
};
use std::sync::Arc;
pub use subscription::{SubscribeRequest, SubscriptionListQuery, UnsubscribeRequest};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RouteQuery {
    pub tenant_id: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RouteListQuery {
    pub tenant_id: Option<String>,
    pub session_id: Option<String>,
    pub topic_filter: Option<String>,
    pub shared_group: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MessageListQuery {
    pub tenant_id: Option<String>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DryRunQuery {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ErrorBody {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PurgeReply {
    pub removed: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RangeListQuery {
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub zombie_only: bool,
    pub range_id_prefix: Option<String>,
    pub owner_node_id: Option<u64>,
    pub tenant_id: Option<String>,
    pub service_kind: Option<String>,
    pub command_id: Option<String>,
    #[serde(default)]
    pub blocked_on_catch_up: bool,
    #[serde(default)]
    pub pending_only: bool,
}

async fn healthz() -> &'static str {
    "ok"
}

async fn list_all_offline_messages<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<MessageListQuery>,
) -> Result<Json<Vec<OfflineMessage>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let messages = admin::list_filtered_offline(broker, &query).await?;
    Ok(Json(messages))
}

async fn list_all_inflight_messages<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<MessageListQuery>,
) -> Result<Json<Vec<InflightMessage>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let messages = admin::list_filtered_inflight(broker, &query).await?;
    Ok(Json(messages))
}

struct ApiError(anyhow::Error);

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: self.0.to_string(),
                server_reference: None,
                node_id: None,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests;
