mod admin;
mod cluster;
mod http_api;
mod publish;
mod query;
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
    SessionDictReassignReply, SessionIdentityQuery,
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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PurgeReply {
    pub removed: usize,
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
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests;
