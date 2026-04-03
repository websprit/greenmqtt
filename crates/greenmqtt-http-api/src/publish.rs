use axum::{extract::State, Json};
use greenmqtt_broker::BrokerRuntime;
use greenmqtt_core::{PublishOutcome, PublishRequest};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::ApiError;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionPublishRequest {
    pub session_id: String,
    pub publish: PublishRequest,
}

pub(crate) async fn publish<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Json(request): Json<SessionPublishRequest>,
) -> Result<Json<PublishOutcome>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .publish(&request.session_id, request.publish)
            .await
            .map_err(ApiError::from)?,
    ))
}
