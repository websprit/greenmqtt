use axum::{
    extract::{Extension, Path, Query, State},
    Json,
};
use greenmqtt_broker::{BrokerRuntime, PeerRegistry};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{ApiError, DryRunQuery, PurgeReply};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PeerUpsertQuery {
    pub rpc_addr: String,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PeerSummary {
    pub node_id: u64,
    pub rpc_addr: Option<String>,
    #[serde(default)]
    pub connected: bool,
}

pub(crate) async fn list_peers(
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
) -> Result<Json<Vec<PeerSummary>>, ApiError> {
    let peers = peers
        .unwrap_or_else(|| Arc::new(greenmqtt_broker::LocalPeerForwarder) as Arc<dyn PeerRegistry>);
    let endpoints = peers.list_peer_endpoints();
    let mut nodes = peers.list_peer_nodes();
    nodes.sort_unstable();
    Ok(Json(
        nodes
            .into_iter()
            .map(|node_id| {
                let rpc_addr = endpoints.get(&node_id).cloned();
                PeerSummary {
                    node_id,
                    connected: rpc_addr.is_some(),
                    rpc_addr,
                }
            })
            .collect(),
    ))
}

pub(crate) async fn delete_peer<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Path(node_id): Path<u64>,
    Query(query): Query<DryRunQuery>,
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let existed = peers
        .as_ref()
        .map(|registry| registry.list_peer_nodes().contains(&node_id))
        .unwrap_or(false);
    let removed = peers
        .map(|registry| {
            if query.dry_run {
                usize::from(existed)
            } else {
                usize::from(registry.remove_peer_node(node_id))
            }
        })
        .unwrap_or(0);
    if !query.dry_run && removed > 0 {
        broker.record_admin_audit(
            "delete_peer",
            "peers",
            BTreeMap::from([
                ("node_id".into(), node_id.to_string()),
                ("removed".into(), removed.to_string()),
            ]),
        );
    }
    Ok(Json(PurgeReply { removed }))
}

pub(crate) async fn upsert_peer<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Path(node_id): Path<u64>,
    Query(query): Query<PeerUpsertQuery>,
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
) -> Result<Json<PurgeReply>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let Some(registry) = peers else {
        return Ok(Json(PurgeReply { removed: 0 }));
    };
    if !query.dry_run {
        registry
            .add_peer_node(node_id, query.rpc_addr.clone())
            .await
            .map_err(ApiError::from)?;
        broker.record_admin_audit(
            "upsert_peer",
            "peers",
            BTreeMap::from([
                ("node_id".into(), node_id.to_string()),
                ("rpc_addr".into(), query.rpc_addr.clone()),
                ("removed".into(), "1".into()),
            ]),
        );
    }
    Ok(Json(PurgeReply { removed: 1 }))
}
