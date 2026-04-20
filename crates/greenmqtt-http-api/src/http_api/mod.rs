use async_trait::async_trait;
use axum::{
    extract::DefaultBodyLimit,
    routing::{delete, get, post, put},
    Extension, Router,
};
use greenmqtt_broker::{BrokerRuntime, PeerRegistry};
use greenmqtt_kv_server::ReplicaRuntime;
use greenmqtt_core::{Lifecycle, ShardControlRegistry};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct HttpApi<A, C, H> {
    broker: Arc<BrokerRuntime<A, C, H>>,
    peers: Option<Arc<dyn PeerRegistry>>,
    shards: Option<Arc<dyn ShardControlRegistry>>,
    range_runtime: Option<Arc<ReplicaRuntime>>,
    metrics: Option<PrometheusHandle>,
    bind: SocketAddr,
}

impl<A, C, H> HttpApi<A, C, H>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    pub fn new(broker: Arc<BrokerRuntime<A, C, H>>, bind: SocketAddr) -> Self {
        Self {
            broker,
            peers: None,
            shards: None,
            range_runtime: None,
            metrics: None,
            bind,
        }
    }

    pub fn with_peers(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Arc<dyn PeerRegistry>,
        bind: SocketAddr,
    ) -> Self {
        Self {
            broker,
            peers: Some(peers),
            shards: None,
            range_runtime: None,
            metrics: None,
            bind,
        }
    }

    pub fn with_peers_shards_and_metrics(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Arc<dyn PeerRegistry>,
        shards: Arc<dyn ShardControlRegistry>,
        metrics: PrometheusHandle,
        bind: SocketAddr,
    ) -> Self {
        Self {
            broker,
            peers: Some(peers),
            shards: Some(shards),
            range_runtime: None,
            metrics: Some(metrics),
            bind,
        }
    }

    pub fn with_peers_shards_metrics_and_ranges(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Arc<dyn PeerRegistry>,
        shards: Arc<dyn ShardControlRegistry>,
        range_runtime: Arc<ReplicaRuntime>,
        metrics: PrometheusHandle,
        bind: SocketAddr,
    ) -> Self {
        Self {
            broker,
            peers: Some(peers),
            shards: Some(shards),
            range_runtime: Some(range_runtime),
            metrics: Some(metrics),
            bind,
        }
    }

    pub fn with_peers_and_metrics(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Arc<dyn PeerRegistry>,
        metrics: PrometheusHandle,
        bind: SocketAddr,
    ) -> Self {
        Self {
            broker,
            peers: Some(peers),
            shards: None,
            range_runtime: None,
            metrics: Some(metrics),
            bind,
        }
    }

    pub fn router(broker: Arc<BrokerRuntime<A, C, H>>) -> Router {
        Self::router_with_peers_shards_metrics_and_ranges(broker, None, None, None, None)
    }

    pub fn router_with_peers(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Option<Arc<dyn PeerRegistry>>,
    ) -> Router {
        Self::router_with_peers_shards_metrics_and_ranges(broker, peers, None, None, None)
    }

    pub fn router_with_peers_and_metrics(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Option<Arc<dyn PeerRegistry>>,
        metrics: Option<PrometheusHandle>,
    ) -> Router {
        Self::router_with_peers_shards_metrics_and_ranges(broker, peers, None, None, metrics)
    }

    pub fn router_with_peers_shards_and_metrics(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Option<Arc<dyn PeerRegistry>>,
        shards: Option<Arc<dyn ShardControlRegistry>>,
        metrics: Option<PrometheusHandle>,
    ) -> Router {
        Self::router_with_peers_shards_metrics_and_ranges(broker, peers, shards, None, metrics)
    }

    pub fn router_with_peers_shards_metrics_and_ranges(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Option<Arc<dyn PeerRegistry>>,
        shards: Option<Arc<dyn ShardControlRegistry>>,
        range_runtime: Option<Arc<ReplicaRuntime>>,
        metrics: Option<PrometheusHandle>,
    ) -> Router {
        Router::new()
            .layer(DefaultBodyLimit::disable())
            .route("/healthz", get(super::healthz))
            .route("/metrics", get(super::query::metrics))
            .route("/v1/shards", get(super::query::list_shards))
            .route(
                "/v1/shards/{kind}/{tenant_id}/{scope}",
                get(super::query::get_shard),
            )
            .route(
                "/v1/shards/{kind}/{tenant_id}/{scope}/drain",
                post(super::shard::drain_shard),
            )
            .route(
                "/v1/shards/{kind}/{tenant_id}/{scope}/move",
                post(super::shard::move_shard),
            )
            .route(
                "/v1/shards/{kind}/{tenant_id}/{scope}/catch-up",
                post(super::shard::catch_up_shard),
            )
            .route(
                "/v1/shards/{kind}/{tenant_id}/{scope}/repair",
                post(super::shard::repair_shard),
            )
            .route(
                "/v1/shards/{kind}/{tenant_id}/{scope}/failover",
                post(super::shard::failover_shard),
            )
            .route("/v1/ranges", get(super::range::list_ranges))
            .route("/v1/ranges/bootstrap", post(super::range::bootstrap_range))
            .route("/v1/ranges/merge", post(super::range::merge_ranges))
            .route("/v1/ranges/zombies", get(super::range::list_zombie_ranges))
            .route(
                "/v1/ranges/{range_id}/change-replicas",
                post(super::range::change_replicas),
            )
            .route(
                "/v1/ranges/{range_id}/transfer-leadership",
                post(super::range::transfer_leadership),
            )
            .route(
                "/v1/ranges/{range_id}/recover",
                post(super::range::recover_range),
            )
            .route("/v1/ranges/{range_id}/split", post(super::range::split_range))
            .route("/v1/ranges/{range_id}/drain", post(super::range::drain_range))
            .route("/v1/ranges/{range_id}", delete(super::range::retire_range))
            .route("/v1/audit", get(super::admin::list_admin_audit))
            .route(
                "/v1/tenants/{tenant_id}/quota",
                get(super::admin::get_tenant_quota).put(super::admin::put_tenant_quota),
            )
            .route("/v1/peers", get(super::cluster::list_peers))
            .route(
                "/v1/peers/{node_id}",
                put(super::cluster::upsert_peer).delete(super::cluster::delete_peer),
            )
            .route("/v1/sessions", get(super::session::list_sessions))
            .route(
                "/v1/sessions/{session_id}",
                delete(super::session::disconnect_session),
            )
            .route(
                "/v1/sessions/{session_id}/offline",
                get(super::session::list_offline_messages),
            )
            .route(
                "/v1/sessions/{session_id}/inflight",
                get(super::session::list_inflight_messages),
            )
            .route(
                "/v1/sessions/{session_id}/subscriptions",
                get(super::session::list_session_subscriptions),
            )
            .route(
                "/v1/subscriptions",
                get(super::subscription::list_all_subscriptions)
                    .delete(super::subscription::purge_subscriptions),
            )
            .route(
                "/v1/offline",
                get(super::list_all_offline_messages).delete(super::admin::purge_offline_messages),
            )
            .route(
                "/v1/inflight",
                get(super::list_all_inflight_messages)
                    .delete(super::admin::purge_inflight_messages),
            )
            .route("/v1/sessiondict", get(super::session::list_sessiondict))
            .route(
                "/v1/sessiondict/kill-all",
                post(super::session::kill_all_sessiondict_records),
            )
            .route(
                "/v1/sessiondict/by-identity",
                get(super::session::lookup_session_by_identity),
            )
            .route(
                "/v1/sessiondict/{session_id}",
                get(super::session::lookup_session_by_id)
                    .put(super::session::reassign_sessiondict_record)
                    .delete(super::session::delete_sessiondict_record),
            )
            .route(
                "/v1/sessiondict/{session_id}/inbox-state",
                get(super::session::get_session_inbox_state),
            )
            .route(
                "/v1/sessiondict/{session_id}/kill",
                post(super::session::kill_sessiondict_record),
            )
            .route(
                "/v1/sessiondict/{session_id}/takeover",
                post(super::session::takeover_sessiondict_record),
            )
            .route(
                "/v1/routes/all",
                get(super::query::list_all_routes).delete(super::admin::purge_routes),
            )
            .route(
                "/v1/retain",
                get(super::query::list_retained).delete(super::admin::delete_retained),
            )
            .route("/v1/routes", get(super::query::list_routes))
            .route("/v1/connect", post(super::session::connect))
            .route("/v1/disconnect", post(super::session::disconnect))
            .route("/v1/subscribe", post(super::subscription::subscribe))
            .route("/v1/unsubscribe", post(super::subscription::unsubscribe))
            .route("/v1/publish", post(super::publish::publish))
            .route("/v1/stats", get(super::query::stats))
            .route(
                "/v1/sessions/{session_id}/deliveries",
                get(super::session::drain_deliveries),
            )
            .layer(Extension(peers))
            .layer(Extension(shards))
            .layer(Extension(range_runtime))
            .layer(Extension(metrics))
            .with_state(broker)
    }
}

#[async_trait]
impl<A, C, H> Lifecycle for HttpApi<A, C, H>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    async fn start(&self) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        axum::serve(
            listener,
            Self::router_with_peers_shards_metrics_and_ranges(
                self.broker.clone(),
                self.peers.clone(),
                self.shards.clone(),
                self.range_runtime.clone(),
                self.metrics.clone(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
