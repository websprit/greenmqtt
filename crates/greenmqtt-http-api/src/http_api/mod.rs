use async_trait::async_trait;
use axum::{
    routing::{delete, get, post, put},
    Extension, Router,
};
use greenmqtt_broker::{BrokerRuntime, PeerRegistry};
use greenmqtt_core::Lifecycle;
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct HttpApi<A, C, H> {
    broker: Arc<BrokerRuntime<A, C, H>>,
    peers: Option<Arc<dyn PeerRegistry>>,
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
            metrics: None,
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
            metrics: Some(metrics),
            bind,
        }
    }

    pub fn router(broker: Arc<BrokerRuntime<A, C, H>>) -> Router {
        Self::router_with_peers_and_metrics(broker, None, None)
    }

    pub fn router_with_peers(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Option<Arc<dyn PeerRegistry>>,
    ) -> Router {
        Self::router_with_peers_and_metrics(broker, peers, None)
    }

    pub fn router_with_peers_and_metrics(
        broker: Arc<BrokerRuntime<A, C, H>>,
        peers: Option<Arc<dyn PeerRegistry>>,
        metrics: Option<PrometheusHandle>,
    ) -> Router {
        Router::new()
            .route("/healthz", get(super::healthz))
            .route("/metrics", get(super::query::metrics))
            .route("/v1/audit", get(super::admin::list_admin_audit))
            .route(
                "/v1/tenants/:tenant_id/quota",
                get(super::admin::get_tenant_quota).put(super::admin::put_tenant_quota),
            )
            .route("/v1/peers", get(super::cluster::list_peers))
            .route(
                "/v1/peers/:node_id",
                put(super::cluster::upsert_peer).delete(super::cluster::delete_peer),
            )
            .route("/v1/sessions", get(super::session::list_sessions))
            .route(
                "/v1/sessions/:session_id",
                delete(super::session::disconnect_session),
            )
            .route(
                "/v1/sessions/:session_id/offline",
                get(super::session::list_offline_messages),
            )
            .route(
                "/v1/sessions/:session_id/inflight",
                get(super::session::list_inflight_messages),
            )
            .route(
                "/v1/sessions/:session_id/subscriptions",
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
                "/v1/sessiondict/by-identity",
                get(super::session::lookup_session_by_identity),
            )
            .route(
                "/v1/sessiondict/:session_id",
                get(super::session::lookup_session_by_id)
                    .put(super::session::reassign_sessiondict_record)
                    .delete(super::session::delete_sessiondict_record),
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
                "/v1/sessions/:session_id/deliveries",
                get(super::session::drain_deliveries),
            )
            .layer(Extension(peers))
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
            Self::router_with_peers_and_metrics(
                self.broker.clone(),
                self.peers.clone(),
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
