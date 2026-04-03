use axum::{
    extract::{Extension, Query, State},
    http::header,
    response::IntoResponse,
    Json,
};
use greenmqtt_broker::{BrokerRuntime, BrokerStats, PeerRegistry};
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, SessionRecord, Subscription,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use sysinfo::{get_current_pid, System};

use crate::{admin, ApiError, RouteListQuery};

type TenantMetricRow<'a> = (&'a str, &'a str, usize);
type TenantMetricSet<'a> = (&'a str, &'a [TenantMetricRow<'a>]);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetainQuery {
    pub tenant_id: String,
    pub topic_filter: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RouteQuery {
    pub tenant_id: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MetricsQuery {
    pub tenant_id: Option<String>,
}

pub(crate) async fn stats<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
) -> Result<Json<BrokerStats>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut stats = broker.stats().await.map_err(ApiError::from)?;
    stats.peer_nodes = peers
        .map(|registry| registry.list_peer_nodes().len())
        .unwrap_or(0);
    Ok(Json(stats))
}

pub(crate) async fn list_retained<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RetainQuery>,
) -> Result<Json<Vec<RetainedMessage>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let retained = if query.topic_filter == "#" {
        broker
            .retain
            .list_tenant_retained(&query.tenant_id)
            .await
            .map_err(ApiError::from)?
    } else if !query.topic_filter.contains('#') && !query.topic_filter.contains('+') {
        broker
            .retain
            .lookup_topic(&query.tenant_id, &query.topic_filter)
            .await
            .map_err(ApiError::from)?
            .into_iter()
            .collect()
    } else {
        broker
            .retain
            .match_topic(&query.tenant_id, &query.topic_filter)
            .await
            .map_err(ApiError::from)?
    };
    Ok(Json(retained))
}

pub(crate) async fn list_routes<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RouteQuery>,
) -> Result<Json<Vec<RouteRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(
        broker
            .dist
            .match_topic(&query.tenant_id, &query.topic)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn list_all_routes<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Query(query): Query<RouteListQuery>,
) -> Result<Json<Vec<RouteRecord>>, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    Ok(Json(admin::list_filtered_routes(broker, &query).await?))
}

pub(crate) async fn metrics<A, C, H>(
    State(broker): State<Arc<BrokerRuntime<A, C, H>>>,
    Extension(peers): Extension<Option<Arc<dyn PeerRegistry>>>,
    Extension(prometheus): Extension<Option<PrometheusHandle>>,
    Query(query): Query<MetricsQuery>,
) -> Result<impl IntoResponse, ApiError>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut stats = broker.stats().await.map_err(ApiError::from)?;
    stats.peer_nodes = peers
        .map(|registry| registry.list_peer_nodes().len())
        .unwrap_or(0);
    let mut output = prometheus_metrics(&broker, &stats, query.tenant_id.as_deref())
        .await
        .map_err(ApiError::from)?;
    if query.tenant_id.is_none() {
        if let Some(handle) = prometheus {
            let rendered = handle.render();
            if !rendered.is_empty() {
                output = format!("{rendered}\n{output}");
            }
        }
    }
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    ))
}

async fn prometheus_metrics<A, C, H>(
    broker: &BrokerRuntime<A, C, H>,
    stats: &BrokerStats,
    tenant_id: Option<&str>,
) -> anyhow::Result<String>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    if let Some(tenant_id) = tenant_id {
        let tenant_session_records = broker.sessiondict.list_sessions(Some(tenant_id)).await?;
        let tenant_route_records = broker.dist.list_routes(Some(tenant_id)).await?;
        let tenant_subscriptions_records =
            broker.inbox.list_tenant_subscriptions(tenant_id).await?;
        let tenant_offline_messages = broker.inbox.list_tenant_offline(tenant_id).await?;
        let tenant_inflight_messages = broker.inbox.list_tenant_inflight(tenant_id).await?;
        let tenant_retained_messages = broker.retain.list_tenant_retained(tenant_id).await?;
        let tenant_metrics = [
            (
                "greenmqtt_tenant_session_records",
                "Current tenant-scoped global session records",
                tenant_session_records.len(),
            ),
            (
                "greenmqtt_tenant_route_records",
                "Current tenant-scoped global route records",
                tenant_route_records.len(),
            ),
            (
                "greenmqtt_tenant_subscription_records",
                "Current tenant-scoped global subscription records",
                tenant_subscriptions_records.len(),
            ),
            (
                "greenmqtt_tenant_offline_messages",
                "Current tenant-scoped offline messages",
                tenant_offline_messages.len(),
            ),
            (
                "greenmqtt_tenant_inflight_messages",
                "Current tenant-scoped inflight messages",
                tenant_inflight_messages.len(),
            ),
            (
                "greenmqtt_tenant_retained_messages",
                "Current tenant-scoped retained messages",
                tenant_retained_messages.len(),
            ),
            (
                "greenmqtt_tenant_session_bytes",
                "Approximate tenant-scoped session bytes",
                tenant_session_records
                    .iter()
                    .map(approx_session_record_bytes)
                    .sum(),
            ),
            (
                "greenmqtt_tenant_route_bytes",
                "Approximate tenant-scoped route bytes",
                tenant_route_records
                    .iter()
                    .map(approx_route_record_bytes)
                    .sum(),
            ),
            (
                "greenmqtt_tenant_subscription_bytes",
                "Approximate tenant-scoped subscription bytes",
                tenant_subscriptions_records
                    .iter()
                    .map(approx_subscription_bytes)
                    .sum(),
            ),
            (
                "greenmqtt_tenant_offline_bytes",
                "Approximate tenant-scoped offline message bytes",
                tenant_offline_messages
                    .iter()
                    .map(approx_offline_message_bytes)
                    .sum(),
            ),
            (
                "greenmqtt_tenant_inflight_bytes",
                "Approximate tenant-scoped inflight message bytes",
                tenant_inflight_messages
                    .iter()
                    .map(approx_inflight_message_bytes)
                    .sum(),
            ),
            (
                "greenmqtt_tenant_retained_bytes",
                "Approximate tenant-scoped retained message bytes",
                tenant_retained_messages
                    .iter()
                    .map(approx_retained_message_bytes)
                    .sum(),
            ),
        ];
        return Ok(prometheus_metrics_from_stats(
            broker,
            stats,
            Some((tenant_id, &tenant_metrics)),
        ));
    }
    let service_sessions = broker.sessiondict.list_sessions(None).await?;
    let service_routes = broker.dist.list_routes(None).await?;
    let service_subscriptions = broker.inbox.list_all_subscriptions().await?;
    let replay_offline = broker.inbox.list_all_offline().await?;
    let replay_inflight = broker.inbox.list_all_inflight().await?;
    let retained_tenants = service_sessions
        .iter()
        .map(|record| record.identity.tenant_id.clone())
        .chain(service_routes.iter().map(|route| route.tenant_id.clone()))
        .chain(
            service_subscriptions
                .iter()
                .map(|subscription| subscription.tenant_id.clone()),
        )
        .chain(
            replay_offline
                .iter()
                .map(|message| message.tenant_id.clone()),
        )
        .chain(
            replay_inflight
                .iter()
                .map(|message| message.tenant_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    let mut service_retained_messages = Vec::new();
    for tenant_id in retained_tenants {
        service_retained_messages.extend(broker.retain.list_tenant_retained(&tenant_id).await?);
    }
    let replay_window_entries = replay_offline.len() + replay_inflight.len();
    let replay_window_bytes = replay_offline
        .iter()
        .map(approx_offline_message_bytes)
        .sum::<usize>()
        + replay_inflight
            .iter()
            .map(approx_inflight_message_bytes)
            .sum::<usize>();
    let extra_metrics = [
        (
            "greenmqtt_replay_window_entries",
            "Current global replay-window entries",
            replay_window_entries,
        ),
        (
            "greenmqtt_replay_window_bytes",
            "Approximate global replay-window bytes",
            replay_window_bytes,
        ),
        (
            "greenmqtt_service_session_bytes",
            "Approximate global sessiondict bytes",
            service_sessions
                .iter()
                .map(approx_session_record_bytes)
                .sum(),
        ),
        (
            "greenmqtt_service_route_bytes",
            "Approximate global route bytes",
            service_routes.iter().map(approx_route_record_bytes).sum(),
        ),
        (
            "greenmqtt_service_subscription_bytes",
            "Approximate global subscription bytes",
            service_subscriptions
                .iter()
                .map(approx_subscription_bytes)
                .sum(),
        ),
        (
            "greenmqtt_service_offline_bytes",
            "Approximate global offline bytes",
            replay_offline
                .iter()
                .map(approx_offline_message_bytes)
                .sum(),
        ),
        (
            "greenmqtt_service_inflight_bytes",
            "Approximate global inflight bytes",
            replay_inflight
                .iter()
                .map(approx_inflight_message_bytes)
                .sum(),
        ),
        (
            "greenmqtt_service_retained_bytes",
            "Approximate global retained bytes",
            service_retained_messages
                .iter()
                .map(approx_retained_message_bytes)
                .sum(),
        ),
        (
            "greenmqtt_local_hot_state_entries",
            "Current broker-local hot-state entries",
            broker.local_hot_state_entries(),
        ),
        (
            "greenmqtt_local_hot_state_bytes",
            "Approximate broker-local hot-state bytes",
            broker.approximate_local_hot_state_bytes(),
        ),
        (
            "greenmqtt_pending_delayed_wills",
            "Current broker-local delayed will generations",
            broker.pending_delayed_will_count(),
        ),
    ];
    let mut output = prometheus_metrics_from_stats(broker, stats, None);
    append_prometheus_metrics(&mut output, &extra_metrics, None);
    Ok(output)
}

fn prometheus_metrics_from_stats<A, C, H>(
    broker: &BrokerRuntime<A, C, H>,
    stats: &BrokerStats,
    tenant_metrics: Option<TenantMetricSet<'_>>,
) -> String
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    let process_rss_bytes = current_process_rss_bytes();
    let process_cpu_usage = current_process_cpu_usage();
    let memory_pressure_level = broker.memory_pressure_level() as usize;
    let metrics = [
        (
            "greenmqtt_peer_nodes",
            "Current configured peer nodes",
            stats.peer_nodes,
        ),
        (
            "greenmqtt_local_online_sessions",
            "Current broker-local online sessions",
            stats.local_online_sessions,
        ),
        (
            "greenmqtt_local_persistent_sessions",
            "Current broker-local persistent sessions",
            stats.local_persistent_sessions,
        ),
        (
            "greenmqtt_local_transient_sessions",
            "Current broker-local transient sessions",
            stats.local_transient_sessions,
        ),
        (
            "greenmqtt_local_pending_deliveries",
            "Current broker-local pending deliveries",
            stats.local_pending_deliveries,
        ),
        (
            "greenmqtt_global_session_records",
            "Current global session records",
            stats.global_session_records,
        ),
        (
            "greenmqtt_sessions_count",
            "Current global session records",
            stats.global_session_records,
        ),
        (
            "greenmqtt_route_records",
            "Current global route records",
            stats.route_records,
        ),
        (
            "greenmqtt_subscription_records",
            "Current global subscription records",
            stats.subscription_records,
        ),
        (
            "greenmqtt_subscriptions_count",
            "Current global subscription records",
            stats.subscription_records,
        ),
        (
            "greenmqtt_offline_messages",
            "Current offline messages",
            stats.offline_messages,
        ),
        (
            "greenmqtt_inflight_messages",
            "Current inflight messages",
            stats.inflight_messages,
        ),
        (
            "greenmqtt_inflight_count",
            "Current inflight messages",
            stats.inflight_messages,
        ),
        (
            "greenmqtt_retained_messages",
            "Current retained messages",
            stats.retained_messages,
        ),
        (
            "greenmqtt_retained_messages_count",
            "Current retained messages",
            stats.retained_messages,
        ),
        (
            "greenmqtt_process_rss_bytes",
            "Current broker process RSS in bytes",
            process_rss_bytes as usize,
        ),
        (
            "greenmqtt_broker_rss_bytes",
            "Current broker process RSS in bytes",
            process_rss_bytes as usize,
        ),
        (
            "greenmqtt_memory_pressure_level",
            "Current broker memory pressure level",
            memory_pressure_level,
        ),
    ];
    let mut output = String::new();
    append_prometheus_metrics(&mut output, &metrics, None);
    append_prometheus_float_metric(
        &mut output,
        "greenmqtt_broker_cpu_usage",
        "Current broker process CPU usage percent",
        process_cpu_usage,
        None,
    );
    if let Some((tenant_id, tenant_metrics)) = tenant_metrics {
        append_prometheus_metrics(&mut output, tenant_metrics, Some(("tenant_id", tenant_id)));
    }
    output
}

fn append_prometheus_metrics(
    output: &mut String,
    metrics: &[(&str, &str, usize)],
    label: Option<(&str, &str)>,
) {
    for (name, help, value) in metrics {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push('\n');
        output.push_str("# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        if let Some((label_key, label_value)) = label {
            output.push('{');
            output.push_str(label_key);
            output.push_str("=\"");
            output.push_str(label_value);
            output.push_str("\"}");
        }
        output.push(' ');
        output.push_str(&value.to_string());
        output.push('\n');
    }
}

fn append_prometheus_float_metric(
    output: &mut String,
    name: &str,
    help: &str,
    value: f64,
    label: Option<(&str, &str)>,
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" gauge\n");
    output.push_str(name);
    if let Some((label_key, label_value)) = label {
        output.push('{');
        output.push_str(label_key);
        output.push_str("=\"");
        output.push_str(label_value);
        output.push_str("\"}");
    }
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

fn approx_string_bytes(value: &str) -> usize {
    value.len()
}
fn approx_option_string_bytes(value: Option<&String>) -> usize {
    value.map_or(0, |value| value.len())
}

fn approx_publish_properties_bytes(properties: &greenmqtt_core::PublishProperties) -> usize {
    properties.content_type.as_deref().map_or(0, str::len)
        + properties.response_topic.as_deref().map_or(0, str::len)
        + properties.correlation_data.as_ref().map_or(0, Vec::len)
        + properties.subscription_identifiers.len() * std::mem::size_of::<u32>()
        + properties
            .user_properties
            .iter()
            .map(|property| property.key.len() + property.value.len())
            .sum::<usize>()
}

fn approx_session_record_bytes(record: &SessionRecord) -> usize {
    approx_string_bytes(&record.session_id)
        + approx_string_bytes(&record.identity.tenant_id)
        + approx_string_bytes(&record.identity.user_id)
        + approx_string_bytes(&record.identity.client_id)
}
fn approx_subscription_bytes(subscription: &Subscription) -> usize {
    approx_string_bytes(&subscription.session_id)
        + approx_string_bytes(&subscription.tenant_id)
        + approx_string_bytes(&subscription.topic_filter)
        + approx_option_string_bytes(subscription.shared_group.as_ref())
}
fn approx_route_record_bytes(route: &RouteRecord) -> usize {
    approx_string_bytes(&route.tenant_id)
        + approx_string_bytes(&route.topic_filter)
        + approx_string_bytes(&route.session_id)
        + approx_option_string_bytes(route.shared_group.as_ref())
}
fn approx_retained_message_bytes(message: &RetainedMessage) -> usize {
    approx_string_bytes(&message.tenant_id)
        + approx_string_bytes(&message.topic)
        + message.payload.len()
}
fn approx_offline_message_bytes(message: &OfflineMessage) -> usize {
    approx_string_bytes(&message.tenant_id)
        + approx_string_bytes(&message.session_id)
        + approx_string_bytes(&message.topic)
        + approx_string_bytes(&message.from_session_id)
        + message.payload.len()
        + approx_publish_properties_bytes(&message.properties)
}
fn approx_inflight_message_bytes(message: &InflightMessage) -> usize {
    approx_string_bytes(&message.tenant_id)
        + approx_string_bytes(&message.session_id)
        + approx_string_bytes(&message.topic)
        + approx_string_bytes(&message.from_session_id)
        + message.payload.len()
        + approx_publish_properties_bytes(&message.properties)
}
fn current_process_rss_bytes() -> u64 {
    let Ok(pid) = get_current_pid() else {
        return 0;
    };
    let system = System::new_all();
    system
        .process(pid)
        .map(|process| process.memory())
        .unwrap_or(0)
}

fn current_process_cpu_usage() -> f64 {
    let Ok(pid) = get_current_pid() else {
        return 0.0;
    };
    let mut system = System::new_all();
    system.refresh_all();
    system
        .process(pid)
        .map(|process| f64::from(process.cpu_usage()))
        .unwrap_or(0.0)
}
