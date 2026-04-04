use greenmqtt_broker::mqtt::{serve_quic, serve_tcp, serve_tls, serve_ws, serve_wss};
use greenmqtt_broker::{BrokerConfig, BrokerRuntime};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership,
    ConnectRequest, Lifecycle, PublishProperties, PublishRequest, ServiceEndpoint, ServiceKind,
    SessionKind,
};
use greenmqtt_dist::{DistHandle, DistRouter, PersistentDistHandle};
use greenmqtt_http_api::HttpApi;
use greenmqtt_inbox::{InboxHandle, InboxService, PersistentInboxHandle};
use greenmqtt_plugin_api::{
    AclAction, AclDecision, AclRule, BridgeEventHook, BridgeRule, ConfiguredAcl, ConfiguredAuth,
    ConfiguredEventHook, HookTarget, HttpAclConfig, HttpAclProvider, HttpAuthConfig,
    HttpAuthProvider, IdentityMatcher, StaticAclProvider, StaticAuthProvider,
    StaticEnhancedAuthProvider, TopicRewriteEventHook, TopicRewriteRule, WebHookConfig,
    WebHookEventHook, BRIDGE_CLIENT_ID_PREFIX,
};
use greenmqtt_retain::{PersistentRetainHandle, RetainHandle, RetainService};
use greenmqtt_rpc::{
    DistGrpcClient, InboxGrpcClient, RetainGrpcClient, RpcRuntime, SessionDictGrpcClient,
    StaticPeerForwarder, StaticServiceEndpointRegistry,
};
use greenmqtt_sessiondict::{PersistentSessionDictHandle, SessionDictHandle, SessionDirectory};
use greenmqtt_storage::{
    RedisInboxStore, RedisInflightStore, RedisRetainStore, RedisRouteStore, RedisSessionStore,
    RedisSubscriptionStore, RocksInboxStore, RocksInflightStore, RocksRetainStore, RocksRouteStore,
    RocksSessionStore, RocksSubscriptionStore, SledInboxStore, SledInflightStore, SledRetainStore,
    SledRouteStore, SledSessionStore, SledSubscriptionStore,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use sysinfo::{get_current_pid, System};

type AppBroker = Arc<BrokerRuntime<ConfiguredAuth, ConfiguredAcl, ConfiguredEventHook>>;
type AppStateServices = (
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
);
type CompareStateServices = (
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
    Option<PathBuf>,
);

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    qos: u8,
    scenario: BenchScenario,
}

#[derive(Debug, Clone, Copy)]
struct SoakConfig {
    iterations: usize,
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    qos: u8,
}

#[derive(Debug, Clone, Copy, Default)]
struct BenchThresholds {
    min_publishes_per_sec: Option<f64>,
    min_deliveries_per_sec: Option<f64>,
    max_pending_before_drain: Option<usize>,
    max_rss_bytes_after: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct SoakThresholds {
    max_final_global_sessions: Option<usize>,
    max_final_routes: Option<usize>,
    max_final_subscriptions: Option<usize>,
    max_final_offline_messages: Option<usize>,
    max_final_inflight_messages: Option<usize>,
    max_final_retained_messages: Option<usize>,
    max_peak_rss_bytes: Option<u64>,
    max_final_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BenchScenario {
    Live,
    OfflineReplay,
    RetainedReplay,
    SharedLive,
}

fn bench_topic_filter(scenario: BenchScenario) -> &'static str {
    match scenario {
        BenchScenario::Live | BenchScenario::OfflineReplay | BenchScenario::SharedLive => {
            "devices/+/state"
        }
        BenchScenario::RetainedReplay => "devices/+/state/+",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BenchReport {
    storage_backend: String,
    scenario: BenchScenario,
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    total_publishes: usize,
    expected_deliveries: usize,
    actual_deliveries: usize,
    matched_routes: usize,
    online_deliveries: usize,
    pending_before_drain: usize,
    global_session_records: usize,
    route_records: usize,
    subscription_records: usize,
    offline_messages_before_drain: usize,
    inflight_messages_before_drain: usize,
    retained_messages: usize,
    rss_bytes_after: u64,
    setup_ms: u128,
    publish_ms: u128,
    drain_ms: u128,
    elapsed_ms: u128,
    publishes_per_sec: f64,
    deliveries_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SoakReport {
    storage_backend: String,
    iterations: usize,
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    total_publishes: usize,
    total_deliveries: usize,
    max_local_online_sessions: usize,
    max_local_pending_deliveries: usize,
    max_global_session_records: usize,
    max_route_records: usize,
    max_subscription_records: usize,
    final_global_session_records: usize,
    final_route_records: usize,
    final_subscription_records: usize,
    final_offline_messages: usize,
    final_inflight_messages: usize,
    final_retained_messages: usize,
    max_rss_bytes: u64,
    final_rss_bytes: u64,
    elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CompareBenchReport {
    reports: Vec<BenchReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ProfileBenchReport {
    reports: Vec<BenchReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CompareSoakReport {
    reports: Vec<SoakReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ShardActionEnvelope {
    previous: Option<greenmqtt_core::ServiceShardAssignment>,
    current: Option<greenmqtt_core::ServiceShardAssignment>,
}

fn run_shard_command(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let addr = std::env::var("GREENMQTT_HTTP_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse::<SocketAddr>()?;
    let (method, path, body, output_mode) = shard_command_request(args)?;
    let response = http_request_body(addr, &method, &path, body.as_deref())?;
    match output_mode {
        OutputMode::Json => println!("{response}"),
        OutputMode::Text => println!("{}", render_shard_response_text(&response)?),
    }
    Ok(())
}

fn shard_command_request(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<(String, String, Option<String>, OutputMode)> {
    let subcommand = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing shard subcommand"))?;
    match subcommand.as_str() {
        "ls" => {
            let mut query = Vec::new();
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--kind" => {
                        let value = args
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --kind"))?;
                        query.push(format!("kind={value}"));
                    }
                    "--tenant-id" => {
                        let value = args
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?;
                        query.push(format!("tenant_id={value}"));
                    }
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard ls flag: {flag}"),
                }
            }
            let path = if query.is_empty() {
                "/v1/shards".to_string()
            } else {
                format!("/v1/shards?{}", query.join("&"))
            };
            Ok(("GET".into(), path, None, output_mode))
        }
        "audit" => {
            let mut limit = None;
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--limit" => {
                        limit = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --limit"))?,
                        );
                    }
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard audit flag: {flag}"),
                }
            }
            let path = match limit {
                Some(limit) => format!("/v1/audit?shard_only=true&limit={limit}"),
                None => "/v1/audit?shard_only=true".to_string(),
            };
            Ok(("GET".into(), path, None, output_mode))
        }
        "drain" => {
            let kind = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard kind"))?;
            let tenant_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing tenant_id"))?;
            let scope = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard scope"))?;
            let mut dry_run = false;
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--dry-run" => dry_run = true,
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard drain flag: {flag}"),
                }
            }
            Ok((
                "POST".into(),
                format!("/v1/shards/{kind}/{tenant_id}/{scope}/drain"),
                Some(format!(r#"{{"dry_run":{dry_run}}}"#)),
                output_mode,
            ))
        }
        "move" | "failover" | "catch-up" | "repair" => {
            let kind = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard kind"))?;
            let tenant_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing tenant_id"))?;
            let scope = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard scope"))?;
            let target_node_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing target_node_id"))?;
            let mut dry_run = false;
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--dry-run" => dry_run = true,
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard {subcommand} flag: {flag}"),
                }
            }
            let action = match subcommand.as_str() {
                "move" => "move",
                "failover" => "failover",
                "catch-up" => "catch-up",
                _ => "repair",
            };
            Ok((
                "POST".into(),
                format!("/v1/shards/{kind}/{tenant_id}/{scope}/{action}"),
                Some(format!(
                    r#"{{"target_node_id":{target_node_id},"dry_run":{dry_run}}}"#
                )),
                output_mode,
            ))
        }
        _ => anyhow::bail!("unknown shard subcommand: {subcommand}"),
    }
}

fn parse_output_mode(value: &str) -> anyhow::Result<OutputMode> {
    match value {
        "json" => Ok(OutputMode::Json),
        "text" => Ok(OutputMode::Text),
        _ => anyhow::bail!("invalid output mode: {value}"),
    }
}

fn render_shard_response_text(body: &str) -> anyhow::Result<String> {
    if let Ok(assignments) =
        serde_json::from_str::<Vec<greenmqtt_core::ServiceShardAssignment>>(body)
    {
        return Ok(assignments
            .into_iter()
            .map(|assignment| {
                format!(
                    "{} tenant={} scope={} owner={} epoch={} fence={} lifecycle={:?}",
                    format!("{:?}", assignment.shard.kind).to_lowercase(),
                    assignment.shard.tenant_id,
                    assignment.shard.scope,
                    assignment.owner_node_id(),
                    assignment.epoch,
                    assignment.fencing_token,
                    assignment.lifecycle,
                )
            })
            .collect::<Vec<_>>()
            .join("\n"));
    }
    if let Ok(assignment) =
        serde_json::from_str::<Option<greenmqtt_core::ServiceShardAssignment>>(body)
    {
        return Ok(assignment
            .map(|assignment| {
                format!(
                    "{} tenant={} scope={} owner={} epoch={} fence={} lifecycle={:?}",
                    format!("{:?}", assignment.shard.kind).to_lowercase(),
                    assignment.shard.tenant_id,
                    assignment.shard.scope,
                    assignment.owner_node_id(),
                    assignment.epoch,
                    assignment.fencing_token,
                    assignment.lifecycle,
                )
            })
            .unwrap_or_else(|| "shard not found".to_string()));
    }
    let reply: ShardActionEnvelope = serde_json::from_str(body)?;
    let previous = reply
        .previous
        .as_ref()
        .map(|assignment| {
            format!(
                "{}:{} owner={} epoch={} fence={} lifecycle={:?}",
                assignment.shard.tenant_id,
                assignment.shard.scope,
                assignment.owner_node_id(),
                assignment.epoch,
                assignment.fencing_token,
                assignment.lifecycle,
            )
        })
        .unwrap_or_else(|| "none".to_string());
    let current = reply
        .current
        .as_ref()
        .map(|assignment| {
            format!(
                "{}:{} owner={} epoch={} fence={} lifecycle={:?}",
                assignment.shard.tenant_id,
                assignment.shard.scope,
                assignment.owner_node_id(),
                assignment.epoch,
                assignment.fencing_token,
                assignment.lifecycle,
            )
        })
        .unwrap_or_else(|| "none".to_string());
    Ok(format!("previous={previous}\ncurrent={current}"))
}

fn http_request_body(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    let payload = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(request.as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("invalid http response"))?;
    Ok(body.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "demo".to_string());
    if mode == "shard" {
        return run_shard_command(args);
    }

    let node_id = std::env::var("GREENMQTT_NODE_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let state_endpoint = std::env::var("GREENMQTT_STATE_ENDPOINT").ok();
    let peer_forwarder = StaticPeerForwarder::default();
    configure_peers(&peer_forwarder).await?;
    let data_dir = std::env::var("GREENMQTT_DATA_DIR").ok();
    let storage_backend = std::env::var("GREENMQTT_STORAGE_BACKEND")
        .unwrap_or_else(|_| "sled".to_string())
        .to_lowercase();
    let redis_url = std::env::var("GREENMQTT_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let hooks = configured_hooks(node_id)?;
    let auth = configured_auth()?;
    let acl = configured_acl()?;
    let output_mode = output_mode();

    let (sessiondict, dist, inbox, retain): AppStateServices = match state_endpoint.as_deref() {
        Some(endpoint) => (
            Arc::new(SessionDictGrpcClient::connect(endpoint).await?),
            Arc::new(DistGrpcClient::connect(endpoint).await?),
            Arc::new(InboxGrpcClient::connect(endpoint).await?),
            Arc::new(RetainGrpcClient::connect(endpoint).await?),
        ),
        None => {
            if storage_backend == "redis" {
                redis_services(&redis_url).await?
            } else {
                match data_dir {
                    Some(ref dir) => durable_services(PathBuf::from(dir), &storage_backend).await?,
                    None => (
                        Arc::new(SessionDictHandle::default()),
                        Arc::new(DistHandle::default()),
                        Arc::new(InboxHandle::default()),
                        Arc::new(RetainHandle::default()),
                    ),
                }
            }
        }
    };

    let broker = Arc::new(BrokerRuntime::with_cluster(
        BrokerConfig {
            node_id,
            enable_tcp: true,
            enable_tls: true,
            enable_ws: true,
            enable_wss: true,
            enable_quic: true,
            server_keep_alive_secs: std::env::var("GREENMQTT_SERVER_KEEP_ALIVE_SECS")
                .ok()
                .map(|value| value.parse())
                .transpose()?,
            max_packet_size: std::env::var("GREENMQTT_MAX_PACKET_SIZE")
                .ok()
                .map(|value| value.parse())
                .transpose()?,
            response_information: std::env::var("GREENMQTT_RESPONSE_INFORMATION")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| Some("greenmqtt".to_string())),
            server_reference: std::env::var("GREENMQTT_SERVER_REFERENCE")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            audit_log_path: data_dir
                .as_ref()
                .map(|dir| PathBuf::from(dir).join("admin-audit.jsonl")),
        },
        auth,
        acl,
        hooks,
        Arc::new(peer_forwarder.clone()),
        sessiondict,
        dist,
        inbox,
        retain,
    ));
    let _ = broker.restore_peer_registry(&peer_forwarder).await?;

    if mode == "serve" {
        let http_bind: SocketAddr = std::env::var("GREENMQTT_HTTP_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()?;
        let mqtt_bind: SocketAddr = std::env::var("GREENMQTT_MQTT_BIND")
            .unwrap_or_else(|_| "127.0.0.1:1883".to_string())
            .parse()?;
        let tls_bind = std::env::var("GREENMQTT_TLS_BIND").ok();
        let tls_cert = std::env::var("GREENMQTT_TLS_CERT").ok();
        let tls_key = std::env::var("GREENMQTT_TLS_KEY").ok();
        let ws_bind = std::env::var("GREENMQTT_WS_BIND").ok();
        let wss_bind = std::env::var("GREENMQTT_WSS_BIND").ok();
        let quic_bind = std::env::var("GREENMQTT_QUIC_BIND").ok();
        let rpc_bind: SocketAddr = std::env::var("GREENMQTT_RPC_BIND")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
            .parse()?;
        println!("greenmqtt http api listening on {http_bind}");
        println!("greenmqtt mqtt tcp listening on {mqtt_bind}");
        if let Some(bind) = &tls_bind {
            println!("greenmqtt mqtt tls listening on {bind}");
        }
        if let Some(bind) = &ws_bind {
            println!("greenmqtt mqtt ws listening on {bind}");
        }
        if let Some(bind) = &wss_bind {
            println!("greenmqtt mqtt wss listening on {bind}");
        }
        if let Some(bind) = &quic_bind {
            println!("greenmqtt mqtt quic listening on {bind}");
        }
        println!("greenmqtt internal grpc listening on {rpc_bind}");
        let shard_registry = Arc::new(StaticServiceEndpointRegistry::default());
        shard_registry
            .upsert_member(ClusterNodeMembership::new(
                node_id,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(
                        ServiceKind::SessionDict,
                        node_id,
                        format!("http://{rpc_bind}"),
                    ),
                    ServiceEndpoint::new(ServiceKind::Dist, node_id, format!("http://{rpc_bind}")),
                    ServiceEndpoint::new(ServiceKind::Inbox, node_id, format!("http://{rpc_bind}")),
                    ServiceEndpoint::new(
                        ServiceKind::Retain,
                        node_id,
                        format!("http://{rpc_bind}"),
                    ),
                ],
            ))
            .await?;
        let metrics_handle = PrometheusBuilder::new().install_recorder()?;
        let http = HttpApi::with_peers_shards_and_metrics(
            broker.clone(),
            Arc::new(peer_forwarder.clone()),
            shard_registry.clone(),
            metrics_handle,
            http_bind,
        );
        let mqtt = serve_tcp(broker.clone(), mqtt_bind);
        let tls_broker = broker.clone();
        let ws_broker = broker.clone();
        let wss_broker = broker.clone();
        let quic_broker = broker.clone();
        let wss_tls_cert = tls_cert.clone();
        let wss_tls_key = tls_key.clone();
        let quic_tls_cert = tls_cert.clone();
        let quic_tls_key = tls_key.clone();
        let tls = async move {
            match (tls_bind, tls_cert, tls_key) {
                (Some(bind), Some(cert), Some(key)) => {
                    serve_tls(tls_broker.clone(), bind.parse()?, cert, key).await
                }
                _ => std::future::pending::<anyhow::Result<()>>().await,
            }
        };
        let ws = async move {
            match ws_bind {
                Some(bind) => serve_ws(ws_broker.clone(), bind.parse()?).await,
                None => std::future::pending::<anyhow::Result<()>>().await,
            }
        };
        let wss = async move {
            match (wss_bind, wss_tls_cert, wss_tls_key) {
                (Some(bind), Some(cert), Some(key)) => {
                    serve_wss(wss_broker.clone(), bind.parse()?, cert, key).await
                }
                _ => std::future::pending::<anyhow::Result<()>>().await,
            }
        };
        let quic = async move {
            match (quic_bind, quic_tls_cert, quic_tls_key) {
                (Some(bind), Some(cert), Some(key)) => {
                    serve_quic(quic_broker.clone(), bind.parse()?, cert, key).await
                }
                _ => std::future::pending::<anyhow::Result<()>>().await,
            }
        };
        let rpc = RpcRuntime {
            sessiondict: broker.sessiondict.clone(),
            dist: broker.dist.clone(),
            inbox: broker.inbox.clone(),
            retain: broker.retain.clone(),
            peer_sink: broker.clone(),
            assignment_registry: Some(shard_registry),
        };
        let http_task = async move { http.start().await };
        let rpc_task = async move { rpc.serve(rpc_bind).await };
        let (http_result, mqtt_result, tls_result, ws_result, wss_result, quic_result, rpc_result) =
            tokio::join!(http_task, mqtt, tls, ws, wss, quic, rpc_task);
        http_result?;
        mqtt_result?;
        tls_result?;
        ws_result?;
        wss_result?;
        quic_result?;
        rpc_result?;
        return Ok(());
    }

    if mode == "bench" {
        let storage_backend_label = storage_backend_label(
            state_endpoint.as_deref(),
            data_dir.as_deref(),
            &storage_backend,
        );
        let config = BenchConfig {
            subscribers: std::env::var("GREENMQTT_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            publishers: std::env::var("GREENMQTT_BENCH_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            qos: std::env::var("GREENMQTT_BENCH_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            scenario: std::env::var("GREENMQTT_BENCH_SCENARIO")
                .ok()
                .as_deref()
                .map(parse_bench_scenario)
                .transpose()?
                .unwrap_or(BenchScenario::Live),
        };
        let report = run_bench(broker, node_id, config, &storage_backend_label).await?;
        print_bench_report(&report, output_mode)?;
        validate_bench_report(&report, bench_thresholds_from_env())?;
        return Ok(());
    }

    if mode == "compare-bench" {
        let config = BenchConfig {
            subscribers: std::env::var("GREENMQTT_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            publishers: std::env::var("GREENMQTT_BENCH_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            qos: std::env::var("GREENMQTT_BENCH_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            scenario: std::env::var("GREENMQTT_BENCH_SCENARIO")
                .ok()
                .as_deref()
                .map(parse_bench_scenario)
                .transpose()?
                .unwrap_or(BenchScenario::Live),
        };
        let backends = std::env::var("GREENMQTT_COMPARE_BACKENDS")
            .ok()
            .as_deref()
            .map(parse_compare_backends)
            .transpose()?
            .unwrap_or_else(|| {
                vec![
                    "memory".to_string(),
                    "sled".to_string(),
                    "rocksdb".to_string(),
                ]
            });
        let report = run_compare_bench(node_id, config, &backends, &redis_url).await?;
        print_compare_bench_report(&report, output_mode)?;
        return Ok(());
    }

    if mode == "profile-bench" {
        anyhow::ensure!(
            state_endpoint.is_none(),
            "profile-bench does not support GREENMQTT_STATE_ENDPOINT"
        );
        let config = BenchConfig {
            subscribers: std::env::var("GREENMQTT_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            publishers: std::env::var("GREENMQTT_BENCH_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            qos: std::env::var("GREENMQTT_BENCH_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            scenario: BenchScenario::Live,
        };
        let scenarios = std::env::var("GREENMQTT_PROFILE_SCENARIOS")
            .ok()
            .as_deref()
            .map(parse_bench_scenarios)
            .transpose()?
            .unwrap_or_else(|| {
                vec![
                    BenchScenario::Live,
                    BenchScenario::OfflineReplay,
                    BenchScenario::RetainedReplay,
                    BenchScenario::SharedLive,
                ]
            });
        let backend = if data_dir.is_some() {
            storage_backend.clone()
        } else {
            "memory".to_string()
        };
        let report = run_profile_bench(node_id, config, &scenarios, &backend, &redis_url).await?;
        print_profile_bench_report(&report, output_mode)?;
        return Ok(());
    }

    if mode == "soak" {
        let storage_backend_label = storage_backend_label(
            state_endpoint.as_deref(),
            data_dir.as_deref(),
            &storage_backend,
        );
        let config = SoakConfig {
            iterations: std::env::var("GREENMQTT_SOAK_ITERATIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            subscribers: std::env::var("GREENMQTT_SOAK_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            publishers: std::env::var("GREENMQTT_SOAK_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_SOAK_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
            qos: std::env::var("GREENMQTT_SOAK_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
        };
        let report = run_soak(broker, node_id, config, &storage_backend_label).await?;
        print_soak_report(&report, output_mode)?;
        validate_soak_report(&report, soak_thresholds_from_env())?;
        return Ok(());
    }

    if mode == "compare-soak" {
        let config = SoakConfig {
            iterations: std::env::var("GREENMQTT_SOAK_ITERATIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            subscribers: std::env::var("GREENMQTT_SOAK_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            publishers: std::env::var("GREENMQTT_SOAK_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_SOAK_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
            qos: std::env::var("GREENMQTT_SOAK_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
        };
        let backends = std::env::var("GREENMQTT_COMPARE_BACKENDS")
            .ok()
            .as_deref()
            .map(parse_compare_backends)
            .transpose()?
            .unwrap_or_else(|| {
                vec![
                    "memory".to_string(),
                    "sled".to_string(),
                    "rocksdb".to_string(),
                ]
            });
        let report = run_compare_soak(node_id, config, &backends, &redis_url).await?;
        print_compare_soak_report(&report, output_mode)?;
        return Ok(());
    }

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "subscriber".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await?;
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await?;
    broker.disconnect(&subscriber.session.session_id).await?;

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "publisher".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await?;
    let outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await?;

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "subscriber".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await?;

    println!(
        "greenmqtt demo: matched_routes={}, offline_messages={}",
        outcome.matched_routes,
        resumed.offline_messages.len()
    );
    Ok(())
}

async fn run_bench<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    node_id: u64,
    config: BenchConfig,
    storage_backend: &str,
) -> anyhow::Result<BenchReport>
where
    A: greenmqtt_plugin_api::AuthProvider,
    C: greenmqtt_plugin_api::AclProvider,
    H: greenmqtt_plugin_api::EventHook,
{
    let setup_started_at = Instant::now();
    let mut publisher_sessions = Vec::with_capacity(config.publishers);
    for index in 0..config.publishers {
        let reply = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "bench".into(),
                    user_id: format!("pub-{index}"),
                    client_id: format!("pub-{index}"),
                },
                node_id,
                kind: SessionKind::Transient,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await?;
        publisher_sessions.push(reply.session.session_id);
    }

    let mut subscriber_sessions = Vec::with_capacity(config.subscribers);
    if !matches!(config.scenario, BenchScenario::RetainedReplay) {
        for index in 0..config.subscribers {
            let reply = broker
                .connect(ConnectRequest {
                    identity: ClientIdentity {
                        tenant_id: "bench".into(),
                        user_id: format!("sub-{index}"),
                        client_id: format!("sub-{index}"),
                    },
                    node_id,
                    kind: match config.scenario {
                        BenchScenario::Live => SessionKind::Transient,
                        BenchScenario::OfflineReplay => SessionKind::Persistent,
                        BenchScenario::RetainedReplay => SessionKind::Transient,
                        BenchScenario::SharedLive => SessionKind::Transient,
                    },
                    clean_start: true,
                    session_expiry_interval_secs: None,
                })
                .await?;
            broker
                .subscribe(
                    &reply.session.session_id,
                    bench_topic_filter(config.scenario),
                    config.qos,
                    None,
                    false,
                    false,
                    0,
                    matches!(config.scenario, BenchScenario::SharedLive)
                        .then_some("workers".to_string()),
                )
                .await?;
            subscriber_sessions.push(reply.session.session_id);
            if matches!(config.scenario, BenchScenario::OfflineReplay) {
                broker
                    .disconnect(subscriber_sessions.last().unwrap())
                    .await?;
            }
        }
    }

    let setup_elapsed = setup_started_at.elapsed();
    let publish_started_at = Instant::now();
    let mut matched_routes = 0usize;
    let mut online_deliveries = 0usize;
    for (publisher_index, publisher_session_id) in publisher_sessions.iter().enumerate() {
        for message_index in 0..config.messages_per_publisher {
            let outcome = broker
                .publish(
                    publisher_session_id,
                    PublishRequest {
                        topic: match config.scenario {
                            BenchScenario::Live
                            | BenchScenario::OfflineReplay
                            | BenchScenario::SharedLive => {
                                format!("devices/d{publisher_index}/state")
                            }
                            BenchScenario::RetainedReplay => {
                                format!("devices/d{publisher_index}/state/{message_index}")
                            }
                        },
                        payload: format!("m-{message_index}").into_bytes().into(),
                        qos: config.qos,
                        retain: matches!(config.scenario, BenchScenario::RetainedReplay),
                        properties: PublishProperties::default(),
                    },
                )
                .await?;
            matched_routes += outcome.matched_routes;
            online_deliveries += outcome.online_deliveries;
        }
    }
    let publish_elapsed = publish_started_at.elapsed();
    let stats_before_drain = broker.stats().await?;
    let pending_before_drain = stats_before_drain.local_pending_deliveries;
    let drain_started_at = Instant::now();
    let mut actual_deliveries = 0usize;
    match config.scenario {
        BenchScenario::Live | BenchScenario::SharedLive => {
            for session_id in &subscriber_sessions {
                actual_deliveries += broker.drain_deliveries(session_id).await?.len();
            }
        }
        BenchScenario::OfflineReplay => {
            for index in 0..config.subscribers {
                let resumed = broker
                    .connect(ConnectRequest {
                        identity: ClientIdentity {
                            tenant_id: "bench".into(),
                            user_id: format!("sub-{index}"),
                            client_id: format!("sub-{index}"),
                        },
                        node_id,
                        kind: SessionKind::Persistent,
                        clean_start: false,
                        session_expiry_interval_secs: None,
                    })
                    .await?;
                actual_deliveries += resumed.offline_messages.len();
                broker.disconnect(&resumed.session.session_id).await?;
            }
        }
        BenchScenario::RetainedReplay => {
            for index in 0..config.subscribers {
                let reply = broker
                    .connect(ConnectRequest {
                        identity: ClientIdentity {
                            tenant_id: "bench".into(),
                            user_id: format!("sub-{index}"),
                            client_id: format!("sub-{index}"),
                        },
                        node_id,
                        kind: SessionKind::Transient,
                        clean_start: true,
                        session_expiry_interval_secs: None,
                    })
                    .await?;
                let retained = broker
                    .subscribe(
                        &reply.session.session_id,
                        bench_topic_filter(config.scenario),
                        config.qos,
                        None,
                        false,
                        false,
                        0,
                        None,
                    )
                    .await?;
                actual_deliveries += retained.len();
                broker.disconnect(&reply.session.session_id).await?;
            }
        }
    }
    let drain_elapsed = drain_started_at.elapsed();
    let elapsed = setup_elapsed + publish_elapsed + drain_elapsed;
    let rss_bytes_after = current_process_rss_bytes();

    let total_publishes = config.publishers * config.messages_per_publisher;
    let expected_deliveries = match config.scenario {
        BenchScenario::SharedLive => total_publishes,
        _ => total_publishes * config.subscribers,
    };
    let elapsed_secs = elapsed.as_secs_f64();
    Ok(BenchReport {
        storage_backend: storage_backend.to_string(),
        scenario: config.scenario,
        subscribers: config.subscribers,
        publishers: config.publishers,
        messages_per_publisher: config.messages_per_publisher,
        total_publishes,
        expected_deliveries,
        actual_deliveries,
        matched_routes,
        online_deliveries,
        pending_before_drain,
        global_session_records: stats_before_drain.global_session_records,
        route_records: stats_before_drain.route_records,
        subscription_records: stats_before_drain.subscription_records,
        offline_messages_before_drain: stats_before_drain.offline_messages,
        inflight_messages_before_drain: stats_before_drain.inflight_messages,
        retained_messages: stats_before_drain.retained_messages,
        rss_bytes_after,
        setup_ms: setup_elapsed.as_millis(),
        publish_ms: publish_elapsed.as_millis(),
        drain_ms: drain_elapsed.as_millis(),
        elapsed_ms: elapsed.as_millis(),
        publishes_per_sec: if elapsed_secs > 0.0 {
            total_publishes as f64 / elapsed_secs
        } else {
            total_publishes as f64
        },
        deliveries_per_sec: if elapsed_secs > 0.0 {
            actual_deliveries as f64 / elapsed_secs
        } else {
            actual_deliveries as f64
        },
    })
}

async fn run_compare_bench(
    node_id: u64,
    config: BenchConfig,
    backends: &[String],
    redis_url: &str,
) -> anyhow::Result<CompareBenchReport> {
    anyhow::ensure!(
        !backends.is_empty(),
        "GREENMQTT_COMPARE_BACKENDS must include at least one backend"
    );
    let mut reports = Vec::with_capacity(backends.len());
    for backend in backends {
        let (broker, cleanup_dir) = compare_bench_broker(node_id, backend, redis_url).await?;
        let report = run_bench(broker, node_id, config, backend).await?;
        if let Some(path) = cleanup_dir {
            let _ = fs::remove_dir_all(path);
        }
        reports.push(report);
    }
    Ok(CompareBenchReport { reports })
}

async fn run_compare_soak(
    node_id: u64,
    config: SoakConfig,
    backends: &[String],
    redis_url: &str,
) -> anyhow::Result<CompareSoakReport> {
    anyhow::ensure!(
        !backends.is_empty(),
        "GREENMQTT_COMPARE_BACKENDS must include at least one backend"
    );
    let mut reports = Vec::with_capacity(backends.len());
    for backend in backends {
        let (broker, cleanup_dir) = compare_bench_broker(node_id, backend, redis_url).await?;
        let report = run_soak(broker, node_id, config, backend).await?;
        if let Some(path) = cleanup_dir {
            let _ = fs::remove_dir_all(path);
        }
        reports.push(report);
    }
    Ok(CompareSoakReport { reports })
}

async fn run_profile_bench(
    node_id: u64,
    config: BenchConfig,
    scenarios: &[BenchScenario],
    backend: &str,
    redis_url: &str,
) -> anyhow::Result<ProfileBenchReport> {
    anyhow::ensure!(
        !scenarios.is_empty(),
        "GREENMQTT_PROFILE_SCENARIOS must include at least one scenario"
    );
    let mut reports = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let (broker, cleanup_dir) = compare_bench_broker(node_id, backend, redis_url).await?;
        let report = run_bench(
            broker,
            node_id,
            BenchConfig {
                scenario: *scenario,
                ..config
            },
            backend,
        )
        .await?;
        if let Some(path) = cleanup_dir {
            let _ = fs::remove_dir_all(path);
        }
        reports.push(report);
    }
    Ok(ProfileBenchReport { reports })
}

async fn run_soak<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    node_id: u64,
    config: SoakConfig,
    storage_backend: &str,
) -> anyhow::Result<SoakReport>
where
    A: greenmqtt_plugin_api::AuthProvider,
    C: greenmqtt_plugin_api::AclProvider,
    H: greenmqtt_plugin_api::EventHook,
{
    let started_at = Instant::now();
    let mut total_publishes = 0usize;
    let mut total_deliveries = 0usize;
    let mut max_local_online_sessions = 0usize;
    let mut max_local_pending_deliveries = 0usize;
    let mut max_global_session_records = 0usize;
    let mut max_route_records = 0usize;
    let mut max_subscription_records = 0usize;
    let mut max_rss_bytes = current_process_rss_bytes();

    for iteration in 0..config.iterations {
        let mut subscriber_sessions = Vec::with_capacity(config.subscribers);
        for index in 0..config.subscribers {
            let reply = broker
                .connect(ConnectRequest {
                    identity: ClientIdentity {
                        tenant_id: "soak".into(),
                        user_id: format!("sub-{iteration}-{index}"),
                        client_id: format!("sub-{iteration}-{index}"),
                    },
                    node_id,
                    kind: SessionKind::Transient,
                    clean_start: true,
                    session_expiry_interval_secs: None,
                })
                .await?;
            broker
                .subscribe(
                    &reply.session.session_id,
                    "devices/+/state",
                    config.qos,
                    None,
                    false,
                    false,
                    0,
                    None,
                )
                .await?;
            subscriber_sessions.push(reply.session.session_id);
        }

        let mut publisher_sessions = Vec::with_capacity(config.publishers);
        for index in 0..config.publishers {
            let reply = broker
                .connect(ConnectRequest {
                    identity: ClientIdentity {
                        tenant_id: "soak".into(),
                        user_id: format!("pub-{iteration}-{index}"),
                        client_id: format!("pub-{iteration}-{index}"),
                    },
                    node_id,
                    kind: SessionKind::Transient,
                    clean_start: true,
                    session_expiry_interval_secs: None,
                })
                .await?;
            publisher_sessions.push(reply.session.session_id);
        }

        for (publisher_index, publisher_session_id) in publisher_sessions.iter().enumerate() {
            for message_index in 0..config.messages_per_publisher {
                broker
                    .publish(
                        publisher_session_id,
                        PublishRequest {
                            topic: format!("devices/d{publisher_index}/state"),
                            payload: format!("soak-{iteration}-{message_index}")
                                .into_bytes()
                                .into(),
                            qos: config.qos,
                            retain: false,
                            properties: PublishProperties::default(),
                        },
                    )
                    .await?;
                total_publishes += 1;
            }
        }

        let stats_during = broker.stats().await?;
        max_local_online_sessions =
            max_local_online_sessions.max(stats_during.local_online_sessions);
        max_local_pending_deliveries =
            max_local_pending_deliveries.max(stats_during.local_pending_deliveries);
        max_global_session_records =
            max_global_session_records.max(stats_during.global_session_records);
        max_route_records = max_route_records.max(stats_during.route_records);
        max_subscription_records = max_subscription_records.max(stats_during.subscription_records);
        max_rss_bytes = max_rss_bytes.max(current_process_rss_bytes());

        for session_id in &subscriber_sessions {
            total_deliveries += broker.drain_deliveries(session_id).await?.len();
        }

        for session_id in subscriber_sessions.iter().chain(publisher_sessions.iter()) {
            broker.disconnect(session_id).await?;
        }
    }

    let final_stats = broker.stats().await?;
    let final_rss_bytes = current_process_rss_bytes();
    Ok(SoakReport {
        storage_backend: storage_backend.to_string(),
        iterations: config.iterations,
        subscribers: config.subscribers,
        publishers: config.publishers,
        messages_per_publisher: config.messages_per_publisher,
        total_publishes,
        total_deliveries,
        max_local_online_sessions,
        max_local_pending_deliveries,
        max_global_session_records,
        max_route_records,
        max_subscription_records,
        final_global_session_records: final_stats.global_session_records,
        final_route_records: final_stats.route_records,
        final_subscription_records: final_stats.subscription_records,
        final_offline_messages: final_stats.offline_messages,
        final_inflight_messages: final_stats.inflight_messages,
        final_retained_messages: final_stats.retained_messages,
        max_rss_bytes,
        final_rss_bytes,
        elapsed_ms: started_at.elapsed().as_millis(),
    })
}

fn output_mode() -> OutputMode {
    match std::env::var("GREENMQTT_OUTPUT")
        .unwrap_or_else(|_| "text".to_string())
        .to_lowercase()
        .as_str()
    {
        "json" => OutputMode::Json,
        _ => OutputMode::Text,
    }
}

fn storage_backend_label(
    state_endpoint: Option<&str>,
    data_dir: Option<&str>,
    storage_backend: &str,
) -> String {
    if state_endpoint.is_some() {
        "remote_grpc".to_string()
    } else if storage_backend == "redis" {
        "redis".to_string()
    } else if data_dir.is_some() {
        storage_backend.to_string()
    } else {
        "memory".to_string()
    }
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

fn bench_thresholds_from_env() -> BenchThresholds {
    BenchThresholds {
        min_publishes_per_sec: std::env::var("GREENMQTT_BENCH_MIN_PUBLISHES_PER_SEC")
            .ok()
            .and_then(|value| value.parse().ok()),
        min_deliveries_per_sec: std::env::var("GREENMQTT_BENCH_MIN_DELIVERIES_PER_SEC")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_pending_before_drain: std::env::var("GREENMQTT_BENCH_MAX_PENDING_BEFORE_DRAIN")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_rss_bytes_after: std::env::var("GREENMQTT_BENCH_MAX_RSS_BYTES_AFTER")
            .ok()
            .and_then(|value| value.parse().ok()),
    }
}

fn soak_thresholds_from_env() -> SoakThresholds {
    SoakThresholds {
        max_final_global_sessions: std::env::var("GREENMQTT_SOAK_MAX_FINAL_GLOBAL_SESSIONS")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_routes: std::env::var("GREENMQTT_SOAK_MAX_FINAL_ROUTES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_subscriptions: std::env::var("GREENMQTT_SOAK_MAX_FINAL_SUBSCRIPTIONS")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_offline_messages: std::env::var("GREENMQTT_SOAK_MAX_FINAL_OFFLINE_MESSAGES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_inflight_messages: std::env::var("GREENMQTT_SOAK_MAX_FINAL_INFLIGHT_MESSAGES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_retained_messages: std::env::var("GREENMQTT_SOAK_MAX_FINAL_RETAINED_MESSAGES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_peak_rss_bytes: std::env::var("GREENMQTT_SOAK_MAX_PEAK_RSS_BYTES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_rss_bytes: std::env::var("GREENMQTT_SOAK_MAX_FINAL_RSS_BYTES")
            .ok()
            .and_then(|value| value.parse().ok()),
    }
}

fn parse_bench_scenario(value: &str) -> anyhow::Result<BenchScenario> {
    match value {
        "live" => Ok(BenchScenario::Live),
        "offline_replay" => Ok(BenchScenario::OfflineReplay),
        "retained_replay" => Ok(BenchScenario::RetainedReplay),
        "shared_live" => Ok(BenchScenario::SharedLive),
        other => anyhow::bail!("unsupported GREENMQTT_BENCH_SCENARIO: {other}"),
    }
}

fn parse_bench_scenarios(value: &str) -> anyhow::Result<Vec<BenchScenario>> {
    let scenarios = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(parse_bench_scenario)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(
        !scenarios.is_empty(),
        "GREENMQTT_PROFILE_SCENARIOS must include at least one scenario"
    );
    Ok(scenarios)
}

fn parse_compare_backends(value: &str) -> anyhow::Result<Vec<String>> {
    let mut backends = Vec::new();
    for backend in value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match backend {
            "memory" | "sled" | "rocksdb" | "redis" => backends.push(backend.to_string()),
            other => anyhow::bail!("unsupported GREENMQTT_COMPARE_BACKENDS value: {other}"),
        }
    }
    anyhow::ensure!(
        !backends.is_empty(),
        "GREENMQTT_COMPARE_BACKENDS must include at least one backend"
    );
    Ok(backends)
}

fn print_bench_report(report: &BenchReport, output_mode: OutputMode) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!(
                "greenmqtt bench: storage_backend={}, scenario={:?}, publishes={}, expected_deliveries={}, actual_deliveries={}, matched_routes={}, online_deliveries={}, pending_before_drain={}, global_sessions={}, routes={}, subscriptions={}, offline_messages={}, inflight_messages={}, retained_messages={}, rss_bytes_after={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, publishes_per_sec={:.2}, deliveries_per_sec={:.2}",
                report.storage_backend,
                report.scenario,
                report.total_publishes,
                report.expected_deliveries,
                report.actual_deliveries,
                report.matched_routes,
                report.online_deliveries,
                report.pending_before_drain,
                report.global_session_records,
                report.route_records,
                report.subscription_records,
                report.offline_messages_before_drain,
                report.inflight_messages_before_drain,
                report.retained_messages,
                report.rss_bytes_after,
                report.setup_ms,
                report.publish_ms,
                report.drain_ms,
                report.elapsed_ms,
                report.publishes_per_sec,
                report.deliveries_per_sec,
            );
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

fn print_compare_bench_report(
    report: &CompareBenchReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!("greenmqtt compare-bench:");
            for item in &report.reports {
                println!(
                    "  storage_backend={}, scenario={:?}, publishes={}, expected_deliveries={}, actual_deliveries={}, matched_routes={}, online_deliveries={}, pending_before_drain={}, rss_bytes_after={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, publishes_per_sec={:.2}, deliveries_per_sec={:.2}",
                    item.storage_backend,
                    item.scenario,
                    item.total_publishes,
                    item.expected_deliveries,
                    item.actual_deliveries,
                    item.matched_routes,
                    item.online_deliveries,
                    item.pending_before_drain,
                    item.rss_bytes_after,
                    item.setup_ms,
                    item.publish_ms,
                    item.drain_ms,
                    item.elapsed_ms,
                    item.publishes_per_sec,
                    item.deliveries_per_sec,
                );
            }
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

fn print_profile_bench_report(
    report: &ProfileBenchReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!("greenmqtt profile-bench:");
            for item in &report.reports {
                println!(
                    "  storage_backend={}, scenario={:?}, publishes={}, expected_deliveries={}, actual_deliveries={}, matched_routes={}, online_deliveries={}, pending_before_drain={}, rss_bytes_after={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, publishes_per_sec={:.2}, deliveries_per_sec={:.2}",
                    item.storage_backend,
                    item.scenario,
                    item.total_publishes,
                    item.expected_deliveries,
                    item.actual_deliveries,
                    item.matched_routes,
                    item.online_deliveries,
                    item.pending_before_drain,
                    item.rss_bytes_after,
                    item.setup_ms,
                    item.publish_ms,
                    item.drain_ms,
                    item.elapsed_ms,
                    item.publishes_per_sec,
                    item.deliveries_per_sec,
                );
            }
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

fn print_compare_soak_report(
    report: &CompareSoakReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!("greenmqtt compare-soak:");
            for item in &report.reports {
                println!(
                    "  storage_backend={}, iterations={}, publishes={}, deliveries={}, max_local_online_sessions={}, max_local_pending_deliveries={}, max_global_sessions={}, max_routes={}, max_subscriptions={}, final_global_sessions={}, final_routes={}, final_subscriptions={}, final_offline_messages={}, final_inflight_messages={}, final_retained_messages={}, max_rss_bytes={}, final_rss_bytes={}, elapsed_ms={}",
                    item.storage_backend,
                    item.iterations,
                    item.total_publishes,
                    item.total_deliveries,
                    item.max_local_online_sessions,
                    item.max_local_pending_deliveries,
                    item.max_global_session_records,
                    item.max_route_records,
                    item.max_subscription_records,
                    item.final_global_session_records,
                    item.final_route_records,
                    item.final_subscription_records,
                    item.final_offline_messages,
                    item.final_inflight_messages,
                    item.final_retained_messages,
                    item.max_rss_bytes,
                    item.final_rss_bytes,
                    item.elapsed_ms,
                );
            }
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

fn print_soak_report(report: &SoakReport, output_mode: OutputMode) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!(
                "greenmqtt soak: storage_backend={}, iterations={}, publishes={}, deliveries={}, max_local_online_sessions={}, max_local_pending_deliveries={}, max_global_sessions={}, max_routes={}, max_subscriptions={}, final_global_sessions={}, final_routes={}, final_subscriptions={}, final_offline_messages={}, final_inflight_messages={}, final_retained_messages={}, max_rss_bytes={}, final_rss_bytes={}, elapsed_ms={}",
                report.storage_backend,
                report.iterations,
                report.total_publishes,
                report.total_deliveries,
                report.max_local_online_sessions,
                report.max_local_pending_deliveries,
                report.max_global_session_records,
                report.max_route_records,
                report.max_subscription_records,
                report.final_global_session_records,
                report.final_route_records,
                report.final_subscription_records,
                report.final_offline_messages,
                report.final_inflight_messages,
                report.final_retained_messages,
                report.max_rss_bytes,
                report.final_rss_bytes,
                report.elapsed_ms,
            );
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

fn validate_bench_report(report: &BenchReport, thresholds: BenchThresholds) -> anyhow::Result<()> {
    if let Some(min) = thresholds.min_publishes_per_sec {
        anyhow::ensure!(
            report.publishes_per_sec >= min,
            "bench publishes_per_sec {:.2} below threshold {:.2}",
            report.publishes_per_sec,
            min
        );
    }
    if let Some(min) = thresholds.min_deliveries_per_sec {
        anyhow::ensure!(
            report.deliveries_per_sec >= min,
            "bench deliveries_per_sec {:.2} below threshold {:.2}",
            report.deliveries_per_sec,
            min
        );
    }
    if let Some(max) = thresholds.max_pending_before_drain {
        anyhow::ensure!(
            report.pending_before_drain <= max,
            "bench pending_before_drain {} exceeds threshold {}",
            report.pending_before_drain,
            max
        );
    }
    if let Some(max) = thresholds.max_rss_bytes_after {
        anyhow::ensure!(
            report.rss_bytes_after <= max,
            "bench rss_bytes_after {} exceeds threshold {}",
            report.rss_bytes_after,
            max
        );
    }
    Ok(())
}

fn validate_soak_report(report: &SoakReport, thresholds: SoakThresholds) -> anyhow::Result<()> {
    if let Some(max) = thresholds.max_final_global_sessions {
        anyhow::ensure!(
            report.final_global_session_records <= max,
            "soak final_global_sessions {} exceeds threshold {}",
            report.final_global_session_records,
            max
        );
    }
    if let Some(max) = thresholds.max_final_routes {
        anyhow::ensure!(
            report.final_route_records <= max,
            "soak final_routes {} exceeds threshold {}",
            report.final_route_records,
            max
        );
    }
    if let Some(max) = thresholds.max_final_subscriptions {
        anyhow::ensure!(
            report.final_subscription_records <= max,
            "soak final_subscriptions {} exceeds threshold {}",
            report.final_subscription_records,
            max
        );
    }
    if let Some(max) = thresholds.max_final_offline_messages {
        anyhow::ensure!(
            report.final_offline_messages <= max,
            "soak final_offline_messages {} exceeds threshold {}",
            report.final_offline_messages,
            max
        );
    }
    if let Some(max) = thresholds.max_final_inflight_messages {
        anyhow::ensure!(
            report.final_inflight_messages <= max,
            "soak final_inflight_messages {} exceeds threshold {}",
            report.final_inflight_messages,
            max
        );
    }
    if let Some(max) = thresholds.max_final_retained_messages {
        anyhow::ensure!(
            report.final_retained_messages <= max,
            "soak final_retained_messages {} exceeds threshold {}",
            report.final_retained_messages,
            max
        );
    }
    if let Some(max) = thresholds.max_peak_rss_bytes {
        anyhow::ensure!(
            report.max_rss_bytes <= max,
            "soak max_rss_bytes {} exceeds threshold {}",
            report.max_rss_bytes,
            max
        );
    }
    if let Some(max) = thresholds.max_final_rss_bytes {
        anyhow::ensure!(
            report.final_rss_bytes <= max,
            "soak final_rss_bytes {} exceeds threshold {}",
            report.final_rss_bytes,
            max
        );
    }
    Ok(())
}

fn configured_hooks(node_id: u64) -> anyhow::Result<ConfiguredEventHook> {
    let mut hooks = Vec::new();
    let rewrite_rules = std::env::var("GREENMQTT_TOPIC_REWRITE_RULES").unwrap_or_default();
    if !rewrite_rules.trim().is_empty() {
        hooks.push(HookTarget::TopicRewrite(TopicRewriteEventHook::new(
            parse_topic_rewrite_rules(&rewrite_rules)?,
        )));
    }
    let rules = std::env::var("GREENMQTT_BRIDGE_RULES").unwrap_or_default();
    if !rules.trim().is_empty() {
        let timeout_ms = std::env::var("GREENMQTT_BRIDGE_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5_000);
        let fail_open = std::env::var("GREENMQTT_BRIDGE_FAIL_OPEN")
            .ok()
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no"
                )
            })
            .unwrap_or(true);
        let retries = std::env::var("GREENMQTT_BRIDGE_RETRIES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let retry_delay_ms = std::env::var("GREENMQTT_BRIDGE_RETRY_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let max_inflight = std::env::var("GREENMQTT_BRIDGE_MAX_INFLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(16);
        hooks.push(HookTarget::Bridge(BridgeEventHook::with_options(
            format!("{BRIDGE_CLIENT_ID_PREFIX}{node_id}"),
            parse_bridge_rules(&rules)?,
            Duration::from_millis(timeout_ms),
            fail_open,
            retries,
            Duration::from_millis(retry_delay_ms),
            max_inflight,
        )));
    }
    if let Some(webhook) = configured_webhook()? {
        hooks.push(HookTarget::WebHook(webhook));
    }
    Ok(ConfiguredEventHook::new(hooks))
}

fn parse_topic_rewrite_rules(raw: &str) -> anyhow::Result<Vec<TopicRewriteRule>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let entry = entry.trim();
            let (scope, rewrite_to) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid topic rewrite rule `{entry}`"))?;
            let (tenant_id, topic_filter) = scope
                .split_once('@')
                .map(|(tenant, filter)| (Some(tenant.trim().to_string()), filter.trim()))
                .unwrap_or((None, scope.trim()));
            anyhow::ensure!(
                !topic_filter.is_empty(),
                "topic rewrite filter must not be empty"
            );
            anyhow::ensure!(
                !rewrite_to.trim().is_empty(),
                "topic rewrite target must not be empty"
            );
            Ok(TopicRewriteRule {
                tenant_id,
                topic_filter: topic_filter.to_string(),
                rewrite_to: rewrite_to.trim().to_string(),
            })
        })
        .collect()
}

fn configured_webhook() -> anyhow::Result<Option<WebHookEventHook>> {
    let url = std::env::var("GREENMQTT_WEBHOOK_URL").unwrap_or_default();
    if url.trim().is_empty() {
        return Ok(None);
    }
    let events = std::env::var("GREENMQTT_WEBHOOK_EVENTS")
        .unwrap_or_else(|_| "publish,offline,retain".to_string());
    let mut connect = false;
    let mut disconnect = false;
    let mut subscribe = false;
    let mut unsubscribe = false;
    let mut publish = false;
    let mut offline = false;
    let mut retain = false;
    for event in events
        .split(',')
        .map(str::trim)
        .filter(|event| !event.is_empty())
    {
        match event {
            "connect" => connect = true,
            "disconnect" => disconnect = true,
            "subscribe" => subscribe = true,
            "unsubscribe" => unsubscribe = true,
            "publish" => publish = true,
            "offline" => offline = true,
            "retain" => retain = true,
            other => anyhow::bail!("unsupported GREENMQTT_WEBHOOK_EVENTS value `{other}`"),
        }
    }
    anyhow::ensure!(
        connect || disconnect || subscribe || unsubscribe || publish || offline || retain,
        "GREENMQTT_WEBHOOK_EVENTS must enable at least one event"
    );
    Ok(Some(WebHookEventHook::new(WebHookConfig {
        url: url.trim().to_string(),
        connect,
        disconnect,
        subscribe,
        unsubscribe,
        publish,
        offline,
        retain,
    })))
}

fn configured_auth() -> anyhow::Result<ConfiguredAuth> {
    let denied_identities = parse_identity_matchers(
        &std::env::var("GREENMQTT_AUTH_DENY_IDENTITIES").unwrap_or_default(),
    )?;
    let enhanced_auth_method = std::env::var("GREENMQTT_ENHANCED_AUTH_METHOD").unwrap_or_default();
    if !enhanced_auth_method.trim().is_empty() {
        let raw_identities = std::env::var("GREENMQTT_AUTH_IDENTITIES").unwrap_or_default();
        let identities = if raw_identities.trim().is_empty() {
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }]
        } else {
            parse_identity_matchers(&raw_identities)?
        };
        let challenge = std::env::var("GREENMQTT_ENHANCED_AUTH_CHALLENGE")
            .unwrap_or_else(|_| "challenge".to_string());
        let response = std::env::var("GREENMQTT_ENHANCED_AUTH_RESPONSE")
            .unwrap_or_else(|_| "response".to_string());
        return Ok(ConfiguredAuth::EnhancedStatic(
            StaticEnhancedAuthProvider::with_denied(
                identities,
                denied_identities,
                enhanced_auth_method.trim().to_string(),
                challenge.into_bytes(),
                response.into_bytes(),
            ),
        ));
    }
    let raw = std::env::var("GREENMQTT_AUTH_IDENTITIES").unwrap_or_default();
    if !raw.trim().is_empty() || !denied_identities.is_empty() {
        let allowed_identities = if raw.trim().is_empty() {
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }]
        } else {
            parse_identity_matchers(&raw)?
        };
        return Ok(ConfiguredAuth::Static(StaticAuthProvider::with_denied(
            allowed_identities,
            denied_identities,
        )));
    }
    let http_auth_url = std::env::var("GREENMQTT_HTTP_AUTH_URL").unwrap_or_default();
    if !http_auth_url.trim().is_empty() {
        return Ok(ConfiguredAuth::Http(HttpAuthProvider::new(
            HttpAuthConfig {
                url: http_auth_url.trim().to_string(),
            },
        )));
    }
    Ok(ConfiguredAuth::default())
}

fn configured_acl() -> anyhow::Result<ConfiguredAcl> {
    let raw = std::env::var("GREENMQTT_ACL_RULES").unwrap_or_default();
    let acl_default_allow = std::env::var("GREENMQTT_ACL_DEFAULT_ALLOW")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    if !raw.trim().is_empty() {
        return Ok(ConfiguredAcl::Static(
            StaticAclProvider::with_default_decision(parse_acl_rules(&raw)?, acl_default_allow),
        ));
    }
    let http_acl_url = std::env::var("GREENMQTT_HTTP_ACL_URL").unwrap_or_default();
    if !http_acl_url.trim().is_empty() {
        return Ok(ConfiguredAcl::Http(HttpAclProvider::new(HttpAclConfig {
            url: http_acl_url.trim().to_string(),
        })));
    }
    Ok(ConfiguredAcl::default())
}

fn parse_identity_matchers(raw: &str) -> anyhow::Result<Vec<IdentityMatcher>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(parse_identity_matcher)
        .collect()
}

fn parse_identity_matcher(entry: &str) -> anyhow::Result<IdentityMatcher> {
    let parts: Vec<_> = entry.trim().split(':').collect();
    anyhow::ensure!(
        parts.len() == 3,
        "identity matcher must be tenant:user:client, got `{entry}`"
    );
    for part in &parts {
        anyhow::ensure!(
            !part.trim().is_empty(),
            "identity matcher segments must not be empty"
        );
    }
    Ok(IdentityMatcher {
        tenant_id: parts[0].trim().to_string(),
        user_id: parts[1].trim().to_string(),
        client_id: parts[2].trim().to_string(),
    })
}

fn parse_acl_rules(raw: &str) -> anyhow::Result<Vec<AclRule>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (lhs, topic_filter) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid acl rule `{entry}`"))?;
            let (decision_action, identity) = lhs
                .split_once('@')
                .ok_or_else(|| anyhow::anyhow!("invalid acl rule `{entry}`"))?;
            let (decision, action) = parse_acl_decision_action(decision_action.trim())?;
            anyhow::ensure!(
                !topic_filter.trim().is_empty(),
                "acl topic filter must not be empty"
            );
            Ok(AclRule {
                decision,
                action,
                identity: parse_identity_matcher(identity.trim())?,
                topic_filter: topic_filter.trim().to_string(),
            })
        })
        .collect()
}

fn parse_acl_decision_action(raw: &str) -> anyhow::Result<(AclDecision, AclAction)> {
    match raw {
        "allow-sub" => Ok((AclDecision::Allow, AclAction::Subscribe)),
        "deny-sub" => Ok((AclDecision::Deny, AclAction::Subscribe)),
        "allow-pub" => Ok((AclDecision::Allow, AclAction::Publish)),
        "deny-pub" => Ok((AclDecision::Deny, AclAction::Publish)),
        _ => anyhow::bail!("unsupported acl rule action `{raw}`"),
    }
}

fn parse_bridge_rules(raw: &str) -> anyhow::Result<Vec<BridgeRule>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (lhs, remote_addr) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid bridge rule `{entry}`"))?;
            let (tenant_id, scoped_filter) = match lhs.split_once('@') {
                Some((tenant_id, topic_filter)) => (
                    Some(tenant_id.trim().to_string()),
                    topic_filter.trim().to_string(),
                ),
                None => (None, lhs.trim().to_string()),
            };
            let (topic_filter, rewrite_to) = match scoped_filter.split_once("->") {
                Some((topic_filter, rewrite_to)) => (
                    topic_filter.trim().to_string(),
                    Some(rewrite_to.trim().to_string()),
                ),
                None => (scoped_filter, None),
            };
            anyhow::ensure!(
                !topic_filter.is_empty(),
                "bridge topic filter must not be empty"
            );
            if let Some(rewrite_to) = &rewrite_to {
                anyhow::ensure!(
                    !rewrite_to.is_empty(),
                    "bridge rewrite target must not be empty"
                );
            }
            anyhow::ensure!(
                !remote_addr.trim().is_empty(),
                "bridge remote address must not be empty"
            );
            Ok(BridgeRule {
                tenant_id,
                topic_filter,
                rewrite_to,
                remote_addr: remote_addr.trim().to_string(),
            })
        })
        .collect()
}

async fn configure_peers(forwarder: &StaticPeerForwarder) -> anyhow::Result<()> {
    let peers = match std::env::var("GREENMQTT_PEERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };

    for item in peers
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (node_id, endpoint) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid GREENMQTT_PEERS entry: {item}"))?;
        forwarder
            .connect_node(node_id.parse()?, endpoint.trim().to_string())
            .await?;
    }
    Ok(())
}

async fn durable_services(
    data_dir: PathBuf,
    backend: &str,
) -> anyhow::Result<(
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
)> {
    std::fs::create_dir_all(&data_dir)?;
    match backend {
        "sled" => {
            let session_store = Arc::new(SledSessionStore::open(data_dir.join("sessions"))?);
            let route_store = Arc::new(SledRouteStore::open(data_dir.join("routes"))?);
            let subscription_store =
                Arc::new(SledSubscriptionStore::open(data_dir.join("subscriptions"))?);
            let inbox_store = Arc::new(SledInboxStore::open(data_dir.join("inbox"))?);
            let inflight_store = Arc::new(SledInflightStore::open(data_dir.join("inflight"))?);
            let retain_store = Arc::new(SledRetainStore::open(data_dir.join("retain"))?);

            Ok((
                Arc::new(PersistentSessionDictHandle::open(session_store).await?),
                Arc::new(PersistentDistHandle::open(route_store).await?),
                Arc::new(PersistentInboxHandle::open(
                    subscription_store,
                    inbox_store,
                    inflight_store,
                )),
                Arc::new(PersistentRetainHandle::open(retain_store)),
            ))
        }
        "rocksdb" => {
            let session_store = Arc::new(RocksSessionStore::open(data_dir.join("sessions"))?);
            let route_store = Arc::new(RocksRouteStore::open(data_dir.join("routes"))?);
            let subscription_store = Arc::new(RocksSubscriptionStore::open(
                data_dir.join("subscriptions"),
            )?);
            let inbox_store = Arc::new(RocksInboxStore::open(data_dir.join("inbox"))?);
            let inflight_store = Arc::new(RocksInflightStore::open(data_dir.join("inflight"))?);
            let retain_store = Arc::new(RocksRetainStore::open(data_dir.join("retain"))?);

            Ok((
                Arc::new(PersistentSessionDictHandle::open(session_store).await?),
                Arc::new(PersistentDistHandle::open(route_store).await?),
                Arc::new(PersistentInboxHandle::open(
                    subscription_store,
                    inbox_store,
                    inflight_store,
                )),
                Arc::new(PersistentRetainHandle::open(retain_store)),
            ))
        }
        other => anyhow::bail!("unsupported GREENMQTT_STORAGE_BACKEND: {other}"),
    }
}

async fn redis_services(
    redis_url: &str,
) -> anyhow::Result<(
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
)> {
    let session_store = Arc::new(RedisSessionStore::open(redis_url)?);
    let route_store = Arc::new(RedisRouteStore::open(redis_url)?);
    let subscription_store = Arc::new(RedisSubscriptionStore::open(redis_url)?);
    let inbox_store = Arc::new(RedisInboxStore::open(redis_url)?);
    let inflight_store = Arc::new(RedisInflightStore::open(redis_url)?);
    let retain_store = Arc::new(RedisRetainStore::open(redis_url)?);

    Ok((
        Arc::new(PersistentSessionDictHandle::open(session_store).await?),
        Arc::new(PersistentDistHandle::open(route_store).await?),
        Arc::new(PersistentInboxHandle::open(
            subscription_store,
            inbox_store,
            inflight_store,
        )),
        Arc::new(PersistentRetainHandle::open(retain_store)),
    ))
}

fn compare_data_dir(backend: &str) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("greenmqtt-compare-{backend}-{timestamp}"))
}

async fn compare_bench_broker(
    node_id: u64,
    backend: &str,
    redis_url: &str,
) -> anyhow::Result<(AppBroker, Option<PathBuf>)> {
    let (sessiondict, dist, inbox, retain, cleanup_dir): CompareStateServices = match backend {
        "memory" => (
            Arc::new(SessionDictHandle::default()),
            Arc::new(DistHandle::default()),
            Arc::new(InboxHandle::default()),
            Arc::new(RetainHandle::default()),
            None,
        ),
        "sled" | "rocksdb" => {
            let path = compare_data_dir(backend);
            let (sessiondict, dist, inbox, retain) =
                durable_services(path.clone(), backend).await?;
            (sessiondict, dist, inbox, retain, Some(path))
        }
        "redis" => {
            let (sessiondict, dist, inbox, retain) = redis_services(redis_url).await?;
            (sessiondict, dist, inbox, retain, None)
        }
        other => anyhow::bail!("unsupported GREENMQTT_COMPARE_BACKENDS value: {other}"),
    };
    Ok((
        Arc::new(BrokerRuntime::with_plugins(
            BrokerConfig {
                node_id,
                enable_tcp: true,
                enable_tls: false,
                enable_ws: false,
                enable_wss: false,
                enable_quic: false,
                server_keep_alive_secs: None,
                max_packet_size: None,
                response_information: None,
                server_reference: None,
                audit_log_path: cleanup_dir
                    .as_ref()
                    .map(|path| path.join("admin-audit.jsonl")),
            },
            ConfiguredAuth::default(),
            ConfiguredAcl::default(),
            ConfiguredEventHook::default(),
            sessiondict,
            dist,
            inbox,
            retain,
        )),
        cleanup_dir,
    ))
}

#[cfg(test)]
#[cfg(test)]
mod main_tests;
