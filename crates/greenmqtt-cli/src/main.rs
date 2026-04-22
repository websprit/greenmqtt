use async_trait::async_trait;
use greenmqtt_broker::mqtt::{
    serve_quic_with_profile, serve_tcp_with_profile, serve_tls_with_profile, serve_ws_with_profile,
    serve_wss_with_profile,
};
use greenmqtt_broker::{BrokerConfig, BrokerRuntime};
use greenmqtt_core::{
    normalize_service_endpoint, ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle,
    ClusterNodeMembership, ConnectRequest, ControlPlaneRegistry, Lifecycle, MetadataRegistry,
    PublishProperties, PublishRequest, RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState,
    ReplicatedRangeDescriptor, ServiceEndpoint, ServiceEndpointTransport, ServiceKind,
    ServiceShardKey, ServiceShardKind, ServiceShardLifecycle, SessionKind, ShardControlRegistry,
};
use greenmqtt_dist::{
    DistBalanceAction, DistBalancePolicy, DistHandle, DistMaintenanceWorker, DistRouter,
    DistRuntime, DistTenantStats, PersistentDistHandle, ReplicatedDistHandle,
    ThresholdDistBalancePolicy,
};
use greenmqtt_http_api::HttpApi;
use greenmqtt_inbox::{
    inbox_tenant_gc_run, DelayedLwtPublish, DelayedLwtSink, InboxBalanceAction, InboxBalancePolicy,
    InboxHandle, InboxService, InboxTenantStats, PersistentInboxHandle, ReplicatedInboxHandle,
    ThresholdInboxBalancePolicy,
};
use greenmqtt_kv_balance::{
    BalanceCommand, BalanceCommandExecutor, BalanceCoordinator, ControlPlaneRuntime,
    ControlPlaneRuntimeConfig, MetadataBalanceController,
};
use greenmqtt_kv_client::{
    classify_kv_error, KvRangeErrorKind, KvRangeExecutor, KvRangeRouter,
    LeaderRoutedKvRangeExecutor, RoutedRangeDataClient,
};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, RocksDbKvEngine,
};
use greenmqtt_kv_raft::{MemoryRaftNode, RaftNode};
use greenmqtt_kv_server::{
    HostedRange, KvRangeHost, MemoryKvRangeHost, RangeLifecycleManager, ReplicaRuntime,
};
use greenmqtt_plugin_api::{
    current_listener_profile, AclAction, AclDecision, AclProvider, AclRule, AuthProvider,
    BridgeEventHook, BridgeRule, ConfiguredAcl, ConfiguredAuth, ConfiguredEventHook, EventHook,
    HookTarget, HttpAclConfig, HttpAclProvider, HttpAuthConfig, HttpAuthProvider, IdentityMatcher,
    StaticAclProvider, StaticAuthProvider, StaticEnhancedAuthProvider, TopicRewriteEventHook,
    TopicRewriteRule, WebHookConfig, WebHookEventHook, BRIDGE_CLIENT_ID_PREFIX,
};
use greenmqtt_retain::{
    retain_tenant_gc_run, PersistentRetainHandle, ReplicatedRetainHandle, RetainBalanceAction,
    RetainBalancePolicy, RetainHandle, RetainService, RetainTenantStats,
    ThresholdRetainBalancePolicy,
};
use greenmqtt_rpc::{
    DistGrpcClient, InboxGrpcClient, KvRangeGrpcClient, KvRangeGrpcExecutorFactory,
    MetadataPlaneLayout, PersistentMetadataRegistry, RaftTransportGrpcClient,
    RangeControlGrpcClient, ReplicatedMetadataRegistry, RetainGrpcClient,
    RoutedRangeControlGrpcClient, RpcRuntime, RpcTrafficGovernor, RpcTrafficGovernorConfig,
    SessionDictGrpcClient, StaticPeerForwarder, StaticServiceEndpointRegistry,
};
use greenmqtt_sessiondict::{
    PersistentSessionDictHandle, ReplicatedSessionDictHandle, SessionDictHandle, SessionDirectory,
};
use greenmqtt_storage::{
    RedisInboxStore, RedisInflightStore, RedisRetainStore, RedisRouteStore, RedisSessionStore,
    RedisSubscriptionStore, SledInboxStore, SledInflightStore, SledRetainStore, SledRouteStore,
    SledSessionStore, SledSubscriptionStore,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::Instant;
use sysinfo::{get_current_pid, System};
use tokio::task::JoinSet;

mod bench;
mod commands;
mod listeners;
mod runtime_support;

pub(crate) use bench::*;
pub(crate) use commands::*;
pub(crate) use listeners::*;
pub(crate) use runtime_support::*;

type AppBroker = Arc<BrokerRuntime<PortMappedAuth, PortMappedAcl, PortMappedEventHook>>;
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

struct LocalRangeRuntimeStack {
    sessiondict: Arc<dyn SessionDirectory>,
    dist: Arc<dyn DistRouter>,
    inbox: Arc<dyn InboxService>,
    retain: Arc<dyn RetainService>,
    range_host: Arc<dyn KvRangeHost>,
    range_runtime: Arc<ReplicaRuntime>,
}

async fn upsert_member_with_startup_retry<R>(
    registry: Arc<R>,
    member: ClusterNodeMembership,
) -> anyhow::Result<()>
where
    R: ClusterMembershipRegistry + ?Sized,
{
    let max_attempts = env_parse_or("GREENMQTT_METADATA_STARTUP_RETRIES", 60usize)?;
    let retry_delay = Duration::from_millis(env_parse_or(
        "GREENMQTT_METADATA_STARTUP_RETRY_DELAY_MS",
        1_000u64,
    )?);
    let mut last_error = None;
    for attempt in 1..=max_attempts {
        match registry.upsert_member(member.clone()).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                let kind = classify_kv_error(&error);
                let retryable = matches!(
                    kind,
                    KvRangeErrorKind::NotLeader(_)
                        | KvRangeErrorKind::EpochMismatch
                        | KvRangeErrorKind::RangeNotFound
                        | KvRangeErrorKind::ConfigChanging
                        | KvRangeErrorKind::SnapshotInProgress
                        | KvRangeErrorKind::TransportUnavailable
                        | KvRangeErrorKind::RetryExhausted
                );
                if !retryable || attempt == max_attempts {
                    return Err(error);
                }
                eprintln!(
                    "greenmqtt metadata startup retry attempt={attempt}/{max_attempts} kind={kind:?} error={error}"
                );
                last_error = Some(error);
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("metadata startup retry exhausted")))
}

async fn upsert_range_with_startup_retry<R>(
    registry: Arc<R>,
    descriptor: ReplicatedRangeDescriptor,
) -> anyhow::Result<()>
where
    R: greenmqtt_core::ReplicatedRangeRegistry + ?Sized,
{
    let max_attempts = env_parse_or("GREENMQTT_METADATA_STARTUP_RETRIES", 60usize)?;
    let retry_delay = Duration::from_millis(env_parse_or(
        "GREENMQTT_METADATA_STARTUP_RETRY_DELAY_MS",
        1_000u64,
    )?);
    let mut last_error = None;
    for attempt in 1..=max_attempts {
        match registry.upsert_range(descriptor.clone()).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                let kind = classify_kv_error(&error);
                let retryable = matches!(
                    kind,
                    KvRangeErrorKind::NotLeader(_)
                        | KvRangeErrorKind::EpochMismatch
                        | KvRangeErrorKind::RangeNotFound
                        | KvRangeErrorKind::ConfigChanging
                        | KvRangeErrorKind::SnapshotInProgress
                        | KvRangeErrorKind::TransportUnavailable
                        | KvRangeErrorKind::RetryExhausted
                );
                if !retryable || attempt == max_attempts {
                    return Err(error);
                }
                eprintln!(
                    "greenmqtt metadata range seed retry attempt={attempt}/{max_attempts} kind={kind:?} range_id={} error={error}",
                    descriptor.id
                );
                last_error = Some(error);
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("metadata range seed retry exhausted")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Text,
    Json,
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

fn advertised_rpc_endpoint(rpc_bind: SocketAddr) -> String {
    let local_fallback = if rpc_bind.ip().is_unspecified() {
        format!("http://127.0.0.1:{}", rpc_bind.port())
    } else {
        format!("http://{rpc_bind}")
    };
    let raw = std::env::var("GREENMQTT_ADVERTISE_RPC_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(local_fallback);
    normalize_service_endpoint(&raw, ServiceEndpointTransport::GrpcHttp)
        .expect("rpc endpoint should normalize")
}

fn resolved_control_plane_endpoints(
    rpc_bind: SocketAddr,
    state_endpoint: Option<&str>,
) -> anyhow::Result<(String, String)> {
    let local_endpoint = advertised_rpc_endpoint(rpc_bind);
    let metadata_endpoint = std::env::var("GREENMQTT_METADATA_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| state_endpoint.map(str::to_string))
        .unwrap_or_else(|| local_endpoint.clone());
    let range_endpoint = std::env::var("GREENMQTT_RANGE_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| state_endpoint.map(str::to_string))
        .unwrap_or_else(|| local_endpoint.clone());
    Ok((
        normalize_service_endpoint(&metadata_endpoint, ServiceEndpointTransport::GrpcHttp)?,
        normalize_service_endpoint(&range_endpoint, ServiceEndpointTransport::GrpcHttp)?,
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "demo".to_string());
    if mode == "shard" {
        return run_shard_command(args);
    }
    if mode == "range" {
        return run_range_command(args).await;
    }
    if mode == "control-command" {
        return run_control_command(args);
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
    let output_mode = output_mode();
    let mut local_range_host: Option<Arc<dyn KvRangeHost>> = None;
    let mut local_range_runtime: Option<Arc<ReplicaRuntime>> = None;
    let (default_sessiondict, default_dist, default_inbox, default_retain): AppStateServices =
        match state_endpoint.as_deref() {
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
                        Some(ref dir) if storage_backend == "rocksdb" => {
                            let stack =
                                range_scoped_rocksdb_stack(PathBuf::from(dir), node_id).await?;
                            local_range_host = Some(stack.range_host.clone());
                            local_range_runtime = Some(stack.range_runtime.clone());
                            (stack.sessiondict, stack.dist, stack.inbox, stack.retain)
                        }
                        Some(ref dir) => {
                            durable_services(PathBuf::from(dir), &storage_backend, node_id).await?
                        }
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

    if mode == "state-serve" {
        anyhow::ensure!(
            state_endpoint.is_none(),
            "state-serve does not support GREENMQTT_STATE_ENDPOINT"
        );
        let rpc_bind: SocketAddr = std::env::var("GREENMQTT_RPC_BIND")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
            .parse()?;
        let rpc_governor = Arc::new(RpcTrafficGovernor::new(
            RpcTrafficGovernorConfig::from_env()?
        ));
        println!("greenmqtt state grpc listening on {rpc_bind}");
        let rpc_endpoint = advertised_rpc_endpoint(rpc_bind);
        let assignment_registry =
            local_state_serve_metadata_registry(local_range_host.clone(), &storage_backend);
        let _replica_runtime_handle = local_range_runtime
            .clone()
            .map(|runtime| runtime.spawn(Duration::from_millis(50)));
        if let (Some(registry), Some(host)) =
            (assignment_registry.clone(), local_range_host.clone())
        {
            upsert_member_with_startup_retry(
                registry.clone(),
                ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![
                        ServiceEndpoint::new(
                            ServiceKind::SessionDict,
                            node_id,
                            rpc_endpoint.clone(),
                        ),
                        ServiceEndpoint::new(ServiceKind::Dist, node_id, rpc_endpoint.clone()),
                        ServiceEndpoint::new(ServiceKind::Inbox, node_id, rpc_endpoint.clone()),
                        ServiceEndpoint::new(ServiceKind::Retain, node_id, rpc_endpoint.clone()),
                    ],
                ),
            )
            .await?;
            let mut seeded_range_ids = Vec::new();
            for status in host.list_ranges().await? {
                seeded_range_ids.push(status.descriptor.id.clone());
                upsert_range_with_startup_retry(registry.clone(), status.descriptor).await?;
            }
            eprintln!(
                "greenmqtt state metadata seeded node_id={node_id} ranges={seeded_range_ids:?}"
            );
        }
        RpcRuntime {
            sessiondict: default_sessiondict,
            dist: default_dist,
            inbox: default_inbox,
            retain: default_retain,
            peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
            assignment_registry,
            range_host: local_range_host,
            range_runtime: local_range_runtime,
            inbox_lwt_sink: None,
        }
        .serve_with_governor(rpc_bind, rpc_governor)
        .await?;
        return Ok(());
    }

    let sessiondict =
        configured_sessiondict_service(default_sessiondict, state_endpoint.as_deref()).await?;
    let dist = configured_dist_service(default_dist, state_endpoint.as_deref()).await?;
    let inbox = configured_inbox_service(default_inbox, state_endpoint.as_deref()).await?;
    let retain = configured_retain_service(default_retain, state_endpoint.as_deref()).await?;

    let tcp_listeners = if mode == "serve" {
        listener_specs_from_env(
            "GREENMQTT_MQTT_BIND",
            "GREENMQTT_MQTT_BINDS",
            "127.0.0.1:1883",
        )?
    } else {
        Vec::new()
    };
    let tls_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_TLS_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let ws_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_WS_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let wss_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_WSS_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let quic_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_QUIC_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let (auth, acl, hooks) = if mode == "serve" {
        let profiles = collect_listener_profiles(&[
            &tcp_listeners,
            &tls_listeners,
            &ws_listeners,
            &wss_listeners,
            &quic_listeners,
        ]);
        configured_listener_profiles(node_id, &profiles)?
    } else {
        (
            PortMappedAuth::new(
                "default",
                HashMap::from([(String::from("default"), configured_auth()?)]),
            ),
            PortMappedAcl::new(
                "default",
                HashMap::from([(String::from("default"), configured_acl()?)]),
            ),
            PortMappedEventHook::new(
                "default",
                HashMap::from([(String::from("default"), configured_hooks(node_id)?)]),
            ),
        )
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
        let tls_bind = std::env::var("GREENMQTT_TLS_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let tls_cert = std::env::var("GREENMQTT_TLS_CERT").ok();
        let tls_key = std::env::var("GREENMQTT_TLS_KEY").ok();
        let ws_bind = std::env::var("GREENMQTT_WS_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let wss_bind = std::env::var("GREENMQTT_WSS_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let quic_bind = std::env::var("GREENMQTT_QUIC_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let rpc_bind: SocketAddr = std::env::var("GREENMQTT_RPC_BIND")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
            .parse()?;
        println!("greenmqtt http api listening on {http_bind}");
        for listener in &tcp_listeners {
            println!(
                "greenmqtt mqtt tcp listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        if let Some(listener) = &tls_bind {
            println!(
                "greenmqtt mqtt tls listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &tls_listeners {
            if tls_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt tls listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        if let Some(listener) = &ws_bind {
            println!(
                "greenmqtt mqtt ws listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &ws_listeners {
            if ws_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt ws listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        if let Some(listener) = &wss_bind {
            println!(
                "greenmqtt mqtt wss listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &wss_listeners {
            if wss_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt wss listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        if let Some(listener) = &quic_bind {
            println!(
                "greenmqtt mqtt quic listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &quic_listeners {
            if quic_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt quic listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        println!("greenmqtt internal grpc listening on {rpc_bind}");
        let rpc_endpoint = advertised_rpc_endpoint(rpc_bind);
        let metadata_backend = std::env::var("GREENMQTT_METADATA_BACKEND")
            .unwrap_or_else(|_| "memory".to_string())
            .to_lowercase();
        let metadata_dir = std::env::var("GREENMQTT_METADATA_DATA_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                data_dir
                    .as_ref()
                    .map(|dir| PathBuf::from(dir).join("metadata"))
            });
        let (metadata_registry, shard_registry, control_plane_registry): (
            Arc<dyn MetadataRegistry>,
            Arc<dyn ShardControlRegistry>,
            Arc<dyn ControlPlaneRegistry>,
        ) = match metadata_backend.as_str() {
            "memory" => {
                let registry = Arc::new(StaticServiceEndpointRegistry::default());
                (registry.clone(), registry.clone(), registry)
            }
            "rocksdb" => {
                let path = metadata_dir.ok_or_else(|| {
                    anyhow::anyhow!(
                        "GREENMQTT_METADATA_BACKEND=rocksdb requires GREENMQTT_METADATA_DATA_DIR or GREENMQTT_DATA_DIR"
                    )
                })?;
                let registry = Arc::new(PersistentMetadataRegistry::open(path).await?);
                (registry.clone(), registry.clone(), registry)
            }
            "replicated" => {
                let range_endpoint = std::env::var("GREENMQTT_RANGE_ENDPOINT")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| state_endpoint.clone())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "GREENMQTT_METADATA_BACKEND=replicated requires GREENMQTT_RANGE_ENDPOINT or GREENMQTT_STATE_ENDPOINT"
                        )
                    })?;
                let layout = metadata_plane_layout_from_env();
                let registry = Arc::new(ReplicatedMetadataRegistry::with_layout(
                    Arc::new(KvRangeGrpcClient::connect(range_endpoint).await?),
                    layout,
                ));
                (registry.clone(), registry.clone(), registry)
            }
            other => anyhow::bail!("unsupported GREENMQTT_METADATA_BACKEND: {other}"),
        };
        upsert_member_with_startup_retry(
            shard_registry.clone(),
            ClusterNodeMembership::new(
                node_id,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(ServiceKind::SessionDict, node_id, rpc_endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Dist, node_id, rpc_endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Inbox, node_id, rpc_endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Retain, node_id, rpc_endpoint.clone()),
                ],
            ),
        )
        .await?;
        let metrics_handle = PrometheusBuilder::new().install_recorder()?;
        let rpc_governor = Arc::new(RpcTrafficGovernor::new(
            RpcTrafficGovernorConfig::from_env()?
        ));
        let http = if let Some(range_runtime) = local_range_runtime.clone() {
            HttpApi::with_peers_shards_metrics_and_ranges(
                broker.clone(),
                Arc::new(peer_forwarder.clone()),
                shard_registry.clone(),
                control_plane_registry.clone(),
                range_runtime,
                metrics_handle,
                http_bind,
            )
            .with_rpc_governor(rpc_governor.clone())
        } else {
            HttpApi::with_peers_shards_and_metrics(
                broker.clone(),
                Arc::new(peer_forwarder.clone()),
                shard_registry.clone(),
                control_plane_registry.clone(),
                metrics_handle,
                http_bind,
            )
            .with_rpc_governor(rpc_governor.clone())
        };
        let rpc = RpcRuntime {
            sessiondict: broker.sessiondict.clone(),
            dist: broker.dist.clone(),
            inbox: broker.inbox.clone(),
            retain: broker.retain.clone(),
            peer_sink: broker.clone(),
            assignment_registry: Some(metadata_registry),
            range_host: local_range_host.clone(),
            range_runtime: local_range_runtime.clone(),
            inbox_lwt_sink: Some(Arc::new(BrokerDelayedLwtPublisher {
                broker: broker.clone(),
            })),
        };
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { http.start().await });
        tasks.spawn(async move { rpc.serve_with_governor(rpc_bind, rpc_governor).await });
        if let (Some(dist_runtime), Some(dist_maintenance)) = (
            replicated_dist_runtime_handle(state_endpoint.as_deref()).await?,
            configured_dist_maintenance()?,
        ) {
            let (metadata_endpoint, range_endpoint) =
                resolved_control_plane_endpoints(rpc_bind, state_endpoint.as_deref())?;
            spawn_dist_maintenance_task(
                &mut tasks,
                dist_runtime,
                control_plane_registry.clone(),
                RoutedRangeControlGrpcClient::connect(metadata_endpoint, range_endpoint).await?,
                dist_maintenance,
            );
        }
        if let (Some(inbox_runtime), Some(inbox_maintenance)) = (
            replicated_inbox_runtime_handle(state_endpoint.as_deref()).await?,
            configured_inbox_maintenance()?,
        ) {
            spawn_inbox_maintenance_task(&mut tasks, inbox_runtime, inbox_maintenance);
        }
        if let (Some(retain_runtime), Some(retain_maintenance)) = (
            replicated_retain_runtime_handle(state_endpoint.as_deref()).await?,
            configured_retain_maintenance()?,
        ) {
            spawn_retain_maintenance_task(&mut tasks, retain_runtime, retain_maintenance);
        }
        let (control_plane_metadata_endpoint, control_plane_range_endpoint) =
            resolved_control_plane_endpoints(rpc_bind, state_endpoint.as_deref())?;
        if let Some(control_plane_runtime) = configured_control_plane_runtime(
            node_id,
            control_plane_registry.clone(),
            control_plane_metadata_endpoint,
            control_plane_range_endpoint,
        )
        .await?
        {
            let runtime = control_plane_runtime;
            tasks.spawn(async move { runtime.serve().await });
        }
        for listener in tcp_listeners {
            let broker = broker.clone();
            tasks.spawn(async move {
                serve_tcp_with_profile(broker, listener.bind, listener.profile).await
            });
        }
        if let (Some(cert), Some(key)) = (tls_cert.clone(), tls_key.clone()) {
            let listeners = if tls_listeners.is_empty() {
                tls_bind.into_iter().collect::<Vec<_>>()
            } else {
                tls_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                let cert = cert.clone();
                let key = key.clone();
                tasks.spawn(async move {
                    serve_tls_with_profile(broker, listener.bind, cert, key, listener.profile).await
                });
            }
        }
        {
            let listeners = if ws_listeners.is_empty() {
                ws_bind.into_iter().collect::<Vec<_>>()
            } else {
                ws_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                tasks.spawn(async move {
                    serve_ws_with_profile(broker, listener.bind, listener.profile).await
                });
            }
        }
        if let (Some(cert), Some(key)) = (tls_cert.clone(), tls_key.clone()) {
            let listeners = if wss_listeners.is_empty() {
                wss_bind.into_iter().collect::<Vec<_>>()
            } else {
                wss_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                let cert = cert.clone();
                let key = key.clone();
                tasks.spawn(async move {
                    serve_wss_with_profile(broker, listener.bind, cert, key, listener.profile).await
                });
            }
        }
        if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            let listeners = if quic_listeners.is_empty() {
                quic_bind.into_iter().collect::<Vec<_>>()
            } else {
                quic_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                let cert = cert.clone();
                let key = key.clone();
                tasks.spawn(async move {
                    serve_quic_with_profile(broker, listener.bind, cert, key, listener.profile)
                        .await
                });
            }
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
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
    node_id: u64,
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
            let stack = range_scoped_rocksdb_stack(data_dir, node_id).await?;
            Ok((stack.sessiondict, stack.dist, stack.inbox, stack.retain))
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

async fn configured_retain_service(
    default_retain: Arc<dyn RetainService>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn RetainService>> {
    match resolved_state_mode("GREENMQTT_RETAIN_MODE")? {
        StateMode::Legacy => Ok(default_retain),
        StateMode::Replicated => {
            let (_metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "retain")?;
            Ok(Arc::new(RetainGrpcClient::connect(range_endpoint).await?))
        }
    }
}

async fn configured_dist_service(
    default_dist: Arc<dyn DistRouter>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn DistRouter>> {
    match resolved_state_mode("GREENMQTT_DIST_MODE")? {
        StateMode::Legacy => Ok(default_dist),
        StateMode::Replicated => {
            let (_metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "dist")?;
            Ok(Arc::new(DistGrpcClient::connect(range_endpoint).await?))
        }
    }
}

async fn configured_sessiondict_service(
    default_sessiondict: Arc<dyn SessionDirectory>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn SessionDirectory>> {
    match resolved_state_mode("GREENMQTT_SESSIONDICT_MODE")? {
        StateMode::Legacy => Ok(default_sessiondict),
        StateMode::Replicated => {
            let (_metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "sessiondict")?;
            Ok(Arc::new(
                SessionDictGrpcClient::connect(range_endpoint).await?,
            ))
        }
    }
}

async fn configured_inbox_service(
    default_inbox: Arc<dyn InboxService>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn InboxService>> {
    match resolved_state_mode("GREENMQTT_INBOX_MODE")? {
        StateMode::Legacy => Ok(default_inbox),
        StateMode::Replicated => {
            let (_metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "inbox")?;
            Ok(Arc::new(InboxGrpcClient::connect(range_endpoint).await?))
        }
    }
}

fn compare_data_dir(backend: &str) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("greenmqtt-compare-{backend}-{timestamp}"))
}

#[derive(Clone)]
struct BrokerDelayedLwtPublisher {
    broker: AppBroker,
}

#[async_trait]
impl DelayedLwtSink for BrokerDelayedLwtPublisher {
    async fn send_lwt(&self, publish: &DelayedLwtPublish) -> anyhow::Result<()> {
        let _ = self
            .broker
            .publish(&publish.session_id, publish.publish.clone())
            .await?;
        Ok(())
    }
}

async fn compare_bench_broker(
    node_id: u64,
    backend: &str,
    redis_url: &str,
) -> anyhow::Result<(AppBroker, Option<PathBuf>)> {
    let (default_auth, default_acl, default_hooks) = default_listener_profiles(
        ConfiguredAuth::default(),
        ConfiguredAcl::default(),
        ConfiguredEventHook::default(),
    );
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
                durable_services(path.clone(), backend, node_id).await?;
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
            default_auth,
            default_acl,
            default_hooks,
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
