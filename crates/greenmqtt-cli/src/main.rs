use async_trait::async_trait;
use greenmqtt_broker::mqtt::{
    serve_quic_with_profile, serve_tcp_with_profile, serve_tls_with_profile, serve_ws_with_profile,
    serve_wss_with_profile,
};
use greenmqtt_broker::{BrokerConfig, BrokerRuntime};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership,
    ConnectRequest, ControlPlaneRegistry, Lifecycle, MetadataRegistry, PublishProperties,
    PublishRequest, RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState,
    ReplicatedRangeDescriptor, ServiceEndpoint, ServiceKind, ServiceShardKey, ServiceShardKind,
    ServiceShardLifecycle, SessionKind, ShardControlRegistry,
};
use greenmqtt_dist::{
    DistBalanceAction, DistBalancePolicy, DistHandle, DistMaintenanceWorker, DistRouter,
    DistRuntime,
    DistTenantStats, PersistentDistHandle, ReplicatedDistHandle, ThresholdDistBalancePolicy,
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

pub(crate) use bench::*;
pub(crate) use commands::*;
pub(crate) use listeners::*;

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
    std::env::var("GREENMQTT_ADVERTISE_RPC_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(local_fallback)
}

fn resolved_control_plane_endpoints(
    rpc_bind: SocketAddr,
    state_endpoint: Option<&str>,
) -> (String, String) {
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
    (metadata_endpoint, range_endpoint)
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
        if let (Some(registry), Some(host)) = (assignment_registry.clone(), local_range_host.clone())
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
                resolved_control_plane_endpoints(rpc_bind, state_endpoint.as_deref());
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
            resolved_control_plane_endpoints(rpc_bind, state_endpoint.as_deref());
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

#[derive(Clone)]
struct HostedKvRangeExecutor {
    host: Arc<dyn KvRangeHost>,
}

#[async_trait]
impl KvRangeExecutor for HostedKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<bytes::Bytes>> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.reader().get(key).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted
            .space
            .reader()
            .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
            .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.writer().apply(mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.checkpoint(checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.snapshot().await
    }
}

#[derive(Clone)]
struct HostedStoreRangeRouter {
    host: Arc<dyn KvRangeHost>,
    descriptors: Arc<RwLock<BTreeMap<String, ReplicatedRangeDescriptor>>>,
}

impl HostedStoreRangeRouter {
    fn new(
        host: Arc<dyn KvRangeHost>,
        descriptors: impl IntoIterator<Item = ReplicatedRangeDescriptor>,
    ) -> Self {
        let router = Self {
            host,
            descriptors: Arc::new(RwLock::new(BTreeMap::new())),
        };
        {
            let mut guard = router
                .descriptors
                .write()
                .expect("hosted store router poisoned");
            for descriptor in descriptors {
                guard.insert(descriptor.id.clone(), descriptor);
            }
        }
        router
    }

    fn refresh_descriptor_from_status(
        &self,
        status: greenmqtt_kv_server::HostedRangeStatus,
    ) -> ReplicatedRangeDescriptor {
        let mut descriptor = status.descriptor;
        descriptor.leader_node_id = status.raft.leader_node_id;
        let mut replicas = status.raft.cluster_config.replicas();
        for replica in &mut replicas {
            replica.sync_state = status
                .raft
                .replica_progress
                .iter()
                .find(|progress| progress.node_id == replica.node_id)
                .map(|progress| {
                    if progress.match_index >= status.raft.commit_index {
                        ReplicaSyncState::Replicating
                    } else {
                        ReplicaSyncState::Probing
                    }
                })
                .unwrap_or(ReplicaSyncState::Offline);
        }
        descriptor.replicas = replicas;
        descriptor.commit_index = status.raft.commit_index;
        descriptor.applied_index = status.raft.applied_index;
        descriptor
    }

    async fn current_descriptors(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let statuses = self.host.list_ranges().await?;
        if statuses.is_empty() {
            return Ok(self
                .descriptors
                .read()
                .expect("hosted store router poisoned")
                .values()
                .cloned()
                .collect());
        }
        Ok(statuses
            .into_iter()
            .map(|status| self.refresh_descriptor_from_status(status))
            .collect())
    }

    fn shard_matches(descriptor: &ReplicatedRangeDescriptor, shard: &ServiceShardKey) -> bool {
        descriptor.shard.kind == shard.kind
            && (descriptor.shard.tenant_id == shard.tenant_id || descriptor.shard.tenant_id == "*")
            && (descriptor.shard.scope == shard.scope || descriptor.shard.scope == "*")
    }

    fn shard_specificity(descriptor: &ReplicatedRangeDescriptor, shard: &ServiceShardKey) -> usize {
        usize::from(descriptor.shard.tenant_id == shard.tenant_id) * 2
            + usize::from(descriptor.shard.scope == shard.scope)
    }
}

#[async_trait]
impl KvRangeRouter for HostedStoreRangeRouter {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        self.descriptors
            .write()
            .expect("hosted store router poisoned")
            .insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .descriptors
            .write()
            .expect("hosted store router poisoned")
            .remove(range_id))
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.current_descriptors().await
    }

    async fn by_id(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .current_descriptors()
            .await?
            .into_iter()
            .find(|descriptor| descriptor.id == range_id)
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor))
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .current_descriptors()
            .await?
            .into_iter()
            .filter(|descriptor| {
                Self::shard_matches(descriptor, shard) && descriptor.contains_key(key)
            })
            .max_by_key(|descriptor| Self::shard_specificity(descriptor, shard))
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor))
    }

    async fn route_shard(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Vec<greenmqtt_kv_client::RangeRoute>> {
        let mut descriptors = self
            .current_descriptors()
            .await?
            .into_iter()
            .filter(|descriptor| Self::shard_matches(descriptor, shard))
            .collect::<Vec<_>>();
        descriptors.sort_by_key(|descriptor| {
            std::cmp::Reverse(Self::shard_specificity(descriptor, shard))
        });
        Ok(descriptors
            .into_iter()
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor)
            .collect())
    }
}

fn range_engine_root(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("range-engine")
}

fn legacy_rocksdb_store_dirs(data_dir: &std::path::Path) -> [PathBuf; 6] {
    [
        data_dir.join("sessions"),
        data_dir.join("routes"),
        data_dir.join("subscriptions"),
        data_dir.join("inbox"),
        data_dir.join("inflight"),
        data_dir.join("retain"),
    ]
}

fn parse_store_raft_peers(node_id: u64) -> anyhow::Result<BTreeMap<u64, String>> {
    let mut peers = BTreeMap::new();
    let Some(raw) = std::env::var("GREENMQTT_STORE_RAFT_PEERS")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        peers.insert(node_id, String::new());
        return Ok(peers);
    };
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (node_id, endpoint) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid GREENMQTT_STORE_RAFT_PEERS entry: {item}"))?;
        peers.insert(node_id.parse()?, endpoint.trim().to_string());
    }
    peers.entry(node_id).or_insert_with(String::new);
    Ok(peers)
}

fn store_range_descriptors(voter_ids: &[u64]) -> Vec<ReplicatedRangeDescriptor> {
    let leader_node_id = voter_ids.first().copied();
    let replicas = voter_ids
        .iter()
        .map(|node_id| {
            RangeReplica::new(*node_id, ReplicaRole::Voter, ReplicaSyncState::Replicating)
        })
        .collect::<Vec<_>>();
    let metadata_layout = metadata_plane_layout_from_env();
    let mut descriptors: Vec<(String, ServiceShardKey)> = vec![
        (
            "__sessiondict".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__dist".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Dist,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__retain".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Retain,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__inbox".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Inbox,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__inflight".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Inflight,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
    ];
    descriptors.extend([
        (
            metadata_layout.assignments_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "assignments".into(),
            },
        ),
        (
            metadata_layout.members_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "members".into(),
            },
        ),
        (
            metadata_layout.ranges_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "ranges".into(),
            },
        ),
        (
            metadata_layout.balancers_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "balancers".into(),
            },
        ),
    ]);
    descriptors
        .into_iter()
        .map(|(range_id, shard)| {
            ReplicatedRangeDescriptor::new(
                range_id,
                shard,
                RangeBoundary::full(),
                1,
                1,
                leader_node_id,
                replicas.clone(),
                0,
                0,
                ServiceShardLifecycle::Serving,
            )
        })
        .collect()
}

#[derive(Clone, Default)]
struct StaticStoreReplicaTransport {
    endpoints: Arc<RwLock<BTreeMap<u64, String>>>,
    clients: Arc<tokio::sync::Mutex<HashMap<u64, RaftTransportGrpcClient>>>,
}

impl StaticStoreReplicaTransport {
    fn new(endpoints: BTreeMap<u64, String>) -> Self {
        Self {
            endpoints: Arc::new(RwLock::new(endpoints)),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn client_for(&self, target_node_id: u64) -> anyhow::Result<RaftTransportGrpcClient> {
        if let Some(client) = self.clients.lock().await.get(&target_node_id).cloned() {
            return Ok(client);
        }
        let endpoint = self
            .endpoints
            .read()
            .expect("store transport endpoints poisoned")
            .get(&target_node_id)
            .cloned()
            .filter(|endpoint| !endpoint.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("no store raft endpoint configured for node {target_node_id}")
            })?;
        let client = RaftTransportGrpcClient::connect(endpoint).await?;
        self.clients
            .lock()
            .await
            .insert(target_node_id, client.clone());
        Ok(client)
    }
}

#[async_trait]
impl greenmqtt_kv_server::ReplicaTransport for StaticStoreReplicaTransport {
    async fn send(
        &self,
        from_node_id: u64,
        target_node_id: u64,
        range_id: &str,
        message: &greenmqtt_kv_raft::RaftMessage,
    ) -> anyhow::Result<()> {
        let client = self.client_for(target_node_id).await?;
        client.send(range_id, from_node_id, message).await
    }
}

async fn store_membership_registry(
    peer_endpoints: &BTreeMap<u64, String>,
) -> anyhow::Result<Arc<StaticServiceEndpointRegistry>> {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    for (node_id, endpoint) in peer_endpoints {
        if endpoint.is_empty() {
            continue;
        }
        registry
            .upsert_member(ClusterNodeMembership::new(
                *node_id,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(ServiceKind::SessionDict, *node_id, endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Dist, *node_id, endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Inbox, *node_id, endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Retain, *node_id, endpoint.clone()),
                ],
            ))
            .await?;
    }
    Ok(registry)
}

fn metadata_plane_layout_from_env() -> MetadataPlaneLayout {
    let range_base =
        std::env::var("GREENMQTT_METADATA_RANGE_ID").unwrap_or_else(|_| "__metadata".to_string());
    MetadataPlaneLayout {
        assignments_range_id: std::env::var("GREENMQTT_METADATA_ASSIGNMENTS_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.assignments")),
        members_range_id: std::env::var("GREENMQTT_METADATA_MEMBERS_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.members")),
        ranges_range_id: std::env::var("GREENMQTT_METADATA_RANGES_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.ranges")),
        balancers_range_id: std::env::var("GREENMQTT_METADATA_BALANCERS_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.balancers")),
    }
}

fn local_state_serve_metadata_registry(
    range_host: Option<Arc<dyn KvRangeHost>>,
    storage_backend: &str,
) -> Option<Arc<dyn MetadataRegistry>> {
    if storage_backend != "rocksdb" {
        return None;
    };
    range_host.map(|host| {
        Arc::new(ReplicatedMetadataRegistry::with_layout(
            Arc::new(HostedKvRangeExecutor { host }),
            metadata_plane_layout_from_env(),
        )) as Arc<dyn MetadataRegistry>
    })
}

async fn range_scoped_rocksdb_stack(
    data_dir: PathBuf,
    node_id: u64,
) -> anyhow::Result<LocalRangeRuntimeStack> {
    let root = range_engine_root(&data_dir);
    let has_legacy_layout = legacy_rocksdb_store_dirs(&data_dir)
        .into_iter()
        .any(|path| path.exists());
    anyhow::ensure!(
        root.exists() || !has_legacy_layout,
        "legacy RocksDB store layout detected in {}; migrate to range-engine layout before using GREENMQTT_STORAGE_BACKEND=rocksdb",
        data_dir.display()
    );

    let engine = Arc::new(RocksDbKvEngine::open(&root)?);
    let peer_endpoints = parse_store_raft_peers(node_id)?;
    let mut voter_ids = peer_endpoints.keys().copied().collect::<Vec<_>>();
    voter_ids.sort_unstable();
    let descriptors = store_range_descriptors(&voter_ids);
    let host = Arc::new(MemoryKvRangeHost::default());
    for descriptor in &descriptors {
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::from(engine.open_range(&descriptor.id).await?);
        let raft_impl = Arc::new(MemoryRaftNode::new(
            node_id,
            &descriptor.id,
            greenmqtt_kv_raft::RaftClusterConfig {
                voters: voter_ids.clone(),
                learners: Vec::new(),
            },
        ));
        raft_impl.recover().await?;
        if let Some(leader_node_id) = descriptor.leader_node_id {
            raft_impl.transfer_leadership(leader_node_id).await?;
        }
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: descriptor.clone(),
            raft,
            space,
        })
        .await?;
    }
    let router: Arc<dyn KvRangeRouter> =
        Arc::new(HostedStoreRangeRouter::new(host.clone(), descriptors));
    let executor: Arc<dyn KvRangeExecutor> = if voter_ids.len() > 1 {
        let membership = store_membership_registry(&peer_endpoints).await?;
        Arc::new(LeaderRoutedKvRangeExecutor::new(
            router.clone(),
            membership,
            Arc::new(HostedKvRangeExecutor { host: host.clone() }),
            Arc::new(KvRangeGrpcExecutorFactory),
        ))
    } else {
        Arc::new(HostedKvRangeExecutor { host: host.clone() })
    };
    let transport: Arc<dyn greenmqtt_kv_server::ReplicaTransport> = if voter_ids.len() > 1 {
        Arc::new(StaticStoreReplicaTransport::new(peer_endpoints))
    } else {
        Arc::new(greenmqtt_rpc::NoopReplicaTransport)
    };
    let range_runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        transport,
        Some(Arc::new(RocksRangeLifecycleManager {
            engine: engine.clone(),
            node_id,
        })),
        Duration::from_secs(1),
        1024,
        64,
        64,
    ));
    Ok(LocalRangeRuntimeStack {
        sessiondict: Arc::new(ReplicatedSessionDictHandle::new(Arc::new(
            RoutedRangeDataClient::new(router.clone(), executor.clone()),
        ))),
        dist: Arc::new(ReplicatedDistHandle::new(Arc::new(RoutedRangeDataClient::new(
            router.clone(),
            executor.clone(),
        )))),
        inbox: Arc::new(ReplicatedInboxHandle::new(Arc::new(RoutedRangeDataClient::new(
            router.clone(),
            executor.clone(),
        )))),
        retain: Arc::new(ReplicatedRetainHandle::new(Arc::new(
            RoutedRangeDataClient::new(router, executor),
        ))),
        range_host: host,
        range_runtime,
    })
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

fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            (!entry.is_empty()).then(|| entry.to_string())
        })
        .collect()
}

fn env_parse_or<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T> {
    match std::env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid value for {key}: {raw}")),
        _ => Ok(default),
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_millis() as u64
}

fn env_parse_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => anyhow::bail!("invalid value for {key}: {raw}"),
        },
        _ => Ok(default),
    }
}

async fn configured_control_plane_runtime(
    node_id: u64,
    registry: Arc<dyn ControlPlaneRegistry>,
    metadata_endpoint: String,
    range_endpoint: String,
) -> anyhow::Result<Option<ControlPlaneRuntime>> {
    if !env_parse_bool("GREENMQTT_CONTROL_PLANE_ENABLED", false)? {
        return Ok(None);
    }
    let controller_node_id =
        env_parse_or::<u64>("GREENMQTT_CONTROL_PLANE_CONTROLLER_NODE_ID", node_id)?;
    if controller_node_id != node_id {
        eprintln!(
            "greenmqtt control plane runtime not started on node {node_id}; controller node is {controller_node_id}"
        );
        return Ok(None);
    }
    let desired_ranges = std::env::var("GREENMQTT_CONTROL_PLANE_DESIRED_RANGES_JSON")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|raw| {
            serde_json::from_str::<Vec<ReplicatedRangeDescriptor>>(&raw).map_err(|error| {
                anyhow::anyhow!(
                    "invalid GREENMQTT_CONTROL_PLANE_DESIRED_RANGES_JSON payload: {error}"
                )
            })
        })
        .transpose()?
        .unwrap_or_default();
    let config = ControlPlaneRuntimeConfig {
        balancer_name: std::env::var("GREENMQTT_CONTROL_PLANE_BALANCER_NAME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "metadata-control-plane".to_string()),
        controller_id: std::env::var("GREENMQTT_CONTROL_PLANE_CONTROLLER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("node-{node_id}")),
        desired_ranges,
        voters_per_range: env_parse_or("GREENMQTT_CONTROL_PLANE_VOTERS_PER_RANGE", 3usize)?,
        learners_per_range: env_parse_or("GREENMQTT_CONTROL_PLANE_LEARNERS_PER_RANGE", 0usize)?,
        bootstrap_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_BOOTSTRAP_INTERVAL_SECS",
            30u64,
        )?),
        replica_reconcile_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_REPLICA_INTERVAL_SECS",
            30u64,
        )?),
        leader_rebalance_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_LEADER_INTERVAL_SECS",
            30u64,
        )?),
        cleanup_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_CLEANUP_INTERVAL_SECS",
            30u64,
        )?),
        recovery_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_RECOVERY_INTERVAL_SECS",
            30u64,
        )?),
        command_reconcile_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_COMMAND_INTERVAL_SECS",
            10u64,
        )?),
    };
    let controller = Arc::new(MetadataBalanceController::new(registry.clone()));
    let coordinator = BalanceCoordinator::with_reconciliation(
        controller.clone(),
        Arc::new(ControlPlaneExecutor {
            registry: registry.clone(),
            range_control: RoutedRangeControlGrpcClient::connect(metadata_endpoint, range_endpoint)
                .await?,
        }),
        registry.clone(),
        config.balancer_name.clone(),
    );
    Ok(Some(ControlPlaneRuntime::new(
        coordinator,
        registry,
        config,
    )))
}

fn configured_dist_maintenance() -> anyhow::Result<Option<DistMaintenanceConfig>> {
    let tenants = std::env::var("GREENMQTT_DIST_MAINTENANCE_TENANTS")
        .ok()
        .map(|raw| parse_csv_list(&raw))
        .unwrap_or_default();
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(DistMaintenanceConfig {
        tenants,
        interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_DIST_MAINTENANCE_INTERVAL_SECS",
            30u64,
        )?),
        policy: ThresholdDistBalancePolicy {
            max_total_routes: env_parse_or("GREENMQTT_DIST_MAX_TOTAL_ROUTES", 5_000usize)?,
            max_wildcard_routes: env_parse_or("GREENMQTT_DIST_MAX_WILDCARD_ROUTES", 1_000usize)?,
            max_shared_routes: env_parse_or("GREENMQTT_DIST_MAX_SHARED_ROUTES", 1_000usize)?,
            desired_voters: env_parse_or("GREENMQTT_DIST_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_DIST_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

async fn replicated_dist_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<dyn DistRuntime>>> {
    if resolved_state_mode("GREENMQTT_DIST_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (_metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "dist")?;
    Ok(Some(Arc::new(DistGrpcClient::connect(range_endpoint).await?)))
}

async fn run_dist_maintenance_tick(
    dist: Arc<dyn DistRuntime>,
    config: &DistMaintenanceConfig,
) -> anyhow::Result<Vec<DistBalanceAction>> {
    let worker = DistMaintenanceWorker::new(dist.clone());
    let tenants = if config.targets_all_tenants() {
        dist.list_routes(None)
            .await?
            .into_iter()
            .map(|route| route.tenant_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        config.tenants.clone()
    };
    if tenants.is_empty() {
        return Ok(Vec::new());
    }
    let mut stats = Vec::new();
    for tenant_id in &tenants {
        let _ = worker.refresh_tenant(tenant_id).await?;
        let routes = dist.list_routes(Some(tenant_id)).await?;
        stats.push(DistTenantStats {
            tenant_id: tenant_id.clone(),
            total_routes: routes.len(),
            wildcard_routes: routes
                .iter()
                .filter(|route| {
                    route.topic_filter.contains('#') || route.topic_filter.contains('+')
                })
                .count(),
            shared_routes: routes
                .iter()
                .filter(|route| route.shared_group.is_some())
                .count(),
        });
    }
    Ok(config.policy.propose(&stats))
}

fn spawn_dist_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    dist: Arc<dyn DistRuntime>,
    registry: Arc<dyn ControlPlaneRegistry>,
    range_control: RoutedRangeControlGrpcClient,
    config: DistMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_dist_maintenance_tick(dist.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!("greenmqtt dist maintenance proposed actions: {:?}", actions);
                for action in &actions {
                    if let DistBalanceAction::SplitTenantRange { tenant_id } = action {
                        let routes = dist.list_routes(Some(tenant_id)).await?;
                        if routes.len() < 2 {
                            continue;
                        }
                        let mut ranges = registry.list_ranges(Some(ServiceShardKind::Dist)).await?;
                        ranges.retain(|range| {
                            range.shard.tenant_id == *tenant_id || range.shard.tenant_id == "*"
                        });
                        let mut selected = None;
                        for range in ranges {
                            let mut range_topic_filters = routes
                                .iter()
                                .map(|route| route.topic_filter.clone())
                                .filter(|topic_filter| range.contains_key(topic_filter.as_bytes()))
                                .collect::<Vec<_>>();
                            if range_topic_filters.len() < 2 {
                                continue;
                            }
                            range_topic_filters.sort();
                            let split_index = range_topic_filters.len() / 2;
                            let split_key = range_topic_filters[split_index].as_bytes().to_vec();
                            if range.boundary.start_key.as_deref() == Some(split_key.as_slice()) {
                                continue;
                            }
                            selected = Some((range.id.clone(), split_key));
                            break;
                        }
                        let Some((range_id, split_key)) = selected else {
                            continue;
                        };
                        match range_control
                            .split_range(&range_id, split_key.clone())
                            .await
                        {
                            Ok((left_range_id, right_range_id)) => eprintln!(
                                "greenmqtt dist maintenance split range_id={} left={} right={} split_key={:?}",
                                range_id,
                                left_range_id,
                                right_range_id,
                                split_key
                            ),
                            Err(error) => eprintln!(
                                "greenmqtt dist maintenance split failed range_id={} split_key={:?} error={error}",
                                range_id,
                                split_key
                            ),
                        }
                    }
                }
            }
        }
    });
}

fn configured_inbox_maintenance() -> anyhow::Result<Option<InboxMaintenanceConfig>> {
    let tenants = std::env::var("GREENMQTT_INBOX_MAINTENANCE_TENANTS")
        .ok()
        .map(|raw| parse_csv_list(&raw))
        .unwrap_or_default();
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(InboxMaintenanceConfig {
        tenants,
        interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_INBOX_MAINTENANCE_INTERVAL_SECS",
            30u64,
        )?),
        policy: ThresholdInboxBalancePolicy {
            max_subscriptions: env_parse_or("GREENMQTT_INBOX_MAX_SUBSCRIPTIONS", 5_000usize)?,
            max_offline_messages: env_parse_or(
                "GREENMQTT_INBOX_MAX_OFFLINE_MESSAGES",
                20_000usize,
            )?,
            max_inflight_messages: env_parse_or(
                "GREENMQTT_INBOX_MAX_INFLIGHT_MESSAGES",
                10_000usize,
            )?,
            desired_voters: env_parse_or("GREENMQTT_INBOX_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_INBOX_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

async fn replicated_inbox_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<dyn InboxService>>> {
    if resolved_state_mode("GREENMQTT_INBOX_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (_metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "inbox")?;
    Ok(Some(Arc::new(InboxGrpcClient::connect(range_endpoint).await?)))
}

async fn run_inbox_maintenance_tick(
    inbox: Arc<dyn InboxService>,
    config: &InboxMaintenanceConfig,
) -> anyhow::Result<Vec<InboxBalanceAction>> {
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let _ = inbox_tenant_gc_run(inbox.as_ref(), tenant_id, current_time_ms()).await?;
        stats.push(InboxTenantStats {
            tenant_id: tenant_id.clone(),
            subscriptions: inbox.count_tenant_subscriptions(tenant_id).await?,
            offline_messages: inbox.count_tenant_offline(tenant_id).await?,
            inflight_messages: inbox.count_tenant_inflight(tenant_id).await?,
        });
    }
    Ok(config.policy.propose(&stats))
}

fn spawn_inbox_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    inbox: Arc<dyn InboxService>,
    config: InboxMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_inbox_maintenance_tick(inbox.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!(
                    "greenmqtt inbox maintenance proposed actions: {:?}",
                    actions
                );
            }
        }
    });
}

fn configured_retain_maintenance() -> anyhow::Result<Option<RetainMaintenanceConfig>> {
    let tenants = std::env::var("GREENMQTT_RETAIN_MAINTENANCE_TENANTS")
        .ok()
        .map(|raw| parse_csv_list(&raw))
        .unwrap_or_default();
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(RetainMaintenanceConfig {
        tenants,
        interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_RETAIN_MAINTENANCE_INTERVAL_SECS",
            30u64,
        )?),
        policy: ThresholdRetainBalancePolicy {
            max_retained_messages: env_parse_or("GREENMQTT_RETAIN_MAX_MESSAGES", 10_000usize)?,
            desired_voters: env_parse_or("GREENMQTT_RETAIN_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_RETAIN_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

async fn replicated_retain_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<dyn RetainService>>> {
    if resolved_state_mode("GREENMQTT_RETAIN_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (_metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "retain")?;
    Ok(Some(Arc::new(RetainGrpcClient::connect(range_endpoint).await?)))
}

async fn run_retain_maintenance_tick(
    retain: Arc<dyn RetainService>,
    config: &RetainMaintenanceConfig,
) -> anyhow::Result<Vec<RetainBalanceAction>> {
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let _ = retain_tenant_gc_run(retain.as_ref(), tenant_id).await?;
        stats.push(RetainTenantStats {
            tenant_id: tenant_id.clone(),
            retained_messages: retain.list_tenant_retained(tenant_id).await?.len(),
        });
    }
    Ok(config.policy.propose(&stats))
}

fn spawn_retain_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    retain: Arc<dyn RetainService>,
    config: RetainMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_retain_maintenance_tick(retain.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!(
                    "greenmqtt retain maintenance proposed actions: {:?}",
                    actions
                );
            }
        }
    });
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
            Ok(Arc::new(SessionDictGrpcClient::connect(range_endpoint).await?))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateMode {
    Legacy,
    Replicated,
}

#[derive(Debug, Clone)]
struct DistMaintenanceConfig {
    tenants: Vec<String>,
    interval: Duration,
    policy: ThresholdDistBalancePolicy,
}

impl DistMaintenanceConfig {
    fn targets_all_tenants(&self) -> bool {
        self.tenants
            .iter()
            .any(|tenant| tenant == "*" || tenant.eq_ignore_ascii_case("all"))
    }
}

#[derive(Debug, Clone)]
struct InboxMaintenanceConfig {
    tenants: Vec<String>,
    interval: Duration,
    policy: ThresholdInboxBalancePolicy,
}

#[derive(Debug, Clone)]
struct RetainMaintenanceConfig {
    tenants: Vec<String>,
    interval: Duration,
    policy: ThresholdRetainBalancePolicy,
}

#[derive(Clone)]
struct ControlPlaneExecutor {
    registry: Arc<dyn ControlPlaneRegistry>,
    range_control: RoutedRangeControlGrpcClient,
}

#[async_trait]
impl BalanceCommandExecutor for ControlPlaneExecutor {
    async fn execute(&self, command: &BalanceCommand) -> anyhow::Result<bool> {
        match command {
            BalanceCommand::BootstrapRange { range_id, .. } => {
                let descriptor = self
                    .registry
                    .resolve_range(range_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("bootstrap range missing from metadata"))?;
                let _ = self.range_control.bootstrap_range(descriptor).await?;
                Ok(true)
            }
            BalanceCommand::ChangeReplicas {
                range_id,
                voters,
                learners,
            } => {
                self.range_control
                    .change_replicas(range_id, voters.clone(), learners.clone())
                    .await?;
                Ok(true)
            }
            BalanceCommand::TransferLeadership {
                range_id,
                to_node_id,
                ..
            } => {
                self.range_control
                    .transfer_leadership(range_id, *to_node_id)
                    .await?;
                Ok(true)
            }
            BalanceCommand::RecoverRange {
                range_id,
                new_leader_node_id,
            } => {
                self.range_control
                    .recover_range(range_id, *new_leader_node_id)
                    .await?;
                Ok(true)
            }
            BalanceCommand::CleanupReplicas { .. }
            | BalanceCommand::MigrateShard { .. }
            | BalanceCommand::FailoverShard { .. }
            | BalanceCommand::RecordBalancerState { .. }
            | BalanceCommand::SplitRange { .. }
            | BalanceCommand::MergeRanges { .. } => Ok(true),
        }
    }
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

#[derive(Clone)]
struct RocksRangeLifecycleManager {
    engine: Arc<RocksDbKvEngine>,
    node_id: u64,
}

#[async_trait]
impl RangeLifecycleManager for RocksRangeLifecycleManager {
    async fn create_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<HostedRange> {
        self.engine
            .bootstrap(KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::from(self.engine.open_range(&descriptor.id).await?);
        let raft_impl = Arc::new(MemoryRaftNode::new(
            self.node_id,
            &descriptor.id,
            greenmqtt_kv_raft::RaftClusterConfig {
                voters: descriptor
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == ReplicaRole::Voter)
                    .map(|replica| replica.node_id)
                    .collect(),
                learners: descriptor
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == ReplicaRole::Learner)
                    .map(|replica| replica.node_id)
                    .collect(),
            },
        ));
        raft_impl.recover().await?;
        if let Some(leader_node_id) = descriptor.leader_node_id {
            raft_impl.transfer_leadership(leader_node_id).await?;
        }
        let raft: Arc<dyn RaftNode> = raft_impl;
        Ok(HostedRange {
            descriptor,
            raft,
            space,
        })
    }

    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.engine.destroy_range(range_id).await
    }
}

fn parse_state_mode(value: &str) -> anyhow::Result<StateMode> {
    match value {
        "legacy" => Ok(StateMode::Legacy),
        "replicated" => Ok(StateMode::Replicated),
        other => anyhow::bail!("unsupported state mode: {other}"),
    }
}

fn resolved_state_mode(service_env_var: &str) -> anyhow::Result<StateMode> {
    if let Some(value) = std::env::var(service_env_var)
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        return parse_state_mode(&value.to_lowercase())
            .map_err(|_| anyhow::anyhow!("unsupported {service_env_var}: {value}"));
    }
    let global = std::env::var("GREENMQTT_STATE_MODE")
        .unwrap_or_else(|_| "legacy".to_string())
        .to_lowercase();
    parse_state_mode(&global)
        .map_err(|_| anyhow::anyhow!("unsupported GREENMQTT_STATE_MODE: {global}"))
}

fn resolved_replicated_endpoints(
    state_endpoint: Option<&str>,
    service_name: &str,
) -> anyhow::Result<(String, String)> {
    let metadata_endpoint = std::env::var("GREENMQTT_METADATA_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| state_endpoint.map(ToString::to_string))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "replicated {service_name} mode requires GREENMQTT_METADATA_ENDPOINT or GREENMQTT_STATE_ENDPOINT"
            )
        })?;
    let range_endpoint = std::env::var("GREENMQTT_RANGE_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| state_endpoint.map(ToString::to_string))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "replicated {service_name} mode requires GREENMQTT_RANGE_ENDPOINT or GREENMQTT_STATE_ENDPOINT"
            )
        })?;
    Ok((metadata_endpoint, range_endpoint))
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
