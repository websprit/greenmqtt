use super::{
    collect_listener_profiles, configured_acl, configured_acl_for_profile, configured_auth,
    configured_auth_for_profile, configured_control_plane_runtime, configured_dist_service,
    configured_hooks, configured_hooks_for_profile, configured_inbox_service,
    configured_listener_profiles, configured_retain_service, configured_sessiondict_service,
    configured_webhook, durable_services, execute_range_command_with_endpoint,
    listener_specs_from_env, parse_acl_rules, parse_bench_scenario, parse_bench_scenarios,
    parse_bridge_rules, parse_compare_backends, parse_identity_matchers, parse_listener_specs,
    parse_topic_rewrite_rules, range_command_request, redis_services, render_shard_response_text,
    replicated_range_clients, resolved_state_mode, run_bench, run_compare_bench, run_compare_soak,
    run_dist_maintenance_tick, run_inbox_maintenance_tick, run_profile_bench,
    run_retain_maintenance_tick, run_shard_command, run_soak, shard_command_request,
    validate_bench_report, validate_soak_report, BenchConfig, BenchReport, BenchScenario,
    BenchThresholds, DistMaintenanceConfig, InboxMaintenanceConfig, ListenerSpec, OutputMode,
    RetainMaintenanceConfig, SoakConfig, SoakReport, SoakThresholds, StateMode,
};
use async_trait::async_trait;
use greenmqtt_broker::{BrokerConfig, BrokerRuntime};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership,
    ConnectRequest, OfflineMessage, PublishProperties, PublishRequest, ReplicatedRangeDescriptor,
    ReplicatedRangeRegistry, RouteRecord, ServiceEndpoint, ServiceEndpointRegistry, ServiceKind,
    ServiceShardAssignment, ServiceShardKey, ServiceShardKind, ServiceShardLifecycle, SessionKind,
    SessionRecord, Subscription,
};
use greenmqtt_dist::{
    DistBalanceAction, DistHandle, DistRouter, ReplicatedDistHandle, ThresholdDistBalancePolicy,
};
use greenmqtt_inbox::{
    inbox_tenant_scan_shard, inflight_tenant_scan_shard, InboxBalanceAction, InboxHandle,
    InboxService, ReplicatedInboxHandle, ThresholdInboxBalancePolicy,
};
use greenmqtt_kv_engine::{KvEngine, KvRangeBootstrap, MemoryKvEngine};
use greenmqtt_kv_raft::{MemoryRaftNode, RaftClusterConfig, RaftNode};
use greenmqtt_kv_server::{HostedRange, KvRangeHost, MemoryKvRangeHost, RangeLifecycleManager};
use greenmqtt_plugin_api::{
    with_listener_profile, AclAction, AclDecision, AclProvider, AllowAllAcl, AllowAllAuth,
    AuthProvider, ConfiguredAcl, ConfiguredAuth, ConfiguredEventHook, EventHook, NoopEventHook,
};
use greenmqtt_retain::{
    retain_tenant_shard, ReplicatedRetainHandle, RetainBalanceAction, RetainHandle, RetainService,
    ThresholdRetainBalancePolicy,
};
use greenmqtt_rpc::{StaticPeerForwarder, StaticServiceEndpointRegistry};
use greenmqtt_sessiondict::SessionDictHandle;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static DURABLE_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

fn with_env_var<T>(key: &str, value: Option<&str>, f: impl FnOnce() -> T) -> T {
    let previous = std::env::var(key).ok();
    match value {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
    let result = f();
    match previous {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
    result
}

fn install_metrics_recorder_once() {
    static METRICS_READY: OnceLock<()> = OnceLock::new();
    let _ = METRICS_READY.get_or_init(|| {
        let _ = PrometheusBuilder::new().install_recorder();
    });
}

fn backpressure_test_broker(
) -> Arc<BrokerRuntime<ConfiguredAuth, ConfiguredAcl, ConfiguredEventHook>> {
    let mut broker = test_broker();
    if let Some(broker) = Arc::get_mut(&mut broker) {
        broker.set_max_online_sessions(1_000_000);
        broker.set_publish_rate_limit_per_connection(1_000_000, 1_000_000);
        broker.set_publish_rate_limit_per_tenant(1_000_000, 1_000_000);
        broker.set_connection_slowdown(1_000_000, Duration::from_millis(1));
    }
    broker
}

fn throughput_drop_within_one_percent(baseline: f64, instrumented: f64) -> bool {
    instrumented >= baseline * 0.99
}

fn test_broker() -> Arc<BrokerRuntime<ConfiguredAuth, ConfiguredAcl, ConfiguredEventHook>> {
    Arc::new(BrokerRuntime::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        ConfiguredAuth::default(),
        ConfiguredAcl::default(),
        ConfiguredEventHook::default(),
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    ))
}

#[derive(Clone)]
struct TestRangeLifecycleManager {
    engine: MemoryKvEngine,
}

#[async_trait]
impl RangeLifecycleManager for TestRangeLifecycleManager {
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
        let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
            self.engine.open_range(&descriptor.id).await?,
        );
        let raft = Arc::new(MemoryRaftNode::single_node(
            descriptor.leader_node_id.unwrap_or(1),
            &descriptor.id,
        ));
        raft.recover().await?;
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

async fn add_hosted_range(
    engine: &MemoryKvEngine,
    host: &MemoryKvRangeHost,
    registry: &StaticServiceEndpointRegistry,
    range_id: &str,
    shard: ServiceShardKey,
) {
    let descriptor = greenmqtt_core::ReplicatedRangeDescriptor::new(
        range_id.to_string(),
        shard.clone(),
        greenmqtt_core::RangeBoundary::full(),
        1,
        1,
        Some(1),
        vec![greenmqtt_core::RangeReplica::new(
            1,
            greenmqtt_core::ReplicaRole::Voter,
            greenmqtt_core::ReplicaSyncState::Replicating,
        )],
        0,
        0,
        ServiceShardLifecycle::Serving,
    );
    add_hosted_range_with_descriptor(engine, host, descriptor.clone(), 1).await;
    registry.upsert_range(descriptor).await.unwrap();
}

async fn add_hosted_range_with_descriptor(
    engine: &MemoryKvEngine,
    host: &MemoryKvRangeHost,
    descriptor: greenmqtt_core::ReplicatedRangeDescriptor,
    local_node_id: u64,
) {
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: descriptor.id.clone(),
            boundary: greenmqtt_core::RangeBoundary::full(),
        })
        .await
        .unwrap();
    let raft_impl = Arc::new(MemoryRaftNode::new(
        local_node_id,
        descriptor.id.clone(),
        RaftClusterConfig {
            voters: vec![local_node_id],
            learners: Vec::new(),
        },
    ));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl;
    host.add_range(HostedRange {
        descriptor: descriptor.clone(),
        raft,
        space: Arc::from(engine.open_range(&descriptor.id).await.unwrap()),
    })
    .await
    .unwrap();
}

#[test]
fn shard_cli_command_drives_http_shard_move() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let broker = test_broker();
        let shards = Arc::new(StaticServiceEndpointRegistry::default());
        shards
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Dist,
                    7,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();
        shards
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Dist,
                    9,
                    "http://127.0.0.1:50090",
                )],
            ))
            .await
            .unwrap();
        shards
            .upsert_assignment(ServiceShardAssignment::new(
                ServiceShardKey::dist("t1"),
                ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
                1,
                10,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        drop(broker);
        let shards_for_server = shards.clone();
        let server = std::thread::spawn(move || {
            let listener = TcpListener::bind(addr).unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            stream.read_to_string(&mut request).unwrap();
            assert!(request.contains("POST /v1/shards/dist/t1/*/move HTTP/1.1"));
            assert!(request.contains(r#"{"target_node_id":9,"dry_run":false}"#));
            let assignment = ServiceShardAssignment::new(
                ServiceShardKey::dist("t1"),
                ServiceEndpoint::new(ServiceKind::Dist, 9, "http://127.0.0.1:50090"),
                2,
                11,
                ServiceShardLifecycle::Draining,
            );
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime
                .block_on(shards_for_server.upsert_assignment(assignment))
                .unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 26\r\n\r\n{\"previous\":null,\"current\":null}";
            stream.write_all(response.as_bytes()).unwrap();
        });

        std::env::set_var("GREENMQTT_HTTP_BIND", addr.to_string());
        run_shard_command(
            vec![
                "move".to_string(),
                "dist".to_string(),
                "t1".to_string(),
                "*".to_string(),
                "9".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        std::env::remove_var("GREENMQTT_HTTP_BIND");

        let assignment = shards
            .resolve_assignment(&ServiceShardKey::dist("t1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(assignment.owner_node_id(), 9);

        server.join().unwrap();
    });
}

#[test]
fn range_cli_command_drives_grpc_range_control_service() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);
        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(greenmqtt_kv_server::ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(greenmqtt_rpc::NoopReplicaTransport),
            Some(Arc::new(TestRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: None,
                range_host: Some(host.clone()),
                range_runtime: Some(runtime.clone()),
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let endpoint = format!("http://{bind}");
        let bootstrap = execute_range_command_with_endpoint(
            vec![
                "bootstrap".to_string(),
                "--range-id".to_string(),
                "range-cli".to_string(),
                "--kind".to_string(),
                "retain".to_string(),
                "--tenant-id".to_string(),
                "t1".to_string(),
                "--scope".to_string(),
                "*".to_string(),
                "--voters".to_string(),
                "1".to_string(),
            ]
            .into_iter(),
            &endpoint,
        )
        .await
        .unwrap();
        assert!(bootstrap.contains("operation=bootstrap"));
        assert!(bootstrap.contains("status=bootstrapped"));
        assert!(bootstrap.contains("reason=\"Range bootstrapped and marked serving.\""));
        assert!(bootstrap.contains("range_id=range-cli"));

        let drain = execute_range_command_with_endpoint(
            vec!["drain".to_string(), "range-cli".to_string()].into_iter(),
            &endpoint,
        )
        .await
        .unwrap();
        assert!(drain.contains("operation=drain"));
        assert!(drain.contains("status=draining"));

        let retire = execute_range_command_with_endpoint(
            vec!["retire".to_string(), "range-cli".to_string()].into_iter(),
            &endpoint,
        )
        .await
        .unwrap();
        assert!(retire.contains("operation=retire"));
        assert!(retire.contains("status=retired"));

        server.abort();
    });
}

#[test]
fn range_cli_command_can_resolve_shard_identity_via_metadata() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);
        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let runtime = Arc::new(greenmqtt_kv_server::ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(greenmqtt_rpc::NoopReplicaTransport),
            Some(Arc::new(TestRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        runtime
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-cli-route",
                ServiceShardKey::retain("t1"),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![greenmqtt_core::RangeReplica::new(
                    1,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    1,
                    format!("http://{bind}"),
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "range-cli-route",
                ServiceShardKey::retain("t1"),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![greenmqtt_core::RangeReplica::new(
                    1,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host.clone()),
                range_runtime: Some(runtime.clone()),
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let endpoint = format!("http://{bind}");
        let output = execute_range_command_with_endpoint(
            vec![
                "change-replicas".to_string(),
                "--kind".to_string(),
                "retain".to_string(),
                "--tenant-id".to_string(),
                "t1".to_string(),
                "--scope".to_string(),
                "*".to_string(),
                "--voters".to_string(),
                "1,2".to_string(),
            ]
            .into_iter(),
            &endpoint,
        )
        .await
        .unwrap();
        assert!(output.contains("operation=change-replicas"));
        assert!(output.contains("status=reconfiguring"));

        let pending = runtime
            .pending_reconfiguration("range-cli-route")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.target_voters, vec![1, 2]);

        server.abort();
    });
}

#[test]
fn range_cli_command_reports_forward_target_when_endpoint_is_wrong_node() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metadata_bind = listener.local_addr().unwrap();
        drop(listener);
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let metadata_server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: None,
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(metadata_bind),
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let wrong_bind = listener.local_addr().unwrap();
        drop(listener);
        let wrong_server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(Arc::new(MemoryKvRangeHost::default())),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(wrong_bind),
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let owner_bind = listener.local_addr().unwrap();
        drop(listener);
        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let owner_runtime = Arc::new(greenmqtt_kv_server::ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(greenmqtt_rpc::NoopReplicaTransport),
            Some(Arc::new(TestRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        owner_runtime
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-cli-forward",
                ServiceShardKey::retain("t1"),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(2),
                vec![greenmqtt_core::RangeReplica::new(
                    2,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                2,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    2,
                    format!("http://{owner_bind}"),
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "range-cli-forward",
                ServiceShardKey::retain("t1"),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(2),
                vec![greenmqtt_core::RangeReplica::new(
                    2,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let owner_server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host.clone()),
                range_runtime: Some(owner_runtime.clone()),
                inbox_lwt_sink: None,
            }
            .serve(owner_bind),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;

        std::env::set_var(
            "GREENMQTT_METADATA_ENDPOINT",
            format!("http://{metadata_bind}"),
        );
        let output = execute_range_command_with_endpoint(
            vec!["drain".to_string(), "range-cli-forward".to_string()].into_iter(),
            &format!("http://{wrong_bind}"),
        )
        .await
        .unwrap();
        assert!(output.contains("forwarded=true"));
        assert!(output.contains("target_node_id=2"));
        assert!(output.contains(&format!("target_endpoint=http://{owner_bind}")));

        let hosted = host.open_range("range-cli-forward").await.unwrap().unwrap();
        assert_eq!(hosted.descriptor.lifecycle, ServiceShardLifecycle::Draining);

        std::env::remove_var("GREENMQTT_METADATA_ENDPOINT");
        metadata_server.abort();
        wrong_server.abort();
        owner_server.abort();
    });
}

#[test]
fn configured_services_can_run_under_global_replicated_state_mode() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let bind = "127.0.0.1:50066".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let host = Arc::new(MemoryKvRangeHost::default());
        let engine = MemoryKvEngine::default();
        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(ServiceKind::SessionDict, 1, "http://127.0.0.1:50066"),
                    ServiceEndpoint::new(ServiceKind::Dist, 1, "http://127.0.0.1:50066"),
                    ServiceEndpoint::new(ServiceKind::Inbox, 1, "http://127.0.0.1:50066"),
                    ServiceEndpoint::new(ServiceKind::Retain, 1, "http://127.0.0.1:50066"),
                ],
            ))
            .await
            .unwrap();

        add_hosted_range(
            &engine,
            &host,
            &registry,
            "retain-range-t1",
            greenmqtt_retain::retain_tenant_shard("t1"),
        )
        .await;
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "dist-range-t1",
            greenmqtt_dist::dist_tenant_shard("t1"),
        )
        .await;
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "sessiondict-range-t1",
            greenmqtt_sessiondict::session_scan_shard("t1"),
        )
        .await;
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "inbox-range-t1",
            greenmqtt_inbox::inbox_tenant_scan_shard("t1"),
        )
        .await;
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "inflight-range-t1",
            greenmqtt_inbox::inflight_tenant_scan_shard("t1"),
        )
        .await;

        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry),
                range_host: Some(host),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let endpoint = "http://127.0.0.1:50066";
        let previous = std::env::var("GREENMQTT_STATE_MODE").ok();
        std::env::set_var("GREENMQTT_STATE_MODE", "replicated");
        let sessiondict =
            configured_sessiondict_service(Arc::new(SessionDictHandle::default()), Some(endpoint))
                .await
                .unwrap();
        let dist = configured_dist_service(Arc::new(DistHandle::default()), Some(endpoint))
            .await
            .unwrap();
        let inbox = configured_inbox_service(Arc::new(InboxHandle::default()), Some(endpoint))
            .await
            .unwrap();
        let retain = configured_retain_service(Arc::new(RetainHandle::default()), Some(endpoint))
            .await
            .unwrap();

        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        sessiondict
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: Some(60),
                expires_at_ms: Some(1),
            })
            .await
            .unwrap();
        assert!(sessiondict
            .lookup_identity(&identity)
            .await
            .unwrap()
            .is_some());

        dist.add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "metrics/#".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
        assert_eq!(
            dist.match_topic("t1", &"metrics/cpu".into())
                .await
                .unwrap()
                .len(),
            1
        );

        inbox
            .subscribe(Subscription {
                session_id: "s1".into(),
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
        inbox
            .enqueue(OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                topic: "devices/a/state".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
        assert_eq!(inbox.fetch(&"s1".into()).await.unwrap().len(), 1);

        retain
            .retain(greenmqtt_core::RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert!(retain
            .lookup_topic("t1", "devices/a/state")
            .await
            .unwrap()
            .is_some());

        match previous {
            Some(value) => std::env::set_var("GREENMQTT_STATE_MODE", value),
            None => std::env::remove_var("GREENMQTT_STATE_MODE"),
        }

        server.abort();
    });
}

#[test]
fn dist_maintenance_tick_refreshes_routes_and_proposes_hot_tenant_actions() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let bind = "127.0.0.1:50068".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let host = Arc::new(MemoryKvRangeHost::default());
        let engine = MemoryKvEngine::default();
        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Dist,
                    1,
                    "http://127.0.0.1:50068",
                )],
            ))
            .await
            .unwrap();
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "dist-maint-range",
            greenmqtt_dist::dist_tenant_shard("hot"),
        )
        .await;

        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (router, executor) =
            replicated_range_clients("http://127.0.0.1:50068", "http://127.0.0.1:50068")
                .await
                .unwrap();
        let dist = Arc::new(ReplicatedDistHandle::new(router, executor));
        for (topic_filter, session_id, shared_group) in [
            ("devices/#", "s1", None),
            ("devices/+/state", "s2", Some("shared")),
            ("alerts/#", "s3", None),
        ] {
            dist.add_route(RouteRecord {
                tenant_id: "hot".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
        }

        let actions = run_dist_maintenance_tick(
            dist,
            &DistMaintenanceConfig {
                tenants: vec!["hot".into()],
                interval: Duration::from_secs(30),
                policy: ThresholdDistBalancePolicy {
                    max_total_routes: 1,
                    max_wildcard_routes: 1,
                    max_shared_routes: 0,
                    desired_voters: 3,
                    desired_learners: 1,
                },
            },
        )
        .await
        .unwrap();

        assert_eq!(
            actions,
            vec![
                DistBalanceAction::RunTenantCleanup {
                    tenant_id: "hot".into(),
                },
                DistBalanceAction::SplitTenantRange {
                    tenant_id: "hot".into(),
                },
                DistBalanceAction::ScaleTenantReplicas {
                    tenant_id: "hot".into(),
                    voters: 3,
                    learners: 1,
                },
            ]
        );

        server.abort();
    });
}

#[test]
fn inbox_maintenance_tick_expires_tenant_state_and_proposes_hot_tenant_actions() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let bind = "127.0.0.1:50069".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let host = Arc::new(MemoryKvRangeHost::default());
        let engine = MemoryKvEngine::default();
        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Inbox,
                    1,
                    "http://127.0.0.1:50069",
                )],
            ))
            .await
            .unwrap();
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "inbox-maint-range",
            inbox_tenant_scan_shard("hot"),
        )
        .await;
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "inflight-maint-range",
            inflight_tenant_scan_shard("hot"),
        )
        .await;

        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (router, executor) =
            replicated_range_clients("http://127.0.0.1:50069", "http://127.0.0.1:50069")
                .await
                .unwrap();
        let inbox = Arc::new(ReplicatedInboxHandle::new(router, executor));
        inbox
            .subscribe(Subscription {
                session_id: "s1".into(),
                tenant_id: "hot".into(),
                topic_filter: "devices/#".into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
        inbox
            .subscribe(Subscription {
                session_id: "s2".into(),
                tenant_id: "hot".into(),
                topic_filter: "alerts/#".into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: Some("shared".into()),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
        for session_id in ["s1", "s2"] {
            inbox
                .enqueue(OfflineMessage {
                    tenant_id: "hot".into(),
                    session_id: session_id.into(),
                    topic: "devices/a/state".into(),
                    payload: b"offline".to_vec().into(),
                    qos: 1,
                    retain: false,
                    from_session_id: "src".into(),
                    properties: PublishProperties::default(),
                })
                .await
                .unwrap();
        }
        inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: "hot".into(),
                session_id: "s1".into(),
                packet_id: 7,
                topic: "devices/a/state".into(),
                payload: b"inflight".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();

        let actions = run_inbox_maintenance_tick(
            inbox,
            &InboxMaintenanceConfig {
                tenants: vec!["hot".into()],
                interval: Duration::from_secs(30),
                retry_delay: Duration::from_millis(5),
                max_retries: 1,
                policy: ThresholdInboxBalancePolicy {
                    max_subscriptions: 1,
                    max_offline_messages: 1,
                    max_inflight_messages: 0,
                    desired_voters: 3,
                    desired_learners: 1,
                },
            },
        )
        .await
        .unwrap();

        assert_eq!(
            actions,
            vec![
                InboxBalanceAction::RunTenantMaintenance {
                    tenant_id: "hot".into(),
                },
                InboxBalanceAction::SplitTenantRange {
                    tenant_id: "hot".into(),
                },
                InboxBalanceAction::ScaleTenantReplicas {
                    tenant_id: "hot".into(),
                    voters: 3,
                    learners: 1,
                },
            ]
        );

        server.abort();
    });
}

#[test]
fn retain_maintenance_tick_refreshes_routes_and_proposes_hot_tenant_actions() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let bind = "127.0.0.1:50070".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let host = Arc::new(MemoryKvRangeHost::default());
        let engine = MemoryKvEngine::default();
        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    1,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();
        add_hosted_range(
            &engine,
            &host,
            &registry,
            "retain-maint-range",
            retain_tenant_shard("hot"),
        )
        .await;

        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (router, executor) =
            replicated_range_clients("http://127.0.0.1:50070", "http://127.0.0.1:50070")
                .await
                .unwrap();
        let retain = Arc::new(ReplicatedRetainHandle::new(router, executor));
        for topic in ["devices/a/state", "devices/b/state"] {
            retain
                .retain(greenmqtt_core::RetainedMessage {
                    tenant_id: "hot".into(),
                    topic: topic.into(),
                    payload: b"retained".to_vec().into(),
                    qos: 1,
                })
                .await
                .unwrap();
        }

        let actions = run_retain_maintenance_tick(
            retain,
            &RetainMaintenanceConfig {
                tenants: vec!["hot".into()],
                interval: Duration::from_secs(30),
                policy: ThresholdRetainBalancePolicy {
                    max_retained_messages: 1,
                    desired_voters: 3,
                    desired_learners: 1,
                },
            },
        )
        .await
        .unwrap();

        assert_eq!(
            actions,
            vec![
                RetainBalanceAction::RunTenantCleanup {
                    tenant_id: "hot".into(),
                },
                RetainBalanceAction::SplitTenantRange {
                    tenant_id: "hot".into(),
                },
                RetainBalanceAction::ScaleTenantReplicas {
                    tenant_id: "hot".into(),
                    voters: 3,
                    learners: 1,
                },
            ]
        );

        server.abort();
    });
}

#[test]
fn broker_can_use_replicated_service_clients_under_global_state_mode() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let bind = "127.0.0.1:50067".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let host = Arc::new(MemoryKvRangeHost::default());
        let engine = MemoryKvEngine::default();
        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(ServiceKind::SessionDict, 1, "http://127.0.0.1:50067"),
                    ServiceEndpoint::new(ServiceKind::Dist, 1, "http://127.0.0.1:50067"),
                    ServiceEndpoint::new(ServiceKind::Inbox, 1, "http://127.0.0.1:50067"),
                    ServiceEndpoint::new(ServiceKind::Retain, 1, "http://127.0.0.1:50067"),
                ],
            ))
            .await
            .unwrap();

        for (range_id, shard) in [
            (
                "retain-range-t1",
                greenmqtt_retain::retain_tenant_shard("t1"),
            ),
            ("dist-range-t1", greenmqtt_dist::dist_tenant_shard("t1")),
            (
                "sessiondict-range-t1",
                greenmqtt_sessiondict::session_scan_shard("t1"),
            ),
            (
                "inbox-range-t1",
                greenmqtt_inbox::inbox_tenant_scan_shard("t1"),
            ),
            (
                "inflight-range-t1",
                greenmqtt_inbox::inflight_tenant_scan_shard("t1"),
            ),
        ] {
            add_hosted_range(&engine, &host, &registry, range_id, shard).await;
        }

        let server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry),
                range_host: Some(host),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(bind),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let endpoint = "http://127.0.0.1:50067";
        let previous = std::env::var("GREENMQTT_STATE_MODE").ok();
        std::env::set_var("GREENMQTT_STATE_MODE", "replicated");
        let broker = Arc::new(BrokerRuntime::with_cluster(
            BrokerConfig {
                node_id: 1,
                enable_tcp: true,
                enable_tls: false,
                enable_ws: false,
                enable_wss: false,
                enable_quic: false,
                server_keep_alive_secs: None,
                max_packet_size: None,
                response_information: None,
                server_reference: None,
                audit_log_path: None,
            },
            AllowAllAuth,
            AllowAllAcl,
            NoopEventHook,
            Arc::new(StaticPeerForwarder::default()),
            configured_sessiondict_service(Arc::new(SessionDictHandle::default()), Some(endpoint))
                .await
                .unwrap(),
            configured_dist_service(Arc::new(DistHandle::default()), Some(endpoint))
                .await
                .unwrap(),
            configured_inbox_service(Arc::new(InboxHandle::default()), Some(endpoint))
                .await
                .unwrap(),
            configured_retain_service(Arc::new(RetainHandle::default()), Some(endpoint))
                .await
                .unwrap(),
        ));

        let subscriber = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "sub".into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
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
            .await
            .unwrap();

        let publisher = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u2".into(),
                    client_id: "pub".into(),
                },
                node_id: 1,
                kind: SessionKind::Transient,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        let outcome = broker
            .publish(
                &publisher.session.session_id,
                PublishRequest {
                    topic: "devices/a/state".into(),
                    payload: b"replicated".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.online_deliveries, 1);

        let deliveries = broker
            .drain_deliveries(&subscriber.session.session_id)
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].payload, b"replicated".to_vec());

        let stats = broker.stats().await.unwrap();
        assert_eq!(stats.global_session_records, 2);
        assert_eq!(stats.route_records, 1);
        assert_eq!(stats.subscription_records, 1);

        match previous {
            Some(value) => std::env::set_var("GREENMQTT_STATE_MODE", value),
            None => std::env::remove_var("GREENMQTT_STATE_MODE"),
        }

        server.abort();
    });
}

#[test]
fn configured_retain_service_survives_leader_failover() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let metadata_bind = "127.0.0.1:50068".parse().unwrap();
        let retain_bind_1 = "127.0.0.1:50069".parse().unwrap();
        let retain_bind_2 = "127.0.0.1:50070".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let retain_range = "retain-range-t1";
        let shard = greenmqtt_retain::retain_tenant_shard("t1");

        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    7,
                    "http://127.0.0.1:50069",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    9,
                    "http://127.0.0.1:50070",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_range(greenmqtt_core::ReplicatedRangeDescriptor::new(
                retain_range,
                shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![greenmqtt_core::RangeReplica::new(
                    7,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let metadata_server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: None,
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(metadata_bind),
        );

        let host_1 = Arc::new(MemoryKvRangeHost::default());
        let engine_1 = MemoryKvEngine::default();
        add_hosted_range_with_descriptor(
            &engine_1,
            &host_1,
            greenmqtt_core::ReplicatedRangeDescriptor::new(
                retain_range,
                shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![greenmqtt_core::RangeReplica::new(
                    7,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            7,
        )
        .await;
        let server_1 = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host_1),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(retain_bind_1),
        );

        let host_2 = Arc::new(MemoryKvRangeHost::default());
        let engine_2 = MemoryKvEngine::default();
        add_hosted_range_with_descriptor(
            &engine_2,
            &host_2,
            greenmqtt_core::ReplicatedRangeDescriptor::new(
                retain_range,
                shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(9),
                vec![greenmqtt_core::RangeReplica::new(
                    9,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            9,
        )
        .await;
        let server_2 = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(host_2),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(retain_bind_2),
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        let previous_state_mode = std::env::var("GREENMQTT_STATE_MODE").ok();
        let previous_metadata = std::env::var("GREENMQTT_METADATA_ENDPOINT").ok();
        let previous_range = std::env::var("GREENMQTT_RANGE_ENDPOINT").ok();
        std::env::set_var("GREENMQTT_STATE_MODE", "replicated");
        std::env::set_var("GREENMQTT_METADATA_ENDPOINT", "http://127.0.0.1:50068");
        std::env::set_var("GREENMQTT_RANGE_ENDPOINT", "http://127.0.0.1:50069");

        let retain = configured_retain_service(Arc::new(RetainHandle::default()), None)
            .await
            .unwrap();
        retain
            .retain(greenmqtt_core::RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"before".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            retain
                .lookup_topic("t1", "devices/a/state")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"before".as_slice()
        );

        registry
            .upsert_range(greenmqtt_core::ReplicatedRangeDescriptor::new(
                retain_range,
                shard,
                greenmqtt_core::RangeBoundary::full(),
                2,
                2,
                Some(9),
                vec![greenmqtt_core::RangeReplica::new(
                    9,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        server_1.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;

        retain
            .retain(greenmqtt_core::RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/b/state".into(),
                payload: b"after".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            retain
                .lookup_topic("t1", "devices/b/state")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"after".as_slice()
        );

        match previous_state_mode {
            Some(value) => std::env::set_var("GREENMQTT_STATE_MODE", value),
            None => std::env::remove_var("GREENMQTT_STATE_MODE"),
        }
        match previous_metadata {
            Some(value) => std::env::set_var("GREENMQTT_METADATA_ENDPOINT", value),
            None => std::env::remove_var("GREENMQTT_METADATA_ENDPOINT"),
        }
        match previous_range {
            Some(value) => std::env::set_var("GREENMQTT_RANGE_ENDPOINT", value),
            None => std::env::remove_var("GREENMQTT_RANGE_ENDPOINT"),
        }

        metadata_server.abort();
        server_2.abort();
    });
}

#[test]
fn broker_can_continue_after_retain_leader_move_under_replicated_state_mode() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let metadata_bind = "127.0.0.1:50071".parse().unwrap();
        let stable_bind = "127.0.0.1:50072".parse().unwrap();
        let retain_bind_1 = "127.0.0.1:50073".parse().unwrap();
        let retain_bind_2 = "127.0.0.1:50074".parse().unwrap();
        let registry = Arc::new(StaticServiceEndpointRegistry::default());

        registry
            .upsert_member(ClusterNodeMembership::new(
                1,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(ServiceKind::SessionDict, 1, "http://127.0.0.1:50072"),
                    ServiceEndpoint::new(ServiceKind::Dist, 1, "http://127.0.0.1:50072"),
                    ServiceEndpoint::new(ServiceKind::Inbox, 1, "http://127.0.0.1:50072"),
                ],
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    7,
                    "http://127.0.0.1:50073",
                )],
            ))
            .await
            .unwrap();
        registry
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    9,
                    "http://127.0.0.1:50074",
                )],
            ))
            .await
            .unwrap();

        let stable_host = Arc::new(MemoryKvRangeHost::default());
        let stable_engine = MemoryKvEngine::default();
        for (range_id, shard) in [
            (
                "sessiondict-range-t1",
                greenmqtt_sessiondict::session_scan_shard("t1"),
            ),
            ("dist-range-t1", greenmqtt_dist::dist_tenant_shard("t1")),
            (
                "inbox-range-t1",
                greenmqtt_inbox::inbox_tenant_scan_shard("t1"),
            ),
            (
                "inflight-range-t1",
                greenmqtt_inbox::inflight_tenant_scan_shard("t1"),
            ),
        ] {
            let descriptor = greenmqtt_core::ReplicatedRangeDescriptor::new(
                range_id,
                shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![greenmqtt_core::RangeReplica::new(
                    1,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            );
            add_hosted_range_with_descriptor(&stable_engine, &stable_host, descriptor.clone(), 1)
                .await;
            registry.upsert_range(descriptor).await.unwrap();
        }

        let retain_shard = greenmqtt_retain::retain_tenant_shard("t1");
        registry
            .upsert_range(greenmqtt_core::ReplicatedRangeDescriptor::new(
                "retain-range-t1",
                retain_shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![greenmqtt_core::RangeReplica::new(
                    7,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let metadata_server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: None,
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(metadata_bind),
        );
        let stable_server = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(stable_host),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(stable_bind),
        );

        let retain_host_1 = Arc::new(MemoryKvRangeHost::default());
        let retain_engine_1 = MemoryKvEngine::default();
        add_hosted_range_with_descriptor(
            &retain_engine_1,
            &retain_host_1,
            greenmqtt_core::ReplicatedRangeDescriptor::new(
                "retain-range-t1",
                retain_shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(7),
                vec![greenmqtt_core::RangeReplica::new(
                    7,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            7,
        )
        .await;
        let retain_server_1 = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(retain_host_1),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(retain_bind_1),
        );

        let retain_host_2 = Arc::new(MemoryKvRangeHost::default());
        let retain_engine_2 = MemoryKvEngine::default();
        add_hosted_range_with_descriptor(
            &retain_engine_2,
            &retain_host_2,
            greenmqtt_core::ReplicatedRangeDescriptor::new(
                "retain-range-t1",
                retain_shard.clone(),
                greenmqtt_core::RangeBoundary::full(),
                1,
                1,
                Some(9),
                vec![greenmqtt_core::RangeReplica::new(
                    9,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            9,
        )
        .await;
        let retain_server_2 = tokio::spawn(
            greenmqtt_rpc::RpcRuntime {
                sessiondict: Arc::new(SessionDictHandle::default()),
                dist: Arc::new(DistHandle::default()),
                inbox: Arc::new(InboxHandle::default()),
                retain: Arc::new(RetainHandle::default()),
                peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
                assignment_registry: Some(registry.clone()),
                range_host: Some(retain_host_2),
                range_runtime: None,
                inbox_lwt_sink: None,
            }
            .serve(retain_bind_2),
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        let previous_state_mode = std::env::var("GREENMQTT_STATE_MODE").ok();
        let previous_metadata = std::env::var("GREENMQTT_METADATA_ENDPOINT").ok();
        let previous_range = std::env::var("GREENMQTT_RANGE_ENDPOINT").ok();
        std::env::set_var("GREENMQTT_STATE_MODE", "replicated");
        std::env::set_var("GREENMQTT_METADATA_ENDPOINT", "http://127.0.0.1:50071");
        std::env::set_var("GREENMQTT_RANGE_ENDPOINT", "http://127.0.0.1:50073");

        let broker = Arc::new(BrokerRuntime::with_cluster(
            BrokerConfig {
                node_id: 1,
                enable_tcp: true,
                enable_tls: false,
                enable_ws: false,
                enable_wss: false,
                enable_quic: false,
                server_keep_alive_secs: None,
                max_packet_size: None,
                response_information: None,
                server_reference: None,
                audit_log_path: None,
            },
            AllowAllAuth,
            AllowAllAcl,
            NoopEventHook,
            Arc::new(StaticPeerForwarder::default()),
            configured_sessiondict_service(Arc::new(SessionDictHandle::default()), None)
                .await
                .unwrap(),
            configured_dist_service(Arc::new(DistHandle::default()), None)
                .await
                .unwrap(),
            configured_inbox_service(Arc::new(InboxHandle::default()), None)
                .await
                .unwrap(),
            configured_retain_service(Arc::new(RetainHandle::default()), None)
                .await
                .unwrap(),
        ));
        let retain = configured_retain_service(Arc::new(RetainHandle::default()), None)
            .await
            .unwrap();

        let publisher = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "pub".into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        broker
            .publish(
                &publisher.session.session_id,
                PublishRequest {
                    topic: "devices/before/state".into(),
                    payload: b"before".to_vec().into(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();

        registry
            .upsert_range(greenmqtt_core::ReplicatedRangeDescriptor::new(
                "retain-range-t1",
                retain_shard,
                greenmqtt_core::RangeBoundary::full(),
                2,
                2,
                Some(9),
                vec![greenmqtt_core::RangeReplica::new(
                    9,
                    greenmqtt_core::ReplicaRole::Voter,
                    greenmqtt_core::ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        retain_server_1.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let outcome = broker
            .publish(
                &publisher.session.session_id,
                PublishRequest {
                    topic: "devices/after/state".into(),
                    payload: b"after".to_vec().into(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.matched_routes, 0);

        assert_eq!(
            retain
                .lookup_topic("t1", "devices/after/state")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"after".as_slice()
        );

        match previous_state_mode {
            Some(value) => std::env::set_var("GREENMQTT_STATE_MODE", value),
            None => std::env::remove_var("GREENMQTT_STATE_MODE"),
        }
        match previous_metadata {
            Some(value) => std::env::set_var("GREENMQTT_METADATA_ENDPOINT", value),
            None => std::env::remove_var("GREENMQTT_METADATA_ENDPOINT"),
        }
        match previous_range {
            Some(value) => std::env::set_var("GREENMQTT_RANGE_ENDPOINT", value),
            None => std::env::remove_var("GREENMQTT_RANGE_ENDPOINT"),
        }

        metadata_server.abort();
        stable_server.abort();
        retain_server_2.abort();
    });
}

#[test]
fn resolved_state_mode_falls_back_to_global_setting() {
    with_env_var("GREENMQTT_STATE_MODE", Some("replicated"), || {
        with_env_var("GREENMQTT_RETAIN_MODE", None, || {
            assert_eq!(
                resolved_state_mode("GREENMQTT_RETAIN_MODE").unwrap(),
                StateMode::Replicated
            );
        })
    });
}

#[test]
fn resolved_state_mode_prefers_service_override_over_global_setting() {
    with_env_var("GREENMQTT_STATE_MODE", Some("replicated"), || {
        with_env_var("GREENMQTT_RETAIN_MODE", Some("legacy"), || {
            assert_eq!(
                resolved_state_mode("GREENMQTT_RETAIN_MODE").unwrap(),
                StateMode::Legacy
            );
        })
    });
}

#[test]
fn shard_cli_command_reads_http_shard_audit() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let server = std::thread::spawn(move || {
            let listener = TcpListener::bind(addr).unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            stream.read_to_string(&mut request).unwrap();
            assert!(request.contains("GET /v1/audit?shard_only=true&limit=5 HTTP/1.1"));
            let body = r#"[{"seq":1,"timestamp_ms":1,"action":"shard_move","target":"shards","details":{"tenant_id":"t1"}}]"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        std::env::set_var("GREENMQTT_HTTP_BIND", addr.to_string());
        std::env::set_var("GREENMQTT_OUTPUT", "json");
        run_shard_command(
            vec![
                "audit".to_string(),
                "--limit".to_string(),
                "5".to_string(),
                "--output".to_string(),
                "json".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        std::env::remove_var("GREENMQTT_HTTP_BIND");
        std::env::remove_var("GREENMQTT_OUTPUT");

        server.join().unwrap();
    });
}

fn run_shard_cli_against_mock_http(
    args: Vec<String>,
    expected_request_fragment: &'static str,
    response_body: &'static str,
) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            stream.read_to_string(&mut request).unwrap();
            assert!(request.contains(expected_request_fragment));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        std::env::set_var("GREENMQTT_HTTP_BIND", addr.to_string());
        run_shard_command(args.into_iter()).unwrap();
        std::env::remove_var("GREENMQTT_HTTP_BIND");
        server.join().unwrap();
    });
}

#[test]
fn shard_cli_command_drives_http_shard_list() {
    run_shard_cli_against_mock_http(
        vec![
            "ls".into(),
            "--kind".into(),
            "dist".into(),
            "--tenant-id".into(),
            "t1".into(),
        ],
        "GET /v1/shards?kind=dist&tenant_id=t1 HTTP/1.1",
        "[]",
    );
}

#[test]
fn shard_cli_command_drives_http_shard_drain() {
    run_shard_cli_against_mock_http(
        vec!["drain".into(), "dist".into(), "t1".into(), "*".into()],
        r#"POST /v1/shards/dist/t1/*/drain HTTP/1.1"#,
        r#"{"previous":null,"current":null}"#,
    );
}

#[test]
fn shard_cli_command_drives_http_shard_catch_up() {
    run_shard_cli_against_mock_http(
        vec![
            "catch-up".into(),
            "dist".into(),
            "t1".into(),
            "*".into(),
            "9".into(),
        ],
        r#"POST /v1/shards/dist/t1/*/catch-up HTTP/1.1"#,
        r#"{"previous":null,"current":null}"#,
    );
}

#[test]
fn shard_cli_command_drives_http_shard_repair() {
    run_shard_cli_against_mock_http(
        vec![
            "repair".into(),
            "dist".into(),
            "t1".into(),
            "*".into(),
            "9".into(),
        ],
        r#"POST /v1/shards/dist/t1/*/repair HTTP/1.1"#,
        r#"{"previous":null,"current":null}"#,
    );
}

#[test]
fn shard_cli_command_drives_http_shard_failover() {
    run_shard_cli_against_mock_http(
        vec![
            "failover".into(),
            "dist".into(),
            "t1".into(),
            "*".into(),
            "9".into(),
        ],
        r#"POST /v1/shards/dist/t1/*/failover HTTP/1.1"#,
        r#"{"previous":null,"current":null}"#,
    );
}

fn assert_bench_phase_timings(report: &BenchReport) {
    assert!(report.elapsed_ms >= report.setup_ms + report.publish_ms + report.drain_ms);
}

fn temp_data_dir(name: &str) -> PathBuf {
    let unique = DURABLE_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("greenmqtt-cli-{name}-{timestamp}-{unique}"))
}

async fn durable_test_broker(
    backend: &str,
) -> (
    Arc<BrokerRuntime<ConfiguredAuth, ConfiguredAcl, ConfiguredEventHook>>,
    PathBuf,
) {
    let data_dir = temp_data_dir(backend);
    let (sessiondict, dist, inbox, retain) = durable_services(data_dir.clone(), backend, 1)
        .await
        .unwrap();
    let broker = Arc::new(BrokerRuntime::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: Some(data_dir.join("admin-audit.jsonl")),
        },
        ConfiguredAuth::default(),
        ConfiguredAcl::default(),
        ConfiguredEventHook::default(),
        sessiondict,
        dist,
        inbox,
        retain,
    ));
    (broker, data_dir)
}

#[tokio::test]
async fn rocksdb_durable_services_persist_via_range_scoped_engine() {
    let data_dir = temp_data_dir("rocksdb-range-engine");
    let (sessiondict, dist, inbox, retain) = durable_services(data_dir.clone(), "rocksdb", 1)
        .await
        .unwrap();

    let identity = ClientIdentity {
        tenant_id: "t1".into(),
        user_id: "u1".into(),
        client_id: "c1".into(),
    };
    sessiondict
        .register(SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: identity.clone(),
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1234),
        })
        .await
        .unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();
    inbox
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    retain
        .retain(greenmqtt_core::RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    drop(sessiondict);
    drop(dist);
    drop(inbox);
    drop(retain);

    assert!(data_dir.join("range-engine").exists());
    assert!(!data_dir.join("sessions").exists());

    let (sessiondict, dist, inbox, retain) = durable_services(data_dir.clone(), "rocksdb", 1)
        .await
        .unwrap();
    assert!(sessiondict
        .lookup_identity(&identity)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        dist.match_topic("t1", &"devices/a/state".into())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(inbox.fetch(&"s1".into()).await.unwrap().len(), 1);
    assert!(retain
        .lookup_topic("t1", "devices/a/state")
        .await
        .unwrap()
        .is_some());

    let _ = std::fs::remove_dir_all(data_dir);
}

struct RedisTestServer {
    _data_dir: PathBuf,
    child: Child,
    url: String,
}

impl RedisTestServer {
    fn start() -> Self {
        let data_dir = temp_data_dir("redis");
        std::fs::create_dir_all(&data_dir).unwrap();
        let port = reserve_addr().port();
        let child = Command::new(redis_server_binary())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--port")
            .arg(port.to_string())
            .arg("--dir")
            .arg(&data_dir)
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--protected-mode")
            .arg("no")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            _data_dir: data_dir,
            child,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        wait_for_port(
            format!("127.0.0.1:{port}").parse().unwrap(),
            Duration::from_secs(10),
        );
        server
    }
}

impl Drop for RedisTestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn redis_server_binary() -> String {
    std::env::var("GREENMQTT_REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".to_string())
}

fn reserve_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn wait_for_port(addr: SocketAddr, timeout: Duration) {
    let started_at = Instant::now();
    while started_at.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("redis test port {addr} did not open in time");
}

async fn redis_test_broker() -> (
    Arc<BrokerRuntime<ConfiguredAuth, ConfiguredAcl, ConfiguredEventHook>>,
    RedisTestServer,
) {
    let redis = RedisTestServer::start();
    let mut last_error = None;
    let recovered_after_retry;
    let (sessiondict, dist, inbox, retain) = {
        let mut services = None;
        for _ in 0..20 {
            match redis_services(&redis.url).await {
                Ok(found) => {
                    services = Some(found);
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        recovered_after_retry = last_error.is_some();
        match services {
            Some(found) => found,
            None => panic!(
                "redis test broker could not initialize services: {:#}",
                last_error.expect("missing redis initialization error")
            ),
        }
    };
    let broker = Arc::new(BrokerRuntime::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        ConfiguredAuth::default(),
        ConfiguredAcl::default(),
        ConfiguredEventHook::default(),
        sessiondict,
        dist,
        inbox,
        retain,
    ));
    if recovered_after_retry {
        eprintln!("greenmqtt redis test broker recovered after retry");
    }
    (broker, redis)
}

#[tokio::test]
async fn bench_report_matches_expected_counts() {
    let report = run_bench(
        test_broker(),
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 3,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.total_publishes, 3);
    assert_eq!(report.expected_deliveries, 6);
    assert_eq!(report.actual_deliveries, 6);
    assert_eq!(report.matched_routes, 6);
    assert_eq!(report.online_deliveries, 6);
    assert_eq!(report.pending_before_drain, 6);
    assert_eq!(report.global_session_records, 3);
    assert_eq!(report.route_records, 2);
    assert_eq!(report.subscription_records, 2);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert_eq!(report.inflight_messages_before_drain, 0);
    assert_eq!(report.retained_messages, 0);
    assert!(report.rss_bytes_after > 0);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_offline_replay_matches_expected_counts() {
    let report = run_bench(
        test_broker(),
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 3,
            qos: 1,
            scenario: BenchScenario::OfflineReplay,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::OfflineReplay);
    assert_eq!(report.total_publishes, 3);
    assert_eq!(report.expected_deliveries, 6);
    assert_eq!(report.actual_deliveries, 6);
    assert_eq!(report.matched_routes, 6);
    assert_eq!(report.online_deliveries, 0);
    assert!(report.offline_messages_before_drain >= 6);
    assert_eq!(report.inflight_messages_before_drain, 0);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_runs_with_sled_durable_backend() {
    let (broker, data_dir) = durable_test_broker("sled").await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        "sled",
    )
    .await
    .unwrap();

    assert_eq!(report.storage_backend, "sled");
    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert!(report.rss_bytes_after > 0);
    assert_bench_phase_timings(&report);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn bench_report_runs_with_rocksdb_durable_backend() {
    let (broker, data_dir) = durable_test_broker("rocksdb").await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 2,
            scenario: BenchScenario::Live,
        },
        "rocksdb",
    )
    .await
    .unwrap();

    assert_eq!(report.storage_backend, "rocksdb");
    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert!(report.rss_bytes_after > 0);
    assert_bench_phase_timings(&report);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn bench_report_runs_with_redis_durable_backend() {
    let (broker, _redis) = redis_test_broker().await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        "redis",
    )
    .await
    .unwrap();

    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert!(report.rss_bytes_after > 0);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_offline_replay_runs_with_redis_durable_backend() {
    let (broker, _redis) = redis_test_broker().await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::OfflineReplay,
        },
        "sled",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::OfflineReplay);
    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert!(report.offline_messages_before_drain >= 4);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_offline_replay_runs_with_sled_durable_backend() {
    let (broker, data_dir) = durable_test_broker("sled").await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::OfflineReplay,
        },
        "redis",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::OfflineReplay);
    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert!(report.offline_messages_before_drain >= 4);
    assert_bench_phase_timings(&report);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn bench_report_retained_replay_matches_expected_counts() {
    let report = run_bench(
        test_broker(),
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 3,
            qos: 1,
            scenario: BenchScenario::RetainedReplay,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::RetainedReplay);
    assert_eq!(report.total_publishes, 3);
    assert_eq!(report.expected_deliveries, 6);
    assert_eq!(report.actual_deliveries, 6);
    assert_eq!(report.matched_routes, 0);
    assert_eq!(report.online_deliveries, 0);
    assert_eq!(report.pending_before_drain, 0);
    assert_eq!(report.retained_messages, 3);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_scales_large_wildcard_retain_fanout_workload() {
    let report = run_bench(
        test_broker(),
        1,
        BenchConfig {
            subscribers: 24,
            publishers: 6,
            messages_per_publisher: 8,
            qos: 1,
            scenario: BenchScenario::RetainedReplay,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::RetainedReplay);
    assert_eq!(report.total_publishes, 48);
    assert_eq!(report.expected_deliveries, 48 * 24);
    assert_eq!(report.actual_deliveries, report.expected_deliveries);
    assert_eq!(report.retained_messages, report.total_publishes);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_retained_replay_runs_with_sled_durable_backend() {
    let (broker, data_dir) = durable_test_broker("sled").await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::RetainedReplay,
        },
        "sled",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::RetainedReplay);
    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert_eq!(report.retained_messages, 2);
    assert_bench_phase_timings(&report);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn bench_report_retained_replay_runs_with_redis_durable_backend() {
    let (broker, _redis) = redis_test_broker().await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::RetainedReplay,
        },
        "redis",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::RetainedReplay);
    assert_eq!(report.total_publishes, 2);
    assert_eq!(report.actual_deliveries, 4);
    assert_eq!(report.retained_messages, 2);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_shared_live_matches_expected_counts() {
    let report = run_bench(
        test_broker(),
        1,
        BenchConfig {
            subscribers: 3,
            publishers: 1,
            messages_per_publisher: 4,
            qos: 1,
            scenario: BenchScenario::SharedLive,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::SharedLive);
    assert_eq!(report.total_publishes, 4);
    assert_eq!(report.expected_deliveries, 4);
    assert_eq!(report.actual_deliveries, 4);
    assert_eq!(report.matched_routes, 4);
    assert_eq!(report.online_deliveries, 4);
    assert_eq!(report.pending_before_drain, 4);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn bench_report_shared_live_runs_with_sled_durable_backend() {
    let (broker, data_dir) = durable_test_broker("sled").await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 3,
            publishers: 1,
            messages_per_publisher: 3,
            qos: 1,
            scenario: BenchScenario::SharedLive,
        },
        "sled",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::SharedLive);
    assert_eq!(report.total_publishes, 3);
    assert_eq!(report.actual_deliveries, 3);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert_bench_phase_timings(&report);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn bench_report_shared_live_runs_with_redis_durable_backend() {
    let (broker, _redis) = redis_test_broker().await;
    let report = run_bench(
        broker,
        1,
        BenchConfig {
            subscribers: 3,
            publishers: 1,
            messages_per_publisher: 3,
            qos: 1,
            scenario: BenchScenario::SharedLive,
        },
        "redis",
    )
    .await
    .unwrap();

    assert_eq!(report.scenario, BenchScenario::SharedLive);
    assert_eq!(report.total_publishes, 3);
    assert_eq!(report.actual_deliveries, 3);
    assert_eq!(report.offline_messages_before_drain, 0);
    assert_bench_phase_timings(&report);
}

#[tokio::test]
async fn compare_bench_report_runs_with_memory_and_sled_backends() {
    let report = run_compare_bench(
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        &["memory".to_string(), "sled".to_string()],
        "redis://127.0.0.1:6379/",
    )
    .await
    .unwrap();

    assert_eq!(report.reports.len(), 2);
    assert_eq!(report.reports[0].storage_backend, "memory");
    assert_eq!(report.reports[1].storage_backend, "sled");
    assert_eq!(report.reports[0].actual_deliveries, 4);
    assert_eq!(report.reports[1].actual_deliveries, 4);
    assert_bench_phase_timings(&report.reports[0]);
    assert_bench_phase_timings(&report.reports[1]);
}

#[tokio::test]
async fn compare_soak_report_runs_with_memory_and_sled_backends() {
    let report = run_compare_soak(
        1,
        SoakConfig {
            iterations: 2,
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
        },
        &["memory".to_string(), "sled".to_string()],
        "redis://127.0.0.1:6379/",
    )
    .await
    .unwrap();

    assert_eq!(report.reports.len(), 2);
    assert_eq!(report.reports[0].storage_backend, "memory");
    assert_eq!(report.reports[1].storage_backend, "sled");
    assert_eq!(report.reports[0].final_global_session_records, 0);
    assert_eq!(report.reports[1].final_global_session_records, 0);
    assert_eq!(report.reports[0].final_offline_messages, 0);
    assert_eq!(report.reports[1].final_offline_messages, 0);
}

#[tokio::test]
async fn compare_bench_report_runs_with_memory_and_redis_backends() {
    let redis = RedisTestServer::start();
    let report = run_compare_bench(
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        &["memory".to_string(), "redis".to_string()],
        &redis.url,
    )
    .await
    .unwrap();

    assert_eq!(report.reports.len(), 2);
    assert_eq!(report.reports[0].storage_backend, "memory");
    assert_eq!(report.reports[1].storage_backend, "redis");
    assert_eq!(report.reports[0].actual_deliveries, 4);
    assert_eq!(report.reports[1].actual_deliveries, 4);
    assert_bench_phase_timings(&report.reports[0]);
    assert_bench_phase_timings(&report.reports[1]);
}

#[tokio::test]
async fn profile_bench_report_runs_multiple_scenarios_for_memory_backend() {
    let report = run_profile_bench(
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        &[
            BenchScenario::Live,
            BenchScenario::OfflineReplay,
            BenchScenario::RetainedReplay,
        ],
        "memory",
        "redis://127.0.0.1:6379/",
    )
    .await
    .unwrap();

    assert_eq!(report.reports.len(), 3);
    assert_eq!(report.reports[0].scenario, BenchScenario::Live);
    assert_eq!(report.reports[1].scenario, BenchScenario::OfflineReplay);
    assert_eq!(report.reports[2].scenario, BenchScenario::RetainedReplay);
    for item in &report.reports {
        assert_eq!(item.storage_backend, "memory");
        assert_bench_phase_timings(item);
    }
}

#[tokio::test]
async fn profile_bench_report_runs_with_redis_backend() {
    let redis = RedisTestServer::start();
    let report = run_profile_bench(
        1,
        BenchConfig {
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
            scenario: BenchScenario::Live,
        },
        &[BenchScenario::Live, BenchScenario::SharedLive],
        "redis",
        &redis.url,
    )
    .await
    .unwrap();

    assert_eq!(report.reports.len(), 2);
    assert_eq!(report.reports[0].storage_backend, "redis");
    assert_eq!(report.reports[1].storage_backend, "redis");
    assert_eq!(report.reports[0].scenario, BenchScenario::Live);
    assert_eq!(report.reports[1].scenario, BenchScenario::SharedLive);
    for item in &report.reports {
        assert_bench_phase_timings(item);
    }
}

#[tokio::test]
async fn compare_soak_report_runs_with_memory_and_redis_backends() {
    let redis = RedisTestServer::start();
    let report = run_compare_soak(
        1,
        SoakConfig {
            iterations: 2,
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
        },
        &["memory".to_string(), "redis".to_string()],
        &redis.url,
    )
    .await
    .unwrap();

    assert_eq!(report.reports.len(), 2);
    assert_eq!(report.reports[0].storage_backend, "memory");
    assert_eq!(report.reports[1].storage_backend, "redis");
    assert_eq!(report.reports[0].final_global_session_records, 0);
    assert_eq!(report.reports[1].final_global_session_records, 0);
    assert_eq!(report.reports[0].final_offline_messages, 0);
    assert_eq!(report.reports[1].final_offline_messages, 0);
}

#[tokio::test]
async fn soak_report_ends_without_state_leak() {
    let report = run_soak(
        test_broker(),
        1,
        SoakConfig {
            iterations: 5,
            subscribers: 3,
            publishers: 2,
            messages_per_publisher: 4,
            qos: 1,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.storage_backend, "memory");
    assert_eq!(report.total_publishes, 40);
    assert_eq!(report.total_deliveries, 120);
    assert_eq!(report.final_global_session_records, 0);
    assert_eq!(report.final_route_records, 0);
    assert_eq!(report.final_subscription_records, 0);
    assert_eq!(report.final_offline_messages, 0);
    assert_eq!(report.final_inflight_messages, 0);
    assert_eq!(report.final_retained_messages, 0);
}

#[tokio::test]
async fn soak_report_handles_large_sessiondict_tenant_workload() {
    let report = run_soak(
        test_broker(),
        1,
        SoakConfig {
            iterations: 3,
            subscribers: 48,
            publishers: 8,
            messages_per_publisher: 3,
            qos: 1,
        },
        "memory",
    )
    .await
    .unwrap();

    assert_eq!(report.storage_backend, "memory");
    assert!(report.max_global_session_records >= 48);
    assert_eq!(report.final_global_session_records, 0);
    assert_eq!(report.final_route_records, 0);
    assert_eq!(report.final_subscription_records, 0);
}

#[tokio::test]
async fn soak_report_runs_with_sled_durable_backend() {
    let (broker, data_dir) = durable_test_broker("sled").await;
    let report = run_soak(
        broker,
        1,
        SoakConfig {
            iterations: 3,
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
        },
        "sled",
    )
    .await
    .unwrap();

    assert_eq!(report.storage_backend, "sled");
    assert_eq!(report.total_publishes, 6);
    assert_eq!(report.total_deliveries, 12);
    assert_eq!(report.final_global_session_records, 0);
    assert_eq!(report.final_route_records, 0);
    assert_eq!(report.final_subscription_records, 0);
    assert_eq!(report.final_offline_messages, 0);
    assert_eq!(report.final_inflight_messages, 0);
    assert_eq!(report.final_retained_messages, 0);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn soak_report_runs_with_rocksdb_durable_backend() {
    let (broker, data_dir) = durable_test_broker("rocksdb").await;
    let report = run_soak(
        broker,
        1,
        SoakConfig {
            iterations: 3,
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 2,
        },
        "rocksdb",
    )
    .await
    .unwrap();

    assert_eq!(report.total_publishes, 6);
    assert_eq!(report.total_deliveries, 12);
    assert_eq!(report.final_global_session_records, 0);
    assert_eq!(report.final_route_records, 0);
    assert_eq!(report.final_subscription_records, 0);
    assert_eq!(report.final_offline_messages, 0);
    assert_eq!(report.final_inflight_messages, 0);
    assert_eq!(report.final_retained_messages, 0);
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn soak_report_runs_with_redis_durable_backend() {
    let (broker, _redis) = redis_test_broker().await;
    let report = run_soak(
        broker,
        1,
        SoakConfig {
            iterations: 3,
            subscribers: 2,
            publishers: 1,
            messages_per_publisher: 2,
            qos: 1,
        },
        "redis",
    )
    .await
    .unwrap();

    assert_eq!(report.total_publishes, 6);
    assert_eq!(report.total_deliveries, 12);
    assert_eq!(report.final_global_session_records, 0);
    assert_eq!(report.final_route_records, 0);
    assert_eq!(report.final_subscription_records, 0);
    assert_eq!(report.final_offline_messages, 0);
    assert_eq!(report.final_inflight_messages, 0);
    assert_eq!(report.final_retained_messages, 0);
}

#[test]
fn bench_threshold_validation_passes_and_fails_as_expected() {
    let report = BenchReport {
        storage_backend: "memory".into(),
        scenario: BenchScenario::Live,
        subscribers: 1,
        publishers: 1,
        messages_per_publisher: 1,
        total_publishes: 1,
        expected_deliveries: 1,
        actual_deliveries: 1,
        matched_routes: 1,
        online_deliveries: 1,
        pending_before_drain: 2,
        global_session_records: 2,
        route_records: 1,
        subscription_records: 1,
        offline_messages_before_drain: 0,
        inflight_messages_before_drain: 0,
        retained_messages: 0,
        rss_bytes_after: 1024,
        setup_ms: 1,
        publish_ms: 2,
        drain_ms: 3,
        elapsed_ms: 1,
        publishes_per_sec: 100.0,
        deliveries_per_sec: 200.0,
    };
    assert!(validate_bench_report(
        &report,
        BenchThresholds {
            min_publishes_per_sec: Some(50.0),
            min_deliveries_per_sec: Some(150.0),
            max_pending_before_drain: Some(5),
            max_rss_bytes_after: Some(2048),
        }
    )
    .is_ok());
    assert!(validate_bench_report(
        &report,
        BenchThresholds {
            min_publishes_per_sec: Some(150.0),
            ..BenchThresholds::default()
        }
    )
    .is_err());
    assert!(validate_bench_report(
        &report,
        BenchThresholds {
            max_rss_bytes_after: Some(512),
            ..BenchThresholds::default()
        }
    )
    .is_err());
}

#[tokio::test]
#[ignore = "manual performance gate for metrics overhead"]
async fn metrics_collection_overhead_stays_below_one_percent() {
    let config = BenchConfig {
        subscribers: 8,
        publishers: 2,
        messages_per_publisher: 100,
        qos: 1,
        scenario: BenchScenario::Live,
    };
    let baseline = run_bench(test_broker(), 1, config, "memory-baseline")
        .await
        .unwrap();

    install_metrics_recorder_once();
    let instrumented = run_bench(test_broker(), 1, config, "memory-metrics")
        .await
        .unwrap();

    assert!(
        throughput_drop_within_one_percent(
            baseline.deliveries_per_sec,
            instrumented.deliveries_per_sec,
        ),
        "metrics overhead too high: baseline {:.2} vs instrumented {:.2}",
        baseline.deliveries_per_sec,
        instrumented.deliveries_per_sec,
    );
}

#[tokio::test]
#[ignore = "manual performance gate for backpressure overhead"]
async fn backpressure_overhead_stays_below_one_percent_under_normal_load() {
    let config = BenchConfig {
        subscribers: 8,
        publishers: 2,
        messages_per_publisher: 100,
        qos: 1,
        scenario: BenchScenario::Live,
    };
    let baseline = run_bench(test_broker(), 1, config, "memory-baseline")
        .await
        .unwrap();
    let instrumented = run_bench(backpressure_test_broker(), 1, config, "memory-backpressure")
        .await
        .unwrap();

    assert!(
        throughput_drop_within_one_percent(
            baseline.deliveries_per_sec,
            instrumented.deliveries_per_sec,
        ),
        "backpressure overhead too high: baseline {:.2} vs instrumented {:.2}",
        baseline.deliveries_per_sec,
        instrumented.deliveries_per_sec,
    );
}

#[test]
fn parse_bench_scenario_supports_live_and_offline_replay() {
    assert_eq!(parse_bench_scenario("live").unwrap(), BenchScenario::Live);
    assert_eq!(
        parse_bench_scenario("offline_replay").unwrap(),
        BenchScenario::OfflineReplay
    );
    assert_eq!(
        parse_bench_scenario("retained_replay").unwrap(),
        BenchScenario::RetainedReplay
    );
    assert_eq!(
        parse_bench_scenario("shared_live").unwrap(),
        BenchScenario::SharedLive
    );
    assert!(parse_bench_scenario("unknown").is_err());
}

#[test]
fn parse_bench_scenarios_supports_multiple_values() {
    assert_eq!(
        parse_bench_scenarios("live, offline_replay,retained_replay").unwrap(),
        vec![
            BenchScenario::Live,
            BenchScenario::OfflineReplay,
            BenchScenario::RetainedReplay,
        ]
    );
    assert!(parse_bench_scenarios("").is_err());
    assert!(parse_bench_scenarios("live,unknown").is_err());
}

#[test]
fn parse_compare_backends_supports_known_backends() {
    assert_eq!(
        parse_compare_backends("memory,sled,rocksdb,redis").unwrap(),
        vec![
            "memory".to_string(),
            "sled".to_string(),
            "rocksdb".to_string(),
            "redis".to_string()
        ]
    );
    assert!(parse_compare_backends("").is_err());
    assert!(parse_compare_backends("memory,unknown").is_err());
}

#[test]
fn soak_threshold_validation_passes_and_fails_as_expected() {
    let report = SoakReport {
        storage_backend: "memory".into(),
        iterations: 1,
        subscribers: 1,
        publishers: 1,
        messages_per_publisher: 1,
        total_publishes: 1,
        total_deliveries: 1,
        max_local_online_sessions: 2,
        max_local_pending_deliveries: 1,
        max_global_session_records: 2,
        max_route_records: 1,
        max_subscription_records: 1,
        final_global_session_records: 0,
        final_route_records: 0,
        final_subscription_records: 0,
        final_offline_messages: 0,
        final_inflight_messages: 0,
        final_retained_messages: 0,
        max_rss_bytes: 4096,
        final_rss_bytes: 2048,
        elapsed_ms: 1,
    };
    assert!(validate_soak_report(
        &report,
        SoakThresholds {
            max_final_global_sessions: Some(0),
            max_final_routes: Some(0),
            max_final_subscriptions: Some(0),
            max_final_offline_messages: Some(0),
            max_final_inflight_messages: Some(0),
            max_final_retained_messages: Some(0),
            max_peak_rss_bytes: Some(8192),
            max_final_rss_bytes: Some(4096),
        }
    )
    .is_ok());
    assert!(validate_soak_report(
        &SoakReport {
            storage_backend: "memory".into(),
            final_route_records: 1,
            ..report
        },
        SoakThresholds {
            max_final_routes: Some(0),
            ..SoakThresholds::default()
        }
    )
    .is_err());
    assert!(validate_soak_report(
        &report,
        SoakThresholds {
            max_peak_rss_bytes: Some(1024),
            ..SoakThresholds::default()
        }
    )
    .is_err());
    assert!(validate_soak_report(
        &report,
        SoakThresholds {
            max_final_rss_bytes: Some(1024),
            ..SoakThresholds::default()
        }
    )
    .is_err());
}

#[test]
fn parse_identity_matchers_supports_wildcards() {
    let matchers = parse_identity_matchers("tenant-a:alice:*,*:ops:cli").unwrap();
    assert_eq!(matchers.len(), 2);
    assert_eq!(matchers[0].tenant_id, "tenant-a");
    assert_eq!(matchers[0].client_id, "*");
    assert_eq!(matchers[1].tenant_id, "*");
    assert_eq!(matchers[1].user_id, "ops");
}

#[test]
fn parse_acl_rules_parses_ordered_decisions() {
    let rules = parse_acl_rules(
        "deny-pub@tenant-a:alice:*=devices/private/#,allow-pub@tenant-a:alice:*=devices/#",
    )
    .unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].decision, AclDecision::Deny);
    assert_eq!(rules[0].action, AclAction::Publish);
    assert_eq!(rules[0].topic_filter, "devices/private/#");
    assert_eq!(rules[1].decision, AclDecision::Allow);
    assert_eq!(rules[1].action, AclAction::Publish);
}

#[test]
fn parse_topic_rewrite_rules_supports_tenant_scoped_rules() {
    let rules = parse_topic_rewrite_rules(
        "tenant-a@devices/+/raw=devices/{1}/normalized,alerts/#=rewritten/{1}",
    )
    .unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].tenant_id.as_deref(), Some("tenant-a"));
    assert_eq!(rules[0].topic_filter, "devices/+/raw");
    assert_eq!(rules[0].rewrite_to, "devices/{1}/normalized");
    assert_eq!(rules[1].tenant_id, None);
    assert_eq!(rules[1].topic_filter, "alerts/#");
    assert_eq!(rules[1].rewrite_to, "rewritten/{1}");
}

#[test]
fn parse_bridge_rules_supports_topic_rewrite_targets() {
    let rules =
        parse_bridge_rules("tenant-a@devices/+/raw->bridge/devices/{1}=127.0.0.1:1883").unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].tenant_id.as_deref(), Some("tenant-a"));
    assert_eq!(rules[0].topic_filter, "devices/+/raw");
    assert_eq!(rules[0].rewrite_to.as_deref(), Some("bridge/devices/{1}"));
    assert_eq!(rules[0].remote_addr, "127.0.0.1:1883");
}

#[test]
fn configured_webhook_parses_selected_events() {
    std::env::set_var("GREENMQTT_WEBHOOK_URL", "http://127.0.0.1:18080/hook");
    std::env::set_var(
        "GREENMQTT_WEBHOOK_EVENTS",
        "connect,subscribe,publish,retain",
    );
    let hook = configured_webhook("default")
        .unwrap()
        .expect("webhook should exist");
    std::env::remove_var("GREENMQTT_WEBHOOK_URL");
    std::env::remove_var("GREENMQTT_WEBHOOK_EVENTS");
    assert!(hook.config().connect);
    assert!(!hook.config().disconnect);
    assert!(hook.config().subscribe);
    assert!(!hook.config().unsubscribe);
    assert!(hook.config().publish);
    assert!(!hook.config().offline);
    assert!(hook.config().retain);
}

#[test]
fn configured_auth_prefers_http_provider_when_no_static_allowlist_exists() {
    std::env::remove_var("GREENMQTT_ENHANCED_AUTH_METHOD");
    std::env::remove_var("GREENMQTT_ENHANCED_AUTH_CHALLENGE");
    std::env::remove_var("GREENMQTT_ENHANCED_AUTH_RESPONSE");
    std::env::remove_var("GREENMQTT_AUTH_IDENTITIES");
    std::env::remove_var("GREENMQTT_AUTH_DENY_IDENTITIES");
    std::env::set_var("GREENMQTT_HTTP_AUTH_URL", "http://127.0.0.1:18080/auth");
    let auth = configured_auth().unwrap();
    std::env::remove_var("GREENMQTT_HTTP_AUTH_URL");
    match auth {
        ConfiguredAuth::Http(provider) => {
            assert_eq!(provider.config().url, "http://127.0.0.1:18080/auth");
        }
        _ => panic!("expected http auth provider"),
    }
}

#[test]
fn configured_auth_prefers_enhanced_static_provider_when_enabled() {
    std::env::set_var("GREENMQTT_ENHANCED_AUTH_METHOD", "custom");
    std::env::set_var("GREENMQTT_ENHANCED_AUTH_CHALLENGE", "server-challenge");
    std::env::set_var("GREENMQTT_ENHANCED_AUTH_RESPONSE", "client-response");
    std::env::remove_var("GREENMQTT_AUTH_IDENTITIES");
    std::env::remove_var("GREENMQTT_AUTH_DENY_IDENTITIES");
    std::env::remove_var("GREENMQTT_HTTP_AUTH_URL");
    let auth = configured_auth().unwrap();
    std::env::remove_var("GREENMQTT_ENHANCED_AUTH_METHOD");
    std::env::remove_var("GREENMQTT_ENHANCED_AUTH_CHALLENGE");
    std::env::remove_var("GREENMQTT_ENHANCED_AUTH_RESPONSE");
    match auth {
        ConfiguredAuth::EnhancedStatic(provider) => {
            assert_eq!(provider.method(), "custom");
            assert_eq!(provider.challenge_data(), b"server-challenge");
            assert_eq!(provider.response_data(), b"client-response");
        }
        _ => panic!("expected enhanced static auth provider"),
    }
}

#[test]
fn configured_auth_supports_static_deny_identities() {
    std::env::set_var("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*");
    std::env::set_var("GREENMQTT_AUTH_DENY_IDENTITIES", "tenant-a:blocked:*");
    let auth = configured_auth().unwrap();
    std::env::remove_var("GREENMQTT_AUTH_IDENTITIES");
    std::env::remove_var("GREENMQTT_AUTH_DENY_IDENTITIES");
    match auth {
        ConfiguredAuth::Static(provider) => {
            let allowed = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async {
                    provider
                        .authenticate(&ClientIdentity {
                            tenant_id: "tenant-a".into(),
                            user_id: "alice".into(),
                            client_id: "c1".into(),
                        })
                        .await
                })
                .unwrap();
            let denied = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async {
                    provider
                        .authenticate(&ClientIdentity {
                            tenant_id: "tenant-a".into(),
                            user_id: "blocked".into(),
                            client_id: "c1".into(),
                        })
                        .await
                })
                .unwrap();
            assert!(allowed);
            assert!(!denied);
        }
        _ => panic!("expected static auth provider"),
    }
}

#[test]
fn parse_listener_specs_supports_profile_suffix() {
    let specs = parse_listener_specs("127.0.0.1:1883,127.0.0.1:1884@guest").unwrap();
    assert_eq!(
        specs,
        vec![
            ListenerSpec {
                bind: "127.0.0.1:1883".parse().unwrap(),
                profile: "default".into(),
            },
            ListenerSpec {
                bind: "127.0.0.1:1884".parse().unwrap(),
                profile: "guest".into(),
            },
        ]
    );
}

#[test]
fn listener_specs_from_env_prefers_plural_env() {
    with_env_var("GREENMQTT_MQTT_BIND", Some("127.0.0.1:1883"), || {
        with_env_var(
            "GREENMQTT_MQTT_BINDS",
            Some("127.0.0.1:1884@guest,127.0.0.1:1885@admin"),
            || {
                let specs = listener_specs_from_env(
                    "GREENMQTT_MQTT_BIND",
                    "GREENMQTT_MQTT_BINDS",
                    "127.0.0.1:1883",
                )
                .unwrap();
                assert_eq!(specs.len(), 2);
                assert_eq!(specs[0].profile, "guest");
                assert_eq!(specs[1].profile, "admin");
            },
        )
    });
}

#[test]
fn configured_auth_for_profile_falls_back_to_default_env() {
    with_env_var("GREENMQTT_AUTH_IDENTITIES", Some("tenant-a:*:*"), || {
        let auth = configured_auth_for_profile("guest").unwrap();
        let allowed = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                auth.authenticate(&ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "c1".into(),
                })
                .await
            })
            .unwrap();
        assert!(allowed);
    });
}

#[test]
fn configured_acl_prefers_http_provider_when_no_static_rules_exist() {
    std::env::remove_var("GREENMQTT_ACL_RULES");
    std::env::remove_var("GREENMQTT_ACL_DEFAULT_ALLOW");
    std::env::set_var("GREENMQTT_HTTP_ACL_URL", "http://127.0.0.1:18080/acl");
    let acl = configured_acl().unwrap();
    std::env::remove_var("GREENMQTT_HTTP_ACL_URL");
    match acl {
        ConfiguredAcl::Http(provider) => {
            assert_eq!(provider.config().url, "http://127.0.0.1:18080/acl");
        }
        _ => panic!("expected http acl provider"),
    }
}

#[test]
fn configured_acl_supports_default_allow_for_static_rules() {
    std::env::set_var(
        "GREENMQTT_ACL_RULES",
        "deny-pub@tenant-a:alice:*=devices/private/#",
    );
    std::env::set_var("GREENMQTT_ACL_DEFAULT_ALLOW", "true");
    let acl = configured_acl().unwrap();
    std::env::remove_var("GREENMQTT_ACL_RULES");
    std::env::remove_var("GREENMQTT_ACL_DEFAULT_ALLOW");
    match acl {
        ConfiguredAcl::Static(provider) => {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let allowed = runtime
                .block_on(async {
                    provider
                        .can_publish(
                            &ClientIdentity {
                                tenant_id: "tenant-a".into(),
                                user_id: "alice".into(),
                                client_id: "c1".into(),
                            },
                            "devices/public/state",
                        )
                        .await
                })
                .unwrap();
            let denied = runtime
                .block_on(async {
                    provider
                        .can_publish(
                            &ClientIdentity {
                                tenant_id: "tenant-a".into(),
                                user_id: "alice".into(),
                                client_id: "c1".into(),
                            },
                            "devices/private/secret",
                        )
                        .await
                })
                .unwrap();
            assert!(allowed);
            assert!(!denied);
        }
        _ => panic!("expected static acl provider"),
    }
}

#[test]
fn configured_acl_for_profile_uses_profile_specific_env() {
    with_env_var(
        "GREENMQTT_ACL_RULES__GUEST",
        Some("allow-pub@tenant-a:alice:*=devices/+/state"),
        || {
            let acl = configured_acl_for_profile("guest").unwrap();
            let allowed = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async {
                    acl.can_publish(
                        &ClientIdentity {
                            tenant_id: "tenant-a".into(),
                            user_id: "alice".into(),
                            client_id: "c1".into(),
                        },
                        "devices/d1/state",
                    )
                    .await
                })
                .unwrap();
            assert!(allowed);
        },
    );
}

#[test]
fn configured_hooks_accepts_bridge_timeout_and_fail_open_env() {
    std::env::set_var(
        "GREENMQTT_TOPIC_REWRITE_RULES",
        "tenant-a@devices/+/raw=devices/{1}/normalized",
    );
    std::env::set_var("GREENMQTT_BRIDGE_RULES", "devices/#=127.0.0.1:1883");
    std::env::set_var("GREENMQTT_BRIDGE_TIMEOUT_MS", "250");
    std::env::set_var("GREENMQTT_BRIDGE_FAIL_OPEN", "false");
    std::env::set_var("GREENMQTT_BRIDGE_RETRIES", "2");
    std::env::set_var("GREENMQTT_BRIDGE_RETRY_DELAY_MS", "25");
    std::env::set_var("GREENMQTT_BRIDGE_MAX_INFLIGHT", "4");
    let hooks = configured_hooks(7);
    std::env::remove_var("GREENMQTT_TOPIC_REWRITE_RULES");
    std::env::remove_var("GREENMQTT_BRIDGE_RULES");
    std::env::remove_var("GREENMQTT_BRIDGE_TIMEOUT_MS");
    std::env::remove_var("GREENMQTT_BRIDGE_FAIL_OPEN");
    std::env::remove_var("GREENMQTT_BRIDGE_RETRIES");
    std::env::remove_var("GREENMQTT_BRIDGE_RETRY_DELAY_MS");
    std::env::remove_var("GREENMQTT_BRIDGE_MAX_INFLIGHT");
    assert!(hooks.is_ok());
}

#[test]
fn configured_hooks_for_profile_accepts_profile_specific_bridge_settings() {
    with_env_var(
        "GREENMQTT_BRIDGE_RULES__GUEST",
        Some("devices/#=127.0.0.1:1883"),
        || {
            with_env_var("GREENMQTT_BRIDGE_TIMEOUT_MS__GUEST", Some("250"), || {
                let hooks = configured_hooks_for_profile(7, "guest");
                assert!(hooks.is_ok());
            })
        },
    );
}

#[test]
fn collect_listener_profiles_merges_unique_profile_names() {
    let tcp = vec![
        ListenerSpec {
            bind: "127.0.0.1:1883".parse().unwrap(),
            profile: "default".into(),
        },
        ListenerSpec {
            bind: "127.0.0.1:1884".parse().unwrap(),
            profile: "guest".into(),
        },
    ];
    let ws = vec![ListenerSpec {
        bind: "127.0.0.1:8083".parse().unwrap(),
        profile: "admin".into(),
    }];

    let profiles = collect_listener_profiles(&[&tcp, &ws]);
    assert_eq!(profiles, vec!["admin", "default", "guest"]);
}

#[test]
fn configured_listener_profiles_apply_hook_per_listener_profile() {
    with_env_var(
        "GREENMQTT_TOPIC_REWRITE_RULES",
        Some("tenant-a@devices/+/state=default/{1}"),
        || {
            with_env_var(
                "GREENMQTT_TOPIC_REWRITE_RULES__GUEST",
                Some("tenant-a@devices/+/state=guest/{1}"),
                || {
                    let profiles = vec!["default".to_string(), "guest".to_string()];
                    let (_, _, hooks) = configured_listener_profiles(7, &profiles).unwrap();
                    let identity = ClientIdentity {
                        tenant_id: "tenant-a".into(),
                        user_id: "alice".into(),
                        client_id: "c1".into(),
                    };
                    let request = PublishRequest {
                        topic: "devices/d1/state".into(),
                        payload: b"hello".to_vec().into(),
                        qos: 0,
                        retain: false,
                        properties: PublishProperties::default(),
                    };
                    let runtime = tokio::runtime::Runtime::new().unwrap();
                    let default_topic = runtime
                        .block_on(with_listener_profile(
                            "default".to_string(),
                            hooks.rewrite_publish(&identity, &request),
                        ))
                        .unwrap()
                        .topic;
                    let guest_topic = runtime
                        .block_on(with_listener_profile(
                            "guest".to_string(),
                            hooks.rewrite_publish(&identity, &request),
                        ))
                        .unwrap()
                        .topic;
                    assert_eq!(default_topic, "default/d1");
                    assert_eq!(guest_topic, "guest/d1");
                },
            )
        },
    );
}

#[test]
fn configured_listener_profiles_apply_acl_per_listener_profile() {
    let profiles = vec!["default".to_string(), "guest".to_string()];
    let rules_default = "allow-pub@tenant-a:alice:*=devices/#";
    let rules_guest = "allow-pub@tenant-a:alice:*=public/#";
    with_env_var("GREENMQTT_ACL_RULES", Some(rules_default), || {
        with_env_var("GREENMQTT_ACL_RULES__GUEST", Some(rules_guest), || {
            let (_, acl, _) = configured_listener_profiles(7, &profiles).unwrap();
            let identity = ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "c1".into(),
            };
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let default_allowed = runtime
                .block_on(with_listener_profile(
                    "default".to_string(),
                    acl.can_publish(&identity, "devices/d1/state"),
                ))
                .unwrap();
            let guest_allowed = runtime
                .block_on(with_listener_profile(
                    "guest".to_string(),
                    acl.can_publish(&identity, "devices/d1/state"),
                ))
                .unwrap();
            assert!(default_allowed);
            assert!(!guest_allowed);
        })
    });
}

#[test]
fn shard_command_request_builds_ls_query() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "ls".to_string(),
            "--kind".to_string(),
            "dist".to_string(),
            "--tenant-id".to_string(),
            "t1".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "GET");
    assert_eq!(path, "/v1/shards?kind=dist&tenant_id=t1");
    assert!(body.is_none());
    assert_eq!(output, OutputMode::Text);
}

#[test]
fn shard_command_request_builds_audit_query() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "audit".to_string(),
            "--limit".to_string(),
            "5".to_string(),
            "--output".to_string(),
            "json".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "GET");
    assert_eq!(path, "/v1/audit?shard_only=true&limit=5");
    assert!(body.is_none());
    assert_eq!(output, OutputMode::Json);
}

#[test]
fn shard_command_request_builds_move_request() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "move".to_string(),
            "dist".to_string(),
            "t1".to_string(),
            "*".to_string(),
            "9".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "POST");
    assert_eq!(path, "/v1/shards/dist/t1/*/move");
    assert_eq!(
        body.as_deref(),
        Some(r#"{"target_node_id":9,"dry_run":false}"#)
    );
    assert_eq!(output, OutputMode::Text);
}

#[test]
fn shard_command_request_builds_drain_request() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "drain".to_string(),
            "dist".to_string(),
            "t1".to_string(),
            "*".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "POST");
    assert_eq!(path, "/v1/shards/dist/t1/*/drain");
    assert_eq!(body.as_deref(), Some(r#"{"dry_run":false}"#));
    assert_eq!(output, OutputMode::Text);
}

#[test]
fn shard_command_request_builds_catch_up_request() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "catch-up".to_string(),
            "dist".to_string(),
            "t1".to_string(),
            "*".to_string(),
            "9".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "POST");
    assert_eq!(path, "/v1/shards/dist/t1/*/catch-up");
    assert_eq!(
        body.as_deref(),
        Some(r#"{"target_node_id":9,"dry_run":false}"#)
    );
    assert_eq!(output, OutputMode::Text);
}

#[test]
fn shard_command_request_builds_repair_request() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "repair".to_string(),
            "dist".to_string(),
            "t1".to_string(),
            "*".to_string(),
            "9".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "POST");
    assert_eq!(path, "/v1/shards/dist/t1/*/repair");
    assert_eq!(
        body.as_deref(),
        Some(r#"{"target_node_id":9,"dry_run":false}"#)
    );
    assert_eq!(output, OutputMode::Text);
}

#[test]
fn shard_command_request_supports_dry_run_and_output_override() {
    let (method, path, body, output) = shard_command_request(
        vec![
            "move".to_string(),
            "dist".to_string(),
            "t1".to_string(),
            "*".to_string(),
            "9".to_string(),
            "--dry-run".to_string(),
            "--output".to_string(),
            "json".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(method, "POST");
    assert_eq!(path, "/v1/shards/dist/t1/*/move");
    assert_eq!(
        body.as_deref(),
        Some(r#"{"target_node_id":9,"dry_run":true}"#)
    );
    assert_eq!(output, OutputMode::Json);
}

#[test]
fn range_command_request_builds_bootstrap_descriptor() {
    let (command, output) = range_command_request(
        vec![
            "bootstrap".to_string(),
            "--range-id".to_string(),
            "range-a".to_string(),
            "--kind".to_string(),
            "retain".to_string(),
            "--tenant-id".to_string(),
            "t1".to_string(),
            "--scope".to_string(),
            "*".to_string(),
            "--voters".to_string(),
            "1,2".to_string(),
            "--learners".to_string(),
            "3".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    match command {
        super::RangeCliCommand::Bootstrap { descriptor } => {
            assert_eq!(descriptor.id, "range-a");
            assert_eq!(descriptor.shard.kind, ServiceShardKind::Retain);
            assert_eq!(descriptor.shard.tenant_id, "t1");
            assert_eq!(descriptor.shard.scope, "*");
            assert_eq!(descriptor.leader_node_id, Some(1));
            assert_eq!(descriptor.replicas.len(), 3);
        }
        other => panic!("unexpected command: {other:?}"),
    }
    assert_eq!(output, OutputMode::Text);
}

#[test]
fn range_command_request_builds_change_replicas_request() {
    let (command, output) = range_command_request(
        vec![
            "change-replicas".to_string(),
            "range-a".to_string(),
            "--voters".to_string(),
            "1,2,3".to_string(),
            "--learners".to_string(),
            "4".to_string(),
            "--output".to_string(),
            "json".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    match command {
        super::RangeCliCommand::ChangeReplicas {
            target,
            voters,
            learners,
        } => {
            assert_eq!(target, super::RangeCliTarget::RangeId("range-a".into()));
            assert_eq!(voters, vec![1, 2, 3]);
            assert_eq!(learners, vec![4]);
        }
        other => panic!("unexpected command: {other:?}"),
    }
    assert_eq!(output, OutputMode::Json);
}

#[test]
fn range_command_request_builds_transfer_and_split_requests() {
    let (transfer, _) = range_command_request(
        vec![
            "transfer-leadership".to_string(),
            "range-a".to_string(),
            "9".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    match transfer {
        super::RangeCliCommand::TransferLeadership {
            target,
            target_node_id,
        } => {
            assert_eq!(target, super::RangeCliTarget::RangeId("range-a".into()));
            assert_eq!(target_node_id, 9);
        }
        other => panic!("unexpected command: {other:?}"),
    }

    let (split, _) = range_command_request(
        vec![
            "split".to_string(),
            "range-a".to_string(),
            "mid".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    match split {
        super::RangeCliCommand::Split { target, split_key } => {
            assert_eq!(target, super::RangeCliTarget::RangeId("range-a".into()));
            assert_eq!(split_key, b"mid".to_vec());
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn range_command_request_accepts_shard_identity_target() {
    let (command, _) = range_command_request(
        vec![
            "change-replicas".to_string(),
            "--kind".to_string(),
            "retain".to_string(),
            "--tenant-id".to_string(),
            "t1".to_string(),
            "--scope".to_string(),
            "*".to_string(),
            "--voters".to_string(),
            "1,2".to_string(),
        ]
        .into_iter(),
    )
    .unwrap();
    match command {
        super::RangeCliCommand::ChangeReplicas { target, voters, .. } => {
            assert_eq!(
                target,
                super::RangeCliTarget::Shard(ServiceShardKey {
                    kind: ServiceShardKind::Retain,
                    tenant_id: "t1".into(),
                    scope: "*".into(),
                })
            );
            assert_eq!(voters, vec![1, 2]);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn render_shard_response_text_formats_action_preview() {
    let body = r#"{
      "previous":{"shard":{"kind":"Dist","tenant_id":"t1","scope":"*"},"endpoint":{"kind":"Dist","node_id":7,"endpoint":"http://127.0.0.1:50070"},"epoch":1,"fencing_token":10,"lifecycle":"Serving"},
      "current":{"shard":{"kind":"Dist","tenant_id":"t1","scope":"*"},"endpoint":{"kind":"Dist","node_id":9,"endpoint":"http://127.0.0.1:50090"},"epoch":2,"fencing_token":11,"lifecycle":"Draining"}
    }"#;
    let text = render_shard_response_text(body).unwrap();
    assert!(text.contains("previous=t1:* owner=7"));
    assert!(text.contains("current=t1:* owner=9"));
}

#[test]
fn configured_control_plane_runtime_skips_non_controller_node() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    with_env_var("GREENMQTT_CONTROL_PLANE_ENABLED", Some("true"), || {
        with_env_var(
            "GREENMQTT_CONTROL_PLANE_CONTROLLER_NODE_ID",
            Some("9"),
            || {
                let result = runtime.block_on(async {
                    configured_control_plane_runtime(
                        7,
                        registry.clone(),
                        "http://127.0.0.1:50051".to_string(),
                    )
                    .await
                });
                assert!(result.unwrap().is_none());
            },
        );
    });
}

#[test]
fn configured_control_plane_runtime_rejects_invalid_desired_ranges_json() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    with_env_var("GREENMQTT_CONTROL_PLANE_ENABLED", Some("true"), || {
        with_env_var(
            "GREENMQTT_CONTROL_PLANE_CONTROLLER_NODE_ID",
            Some("7"),
            || {
                with_env_var(
                    "GREENMQTT_CONTROL_PLANE_DESIRED_RANGES_JSON",
                    Some("not-json"),
                    || {
                        let result = runtime.block_on(async {
                            configured_control_plane_runtime(
                                7,
                                registry.clone(),
                                "http://127.0.0.1:50051".to_string(),
                            )
                            .await
                        });
                        assert!(result.is_err());
                    },
                );
            },
        );
    });
}
