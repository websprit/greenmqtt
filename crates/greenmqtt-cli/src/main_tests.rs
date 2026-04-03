use super::{
    configured_acl, configured_auth, configured_hooks, configured_webhook, durable_services,
    parse_acl_rules, parse_bench_scenario, parse_bench_scenarios, parse_bridge_rules,
    parse_compare_backends, parse_identity_matchers, parse_topic_rewrite_rules, redis_services,
    run_bench, run_compare_bench, run_compare_soak, run_profile_bench, run_soak,
    validate_bench_report, validate_soak_report, BenchConfig, BenchReport, BenchScenario,
    BenchThresholds, SoakConfig, SoakReport, SoakThresholds,
};
use greenmqtt_broker::{BrokerConfig, BrokerRuntime};
use greenmqtt_core::ClientIdentity;
use greenmqtt_dist::DistHandle;
use greenmqtt_inbox::InboxHandle;
use greenmqtt_plugin_api::{
    AclAction, AclDecision, AclProvider, AuthProvider, ConfiguredAcl, ConfiguredAuth,
    ConfiguredEventHook,
};
use greenmqtt_retain::RetainHandle;
use greenmqtt_sessiondict::SessionDictHandle;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static DURABLE_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    let (sessiondict, dist, inbox, retain) =
        durable_services(data_dir.clone(), backend).await.unwrap();
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
    let hook = configured_webhook().unwrap().expect("webhook should exist");
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
