use greenmqtt_core::{RouteRecord, SessionRecord};
use greenmqtt_http_api::{PurgeReply, SessionDictReassignReply};
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
struct PeerSummary {
    node_id: u64,
    rpc_addr: Option<String>,
    connected: bool,
}

fn cluster_test_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

#[derive(Debug)]
struct ClusterBenchReport {
    scenario: String,
    subscribers: usize,
    messages: usize,
    qos: u8,
    deliveries: usize,
    setup_ms: u128,
    publish_ms: u128,
    drain_ms: u128,
    elapsed_ms: u128,
    deliveries_per_sec: f64,
    node1_rss_bytes: u64,
    node2_rss_bytes: u64,
}

fn assert_cluster_bench_timings(report: &ClusterBenchReport) {
    assert!(report.elapsed_ms >= report.setup_ms + report.publish_ms + report.drain_ms);
}

#[test]
fn cluster_redis_cross_node_online_delivery() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let mut subscriber = mqtt_connect(node1.mqtt_bind, "sub", true);
    write_all(&mut subscriber, &subscribe_packet(1, "cluster/+/state"));
    assert_eq!(read_packet_type(&mut subscriber, "subscriber suback"), 9);
    read_packet_body(&mut subscriber, "subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher = mqtt_connect(node2.mqtt_bind, "pub", true);
    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"online"),
    );

    let online = read_publish_payload(&mut subscriber, "subscriber online publish");
    assert_eq!(online, b"online");

    write_all(&mut subscriber, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_dynamic_peer_upsert_enables_cross_node_delivery() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(2, &redis.url, None, ports[3], ports[4], ports[5]);
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.rpc_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let body = http_request_body(
        node1.http_bind,
        "PUT",
        &format!("/v1/peers/2?rpc_addr=http://{}", node2.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let body = http_request_body(
        node2.http_bind,
        "PUT",
        &format!("/v1/peers/1?rpc_addr=http://{}", node1.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let peers1: Vec<PeerSummary> =
        serde_json::from_str(&http_get_body(node1.http_bind, "/v1/peers")).unwrap();
    let peers2: Vec<PeerSummary> =
        serde_json::from_str(&http_get_body(node2.http_bind, "/v1/peers")).unwrap();
    assert_eq!(
        peers1,
        vec![PeerSummary {
            node_id: 2,
            rpc_addr: Some(format!("http://{}", node2.rpc_bind)),
            connected: true,
        }]
    );
    assert_eq!(
        peers2,
        vec![PeerSummary {
            node_id: 1,
            rpc_addr: Some(format!("http://{}", node1.rpc_bind)),
            connected: true,
        }]
    );
    let stats1 = http_stats(node1.http_bind);
    let stats2 = http_stats(node2.http_bind);
    assert_eq!(stats1.peer_nodes, 1);
    assert_eq!(stats2.peer_nodes, 1);

    let mut subscriber = mqtt_connect(node1.mqtt_bind, "sub", true);
    write_all(&mut subscriber, &subscribe_packet(1, "cluster/+/state"));
    assert_eq!(read_packet_type(&mut subscriber, "subscriber suback"), 9);
    read_packet_body(&mut subscriber, "subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher = mqtt_connect(node2.mqtt_bind, "pub", true);
    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"dynamic-peer"),
    );

    let online = read_publish_payload(&mut subscriber, "subscriber online publish");
    assert_eq!(online, b"dynamic-peer");

    let body = http_request_body(node1.http_bind, "DELETE", "/v1/peers/2");
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);
    let body = http_request_body(node2.http_bind, "DELETE", "/v1/peers/1");
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let peers1: Vec<PeerSummary> =
        serde_json::from_str(&http_get_body(node1.http_bind, "/v1/peers")).unwrap();
    let peers2: Vec<PeerSummary> =
        serde_json::from_str(&http_get_body(node2.http_bind, "/v1/peers")).unwrap();
    assert!(peers1.is_empty());
    assert!(peers2.is_empty());
    let stats1 = http_stats(node1.http_bind);
    let stats2 = http_stats(node2.http_bind);
    assert_eq!(stats1.peer_nodes, 0);
    assert_eq!(stats2.peer_nodes, 0);

    write_all(&mut subscriber, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_dynamic_peer_delete_falls_back_to_offline_replay() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(8);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(2, &redis.url, None, ports[3], ports[4], ports[5]);
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.rpc_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let body = http_request_body(
        node1.http_bind,
        "PUT",
        &format!("/v1/peers/2?rpc_addr=http://{}", node2.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);
    let body = http_request_body(
        node2.http_bind,
        "PUT",
        &format!("/v1/peers/1?rpc_addr=http://{}", node1.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let mut subscriber = mqtt_connect(node1.mqtt_bind, "sub", true);
    write_all(&mut subscriber, &subscribe_packet(1, "cluster/+/state"));
    assert_eq!(read_packet_type(&mut subscriber, "subscriber suback"), 9);
    read_packet_body(&mut subscriber, "subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher = mqtt_connect(node2.mqtt_bind, "pub", true);
    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"before-update"),
    );
    let online = read_publish_payload(&mut subscriber, "subscriber online publish");
    assert_eq!(online, b"before-update");

    let body = http_request_body(node2.http_bind, "DELETE", "/v1/peers/1");
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);
    let peers2: Vec<PeerSummary> =
        serde_json::from_str(&http_get_body(node2.http_bind, "/v1/peers")).unwrap();
    assert!(peers2.is_empty());

    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"after-update"),
    );
    thread::sleep(Duration::from_millis(250));

    assert!(
        try_read_publish(&mut subscriber, Duration::from_millis(100)).is_none(),
        "live forwarding should stop after peer removal"
    );

    let stats = http_stats(node2.http_bind);
    assert_eq!(stats.offline_messages, 1);
    assert_eq!(stats.peer_nodes, 0);

    write_all(&mut subscriber, &disconnect_packet());
    thread::sleep(Duration::from_millis(100));
    let mut resumed = mqtt_connect(node1.mqtt_bind, "sub", false);
    let offline = read_publish_payload(&mut resumed, "subscriber offline replay after peer delete");
    assert_eq!(offline, b"after-update");

    write_all(&mut resumed, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_dynamic_peer_reupsert_restores_live_forwarding() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(8);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(2, &redis.url, None, ports[3], ports[4], ports[5]);
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.rpc_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let body = http_request_body(
        node1.http_bind,
        "PUT",
        &format!("/v1/peers/2?rpc_addr=http://{}", node2.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);
    let body = http_request_body(
        node2.http_bind,
        "PUT",
        &format!("/v1/peers/1?rpc_addr=http://{}", node1.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let mut subscriber = mqtt_connect(node1.mqtt_bind, "sub", true);
    write_all(&mut subscriber, &subscribe_packet(1, "cluster/+/state"));
    assert_eq!(read_packet_type(&mut subscriber, "subscriber suback"), 9);
    read_packet_body(&mut subscriber, "subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher = mqtt_connect(node2.mqtt_bind, "pub", true);
    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"before-delete"),
    );
    let online = read_publish_payload(&mut subscriber, "subscriber live publish before delete");
    assert_eq!(online, b"before-delete");

    let body = http_request_body(node2.http_bind, "DELETE", "/v1/peers/1");
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"after-delete"),
    );
    thread::sleep(Duration::from_millis(250));
    assert!(
        try_read_publish(&mut subscriber, Duration::from_millis(100)).is_none(),
        "live forwarding should stop after peer removal"
    );
    write_all(&mut subscriber, &disconnect_packet());
    let mut resumed = mqtt_connect(node1.mqtt_bind, "sub", false);
    let offline = read_publish_payload(&mut resumed, "subscriber offline replay after delete");
    assert_eq!(offline, b"after-delete");

    let body = http_request_body(
        node2.http_bind,
        "PUT",
        &format!("/v1/peers/1?rpc_addr=http://{}", node1.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);
    thread::sleep(Duration::from_millis(150));

    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"after-reupsert"),
    );
    let restored = read_publish_payload(&mut resumed, "subscriber live publish after reupsert");
    assert_eq!(restored, b"after-reupsert");

    write_all(&mut resumed, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_dynamic_peer_restores_after_restart_from_audit_log() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(8);
    let data_dir = temp_data_dir("cluster-peer-restore");
    let mut node1 = BrokerNode::start_with_data_dir(
        1,
        &redis.url,
        None,
        ports[0],
        ports[1],
        ports[2],
        Some(&data_dir),
    );
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(2, &redis.url, None, ports[3], ports[4], ports[5]);
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.rpc_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let body = http_request_body(
        node1.http_bind,
        "PUT",
        &format!("/v1/peers/2?rpc_addr=http://{}", node2.rpc_bind),
    );
    let reply: PurgeReply = serde_json::from_str(&body).unwrap();
    assert_eq!(reply.removed, 1);

    node1.kill();
    wait_for_port_closed(node1.http_bind, Duration::from_secs(10));
    node1 = BrokerNode::start_with_data_dir(
        1,
        &redis.url,
        None,
        ports[0],
        ports[1],
        ports[2],
        Some(&data_dir),
    );
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let peers1: Vec<PeerSummary> =
        serde_json::from_str(&http_get_body(node1.http_bind, "/v1/peers")).unwrap();
    assert_eq!(
        peers1,
        vec![PeerSummary {
            node_id: 2,
            rpc_addr: Some(format!("http://{}", node2.rpc_bind)),
            connected: true,
        }]
    );

    let mut subscriber =
        mqtt_connect_with_password(node2.mqtt_bind, "restore-sub", true, Some(b"transient"));
    write_all(
        &mut subscriber,
        &subscribe_packet(1, "cluster/restore/state"),
    );
    assert_eq!(
        read_packet_type(&mut subscriber, "restore subscriber suback"),
        9
    );
    read_packet_body(&mut subscriber, "restore subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher =
        mqtt_connect_with_password(node1.mqtt_bind, "restore-pub", true, Some(b"transient"));
    write_all(
        &mut publisher,
        &publish_packet("cluster/restore/state", b"restored-peer"),
    );
    let online = read_publish_payload(&mut subscriber, "subscriber live publish after restart");
    assert_eq!(online, b"restored-peer");

    write_all(&mut subscriber, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(2, &redis.url, None, ports[3], ports[4], ports[5]);
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let mut subscriber = mqtt_connect(node1.mqtt_bind, "handoff-sub", true);
    write_all(
        &mut subscriber,
        &subscribe_packet(1, "cluster/handoff/state"),
    );
    assert_eq!(
        read_packet_type(&mut subscriber, "handoff subscriber suback"),
        9
    );
    read_packet_body(&mut subscriber, "handoff subscriber suback body");

    let session: Vec<SessionRecord> =
        serde_json::from_str(&http_get_body(node1.http_bind, "/v1/sessiondict")).unwrap();
    let session = session
        .into_iter()
        .find(|record| record.identity.client_id == "handoff-sub")
        .expect("sessiondict record must exist");
    assert_eq!(session.node_id, 1);

    write_all(&mut subscriber, &disconnect_packet());
    thread::sleep(Duration::from_millis(100));

    let reply: SessionDictReassignReply = serde_json::from_str(&http_request_body(
        node2.http_bind,
        "PUT",
        &format!("/v1/sessiondict/{}?node_id=2", session.session_id),
    ))
    .unwrap();
    assert_eq!(reply.updated_sessions, 1);
    assert_eq!(reply.updated_routes, 1);

    let reassigned: Option<SessionRecord> = serde_json::from_str(&http_get_body(
        node2.http_bind,
        &format!("/v1/sessiondict/{}", session.session_id),
    ))
    .unwrap();
    let reassigned = reassigned.expect("reassigned sessiondict record must exist");
    assert_eq!(reassigned.node_id, 2);

    let routes: Vec<RouteRecord> = serde_json::from_str(&http_get_body(
        node2.http_bind,
        &format!(
            "/v1/routes/all?tenant_id={}&session_id={}",
            reassigned.identity.tenant_id, session.session_id
        ),
    ))
    .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].node_id, 2);

    let mut publisher =
        mqtt_connect_with_password(node2.mqtt_bind, "handoff-pub", true, Some(b"transient"));
    write_all(
        &mut publisher,
        &publish_packet("cluster/handoff/state", b"reassigned"),
    );
    thread::sleep(Duration::from_millis(250));

    let stats = http_stats(node2.http_bind);
    assert_eq!(stats.offline_messages, 1);

    let mut resumed = mqtt_connect(node2.mqtt_bind, "handoff-sub", false);
    let offline = read_publish_payload(&mut resumed, "handoff offline replay after reassign");
    assert_eq!(offline, b"reassigned");

    write_all(&mut resumed, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
#[ignore = "flaky cross-node soak timing on local redis-backed cluster runs"]
fn cluster_redis_soak_leaves_no_residual_state() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));
    wait_for_peer_nodes(&[node2.http_bind], 1, Duration::from_secs(5));

    for iteration in 0..10 {
        let mut subscriber = mqtt_connect_with_password(
            node1.mqtt_bind,
            &format!("sub-{iteration}"),
            true,
            Some(b"transient"),
        );
        write_all(
            &mut subscriber,
            &subscribe_packet(1, &format!("cluster/{iteration}/+")),
        );
        assert_eq!(read_packet_type(&mut subscriber, "subscriber suback"), 9);
        read_packet_body(&mut subscriber, "subscriber suback body");
        wait_for_route_records(node2.http_bind, 1, Duration::from_secs(5));

        let mut publisher = mqtt_connect_with_password(
            node2.mqtt_bind,
            &format!("pub-{iteration}"),
            true,
            Some(b"transient"),
        );
        let payload = format!("msg-{iteration}");
        let online = publish_until_delivered(
            &mut publisher,
            &mut subscriber,
            &format!("cluster/{iteration}/state"),
            payload.as_bytes(),
            Duration::from_secs(5),
            "subscriber soak publish",
        );
        assert_eq!(online, payload.as_bytes());

        write_all(&mut subscriber, &disconnect_packet());
        write_all(&mut publisher, &disconnect_packet());
        wait_for_cluster_stats_settled(
            &[node1.http_bind, node2.http_bind],
            Duration::from_secs(5),
            false,
            true,
        );
    }

    wait_for_cluster_stats_settled(
        &[node1.http_bind, node2.http_bind],
        Duration::from_secs(5),
        false,
        true,
    );
}

#[test]
fn cluster_redis_cross_node_bench_meets_delivery_thresholds() {
    let _guard = cluster_test_guard();
    let subscribers = env_usize("GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS", 10);
    let messages = env_usize("GREENMQTT_CLUSTER_BENCH_MESSAGES", 20);
    let qos = env_u8("GREENMQTT_CLUSTER_BENCH_QOS", 0);
    let scenario = env_string("GREENMQTT_CLUSTER_BENCH_SCENARIO", "fanout");
    let min_deliveries_per_sec = env_f64("GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC", 500.0);
    let max_node_rss_bytes = env_u64("GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES", 134_217_728);
    let report = run_cluster_bench(
        subscribers,
        messages,
        qos,
        &scenario,
        min_deliveries_per_sec,
        max_node_rss_bytes,
    );
    assert_eq!(report.scenario, scenario);
    assert_eq!(report.subscribers, subscribers);
    assert_eq!(report.messages, messages);
    assert_eq!(report.qos, qos);
    assert_cluster_bench_timings(&report);
}

#[test]
fn cluster_redis_cross_node_shared_bench_meets_delivery_thresholds() {
    let _guard = cluster_test_guard();
    let report = run_cluster_bench(10, 20, 1, "shared_live", 10.0, 134_217_728);
    assert_eq!(report.scenario, "shared_live");
    assert_eq!(report.deliveries, 20);
    assert_cluster_bench_timings(&report);
}

#[test]
fn cluster_redis_cross_node_offline_replay_bench_meets_delivery_thresholds() {
    let _guard = cluster_test_guard();
    let report = run_cluster_bench(10, 20, 1, "offline_replay", 50.0, 134_217_728);
    assert_eq!(report.scenario, "offline_replay");
    assert_eq!(report.deliveries, 200);
    assert_cluster_bench_timings(&report);
}

fn run_cluster_bench(
    subscribers: usize,
    messages: usize,
    qos: u8,
    scenario: &str,
    min_deliveries_per_sec: f64,
    max_node_rss_bytes: u64,
) -> ClusterBenchReport {
    let setup_started_at = Instant::now();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    assert!(qos <= 2, "GREENMQTT_CLUSTER_BENCH_QOS must be 0, 1, or 2");
    let (shared_live, offline_replay) = match scenario {
        "fanout" => (false, false),
        "shared_live" => (true, false),
        "offline_replay" => (false, true),
        other => panic!("unsupported GREENMQTT_CLUSTER_BENCH_SCENARIO: {other}"),
    };
    let topic_filter = if shared_live {
        "$share/workers/cluster/bench/+"
    } else {
        "cluster/bench/+"
    };

    let mut subscriber_streams = Vec::with_capacity(subscribers);
    for index in 0..subscribers {
        let mut subscriber = mqtt_connect_with_password(
            node1.mqtt_bind,
            &format!("bench-sub-{index}"),
            !offline_replay,
            if offline_replay {
                None
            } else {
                Some(b"transient")
            },
        );
        write_all(
            &mut subscriber,
            &subscribe_packet_with_qos(1, topic_filter, qos),
        );
        assert_eq!(read_packet_type(&mut subscriber, "cluster bench suback"), 9);
        read_packet_body(&mut subscriber, "cluster bench suback body");
        if offline_replay {
            subscriber.shutdown(Shutdown::Both).unwrap();
        } else {
            subscriber_streams.push(subscriber);
        }
    }
    thread::sleep(Duration::from_millis(250));
    let setup_elapsed = setup_started_at.elapsed();

    let mut publisher =
        mqtt_connect_with_password(node2.mqtt_bind, "bench-pub", true, Some(b"transient"));

    let publish_started_at = Instant::now();
    for message_index in 0..messages {
        let packet_id = qos1_packet_id(message_index);
        write_all(
            &mut publisher,
            &publish_packet_with_qos(
                qos,
                packet_id,
                "cluster/bench/state",
                format!("bench-{message_index}").as_bytes(),
            ),
        );
        if qos == 1 {
            assert_eq!(read_packet_type(&mut publisher, "cluster bench puback"), 4);
            let body = read_packet_body(&mut publisher, "cluster bench puback body");
            assert_eq!(u16::from_be_bytes([body[0], body[1]]), packet_id);
        } else if qos == 2 {
            assert_eq!(read_packet_type(&mut publisher, "cluster bench pubrec"), 5);
            let body = read_packet_body(&mut publisher, "cluster bench pubrec body");
            assert_eq!(u16::from_be_bytes([body[0], body[1]]), packet_id);
            write_all(&mut publisher, &pubrel_packet(packet_id));
            assert_eq!(read_packet_type(&mut publisher, "cluster bench pubcomp"), 7);
            let body = read_packet_body(&mut publisher, "cluster bench pubcomp body");
            assert_eq!(u16::from_be_bytes([body[0], body[1]]), packet_id);
        }
    }
    let publish_elapsed = publish_started_at.elapsed();

    let drain_started_at = Instant::now();
    let deliveries = if shared_live {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut payloads = Vec::with_capacity(messages);
        while payloads.len() < messages && Instant::now() < deadline {
            let mut progressed = false;
            for subscriber in &mut subscriber_streams {
                if let Some(publish) = try_read_publish(subscriber, Duration::from_millis(5)) {
                    acknowledge_publish(subscriber, &publish, "cluster bench shared");
                    payloads
                        .push(String::from_utf8(publish.payload).expect("payload should be utf8"));
                    progressed = true;
                    if payloads.len() == messages {
                        break;
                    }
                }
            }
            if !progressed {
                thread::sleep(Duration::from_millis(10));
            }
        }
        let mut expected_payloads = (0..messages)
            .map(|message_index| format!("bench-{message_index}"))
            .collect::<Vec<_>>();
        payloads.sort();
        expected_payloads.sort();
        assert_eq!(payloads, expected_payloads);
        messages
    } else if offline_replay {
        let mut deliveries = 0usize;
        for index in 0..subscribers {
            let mut resumed = mqtt_connect(node1.mqtt_bind, &format!("bench-sub-{index}"), false);
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut payloads = Vec::with_capacity(messages);
            while payloads.len() < messages && Instant::now() < deadline {
                if let Some(publish) = try_read_publish(&mut resumed, Duration::from_millis(25)) {
                    acknowledge_publish(&mut resumed, &publish, "cluster bench offline replay");
                    payloads
                        .push(String::from_utf8(publish.payload).expect("payload should be utf8"));
                }
            }
            let expected_payloads = (0..messages)
                .map(|message_index| format!("bench-{message_index}"))
                .collect::<Vec<_>>();
            assert_eq!(payloads, expected_payloads);
            deliveries += payloads.len();
            if qos > 0 {
                thread::sleep(Duration::from_millis(50));
            }
            write_all(&mut resumed, &disconnect_packet());
            let mut cleanup = mqtt_connect(node1.mqtt_bind, &format!("bench-sub-{index}"), true);
            write_all(&mut cleanup, &disconnect_packet());
        }
        deliveries
    } else {
        let mut deliveries = 0usize;
        for subscriber in &mut subscriber_streams {
            for message_index in 0..messages {
                let publish = read_publish(subscriber, "cluster bench publish");
                assert_eq!(publish.payload, format!("bench-{message_index}").as_bytes());
                acknowledge_publish(subscriber, &publish, "cluster bench");
                deliveries += 1;
            }
        }
        deliveries
    };
    let drain_elapsed = drain_started_at.elapsed();
    let throughput_elapsed = publish_elapsed + drain_elapsed;
    let elapsed = setup_elapsed + throughput_elapsed;
    let elapsed_secs = throughput_elapsed.as_secs_f64();
    let deliveries_per_sec = if elapsed_secs > 0.0 {
        deliveries as f64 / elapsed_secs
    } else {
        deliveries as f64
    };
    assert!(
        deliveries_per_sec >= min_deliveries_per_sec,
        "cluster bench deliveries_per_sec {:.2} below threshold {:.2}",
        deliveries_per_sec,
        min_deliveries_per_sec
    );
    let rss1 = http_metric(node1.http_bind, "greenmqtt_process_rss_bytes");
    let rss2 = http_metric(node2.http_bind, "greenmqtt_process_rss_bytes");
    assert!(
        rss1 <= max_node_rss_bytes,
        "cluster bench node1 rss {} exceeds threshold {}",
        rss1,
        max_node_rss_bytes
    );
    assert!(
        rss2 <= max_node_rss_bytes,
        "cluster bench node2 rss {} exceeds threshold {}",
        rss2,
        max_node_rss_bytes
    );
    let report = ClusterBenchReport {
        scenario: scenario.to_string(),
        subscribers,
        messages,
        qos,
        deliveries,
        setup_ms: setup_elapsed.as_millis(),
        publish_ms: publish_elapsed.as_millis(),
        drain_ms: drain_elapsed.as_millis(),
        elapsed_ms: elapsed.as_millis(),
        deliveries_per_sec,
        node1_rss_bytes: rss1,
        node2_rss_bytes: rss2,
    };
    println!(
        "greenmqtt cluster-bench: scenario={}, subscribers={}, messages={}, qos={}, deliveries={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, deliveries_per_sec={:.2}, node1_rss_bytes={}, node2_rss_bytes={}",
        report.scenario,
        report.subscribers,
        report.messages,
        report.qos,
        report.deliveries,
        report.setup_ms,
        report.publish_ms,
        report.drain_ms,
        report.elapsed_ms,
        report.deliveries_per_sec,
        report.node1_rss_bytes,
        report.node2_rss_bytes,
    );

    if !offline_replay {
        for subscriber in &mut subscriber_streams {
            write_all(subscriber, &disconnect_packet());
        }
    }
    write_all(&mut publisher, &disconnect_packet());
    wait_for_cluster_stats_settled(
        &[node1.http_bind, node2.http_bind],
        Duration::from_secs(5),
        offline_replay,
        offline_replay,
    );
    report
}

fn acknowledge_publish(stream: &mut TcpStream, publish: &PublishFrame, context: &str) {
    if publish.qos == 1 {
        let packet_id = publish
            .packet_id
            .expect("qos1 publish should include packet id");
        write_all(stream, &puback_packet(packet_id));
    } else if publish.qos == 2 {
        let packet_id = publish
            .packet_id
            .expect("qos2 publish should include packet id");
        write_all(stream, &pubrec_packet(packet_id));
        assert_eq!(read_packet_type(stream, &format!("{context} pubrel")), 6);
        let body = read_packet_body(stream, &format!("{context} pubrel body"));
        assert_eq!(u16::from_be_bytes([body[0], body[1]]), packet_id);
        write_all(stream, &pubcomp_packet(packet_id));
    }
}

#[test]
fn cluster_redis_cross_node_qos1_delivery_and_puback_flow() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let mut subscriber =
        mqtt_connect_with_password(node1.mqtt_bind, "qos1-sub", true, Some(b"transient"));
    write_all(
        &mut subscriber,
        &subscribe_packet_with_qos(1, "cluster/qos1/+", 1),
    );
    assert_eq!(
        read_packet_type(&mut subscriber, "qos1 subscriber suback"),
        9
    );
    read_packet_body(&mut subscriber, "qos1 subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher =
        mqtt_connect_with_password(node2.mqtt_bind, "qos1-pub", true, Some(b"transient"));
    write_all(
        &mut publisher,
        &publish_packet_with_qos(1, 11, "cluster/qos1/state", b"qos1-online"),
    );
    assert_eq!(read_packet_type(&mut publisher, "qos1 publisher puback"), 4);
    let puback = read_packet_body(&mut publisher, "qos1 publisher puback body");
    assert_eq!(u16::from_be_bytes([puback[0], puback[1]]), 11);

    let publish = read_publish(&mut subscriber, "qos1 subscriber publish");
    assert_eq!(publish.qos, 1);
    assert_eq!(publish.payload, b"qos1-online");
    let packet_id = publish
        .packet_id
        .expect("qos1 publish should include packet id");
    write_all(&mut subscriber, &puback_packet(packet_id));

    write_all(&mut subscriber, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_cross_node_qos2_delivery_and_pubrel_flow() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let mut subscriber =
        mqtt_connect_with_password(node1.mqtt_bind, "qos2-sub", true, Some(b"transient"));
    write_all(
        &mut subscriber,
        &subscribe_packet_with_qos(1, "cluster/qos2/+", 2),
    );
    assert_eq!(
        read_packet_type(&mut subscriber, "qos2 subscriber suback"),
        9
    );
    read_packet_body(&mut subscriber, "qos2 subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    let mut publisher =
        mqtt_connect_with_password(node2.mqtt_bind, "qos2-pub", true, Some(b"transient"));
    write_all(
        &mut publisher,
        &publish_packet_with_qos(2, 11, "cluster/qos2/state", b"qos2-online"),
    );
    assert_eq!(read_packet_type(&mut publisher, "qos2 publisher pubrec"), 5);
    let pubrec = read_packet_body(&mut publisher, "qos2 publisher pubrec body");
    assert_eq!(u16::from_be_bytes([pubrec[0], pubrec[1]]), 11);
    write_all(&mut publisher, &pubrel_packet(11));
    assert_eq!(
        read_packet_type(&mut publisher, "qos2 publisher pubcomp"),
        7
    );
    let pubcomp = read_packet_body(&mut publisher, "qos2 publisher pubcomp body");
    assert_eq!(u16::from_be_bytes([pubcomp[0], pubcomp[1]]), 11);

    let publish = read_publish(&mut subscriber, "qos2 subscriber publish");
    assert_eq!(publish.qos, 2);
    assert_eq!(publish.payload, b"qos2-online");
    let packet_id = publish
        .packet_id
        .expect("qos2 publish should include packet id");
    write_all(&mut subscriber, &pubrec_packet(packet_id));
    assert_eq!(
        read_packet_type(&mut subscriber, "qos2 subscriber pubrel"),
        6
    );
    let pubrel = read_packet_body(&mut subscriber, "qos2 subscriber pubrel body");
    assert_eq!(u16::from_be_bytes([pubrel[0], pubrel[1]]), packet_id);
    write_all(&mut subscriber, &pubcomp_packet(packet_id));

    write_all(&mut subscriber, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let mut node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    let mut subscriber = mqtt_connect(node1.mqtt_bind, "sub", true);
    write_all(&mut subscriber, &subscribe_packet(1, "cluster/+/state"));
    assert_eq!(read_packet_type(&mut subscriber, "subscriber suback"), 9);
    read_packet_body(&mut subscriber, "subscriber suback body");
    thread::sleep(Duration::from_millis(250));

    node1.kill();
    wait_for_port_closed(node1.rpc_bind, Duration::from_secs(10));

    let mut publisher = mqtt_connect(node2.mqtt_bind, "pub", true);
    write_all(
        &mut publisher,
        &publish_packet("cluster/d1/state", b"failover"),
    );

    thread::sleep(Duration::from_millis(250));
    let stats = http_stats(node2.http_bind);
    assert_eq!(stats.offline_messages, 1);

    let mut resumed = mqtt_connect(node2.mqtt_bind, "sub", false);
    let offline = read_publish_payload(&mut resumed, "subscriber offline replay after failover");
    assert_eq!(offline, b"failover");

    write_all(&mut resumed, &disconnect_packet());
    write_all(&mut publisher, &disconnect_packet());
}

#[test]
fn cluster_redis_repeated_failover_replays_without_offline_leak() {
    let _guard = cluster_test_guard();
    let redis = RedisServer::start();
    let ports = reserve_unique_addrs(6);
    let iterations = env_usize("GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS", 3);

    let mut node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
    wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node1.rpc_bind, Duration::from_secs(10));
    wait_for_port(node1.http_bind, Duration::from_secs(10));

    let node2 = BrokerNode::start(
        2,
        &redis.url,
        Some(format!("1=http://{}", node1.rpc_bind)),
        ports[3],
        ports[4],
        ports[5],
    );
    wait_for_port(node2.mqtt_bind, Duration::from_secs(10));
    wait_for_port(node2.http_bind, Duration::from_secs(10));

    for iteration in 0..iterations {
        let topic = format!("cluster/failover/{iteration}/state");
        let payload = format!("failover-{iteration}");

        let mut subscriber = mqtt_connect(node1.mqtt_bind, "failover-sub", true);
        write_all(&mut subscriber, &subscribe_packet(1, &topic));
        assert_eq!(
            read_packet_type(&mut subscriber, "failover subscriber suback"),
            9
        );
        read_packet_body(&mut subscriber, "failover subscriber suback body");
        thread::sleep(Duration::from_millis(250));

        node1.kill();
        wait_for_port_closed(node1.rpc_bind, Duration::from_secs(10));

        let mut publisher = mqtt_connect_with_password(
            node2.mqtt_bind,
            &format!("failover-pub-{iteration}"),
            true,
            Some(b"transient"),
        );
        write_all(&mut publisher, &publish_packet(&topic, payload.as_bytes()));
        thread::sleep(Duration::from_millis(250));

        let mut resumed = mqtt_connect(node2.mqtt_bind, "failover-sub", false);
        let offline = read_publish_payload(&mut resumed, "failover offline replay");
        assert_eq!(offline, payload.as_bytes());

        write_all(&mut resumed, &disconnect_packet());
        write_all(&mut publisher, &disconnect_packet());

        let stats = http_stats(node2.http_bind);
        assert_eq!(stats.offline_messages, 0);
        assert_eq!(stats.inflight_messages, 0);

        if iteration + 1 < iterations {
            node1 = BrokerNode::start(1, &redis.url, None, ports[0], ports[1], ports[2]);
            wait_for_port(node1.mqtt_bind, Duration::from_secs(10));
            wait_for_port(node1.rpc_bind, Duration::from_secs(10));
            wait_for_port(node1.http_bind, Duration::from_secs(10));
        }
    }
}

struct RedisServer {
    _dir: tempfile::TempDir,
    child: Child,
    url: String,
}

impl RedisServer {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let port = reserve_addr().port();
        let child = Command::new(redis_server_binary())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--port")
            .arg(port.to_string())
            .arg("--dir")
            .arg(dir.path())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--protected-mode")
            .arg("no")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            _dir: dir,
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

fn redis_server_binary() -> String {
    std::env::var("GREENMQTT_REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".to_string())
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct BrokerNode {
    _log_file: tempfile::NamedTempFile,
    child: Child,
    http_bind: SocketAddr,
    mqtt_bind: SocketAddr,
    rpc_bind: SocketAddr,
}

impl BrokerNode {
    fn start(
        node_id: u64,
        redis_url: &str,
        peers: Option<String>,
        http_bind: SocketAddr,
        mqtt_bind: SocketAddr,
        rpc_bind: SocketAddr,
    ) -> Self {
        Self::start_with_data_dir(
            node_id, redis_url, peers, http_bind, mqtt_bind, rpc_bind, None,
        )
    }

    fn start_with_data_dir(
        node_id: u64,
        redis_url: &str,
        peers: Option<String>,
        http_bind: SocketAddr,
        mqtt_bind: SocketAddr,
        rpc_bind: SocketAddr,
        data_dir: Option<&Path>,
    ) -> Self {
        let log_file = tempfile::NamedTempFile::new().unwrap();
        let log = log_file.reopen().unwrap();
        let mut command = Command::new(binary_path());
        command
            .arg("serve")
            .env("GREENMQTT_NODE_ID", node_id.to_string())
            .env("GREENMQTT_STORAGE_BACKEND", "redis")
            .env("GREENMQTT_REDIS_URL", redis_url)
            .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
            .env("GREENMQTT_MQTT_BIND", mqtt_bind.to_string())
            .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
            .env_remove("GREENMQTT_STATE_ENDPOINT")
            .env_remove("GREENMQTT_TLS_BIND")
            .env_remove("GREENMQTT_WS_BIND")
            .env_remove("GREENMQTT_WSS_BIND")
            .env_remove("GREENMQTT_QUIC_BIND")
            .env("GREENMQTT_PEERS", peers.unwrap_or_default())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log.try_clone().unwrap()));
        if let Some(path) = data_dir {
            command.env("GREENMQTT_DATA_DIR", path);
        } else {
            command.env_remove("GREENMQTT_DATA_DIR");
        }
        let child = command.spawn().unwrap();

        Self {
            _log_file: log_file,
            child,
            http_bind,
            mqtt_bind,
            rpc_bind,
        }
    }
}

fn temp_data_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "greenmqtt-cluster-{name}-{}",
        reserve_addr().port()
    ))
}

impl Drop for BrokerNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl BrokerNode {
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_greenmqtt-cli")
}

fn reserve_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn reserve_unique_addrs(count: usize) -> Vec<SocketAddr> {
    let mut addrs = Vec::with_capacity(count);
    while addrs.len() < count {
        let addr = reserve_addr();
        if !addrs.contains(&addr) {
            addrs.push(addr);
        }
    }
    addrs
}

fn wait_for_port(addr: SocketAddr, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("port {addr} did not open in time");
}

fn wait_for_port_closed(addr: SocketAddr, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_err() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("port {addr} did not close in time");
}

fn mqtt_connect(addr: SocketAddr, client_id: &str, clean_start: bool) -> TcpStream {
    mqtt_connect_with_password(addr, client_id, clean_start, None)
}

fn mqtt_connect_with_password(
    addr: SocketAddr,
    client_id: &str,
    clean_start: bool,
    password: Option<&[u8]>,
) -> TcpStream {
    let started_at = Instant::now();
    let timeout = Duration::from_secs(10);
    loop {
        if let Ok(mut stream) = TcpStream::connect(addr) {
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            write_all(
                &mut stream,
                &connect_packet(client_id, clean_start, password),
            );
            if let Ok(packet_type) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                read_packet_type(&mut stream, "connect connack")
            })) {
                assert_eq!(packet_type, 2);
                read_packet_body(&mut stream, "connect connack body");
                return stream;
            }
        }
        assert!(
            started_at.elapsed() < timeout,
            "timed out connecting mqtt client {client_id} to {addr}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn connect_packet(client_id: &str, clean_start: bool, password: Option<&[u8]>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(4);
    let mut connect_flags = if clean_start { 0b0000_0010 } else { 0 };
    if password.is_some() {
        connect_flags |= 0b0100_0000;
    }
    body.push(connect_flags);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    if let Some(password) = password {
        body.extend_from_slice(&(password.len() as u16).to_be_bytes());
        body.extend_from_slice(password);
    }
    packet(0x10, &body)
}

fn subscribe_packet(packet_id: u16, topic_filter: &str) -> Vec<u8> {
    subscribe_packet_with_qos(packet_id, topic_filter, 1)
}

fn subscribe_packet_with_qos(packet_id: u16, topic_filter: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(&(topic_filter.len() as u16).to_be_bytes());
    body.extend_from_slice(topic_filter.as_bytes());
    body.push(qos & 0b11);
    packet(0x82, &body)
}

fn publish_packet(topic: &str, payload: &[u8]) -> Vec<u8> {
    publish_packet_with_qos(0, 0, topic, payload)
}

fn publish_packet_with_qos(qos: u8, packet_id: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&packet_id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    packet(0x30 | ((qos & 0b11) << 1), &body)
}

fn puback_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    packet(0x40, &body)
}

fn pubrec_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    packet(0x50, &body)
}

fn pubrel_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    packet(0x62, &body)
}

fn pubcomp_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    packet(0x70, &body)
}

fn disconnect_packet() -> Vec<u8> {
    vec![0xe0, 0x00]
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![header];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(body);
    packet
}

fn encode_remaining_length(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

fn read_packet_type(stream: &mut TcpStream, context: &str) -> u8 {
    let mut header = [0u8; 1];
    stream
        .read_exact(&mut header)
        .unwrap_or_else(|error| panic!("{context}: failed to read packet header: {error}"));
    header[0] >> 4
}

fn read_packet_body(stream: &mut TcpStream, context: &str) -> Vec<u8> {
    let remaining_len = read_remaining_length(stream, context);
    let mut body = vec![0u8; remaining_len];
    stream
        .read_exact(&mut body)
        .unwrap_or_else(|error| panic!("{context}: failed to read packet body: {error}"));
    body
}

struct PublishFrame {
    qos: u8,
    packet_id: Option<u16>,
    payload: Vec<u8>,
}

fn read_publish(stream: &mut TcpStream, context: &str) -> PublishFrame {
    let mut header = [0u8; 1];
    stream
        .read_exact(&mut header)
        .unwrap_or_else(|error| panic!("{context}: failed to read packet header: {error}"));
    assert_eq!(header[0] >> 4, 3, "{context}: expected PUBLISH");
    let qos = (header[0] >> 1) & 0b11;
    assert!(qos <= 2, "{context}: unsupported qos {qos} in test helper");
    let body = read_packet_body(stream, context);
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2 + topic_len;
    let packet_id = if qos > 0 {
        let packet_id = u16::from_be_bytes([body[cursor], body[cursor + 1]]);
        cursor += 2;
        Some(packet_id)
    } else {
        None
    };
    PublishFrame {
        qos,
        packet_id,
        payload: body[cursor..].to_vec(),
    }
}

fn read_publish_payload(stream: &mut TcpStream, context: &str) -> Vec<u8> {
    read_publish(stream, context).payload
}

fn try_read_publish(stream: &mut TcpStream, timeout: Duration) -> Option<PublishFrame> {
    stream.set_read_timeout(Some(timeout)).unwrap();
    let mut header = [0u8; 1];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(error)
            if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut =>
        {
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            return None;
        }
        Err(error) => panic!("shared cluster bench: failed to read packet header: {error}"),
    }
    assert_eq!(header[0] >> 4, 3, "shared cluster bench: expected PUBLISH");
    let qos = (header[0] >> 1) & 0b11;
    let remaining_len = read_remaining_length_with_timeout(stream, "shared cluster bench", timeout);
    let mut body = vec![0u8; remaining_len];
    stream.read_exact(&mut body).unwrap_or_else(|error| {
        panic!("shared cluster bench: failed to read packet body: {error}")
    });
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2 + topic_len;
    let packet_id = if qos > 0 {
        let packet_id = u16::from_be_bytes([body[cursor], body[cursor + 1]]);
        cursor += 2;
        Some(packet_id)
    } else {
        None
    };
    Some(PublishFrame {
        qos,
        packet_id,
        payload: body[cursor..].to_vec(),
    })
}

fn publish_until_delivered(
    publisher: &mut TcpStream,
    subscriber: &mut TcpStream,
    topic: &str,
    payload: &[u8],
    timeout: Duration,
    context: &str,
) -> Vec<u8> {
    let started_at = Instant::now();
    while started_at.elapsed() < timeout {
        write_all(publisher, &publish_packet(topic, payload));
        if let Some(publish) = try_read_publish(subscriber, Duration::from_millis(100)) {
            return publish.payload;
        }
    }
    panic!("{context}: publish did not arrive within {timeout:?}");
}

fn read_remaining_length(stream: &mut TcpStream, context: &str) -> usize {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    loop {
        let mut byte = [0u8; 1];
        stream
            .read_exact(&mut byte)
            .unwrap_or_else(|error| panic!("{context}: failed to read remaining length: {error}"));
        value += ((byte[0] & 127) as usize) * multiplier;
        if byte[0] & 128 == 0 {
            return value;
        }
        multiplier *= 128;
    }
}

fn read_remaining_length_with_timeout(
    stream: &mut TcpStream,
    context: &str,
    timeout: Duration,
) -> usize {
    stream.set_read_timeout(Some(timeout)).unwrap();
    let value = read_remaining_length(stream, context);
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    value
}

fn write_all(stream: &mut TcpStream, bytes: &[u8]) {
    stream.write_all(bytes).unwrap();
    stream.flush().unwrap();
}

#[derive(Debug, serde::Deserialize)]
struct StatsResponse {
    peer_nodes: usize,
    global_session_records: usize,
    route_records: usize,
    subscription_records: usize,
    offline_messages: usize,
    inflight_messages: usize,
    retained_messages: usize,
}

fn http_stats(addr: SocketAddr) -> StatsResponse {
    let started_at = Instant::now();
    let timeout = Duration::from_secs(5);
    let mut last_body = String::new();
    while started_at.elapsed() < timeout {
        let body = http_get_body(addr, "/v1/stats");
        match serde_json::from_str(&body) {
            Ok(stats) => return stats,
            Err(error) => {
                last_body = format!("{body} ({error})");
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    panic!("failed to parse /v1/stats from {addr}: {last_body}");
}

fn stats_settled(
    stats: &StatsResponse,
    offline_replay: bool,
    require_empty_sessions: bool,
) -> bool {
    if stats.offline_messages != 0 || stats.inflight_messages != 0 || stats.retained_messages != 0 {
        return false;
    }
    if offline_replay {
        stats.route_records == 0 && stats.subscription_records == 0
    } else {
        (!require_empty_sessions || stats.global_session_records == 0)
            && stats.route_records == 0
            && stats.subscription_records == 0
    }
}

fn wait_for_cluster_stats_settled(
    addrs: &[SocketAddr],
    timeout: Duration,
    offline_replay: bool,
    require_empty_sessions: bool,
) {
    let started_at = Instant::now();
    let mut last_stats = Vec::new();
    while started_at.elapsed() < timeout {
        last_stats = addrs.iter().copied().map(http_stats).collect();
        if last_stats
            .iter()
            .all(|stats| stats_settled(stats, offline_replay, require_empty_sessions))
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("cluster stats did not settle within {timeout:?}: {last_stats:?}");
}

fn wait_for_route_records(addr: SocketAddr, minimum_routes: usize, timeout: Duration) {
    let started_at = Instant::now();
    let mut last_stats = None;
    while started_at.elapsed() < timeout {
        let stats = http_stats(addr);
        if stats.route_records >= minimum_routes {
            return;
        }
        last_stats = Some(stats);
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "route records on {addr} did not reach {minimum_routes} within {timeout:?}: {last_stats:?}"
    );
}

fn wait_for_peer_nodes(addrs: &[SocketAddr], expected_peers: usize, timeout: Duration) {
    let started_at = Instant::now();
    let mut last_stats = Vec::new();
    while started_at.elapsed() < timeout {
        last_stats = addrs.iter().copied().map(http_stats).collect();
        if last_stats
            .iter()
            .all(|stats| stats.peer_nodes == expected_peers)
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("peer nodes did not reach {expected_peers} within {timeout:?}: {last_stats:?}");
}

fn http_metric(addr: SocketAddr, metric_name: &str) -> u64 {
    let body = http_get_body(addr, "/metrics");
    body.lines()
        .find_map(|line| {
            line.strip_prefix(metric_name).and_then(|rest| {
                let value = rest.trim();
                if value.is_empty() {
                    None
                } else {
                    value.parse().ok()
                }
            })
        })
        .or_else(|| {
            (metric_name == "greenmqtt_process_rss_bytes").then(|| {
                body.lines().find_map(|line| {
                    line.strip_prefix("greenmqtt_broker_rss_bytes")
                        .and_then(|rest| {
                            let value = rest.trim();
                            if value.is_empty() {
                                None
                            } else {
                                value.parse().ok()
                            }
                        })
                })
            })?
        })
        .unwrap_or_else(|| {
            if metric_name == "greenmqtt_process_rss_bytes" {
                0
            } else {
                panic!("metric {metric_name} missing from {addr}")
            }
        })
}

fn http_get_body(addr: SocketAddr, path: &str) -> String {
    http_request_body(addr, "GET", path)
}

fn http_request_body(addr: SocketAddr, method: &str, path: &str) -> String {
    let request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                write_all(&mut stream, request.as_bytes());
                let mut response = Vec::new();
                match stream.read_to_end(&mut response) {
                    Ok(_) => {
                        let response = String::from_utf8(response).unwrap();
                        let (_, body) = response.split_once("\r\n\r\n").unwrap_or_else(|| {
                            panic!("invalid http response from {addr}: {response}")
                        });
                        return body.to_string();
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            ErrorKind::ConnectionReset
                                | ErrorKind::ConnectionRefused
                                | ErrorKind::TimedOut
                                | ErrorKind::UnexpectedEof
                        ) && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("http read_to_end failed for {addr}: {error}"),
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionRefused
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                ) && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("http connect failed for {addr}: {error}"),
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn qos1_packet_id(message_index: usize) -> u16 {
    u16::try_from(message_index + 1).expect("message index exceeds qos1 packet id range")
}
