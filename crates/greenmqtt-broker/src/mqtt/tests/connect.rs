use super::*;
use crate::{ClientBalancer, Redirection};
use crate::{PeerRegistry, RoundRobinBalancer};
use std::collections::BTreeMap;
use std::sync::RwLock;

#[derive(Default)]
struct RedirectBalancer;

impl ClientBalancer for RedirectBalancer {
    fn need_redirect(&self, _identity: &ClientIdentity) -> Option<Redirection> {
        Some(Redirection {
            permanent: false,
            server_reference: "mqtt://127.0.0.1:2883".into(),
        })
    }
}

#[derive(Default)]
struct RedirectPeerRegistry {
    endpoints: RwLock<BTreeMap<u64, String>>,
}

#[async_trait::async_trait]
impl PeerRegistry for RedirectPeerRegistry {
    fn list_peer_nodes(&self) -> Vec<u64> {
        self.endpoints
            .read()
            .expect("peer registry poisoned")
            .keys()
            .copied()
            .collect()
    }

    fn list_peer_endpoints(&self) -> BTreeMap<u64, String> {
        self.endpoints
            .read()
            .expect("peer registry poisoned")
            .clone()
    }

    fn remove_peer_node(&self, node_id: u64) -> bool {
        self.endpoints
            .write()
            .expect("peer registry poisoned")
            .remove(&node_id)
            .is_some()
    }

    async fn add_peer_node(&self, node_id: u64, endpoint: String) -> anyhow::Result<()> {
        self.endpoints
            .write()
            .expect("peer registry poisoned")
            .insert(node_id, endpoint);
        Ok(())
    }
}

#[tokio::test]
async fn mqtt_tcp_connection_limit_rejects_second_client() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_connection_limit(1);
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut first = None;
    for _ in 0..10 {
        let mut candidate = connect_tcp_with_retry(bind).await.unwrap();
        if candidate.write_all(&connect_packet("first")).await.is_err() {
            sleep(Duration::from_millis(50)).await;
            continue;
        }
        match candidate.read_u8().await {
            Ok(header) if header >> 4 == PACKET_TYPE_CONNACK => {
                let connack_len = read_remaining_length_for_test(&mut candidate).await;
                let mut connack = vec![0u8; connack_len];
                candidate.read_exact(&mut connack).await.unwrap();
                first = Some(candidate);
                break;
            }
            _ => sleep(Duration::from_millis(50)).await,
        }
    }
    let _first = first.expect("expected first tcp client to connect");

    let mut second = connect_tcp_with_retry(bind).await.unwrap();
    let _ = second.write_all(&connect_packet("second")).await;
    let error = second
        .read_u8()
        .await
        .expect_err("expected second client to be rejected");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
    ));
    let _still_open = &_first;

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_pressure_reject_returns_server_busy_connack() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_max_online_sessions(1);
    let active = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "active-v5".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker.clone(), bind));
    sleep(Duration::from_millis(100)).await;
    assert_eq!(broker.stats().await.unwrap().local_online_sessions, 1);

    let mut second = connect_tcp_with_retry(bind).await.unwrap();
    let _ = second.write_all(&connect_packet_v5("second")).await;
    match second.read_u8().await {
        Ok(header) => {
            assert_eq!(header >> 4, PACKET_TYPE_CONNACK);
            let second_connack_len = read_remaining_length_for_test(&mut second).await;
            let mut second_connack = vec![0u8; second_connack_len];
            second.read_exact(&mut second_connack).await.unwrap();
            let parsed = parse_v5_connack_packet(&second_connack);
            assert_eq!(parsed.reason_code, 0x89);
            assert_eq!(parsed.reason_string.as_deref(), Some("server busy"));
        }
        Err(error) => {
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ));
        }
    }
    broker.disconnect(&active.session.session_id).await.unwrap();

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_memory_pressure_reject_returns_server_busy_connack() {
    let broker = test_broker();
    broker.force_memory_pressure_level(crate::broker::pressure::PressureLevel::Critical);
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker.clone(), bind));
    sleep(Duration::from_millis(100)).await;

    let mut second = connect_tcp_with_retry(bind).await.unwrap();
    let _ = second.write_all(&connect_packet_v5("second")).await;
    match second.read_u8().await {
        Ok(header) => {
            assert_eq!(header >> 4, PACKET_TYPE_CONNACK);
            let second_connack_len = read_remaining_length_for_test(&mut second).await;
            let mut second_connack = vec![0u8; second_connack_len];
            second.read_exact(&mut second_connack).await.unwrap();
            let parsed = parse_v5_connack_packet(&second_connack);
            assert_eq!(parsed.reason_code, 0x89);
            assert_eq!(parsed.reason_string.as_deref(), Some("server busy"));
        }
        Err(error) => {
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ));
        }
    }

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_tenant_connection_quota_exceeded_returns_quota_exceeded_connack() {
    let broker = test_broker();
    broker.set_tenant_quota(
        "public",
        greenmqtt_core::TenantQuota {
            max_connections: 0,
            max_subscriptions: 10,
            max_msg_per_sec: 10,
            max_memory_bytes: u64::MAX,
        },
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker.clone(), bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("tenant-quota"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let parsed = parse_v5_connack_packet(&connack);
    assert_eq!(parsed.reason_code, 0x97);
    assert_eq!(parsed.reason_string.as_deref(), Some("quota exceeded"));

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_client_balancer_redirect_returns_use_another_server_connack() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_client_balancer(Arc::new(RedirectBalancer));
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("redirected"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let parsed = parse_v5_connack_packet(&connack);
    assert_eq!(parsed.reason_code, 0x9C);
    assert_eq!(parsed.reason_string.as_deref(), Some("use another server"));
    assert_eq!(
        parsed.server_reference.as_deref(),
        Some("mqtt://127.0.0.1:2883")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_high_load_node_redirects_new_connection_to_peer() {
    let mut broker = test_broker();
    let peer_registry = Arc::new(RedirectPeerRegistry::default());
    peer_registry
        .endpoints
        .write()
        .expect("peer registry poisoned")
        .insert(2, "mqtt://127.0.0.1:2883".into());
    let local_load = Arc::new(AtomicUsize::new(0));
    let balancer = RoundRobinBalancer::with_threshold(
        1,
        peer_registry,
        Arc::new({
            let local_load = Arc::clone(&local_load);
            move || local_load.load(Ordering::Relaxed)
        }),
        1,
        false,
    );
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_client_balancer(Arc::new(balancer));
    let active = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "busy".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    local_load.store(1, Ordering::Relaxed);
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker.clone(), bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("redirected-high-load"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let parsed = parse_v5_connack_packet(&connack);
    assert_eq!(parsed.reason_code, 0x9C);
    assert_eq!(
        parsed.server_reference.as_deref(),
        Some("mqtt://127.0.0.1:2883")
    );

    broker.disconnect(&active.session.session_id).await.unwrap();
    server.abort();
}
#[tokio::test]
async fn mqtt_v3_pressure_reject_returns_server_unavailable_connack() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_max_online_sessions(1);
    let active = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "user-a".into(),
                client_id: "active-v3".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker.clone(), bind));
    sleep(Duration::from_millis(100)).await;

    let mut second = connect_tcp_with_retry(bind).await.unwrap();
    second.write_all(&connect_packet("second")).await.unwrap();
    assert_eq!(second.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut second).await;
    let mut connack = vec![0u8; connack_len];
    second.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack.get(1).copied(), Some(0x03));
    broker.disconnect(&active.session.session_id).await.unwrap();

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_auth_failure_returns_not_authorized_connack() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::Static(StaticAuthProvider::new(vec![IdentityMatcher {
            tenant_id: "demo".to_string(),
            user_id: "alice".to_string(),
            client_id: "allowed".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties("denied", true, &[]))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x87);
    assert_eq!(
        connack.reason_string.as_deref(),
        Some("authentication failed")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_auth_failure_includes_server_reference() {
    let broker = test_broker_with_auth(
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
            server_reference: Some("greenmqtt://node-2".to_string()),
            audit_log_path: None,
        },
        ConfiguredAuth::Static(StaticAuthProvider::new(vec![IdentityMatcher {
            tenant_id: "demo".to_string(),
            user_id: "alice".to_string(),
            client_id: "allowed".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties("denied", true, &[]))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x87);
    assert_eq!(
        connack.server_reference.as_deref(),
        Some("greenmqtt://node-2")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_auth_failure_request_problem_information_false_suppresses_connack_reason_string() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::Static(StaticAuthProvider::new(vec![IdentityMatcher {
            tenant_id: "demo".to_string(),
            user_id: "alice".to_string(),
            client_id: "allowed".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "denied",
            true,
            &request_problem_information_property(false),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x87);
    assert_eq!(connack.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_enhanced_auth_challenge_flow_returns_auth_then_connack() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::EnhancedStatic(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_auth(
            "enhanced-auth",
            true,
            "custom",
            Some(b"client-hello"),
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x18);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert_eq!(auth.auth_data.as_deref(), Some(&b"server-challenge"[..]));

    client
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-response")))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x00);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reauth_challenge_flow_returns_auth_success() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::EnhancedStatic(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_auth(
            "reauth-flow",
            true,
            "custom",
            Some(b"client-hello"),
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x18);

    client
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-response")))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x00);

    client
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-hello")))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x18);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert_eq!(auth.auth_data.as_deref(), Some(&b"server-challenge"[..]));

    client
        .write_all(&auth_packet_v5_with_reason(
            0x00,
            Some("custom"),
            Some(b"client-response"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x00);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert!(auth.auth_data.is_none());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reauth_can_start_after_plain_connect() {
    let broker = test_broker_with_custom_auth(
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
        AllowThenStaticEnhancedAuthProvider::new(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("plain-reauth"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-hello")))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x18);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert_eq!(auth.auth_data.as_deref(), Some(&b"server-challenge"[..]));

    client
        .write_all(&auth_packet_v5_with_reason(
            0x00,
            Some("custom"),
            Some(b"client-response"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x00);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert!(auth.auth_data.is_none());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reauthenticate_reason_code_starts_reauth_after_plain_connect() {
    let broker = test_broker_with_custom_auth(
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
        AllowThenStaticEnhancedAuthProvider::new(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("reauth-with-reauthenticate"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&auth_packet_v5_with_reason(
            0x19,
            Some("custom"),
            Some(b"client-hello"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x18);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert_eq!(auth.auth_data.as_deref(), Some(&b"server-challenge"[..]));

    client
        .write_all(&auth_packet_v5_with_reason(
            0x00,
            Some("custom"),
            Some(b"client-response"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x00);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert!(auth.auth_data.is_none());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_auth_success_code_cannot_start_reauth() {
    let broker = test_broker_with_custom_auth(
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
        AllowThenStaticEnhancedAuthProvider::new(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-reauth-start"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&auth_packet_v5_with_reason(
            0x00,
            Some("custom"),
            Some(b"client-hello"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_enhanced_auth_bad_method_returns_connack_error() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::EnhancedStatic(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_auth(
            "enhanced-auth-denied",
            true,
            "other",
            None,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x8C);
    assert_eq!(
        connack.reason_string.as_deref(),
        Some("unsupported authentication method")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reauth_bad_method_disconnects() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::EnhancedStatic(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_auth(
            "reauth-bad-method",
            true,
            "custom",
            Some(b"client-hello"),
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    client
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-response")))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x00);

    client
        .write_all(&auth_packet_v5(Some("other"), None))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x8C);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unsupported authentication method")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_enhanced_auth_rejects_non_auth_packet_before_completion() {
    let broker = test_broker_with_auth(
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
            server_reference: Some("greenmqtt://enhanced-auth".to_string()),
            audit_log_path: None,
        },
        ConfiguredAuth::EnhancedStatic(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_auth(
            "enhanced-auth-protocol-error",
            true,
            "custom",
            None,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let _ = parse_v5_auth_packet(&auth);

    client
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected packet before enhanced auth completion")
    );
    assert_eq!(
        disconnect.server_reference.as_deref(),
        Some("greenmqtt://enhanced-auth")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_auth_reason_code_disconnects_protocol_error() {
    let broker = test_broker_with_custom_auth(
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
        AllowThenStaticEnhancedAuthProvider::new(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        )),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-auth-reason"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&auth_packet_v5_with_reason(
            0x17,
            Some("custom"),
            Some(b"client-hello"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_auth_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-auth-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&auth_packet_v5_with_properties(
            0x18,
            &receive_maximum_property(10),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_second_connect_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-connect"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&connect_packet_v5("duplicate-connect"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected connect packet")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_malformed_remaining_length_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("bad-remaining-length"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&malformed_remaining_length_packet())
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("malformed remaining length")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pingreq_flags_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("bad-pingreq-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pingreq_packet_with_flags(0x01))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pingreq flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_disconnect_flags_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("bad-disconnect-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&disconnect_packet_v5_with_flags(0x01))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unexpected_pubrec_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("unexpected-pubrec"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&pubrec_client_packet(7)).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubrec packet id")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unexpected_pubrel_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("unexpected-pubrel"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&pubrel_packet(7)).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubrel packet id")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unexpected_pubcomp_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("unexpected-pubcomp"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&pubcomp_client_packet(7)).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubcomp packet id")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_pingreq_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&pingreq_packet()).await.unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_puback_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&puback_client_packet(1)).await.unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_pubrec_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&pubrec_client_packet(1)).await.unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_pubrel_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&pubrel_packet(1)).await.unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_pubcomp_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&pubcomp_client_packet(1)).await.unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_disconnect_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&disconnect_packet()).await.unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_pingreq_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(pingreq_packet()))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_puback_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(puback_client_packet(1)))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_pubrec_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(pubrec_client_packet(1)))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_pubrel_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(pubrel_packet(1)))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_pubcomp_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(pubcomp_client_packet(1)))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_disconnect_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(250)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(disconnect_packet()))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_auth_data_without_method_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("auth-data-without-method"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&auth_packet_v5_with_reason(
            0x18,
            None,
            Some(b"client-hello"),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("auth data without auth method")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_auth_flags_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-auth-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut auth = auth_packet_v5(Some("custom"), Some(b"client-hello"));
    auth[0] = (PACKET_TYPE_AUTH << 4) | 0x01;
    client.write_all(&auth).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_auth_method_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-auth-method"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&auth_method_property("custom"));
    client
        .write_all(&auth_packet_v5_with_properties(0x18, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth method")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_auth_data_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-auth-data"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&auth_data_property(b"hello"));
    properties.extend_from_slice(&auth_data_property(b"again"));
    client
        .write_all(&auth_packet_v5_with_properties(0x18, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth data")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_auth_reason_string_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-auth-reason-string"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&reason_string_property("first"));
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .write_all(&auth_packet_v5_with_properties(0x18, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth reason string")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_http_enhanced_auth_challenge_flow_returns_auth_then_connack() {
    let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        for index in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request_for_test(&mut stream).await.unwrap();
            let body = if index == 0 {
                "{\"result\":\"continue\",\"auth_data\":[115,101,114,118,101,114],\"reason_string\":\"challenge\"}"
            } else {
                "{\"result\":\"success\"}"
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            tx.send(request).unwrap();
        }
    });

    let broker = test_broker_with_auth(
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
        ConfiguredAuth::Http(HttpAuthProvider::new(HttpAuthConfig {
            url: format!("http://{http_addr}/auth"),
        })),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_auth(
            "http-enhanced-auth",
            true,
            "custom",
            Some(b"client-hello"),
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut client).await;
    let mut auth = vec![0u8; auth_len];
    client.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert_eq!(auth.auth_data.as_deref(), Some(&b"server"[..]));

    client
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-response")))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x00);

    let begin_request = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let continue_request = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(begin_request.contains("\"phase\":\"begin\""));
    assert!(begin_request.contains("\"method\":\"custom\""));
    assert!(continue_request.contains("\"phase\":\"continue\""));
    assert!(continue_request.contains("\"method\":\"custom\""));

    server.abort();
}
#[tokio::test]
async fn mqtt_v3_auth_failure_returns_connection_refused_not_authorized() {
    let broker = test_broker_with_auth(
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
        ConfiguredAuth::Static(StaticAuthProvider::new(vec![IdentityMatcher {
            tenant_id: "demo".to_string(),
            user_id: "alice".to_string(),
            client_id: "allowed".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client.write_all(&connect_packet("denied")).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack[1], 0x05);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reserved_connect_flag_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut packet = connect_packet_v5("invalid-connect-flags");
    packet[9] |= 0b0000_0001;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&packet).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x82);
    assert_eq!(
        connack.reason_string.as_deref(),
        Some("invalid connect flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscription_identifier_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-subscription-id"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(0),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscription identifier")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_subscription_identifier_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-subscription-id"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = subscription_identifier_property(7);
    properties.extend_from_slice(&subscription_identifier_property(9));
    client
        .write_all(&subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate subscription identifier")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_shared_subscription_with_no_local_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("shared-no-local"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "$share/g1/devices/+/state",
            0b0000_0101,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_payload_format_indicator_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-payload-format-indicator"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &publish_properties(Some(2), None, None, None, None, &[]),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid payload format indicator")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_payload_format_indicator_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-payload-format-indicator"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(Some(1), None, None, None, None, &[]);
    properties.extend_from_slice(&publish_properties(Some(1), None, None, None, None, &[]));
    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate payload format indicator")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_content_type_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-content-type"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, Some("text/plain"), None, None, None, &[]);
    properties.extend_from_slice(&publish_properties(
        None,
        Some("application/json"),
        None,
        None,
        None,
        &[],
    ));
    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate content type")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_will_utf8_payload_indicator_invalid_payload_returns_payload_format_invalid_connack(
) {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_will(
            "will-invalid-payload",
            "devices/d1/state",
            &[0xff, 0xfe],
            0,
            false,
            &publish_properties(Some(1), None, None, None, None, &[]),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let parsed = parse_v5_connack_packet(&connack);
    assert_eq!(parsed.reason_code, 0x99);
    assert_eq!(
        parsed.reason_string.as_deref(),
        Some("payload format invalid")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_message_expiry_interval_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-message-expiry-interval"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, None, Some(10), None, None, &[]);
    properties.extend_from_slice(&publish_properties(None, None, Some(20), None, None, &[]));
    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate message expiry interval")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_response_topic_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-response-topic"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, None, None, Some("devices/replies/1"), None, &[]);
    properties.extend_from_slice(&publish_properties(
        None,
        None,
        None,
        Some("devices/replies/2"),
        None,
        &[],
    ));
    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate response topic")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_correlation_data_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-correlation-data"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, None, None, None, Some(b"trace-1"), &[]);
    properties.extend_from_slice(&publish_properties(
        None,
        None,
        None,
        None,
        Some(b"trace-2"),
        &[],
    ));
    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate correlation data")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_disconnect_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-disconnect-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&disconnect_packet_v5_with_properties(
            &receive_maximum_property(10),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_disconnect_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-disconnect-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&disconnect_packet_v5_with_reason_and_properties(0x03, &[]))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_disconnect_reason_string_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-disconnect-reason-string"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .write_all(&disconnect_packet_v5_with_properties(&properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate disconnect reason string")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubrel_flags_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubrel-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut packet = pubrel_packet(7);
    packet[0] = 0x60;
    client.write_all(&packet).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubrel_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubrel-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pubrel_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_pubrel_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-pubrel-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .write_all(&pubrel_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate pubrel property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_puback_flags_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-puback-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let packet = vec![0x41, 0x02, 0x00, 0x01];
    client.write_all(&packet).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_puback_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-puback-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&puback_client_packet_v5_with_reason_code(7, 0x02))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_puback_properties_without_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("puback-properties-without-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet(PACKET_TYPE_PUBACK << 4, &body);
    client.write_all(&packet).await.unwrap();

    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubrec_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubrec-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pubrec_client_packet_v5_with_reason_code(7, 0x02))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_pubrec_properties_without_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("pubrec-properties-without-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet(PACKET_TYPE_PUBREC << 4, &body);
    client.write_all(&packet).await.unwrap();

    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubrec_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubrec-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pubrec_client_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_pubrec_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-pubrec-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .write_all(&pubrec_client_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubrel_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubrel-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pubrel_packet_v5_with_reason_code(7, 0x10))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_pubrel_properties_without_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("pubrel-properties-without-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet((PACKET_TYPE_PUBREL << 4) | 0b0010, &body);
    client.write_all(&packet).await.unwrap();

    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_reason_code_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubcomp-reason-code"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pubcomp_client_packet_v5_with_reason_code(7, 0x10))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubcomp reason code")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_ws_zero_length_client_id_receives_assigned_client_identifier_and_response_information(
) {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: false,
        enable_tls: false,
        enable_ws: true,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: None,
        max_packet_size: None,
        response_information: Some("greenmqtt://broker".to_string()),
        server_reference: None,
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5_with_properties(
            "",
            true,
            &request_response_information_property(true),
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&connack, &mut cursor).unwrap();
    let properties = parse_v5_connack_packet(&connack[cursor..]);
    let assigned_client_id = properties
        .assigned_client_identifier
        .expect("assigned client id must be present");
    assert!(assigned_client_id.starts_with("greenmqtt-"));
    assert_eq!(
        properties.response_information.as_deref(),
        Some("greenmqtt://broker")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_server_keep_alive_disconnects_idle_clients() {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: false,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: Some(1),
        max_packet_size: None,
        response_information: None,
        server_reference: None,
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_keep_alive("idle", true, 30, &[]))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let properties = parse_v5_connack_packet(&connack);
    assert_eq!(properties.server_keep_alive_secs, Some(1));

    sleep(Duration::from_millis(1700)).await;
    let read_result = tokio::time::timeout(Duration::from_secs(1), client.read_u8()).await;
    assert!(matches!(read_result, Ok(Err(_))));

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_packet_too_large_disconnect_includes_server_reference() {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: false,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: None,
        max_packet_size: None,
        response_information: None,
        server_reference: Some("greenmqtt://oversize-redirect".to_string()),
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "small-client-ref",
            true,
            &maximum_packet_size_property(48),
        ))
        .await
        .unwrap();
    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut connack = vec![0u8; connack_len];
    subscriber.read_exact(&mut connack).await.unwrap();

    subscriber
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut publisher = connect_tcp_with_retry(bind).await.unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", &[b'x'; 96]))
        .await
        .unwrap();

    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_DISCONNECT
    );
    let disconnect_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut disconnect = vec![0u8; disconnect_len];
    subscriber.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x95);
    assert_eq!(
        disconnect.server_reference.as_deref(),
        Some("greenmqtt://oversize-redirect")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_puback_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-puback-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&puback_client_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_puback_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-puback-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .write_all(&puback_client_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-pubcomp-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&pubcomp_client_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_pubcomp_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-pubcomp-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .write_all(&pubcomp_client_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_request_problem_information_false_suppresses_disconnect_reason_string() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    let mut properties = maximum_packet_size_property(48);
    properties.extend_from_slice(&request_problem_information_property(false));
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "small-client-no-problem-info",
            true,
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut connack = vec![0u8; connack_len];
    subscriber.read_exact(&mut connack).await.unwrap();

    subscriber
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", &[b'x'; 96]))
        .await
        .unwrap();

    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_DISCONNECT
    );
    let disconnect_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut disconnect = vec![0u8; disconnect_len];
    subscriber.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x95);
    assert_eq!(disconnect.reason_string, None);

    server.abort();
}
#[test]
fn mqtt_v5_disconnect_builder_encodes_server_reference() {
    let packet = build_disconnect_packet_with_server_reference(
        5,
        Some(0x82),
        Some("invalid subscription qos"),
        Some("greenmqtt://redirect-node"),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&packet[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.server_reference.as_deref(),
        Some("greenmqtt://redirect-node")
    );
}
