use super::*;
use crate::{ClientBalancer, Redirection};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct SessionRedirectBalancer {
    checks: AtomicUsize,
}

impl ClientBalancer for SessionRedirectBalancer {
    fn need_redirect(&self, _identity: &ClientIdentity) -> Option<Redirection> {
        if self.checks.fetch_add(1, Ordering::Relaxed) == 0 {
            return None;
        }
        Some(Redirection {
            permanent: false,
            server_reference: "mqtt://127.0.0.1:3883".into(),
        })
    }
}

#[tokio::test]
async fn mqtt_tcp_persistent_replay_on_reconnect() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_with_clean_start("sub", true))
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
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    subscriber.write_all(&disconnect_packet()).await.unwrap();
    drop(subscriber);

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", b"offline"))
        .await
        .unwrap();

    let mut resumed = connect_tcp_with_retry(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_with_clean_start("sub", false))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x01);

    let header = resumed.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut resumed).await;
    let mut publish = vec![0u8; publish_len];
    resumed.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(7).any(|window| window == b"offline"));

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_session_balancer_recheck_disconnects_with_server_reference() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_client_balancer(Arc::new(SessionRedirectBalancer::default()));
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("session-redirect"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x9C);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("use another server")
    );
    assert_eq!(
        disconnect.server_reference.as_deref(),
        Some("mqtt://127.0.0.1:3883")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_session_expiry_zero_is_transient() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &session_expiry_interval_property(0),
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
        .write_all(&subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(3),
        ))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    subscriber.write_all(&disconnect_packet()).await.unwrap();
    drop(subscriber);

    let mut publisher = connect_tcp_with_retry(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            2,
            "devices/d1/state",
            b"offline-v5",
            &user_property("trace", "session-expiry-zero"),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let mut resumed = TcpStream::connect(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            false,
            &session_expiry_interval_property(0),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x00);

    let timed_out = tokio::time::timeout(Duration::from_millis(200), resumed.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_persistent_reconnect_sets_connack_session_present() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut first = connect_tcp_with_retry(bind).await.unwrap();
    first
        .write_all(&connect_packet_v5("persist"))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let first_connack_len = read_remaining_length_for_test(&mut first).await;
    let mut first_connack = vec![0u8; first_connack_len];
    first.read_exact(&mut first_connack).await.unwrap();
    let first_connack = parse_v5_connack_packet(&first_connack);
    assert!(!first_connack.session_present);

    first.write_all(&disconnect_packet()).await.unwrap();
    drop(first);

    let mut resumed = connect_tcp_with_retry(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5_with_properties("persist", false, &[]))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    let resumed_connack = parse_v5_connack_packet(&resumed_connack);
    assert!(resumed_connack.session_present);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_session_takeover_disconnects_stale_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let properties = session_expiry_interval_property(60);

    let mut first = connect_tcp_with_retry(bind).await.unwrap();
    first
        .write_all(&connect_packet_v5_with_properties(
            "takeover",
            false,
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let first_connack_len = read_remaining_length_for_test(&mut first).await;
    let mut first_connack = vec![0u8; first_connack_len];
    first.read_exact(&mut first_connack).await.unwrap();
    let first_connack = parse_v5_connack_packet(&first_connack);
    assert!(!first_connack.session_present);

    let mut second = connect_tcp_with_retry(bind).await.unwrap();
    second
        .write_all(&connect_packet_v5_with_properties(
            "takeover",
            false,
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(second.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let second_connack_len = read_remaining_length_for_test(&mut second).await;
    let mut second_connack = vec![0u8; second_connack_len];
    second.read_exact(&mut second_connack).await.unwrap();
    let second_connack = parse_v5_connack_packet(&second_connack);
    assert!(second_connack.session_present);

    first.write_all(&pingreq_packet()).await.unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut first).await;
    let mut disconnect = vec![0u8; disconnect_len];
    first.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x8E);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("session taken over")
    );

    second.write_all(&pingreq_packet()).await.unwrap();
    assert_eq!(second.read_u8().await.unwrap() >> 4, PACKET_TYPE_PINGRESP);
    let pingresp_len = read_remaining_length_for_test(&mut second).await;
    assert_eq!(pingresp_len, 0);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_clean_start_recreates_session_without_offline_replay() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let properties = session_expiry_interval_property(60);

    let mut first = connect_tcp_with_retry(bind).await.unwrap();
    first
        .write_all(&connect_packet_v5_with_properties(
            "clean",
            true,
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let first_connack_len = read_remaining_length_for_test(&mut first).await;
    let mut first_connack = vec![0u8; first_connack_len];
    first.read_exact(&mut first_connack).await.unwrap();
    let first_connack = parse_v5_connack_packet(&first_connack);
    assert!(!first_connack.session_present);

    first
        .write_all(&subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(7),
        ))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut first).await;
    let mut suback = vec![0u8; suback_len];
    first.read_exact(&mut suback).await.unwrap();

    first.write_all(&disconnect_packet()).await.unwrap();
    drop(first);

    let mut publisher = connect_tcp_with_retry(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            11,
            "devices/d1/state",
            b"stale-offline",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let mut clean = connect_tcp_with_retry(bind).await.unwrap();
    clean
        .write_all(&connect_packet_v5_with_properties(
            "clean",
            true,
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(clean.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let clean_connack_len = read_remaining_length_for_test(&mut clean).await;
    let mut clean_connack = vec![0u8; clean_connack_len];
    clean.read_exact(&mut clean_connack).await.unwrap();
    let clean_connack = parse_v5_connack_packet(&clean_connack);
    assert!(!clean_connack.session_present);

    let timed_out = tokio::time::timeout(Duration::from_millis(200), clean.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_session_expiry_interval_expires_persistent_session() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &session_expiry_interval_property(1),
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
        .write_all(&subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(3),
        ))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    subscriber.write_all(&disconnect_packet()).await.unwrap();
    drop(subscriber);
    sleep(Duration::from_millis(1_200)).await;

    let mut publisher = connect_tcp_with_retry(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            2,
            "devices/d1/state",
            b"expired-session",
            &user_property("trace", "session-expired"),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let mut resumed = connect_tcp_with_retry(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            false,
            &session_expiry_interval_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x00);

    let timed_out = tokio::time::timeout(Duration::from_millis(200), resumed.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_disconnect_session_expiry_zero_purges_session_state() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &session_expiry_interval_property(60),
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
        .write_all(&subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(3),
        ))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    subscriber
        .write_all(&disconnect_packet_v5_with_properties(
            &session_expiry_interval_property(0),
        ))
        .await
        .unwrap();
    drop(subscriber);

    let mut publisher = connect_tcp_with_retry(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            2,
            "devices/d1/state",
            b"post-disconnect-zero-expiry",
            &user_property("trace", "disconnect-session-expiry-zero"),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let mut resumed = connect_tcp_with_retry(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            false,
            &session_expiry_interval_property(60),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x00);

    let timed_out = tokio::time::timeout(Duration::from_millis(200), resumed.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_ungraceful_disconnect_replays_on_reconnect() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_with_clean_start("sub", true))
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
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", b"offline-abrupt"))
        .await
        .unwrap();

    let mut resumed = TcpStream::connect(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_with_clean_start("sub", false))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x01);

    let header = resumed.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut resumed).await;
    let mut publish = vec![0u8; publish_len];
    resumed.read_exact(&mut publish).await.unwrap();
    assert!(publish
        .windows(14)
        .any(|window| window == b"offline-abrupt"));

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_ungraceful_disconnect_publishes_will() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber.write_all(&connect_packet("sub")).await.unwrap();
    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut connack = vec![0u8; connack_len];
    subscriber.read_exact(&mut connack).await.unwrap();
    subscriber
        .write_all(&subscribe_packet(1, "clients/+/status"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut will_client = connect_tcp_with_retry(bind).await.unwrap();
    will_client
        .write_all(&connect_packet_with_will(
            "will-client",
            "clients/will-client/status",
            b"offline",
            1,
            false,
        ))
        .await
        .unwrap();
    assert_eq!(
        will_client.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut will_client).await;
    let mut connack = vec![0u8; connack_len];
    will_client.read_exact(&mut connack).await.unwrap();
    drop(will_client);

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(7).any(|window| window == b"offline"));

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_disconnect_suppresses_will() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber.write_all(&connect_packet("sub")).await.unwrap();
    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut connack = vec![0u8; connack_len];
    subscriber.read_exact(&mut connack).await.unwrap();
    subscriber
        .write_all(&subscribe_packet(1, "clients/+/status"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut will_client = connect_tcp_with_retry(bind).await.unwrap();
    will_client
        .write_all(&connect_packet_with_will(
            "will-client",
            "clients/will-client/status",
            b"offline",
            1,
            false,
        ))
        .await
        .unwrap();
    assert_eq!(
        will_client.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut will_client).await;
    let mut connack = vec![0u8; connack_len];
    will_client.read_exact(&mut connack).await.unwrap();
    will_client.write_all(&disconnect_packet()).await.unwrap();

    let timed_out = tokio::time::timeout(Duration::from_millis(200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_ungraceful_disconnect_publishes_will_with_properties() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5("sub"))
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
        .write_all(&subscribe_packet_v5(1, "clients/+/status"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut will_client = TcpStream::connect(bind).await.unwrap();
    let will_properties = publish_properties(
        Some(1),
        Some("application/json"),
        Some(5),
        Some("replies/will-client"),
        Some(b"corr-will"),
        &[("origin", "will"), ("trace", "disconnect")],
    );
    will_client
        .write_all(&connect_packet_v5_with_will(
            "will-client",
            "clients/will-client/status",
            b"{\"status\":\"offline\"}",
            1,
            false,
            &will_properties,
        ))
        .await
        .unwrap();
    assert_eq!(
        will_client.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut will_client).await;
    let mut connack = vec![0u8; connack_len];
    will_client.read_exact(&mut connack).await.unwrap();
    drop(will_client);

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    let (topic, properties, payload) = parse_v5_publish_packet(&publish);
    assert_eq!(topic, "clients/will-client/status");
    assert_eq!(payload, br#"{"status":"offline"}"#);
    assert_eq!(properties.payload_format_indicator, Some(1));
    assert_eq!(properties.content_type.as_deref(), Some("application/json"));
    assert!(matches!(
        properties.message_expiry_interval_secs,
        Some(value) if (1..=5).contains(&value)
    ));
    assert_eq!(
        properties.response_topic.as_deref(),
        Some("replies/will-client")
    );
    assert_eq!(
        properties.correlation_data.as_deref(),
        Some(&b"corr-will"[..])
    );
    assert_eq!(properties.user_properties.len(), 2);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_will_delay_interval_delays_ungraceful_will_publish() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5("sub"))
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
        .write_all(&subscribe_packet_v5(1, "clients/+/status"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut will_client = TcpStream::connect(bind).await.unwrap();
    let mut will_properties = will_delay_interval_property(1);
    will_properties.extend_from_slice(&publish_properties(
        Some(1),
        None,
        None,
        None,
        None,
        &[("origin", "delayed-will")],
    ));
    will_client
        .write_all(&connect_packet_v5_with_will(
            "delay-will-client",
            "clients/delay-will-client/status",
            b"offline",
            1,
            false,
            &will_properties,
        ))
        .await
        .unwrap();
    assert_eq!(
        will_client.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut will_client).await;
    let mut connack = vec![0u8; connack_len];
    will_client.read_exact(&mut connack).await.unwrap();
    drop(will_client);

    let early = tokio::time::timeout(Duration::from_millis(200), subscriber.read_u8()).await;
    assert!(early.is_err());

    let header = tokio::time::timeout(Duration::from_secs(2), subscriber.read_u8())
        .await
        .expect("delayed will should publish")
        .unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(7).any(|window| window == b"offline"));

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reconnect_before_will_delay_interval_suppresses_will() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5("sub"))
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
        .write_all(&subscribe_packet_v5(1, "clients/+/status"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut will_client = TcpStream::connect(bind).await.unwrap();
    will_client
        .write_all(&connect_packet_v5_with_will(
            "delay-will-client",
            "clients/delay-will-client/status",
            b"offline",
            1,
            false,
            &will_delay_interval_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(
        will_client.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut will_client).await;
    let mut connack = vec![0u8; connack_len];
    will_client.read_exact(&mut connack).await.unwrap();
    drop(will_client);

    sleep(Duration::from_millis(200)).await;
    let mut resumed = connect_tcp_with_retry(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5("delay-will-client"))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut connack = vec![0u8; connack_len];
    resumed.read_exact(&mut connack).await.unwrap();
    resumed.write_all(&disconnect_packet()).await.unwrap();

    let timed_out = tokio::time::timeout(Duration::from_millis(1200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_graceful_disconnect_before_will_delay_interval_suppresses_will() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5("sub"))
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
        .write_all(&subscribe_packet_v5(1, "clients/+/status"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut will_client = TcpStream::connect(bind).await.unwrap();
    will_client
        .write_all(&connect_packet_v5_with_will(
            "delay-will-graceful",
            "clients/delay-will-graceful/status",
            b"offline",
            1,
            false,
            &will_delay_interval_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(
        will_client.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut will_client).await;
    let mut connack = vec![0u8; connack_len];
    will_client.read_exact(&mut connack).await.unwrap();
    will_client.write_all(&disconnect_packet()).await.unwrap();

    let timed_out = tokio::time::timeout(Duration::from_millis(1200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_qos2_delivery_replays_after_reconnect() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_with_clean_start("sub", true))
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
        .write_all(&subscribe_packet(1, "devices/+/state"))
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
        .write_all(&publish_packet_qos2(7, "devices/d1/state", b"replay-qos2"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREC);
    let pubrec_len = read_remaining_length_for_test(&mut publisher).await;
    let mut pubrec = vec![0u8; pubrec_len];
    publisher.read_exact(&mut pubrec).await.unwrap();
    publisher.write_all(&pubrel_packet(7)).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBCOMP);
    let pubcomp_len = read_remaining_length_for_test(&mut publisher).await;
    let mut pubcomp = vec![0u8; pubcomp_len];
    publisher.read_exact(&mut pubcomp).await.unwrap();

    let publish_header = subscriber.read_u8().await.unwrap();
    assert_eq!(publish_header >> 4, PACKET_TYPE_PUBLISH);
    assert_eq!((publish_header >> 1) & 0b11, 2);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(11).any(|window| window == b"replay-qos2"));
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;

    let mut resumed = TcpStream::connect(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_with_clean_start("sub", false))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x01);

    let publish_header = resumed.read_u8().await.unwrap();
    assert_eq!(publish_header >> 4, PACKET_TYPE_PUBLISH);
    assert_eq!((publish_header >> 1) & 0b11, 2);
    let publish_len = read_remaining_length_for_test(&mut resumed).await;
    let mut publish = vec![0u8; publish_len];
    resumed.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(11).any(|window| window == b"replay-qos2"));

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_transient_session_does_not_replay() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_with_username(
            "sub",
            Some("tenant-a:alice"),
            Some(b"transient"),
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
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();
    subscriber.write_all(&disconnect_packet()).await.unwrap();
    drop(subscriber);

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_with_username(
            "pub",
            Some("tenant-a:bob"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", b"no-replay"))
        .await
        .unwrap();

    let mut resumed = TcpStream::connect(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_with_username(
            "sub",
            Some("tenant-a:alice"),
            Some(b"transient"),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    let timed_out = tokio::time::timeout(Duration::from_millis(200), resumed.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_reauth_can_restart_after_reconnect() {
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

    let mut first = connect_tcp_with_retry(bind).await.unwrap();
    first
        .write_all(&connect_packet_v5("reauth-reconnect"))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut first).await;
    let mut connack = vec![0u8; connack_len];
    first.read_exact(&mut connack).await.unwrap();
    first.write_all(&disconnect_packet()).await.unwrap();
    drop(first);
    sleep(Duration::from_millis(100)).await;

    let mut resumed = connect_tcp_with_retry(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5("reauth-reconnect"))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut connack = vec![0u8; connack_len];
    resumed.read_exact(&mut connack).await.unwrap();

    resumed
        .write_all(&auth_packet_v5(Some("custom"), Some(b"client-hello")))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut resumed).await;
    let mut auth = vec![0u8; auth_len];
    resumed.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x18);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert_eq!(auth.auth_data.as_deref(), Some(&b"server-challenge"[..]));

    resumed
        .write_all(&auth_packet_v5_with_reason(
            0x00,
            Some("custom"),
            Some(b"client-response"),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_AUTH);
    let auth_len = read_remaining_length_for_test(&mut resumed).await;
    let mut auth = vec![0u8; auth_len];
    resumed.read_exact(&mut auth).await.unwrap();
    let auth = parse_v5_auth_packet(&auth);
    assert_eq!(auth.reason_code, 0x00);
    assert_eq!(auth.auth_method.as_deref(), Some("custom"));
    assert!(auth.auth_data.is_none());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_session_expiry_interval_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = session_expiry_interval_property(30);
    properties.extend_from_slice(&session_expiry_interval_property(60));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "duplicate-session-expiry-interval",
            true,
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let connack = parse_v5_connack_packet(&connack);
    assert_eq!(connack.reason_code, 0x82);
    assert_eq!(
        connack.reason_string.as_deref(),
        Some("duplicate session expiry interval")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_will_qos_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut packet = connect_packet_v5("invalid-will-qos");
    packet[9] = 0b0001_1010;

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
async fn mqtt_v5_duplicate_disconnect_session_expiry_interval_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-disconnect-session-expiry"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = session_expiry_interval_property(30);
    properties.extend_from_slice(&session_expiry_interval_property(60));
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
        Some("duplicate disconnect session expiry interval")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_assigned_client_identifier_can_resume_persistent_session() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut first = TcpStream::connect(bind).await.unwrap();
    first.write_all(&connect_packet_v5("")).await.unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut first).await;
    let mut connack = vec![0u8; connack_len];
    first.read_exact(&mut connack).await.unwrap();
    let assigned_client_id = parse_v5_connack_packet(&connack)
        .assigned_client_identifier
        .expect("assigned client id must be present");

    first
        .write_all(&subscribe_packet_v5(1, "devices/assigned"))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut first).await;
    let mut suback = vec![0u8; suback_len];
    first.read_exact(&mut suback).await.unwrap();
    first.write_all(&disconnect_packet()).await.unwrap();

    let mut resumed = TcpStream::connect(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_v5_with_properties(
            &assigned_client_id,
            false,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();
    assert_eq!(resumed_connack[0] & 0x01, 0x01);
    let resumed_properties = parse_v5_connack_packet(&resumed_connack);
    assert_eq!(resumed_properties.assigned_client_identifier, None);

    server.abort();
}
