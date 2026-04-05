use super::*;
use crate::mqtt::session::{SessionTransport, TcpTransport};
use std::time::Instant;
use tokio::io::{duplex, AsyncReadExt};

#[tokio::test]
async fn mqtt_tcp_connect_subscribe_publish_flow() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

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
        .write_all(&subscribe_packet(1, "devices/+/state"))
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
        .write_all(&publish_packet("devices/d1/state", b"up"))
        .await
        .unwrap();

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(2).any(|window| window == b"up"));

    server.abort();
}
#[tokio::test]
#[ignore = "debounce network path is covered by stable unit tests; local tcp startup is flaky"]
async fn mqtt_v5_connect_debounce_returns_server_busy_connack() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_connect_debounce_window(Duration::from_secs(1));
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut first = connect_tcp_with_retry(bind).await.unwrap();
    first
        .write_all(&connect_packet_v5("same-client"))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let first_connack_len = read_remaining_length_for_test(&mut first).await;
    let mut first_connack = vec![0u8; first_connack_len];
    first.read_exact(&mut first_connack).await.unwrap();

    let mut second = connect_tcp_with_retry(bind).await.unwrap();
    second
        .write_all(&connect_packet_v5("same-client"))
        .await
        .unwrap();
    assert_eq!(second.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let second_connack_len = read_remaining_length_for_test(&mut second).await;
    let mut second_connack = vec![0u8; second_connack_len];
    second.read_exact(&mut second_connack).await.unwrap();
    let parsed = parse_v5_connack_packet(&second_connack);
    assert_eq!(parsed.reason_code, 0x89);
    assert_eq!(
        parsed.reason_string.as_deref(),
        Some("connect debounce active")
    );
    let _still_open = &first;

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_connect_subscribe_publish_flow() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
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
    assert_eq!(connack[1], 0);
    let connack_properties = parse_v5_connack_packet(&connack);
    assert!(connack_properties.retain_available);
    assert!(connack_properties.wildcard_subscription_available);
    assert!(connack_properties.subscription_identifiers_available);
    assert!(connack_properties.shared_subscription_available);

    subscriber
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();
    assert_eq!(suback[2], 0);

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
        .write_all(&publish_packet_v5_qos1(9, "devices/d1/state", b"v5-up"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();
    assert_eq!(puback[2], 0);

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(5).any(|window| window == b"v5-up"));

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_topic_alias_publish_flow() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &user_property("role", "subscriber"),
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
            &subscription_identifier_property(7),
        ))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5_with_properties(
            "pub",
            true,
            &user_property("role", "publisher"),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();

    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            9,
            "devices/d1/state",
            b"first",
            &topic_alias_property(11),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            10,
            "",
            b"second",
            &topic_alias_and_user_property(11, "trace", "alias-reuse"),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    let (_, properties, payload) = parse_v5_publish_packet(&publish);
    assert_eq!(properties.subscription_identifiers, vec![7]);
    assert_eq!(payload, b"first".to_vec());

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    let (_, properties, payload) = parse_v5_publish_packet(&publish);
    assert_eq!(properties.subscription_identifiers, vec![7]);
    assert_eq!(payload, b"second".to_vec());

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_topic_alias_zero_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("alias-zero"))
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
            b"bad-alias-zero",
            &topic_alias_property(0),
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
        Some("invalid topic alias")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_topic_alias_zero_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(50)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-alias-zero"))
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
            b"bad-alias-zero",
            &topic_alias_property(0),
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
        Some("invalid topic alias")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unknown_topic_alias_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("alias-unknown"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "",
            b"bad-alias-unknown",
            &topic_alias_property(11),
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
        Some("invalid topic alias")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unknown_topic_alias_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(50)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-alias-unknown"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            1,
            "",
            b"bad-alias-unknown",
            &topic_alias_property(11),
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
        Some("invalid topic alias")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_properties_are_delivered() {
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
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();

    let properties = publish_properties(
        Some(1),
        Some("application/json"),
        Some(5),
        Some("replies/d1"),
        Some(b"corr-1"),
        &[("trace", "abc"), ("source", "integration-test")],
    );
    publisher
        .write_all(&publish_packet_v5_qos1_with_properties(
            12,
            "devices/d1/state",
            b"props-up",
            &properties,
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    let (topic, properties, payload) = parse_v5_publish_packet(&publish);
    assert_eq!(topic, "devices/d1/state");
    assert_eq!(payload, b"props-up");
    assert_eq!(properties.payload_format_indicator, Some(1));
    assert_eq!(properties.content_type.as_deref(), Some("application/json"));
    assert!(matches!(
        properties.message_expiry_interval_secs,
        Some(value) if (1..=5).contains(&value)
    ));
    assert_eq!(properties.response_topic.as_deref(), Some("replies/d1"));
    assert_eq!(properties.correlation_data.as_deref(), Some(&b"corr-1"[..]));
    assert_eq!(properties.user_properties.len(), 2);
    assert_eq!(properties.user_properties[0].key, "trace");
    assert_eq!(properties.user_properties[0].value, "abc");
    assert_eq!(properties.user_properties[1].key, "source");
    assert_eq!(properties.user_properties[1].value, "integration-test");

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_receive_maximum_limits_inflight_deliveries() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &receive_maximum_property(1),
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
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();

    publisher
        .write_all(&publish_packet_v5_qos1(21, "devices/d1/state", b"first"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    publisher
        .write_all(&publish_packet_v5_qos1(22, "devices/d2/state", b"second"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    let packet_id = parse_outbound_publish_packet_id(&publish);
    assert!(publish.windows(5).any(|window| window == b"first"));

    let timed_out = tokio::time::timeout(Duration::from_millis(200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());

    subscriber
        .write_all(&puback_client_packet(packet_id))
        .await
        .unwrap();

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(6).any(|window| window == b"second"));

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_receive_maximum_limits_offline_replay_deliveries() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &receive_maximum_property(1),
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
    subscriber.write_all(&disconnect_packet()).await.unwrap();
    drop(subscriber);

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();

    publisher
        .write_all(&publish_packet_v5_qos1(31, "devices/d1/state", b"first"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();

    publisher
        .write_all(&publish_packet_v5_qos1(32, "devices/d2/state", b"second"))
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
            &receive_maximum_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();

    let header = resumed.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut resumed).await;
    let mut publish = vec![0u8; publish_len];
    resumed.read_exact(&mut publish).await.unwrap();
    let packet_id = parse_outbound_publish_packet_id(&publish);
    assert!(publish.windows(5).any(|window| window == b"first"));

    let timed_out = tokio::time::timeout(Duration::from_millis(200), resumed.read_u8()).await;
    assert!(timed_out.is_err());

    resumed
        .write_all(&[0x40, 0x02, (packet_id >> 8) as u8, packet_id as u8])
        .await
        .unwrap();

    let header = resumed.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut resumed).await;
    let mut publish = vec![0u8; publish_len];
    resumed.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(6).any(|window| window == b"second"));

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_offline_replay_uses_batch_inflight_stage() {
    let inflight_store = Arc::new(CountingReplayInflightStore::default());
    let broker = test_broker_with_inflight_store(
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
        inflight_store.clone(),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = connect_tcp_with_retry(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "sub",
            true,
            &receive_maximum_property(1),
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
        .write_all(&publish_packet_v5_qos1(41, "devices/d1/state", b"first"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_qos1(42, "devices/d2/state", b"second"))
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
            &receive_maximum_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let resumed_connack_len = read_remaining_length_for_test(&mut resumed).await;
    let mut resumed_connack = vec![0u8; resumed_connack_len];
    resumed.read_exact(&mut resumed_connack).await.unwrap();

    let header = resumed.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut resumed).await;
    let mut publish = vec![0u8; publish_len];
    resumed.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(5).any(|window| window == b"first"));

    assert_eq!(inflight_store.save_inflight_batch_calls(), 1);
    assert_eq!(inflight_store.save_inflight_calls(), 0);

    server.abort();
}
#[test]
fn mqtt_v5_subscribe_retain_options_are_parsed() {
    let packet = subscribe_packet_v5_with_options(1, "devices/+/state", 0b0001_1001, &[]);
    let parsed = parse_packet_frame(&packet, 5).unwrap();
    let Packet::Subscribe(subscribe) = parsed else {
        panic!("expected subscribe packet");
    };
    assert!(subscribe.subscriptions[0].retain_as_published);
    assert_eq!(subscribe.subscriptions[0].retain_handling, 1);
}
#[tokio::test]
async fn mqtt_v5_subscribe_replays_retained_with_rap_flag() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_retain("devices/d1/state", b"retained"))
        .await
        .unwrap();

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
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_0001,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();
    let publish_header = tokio::time::timeout(Duration::from_secs(1), subscriber.read_u8())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(publish_header >> 4, PACKET_TYPE_PUBLISH);
    assert_eq!(publish_header & 0b0001, 0);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(8).any(|window| window == b"retained"));

    let mut rap_subscriber = TcpStream::connect(bind).await.unwrap();
    rap_subscriber
        .write_all(&connect_packet_v5("sub-rap"))
        .await
        .unwrap();
    assert_eq!(
        rap_subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut rap_subscriber).await;
    let mut connack = vec![0u8; connack_len];
    rap_subscriber.read_exact(&mut connack).await.unwrap();
    rap_subscriber
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_1001,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(
        rap_subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_SUBACK
    );
    let suback_len = read_remaining_length_for_test(&mut rap_subscriber).await;
    let mut suback = vec![0u8; suback_len];
    rap_subscriber.read_exact(&mut suback).await.unwrap();
    let publish_header = tokio::time::timeout(Duration::from_secs(1), rap_subscriber.read_u8())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(publish_header >> 4, PACKET_TYPE_PUBLISH);
    assert_eq!(publish_header & 0b0001, 1);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_retain_handling_two_skips_retained_replay() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher
        .write_all(&connect_packet_v5("pub"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_v5_retain("devices/d1/state", b"retained"))
        .await
        .unwrap();

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
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0010_0001,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();
    let timed_out = tokio::time::timeout(Duration::from_millis(200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_unsubscribe_stops_delivery() {
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
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut suback = vec![0u8; suback_len];
    subscriber.read_exact(&mut suback).await.unwrap();
    subscriber
        .write_all(&unsubscribe_packet(2, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(
        subscriber.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_UNSUBACK
    );
    let unsuback_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut unsuback = vec![0u8; unsuback_len];
    subscriber.read_exact(&mut unsuback).await.unwrap();
    assert!(parse_unsuback_return_codes(&unsuback, 4).is_empty());

    let mut publisher = TcpStream::connect(bind).await.unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", b"up"))
        .await
        .unwrap();

    let timed_out = tokio::time::timeout(Duration::from_millis(200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());
    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_shared_subscription_delivers_to_one_member() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut first = TcpStream::connect(bind).await.unwrap();
    first
        .write_all(&connect_packet_with_username(
            "worker-a",
            Some("tenant-a:alice"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let len = read_remaining_length_for_test(&mut first).await;
    let mut buf = vec![0u8; len];
    first.read_exact(&mut buf).await.unwrap();
    first
        .write_all(&subscribe_packet(1, "$share/workers/devices/+/state"))
        .await
        .unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let len = read_remaining_length_for_test(&mut first).await;
    let mut buf = vec![0u8; len];
    first.read_exact(&mut buf).await.unwrap();

    let mut second = TcpStream::connect(bind).await.unwrap();
    second
        .write_all(&connect_packet_with_username(
            "worker-b",
            Some("tenant-a:alice"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(second.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let len = read_remaining_length_for_test(&mut second).await;
    let mut buf = vec![0u8; len];
    second.read_exact(&mut buf).await.unwrap();
    second
        .write_all(&subscribe_packet(1, "$share/workers/devices/+/state"))
        .await
        .unwrap();
    assert_eq!(second.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let len = read_remaining_length_for_test(&mut second).await;
    let mut buf = vec![0u8; len];
    second.read_exact(&mut buf).await.unwrap();

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
        .write_all(&publish_packet("devices/d1/state", b"shared"))
        .await
        .unwrap();

    let first_recv = tokio::time::timeout(Duration::from_millis(300), first.read_u8())
        .await
        .ok();
    let second_recv = tokio::time::timeout(Duration::from_millis(300), second.read_u8())
        .await
        .ok();
    assert!(first_recv.is_some() ^ second_recv.is_some());
    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_qos2_publish_handshake() {
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
        .write_all(&publish_packet_qos2(7, "devices/d1/state", b"qos2"))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREC);
    let pubrec_len = read_remaining_length_for_test(&mut publisher).await;
    let mut pubrec = vec![0u8; pubrec_len];
    publisher.read_exact(&mut pubrec).await.unwrap();
    assert_eq!(&pubrec[..2], &7u16.to_be_bytes());

    publisher.write_all(&pubrel_packet(7)).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBCOMP);
    let pubcomp_len = read_remaining_length_for_test(&mut publisher).await;
    let mut pubcomp = vec![0u8; pubcomp_len];
    publisher.read_exact(&mut pubcomp).await.unwrap();
    assert_eq!(&pubcomp[..2], &7u16.to_be_bytes());

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(4).any(|window| window == b"qos2"));

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_pending_inbound_qos2_limit_disconnects_quota_exceeded() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("qos2-budget"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    for packet_id in 1..=crate::mqtt::session::MAX_PENDING_INBOUND_QOS2 as u16 {
        client
            .write_all(&publish_packet_v5_qos2(
                packet_id,
                "devices/d1/state",
                b"budget-test",
            ))
            .await
            .unwrap();
        assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREC);
        let pubrec_len = read_remaining_length_for_test(&mut client).await;
        let mut pubrec = vec![0u8; pubrec_len];
        client.read_exact(&mut pubrec).await.unwrap();
    }

    client
        .write_all(&publish_packet_v5_qos2(
            (crate::mqtt::session::MAX_PENDING_INBOUND_QOS2 + 1) as u16,
            "devices/d1/state",
            b"budget-overflow",
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x97);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("too many pending qos2 publishes")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_ws_pending_inbound_qos2_limit_disconnects_quota_exceeded() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary((connect_packet_v5("qos2-budget-ws")).into()))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    for packet_id in 1..=crate::mqtt::session::MAX_PENDING_INBOUND_QOS2 as u16 {
        client
            .send(Message::Binary((publish_packet_v5_qos2(
                packet_id,
                "devices/d1/state",
                b"budget-test",
            )).into()))
            .await
            .unwrap();
        let pubrec = client.next().await.unwrap().unwrap().into_data();
        assert_eq!(pubrec[0] >> 4, PACKET_TYPE_PUBREC);
    }

    client
        .send(Message::Binary((publish_packet_v5_qos2(
            (crate::mqtt::session::MAX_PENDING_INBOUND_QOS2 + 1) as u16,
            "devices/d1/state",
            b"budget-overflow",
        )).into()))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x97);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("too many pending qos2 publishes")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_qos2_delivery_handshake() {
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
        .write_all(&publish_packet_qos2(
            7,
            "devices/d1/state",
            b"downlink-qos2",
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREC);
    let pubrec_len = read_remaining_length_for_test(&mut publisher).await;
    let mut pubrec = vec![0u8; pubrec_len];
    publisher.read_exact(&mut pubrec).await.unwrap();
    assert_eq!(&pubrec[..2], &7u16.to_be_bytes());

    publisher.write_all(&pubrel_packet(7)).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBCOMP);
    let pubcomp_len = read_remaining_length_for_test(&mut publisher).await;
    let mut pubcomp = vec![0u8; pubcomp_len];
    publisher.read_exact(&mut pubcomp).await.unwrap();
    assert_eq!(&pubcomp[..2], &7u16.to_be_bytes());

    let publish_header = subscriber.read_u8().await.unwrap();
    assert_eq!(publish_header >> 4, PACKET_TYPE_PUBLISH);
    assert_eq!((publish_header >> 1) & 0b11, 2);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    let packet_id = parse_outbound_publish_packet_id(&publish);
    assert!(publish.windows(13).any(|window| window == b"downlink-qos2"));

    subscriber
        .write_all(&pubrec_client_packet(packet_id))
        .await
        .unwrap();
    assert_eq!(subscriber.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREL);
    let pubrel_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut pubrel = vec![0u8; pubrel_len];
    subscriber.read_exact(&mut pubrel).await.unwrap();
    assert_eq!(&pubrel[..2], &packet_id.to_be_bytes());

    subscriber
        .write_all(&pubcomp_client_packet(packet_id))
        .await
        .unwrap();
    let timed_out = tokio::time::timeout(Duration::from_millis(200), subscriber.read_u8()).await;
    assert!(timed_out.is_err());

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_qos2_inflight_replays_after_broker_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let bind1 = next_test_bind();
    let bind2 = next_test_bind();

    let broker1 = persistent_test_broker(&data_dir).await;
    let server1 = tokio::spawn(serve_tcp(broker1.clone(), bind1));
    sleep(Duration::from_millis(50)).await;

    let mut subscriber = TcpStream::connect(bind1).await.unwrap();
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

    let mut publisher = TcpStream::connect(bind1).await.unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet_qos2(7, "devices/d1/state", b"restart-qos2"))
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
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(12).any(|window| window == b"restart-qos2"));
    drop(subscriber);
    server1.abort();
    let _ = server1.await;
    drop(publisher);
    drop(broker1);
    sleep(Duration::from_millis(100)).await;

    let broker2 = persistent_test_broker(&data_dir).await;
    let server2 = tokio::spawn(serve_tcp(broker2.clone(), bind2));
    sleep(Duration::from_millis(50)).await;

    let mut resumed = TcpStream::connect(bind2).await.unwrap();
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
    assert!(publish.windows(12).any(|window| window == b"restart-qos2"));

    server2.abort();
}
#[tokio::test]
async fn mqtt_tls_connect_subscribe_publish_flow() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();

    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let subscriber_stream = TcpStream::connect(bind).await.unwrap();
    let mut subscriber = connector
        .connect(
            ServerName::try_from("localhost").unwrap(),
            subscriber_stream,
        )
        .await
        .unwrap();
    subscriber.write_all(&connect_packet("sub")).await.unwrap();
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

    let publisher_stream = TcpStream::connect(bind).await.unwrap();
    let mut publisher = connector
        .connect(ServerName::try_from("localhost").unwrap(), publisher_stream)
        .await
        .unwrap();
    publisher.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher.read_exact(&mut publisher_connack).await.unwrap();
    publisher
        .write_all(&publish_packet("devices/d1/state", b"secure"))
        .await
        .unwrap();

    let header = subscriber.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber).await;
    let mut publish = vec![0u8; publish_len];
    subscriber.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(6).any(|window| window == b"secure"));

    server.abort();
}
#[tokio::test]
async fn mqtt_ws_connect_subscribe_publish_flow() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut subscriber = connect_ws_with_retry(&ws_url).await.unwrap();
    subscriber
        .send(Message::Binary((connect_packet("sub")).into()))
        .await
        .unwrap();
    let connack = subscriber.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    subscriber
        .send(Message::Binary((subscribe_packet(1, "devices/+/state")).into()))
        .await
        .unwrap();
    let suback = subscriber.next().await.unwrap().unwrap().into_data();
    assert_eq!(suback[0] >> 4, PACKET_TYPE_SUBACK);

    let mut publisher = connect_ws_with_retry(&ws_url).await.unwrap();
    publisher
        .send(Message::Binary((connect_packet("pub")).into()))
        .await
        .unwrap();
    let publisher_connack = publisher.next().await.unwrap().unwrap().into_data();
    assert_eq!(publisher_connack[0] >> 4, PACKET_TYPE_CONNACK);
    publisher
        .send(Message::Binary((publish_packet(
            "devices/d1/state",
            b"ws-up",
        )).into()))
        .await
        .unwrap();

    let publish = subscriber.next().await.unwrap().unwrap().into_data();
    assert_eq!(publish[0] >> 4, PACKET_TYPE_PUBLISH);
    assert!(publish.windows(5).any(|window| window == b"ws-up"));

    server.abort();
}
#[tokio::test]
async fn mqtt_publish_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&publish_packet("devices/d1/state", b"up"))
        .await
        .unwrap();
    let error = client
        .read_u8()
        .await
        .expect_err("expected connection to close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.abort();
}
#[tokio::test]
async fn mqtt_publish_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary((publish_packet("devices/d1/state", b"up")).into()))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_retain_handling_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-retain-handling"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0011_0001,
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
async fn mqtt_v5_invalid_retain_handling_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-invalid-retain-handling"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0011_0001,
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
async fn mqtt_v5_invalid_subscription_qos_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-subscription-qos"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_0011,
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
        Some("invalid subscription qos")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscription_qos_disconnect_includes_server_reference() {
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
        server_reference: Some("greenmqtt://redirect-node".to_string()),
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-subscription-qos-ref"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_0011,
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
        disconnect.server_reference.as_deref(),
        Some("greenmqtt://redirect-node")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscription_qos_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-invalid-subscription-qos"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_0011,
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
        Some("invalid subscription qos")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_ws_invalid_subscription_qos_disconnect_includes_server_reference() {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: false,
        enable_tls: false,
        enable_ws: true,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: None,
        max_packet_size: None,
        response_information: None,
        server_reference: Some("greenmqtt://ws-redirect-node".to_string()),
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary((connect_packet_v5(
            "invalid-subscription-qos-ws-ref",
        )).into()))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary((subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_0011,
            &[],
        )).into()))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.server_reference.as_deref(),
        Some("greenmqtt://ws-redirect-node")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_publish_qos_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-publish-qos"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_with_flags(
            0b0110,
            "devices/d1/state",
            b"bad",
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
        Some("invalid publish qos")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_publish_qos_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-invalid-publish-qos"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_with_flags(
            0b0110,
            "devices/d1/state",
            b"bad",
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
        Some("invalid publish qos")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_publish_qos_request_problem_information_false_suppresses_reason_string() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-publish-qos-no-problem-info",
            true,
            &request_problem_information_property(false),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_with_flags(
            0b0110,
            "devices/d1/state",
            b"bad",
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(disconnect.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_publish_dup_qos0_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-publish-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_with_flags(
            0b1000,
            "devices/d1/state",
            b"bad",
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
        Some("invalid publish flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_publish_dup_qos0_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-invalid-publish-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_with_flags(
            0b1000,
            "devices/d1/state",
            b"bad",
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
        Some("invalid publish flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_publish_topic_alias_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("duplicate-publish-topic-alias"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = topic_alias_property(1);
    properties.extend_from_slice(&topic_alias_property(2));
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
        Some("duplicate topic alias")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_publish_topic_alias_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-duplicate-publish-topic-alias"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = topic_alias_property(1);
    properties.extend_from_slice(&topic_alias_property(2));
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
        Some("duplicate topic alias")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_subscription_identifier_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("publish-subscription-identifier"))
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
            &subscription_identifier_property(7),
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
        Some("subscription identifier not allowed on publish")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_subscription_identifier_disconnects_protocol_error_over_tls() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: true,
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
    );
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tls(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let mut client = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();

    client
        .write_all(&connect_packet_v5("tls-publish-subscription-identifier"))
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
            &subscription_identifier_property(7),
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
        Some("subscription identifier not allowed on publish")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_publish_topic_alias_request_problem_information_false_suppresses_reason_string(
) {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "duplicate-publish-topic-alias-no-problem-info",
            true,
            &request_problem_information_property(false),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut properties = topic_alias_property(1);
    properties.extend_from_slice(&topic_alias_property(2));
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
    assert_eq!(disconnect.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_denied_returns_not_authorized_puback() {
    let broker = test_broker_with_acl(
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![AclRule {
            decision: AclDecision::Deny,
            action: AclAction::Publish,
            identity: IdentityMatcher {
                tenant_id: "public".to_string(),
                user_id: "mqtt".to_string(),
                client_id: "pub-denied".to_string(),
            },
            topic_filter: "devices/#".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("pub-denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1(1, "devices/d1/state", b"denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut client).await;
    let mut puback = vec![0u8; puback_len];
    client.read_exact(&mut puback).await.unwrap();
    assert_eq!(parse_puback_reason_code(&puback), 0x87);
    assert_eq!(
        parse_puback_reason_string(&puback).as_deref(),
        Some("publish denied")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_publish_rate_limit_returns_quota_exceeded_puback() {
    let mut broker = test_broker();
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_publish_rate_limit_per_connection(1, 1);
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("pub-rate-limit"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1(1, "devices/d1/state", b"first"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut client).await;
    let mut puback = vec![0u8; puback_len];
    client.read_exact(&mut puback).await.unwrap();
    assert_eq!(parse_puback_reason_code(&puback), 0x00);

    client
        .write_all(&publish_packet_v5_qos1(2, "devices/d1/state", b"second"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut client).await;
    let mut puback = vec![0u8; puback_len];
    client.read_exact(&mut puback).await.unwrap();
    assert_eq!(parse_puback_reason_code(&puback), 0x97);
    assert_eq!(
        parse_puback_reason_string(&puback).as_deref(),
        Some("quota exceeded")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_tenant_publish_quota_exceeded_returns_quota_exceeded_puback() {
    let broker = test_broker();
    broker.set_tenant_quota(
        "public",
        greenmqtt_core::TenantQuota {
            max_connections: 10,
            max_subscriptions: 10,
            max_msg_per_sec: 0,
            max_memory_bytes: u64::MAX,
        },
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("tenant-publish-quota"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1(9, "devices/d1/state", b"quota"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut client).await;
    let mut puback = vec![0u8; puback_len];
    client.read_exact(&mut puback).await.unwrap();
    assert_eq!(parse_puback_reason_code(&puback), 0x97);
    assert_eq!(
        parse_puback_reason_string(&puback).as_deref(),
        Some("quota exceeded")
    );

    server.abort();
}

#[tokio::test]
#[ignore = "manual throughput gate for outbound bandwidth shaping"]
async fn mqtt_tcp_outbound_bandwidth_limit_keeps_throughput_under_1_2kbps() {
    let (writer, mut reader) = duplex(8192);
    let mut transport = TcpTransport::with_bandwidth(writer, None, Some((1024, 1024)));
    let payload = vec![b'x'; 8192];
    let write_payload = payload.clone();
    let started = Instant::now();
    let write_task = tokio::spawn(async move { transport.write_bytes(&write_payload).await });
    let mut received = vec![0u8; payload.len()];
    reader.read_exact(&mut received).await.unwrap();
    write_task.await.unwrap().unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(6500),
        "expected 8KB payload at 1KB/s with 1KB burst to take at least ~6.5s, got {:?}",
        elapsed
    );
}

#[tokio::test]
async fn mqtt_v5_utf8_payload_indicator_invalid_payload_returns_payload_format_invalid_puback() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("payload-format-invalid"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1_with_properties(
            7,
            "devices/d1/state",
            &[0xff, 0xfe],
            &publish_properties(Some(1), None, None, None, None, &[]),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut client).await;
    let mut puback = vec![0u8; puback_len];
    client.read_exact(&mut puback).await.unwrap();
    assert_eq!(parse_puback_reason_code(&puback), 0x99);
    assert_eq!(
        parse_puback_reason_string(&puback).as_deref(),
        Some("payload format invalid")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_denied_request_problem_information_false_suppresses_puback_reason_string()
{
    let broker = test_broker_with_acl(
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![AclRule {
            decision: AclDecision::Deny,
            action: AclAction::Publish,
            identity: IdentityMatcher {
                tenant_id: "public".to_string(),
                user_id: "mqtt".to_string(),
                client_id: "pub-denied".to_string(),
            },
            topic_filter: "devices/#".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "pub-denied",
            true,
            &request_problem_information_property(false),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos1(1, "devices/d1/state", b"denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut client).await;
    let mut puback = vec![0u8; puback_len];
    client.read_exact(&mut puback).await.unwrap();
    assert_eq!(parse_puback_reason_code(&puback), 0x87);
    assert_eq!(parse_puback_reason_string(&puback), None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_denied_returns_not_authorized_pubrec() {
    let broker = test_broker_with_acl(
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![AclRule {
            decision: AclDecision::Deny,
            action: AclAction::Publish,
            identity: IdentityMatcher {
                tenant_id: "public".to_string(),
                user_id: "mqtt".to_string(),
                client_id: "pub-denied-qos2".to_string(),
            },
            topic_filter: "devices/#".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("pub-denied-qos2"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos2(1, "devices/d1/state", b"denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREC);
    let pubrec_len = read_remaining_length_for_test(&mut client).await;
    let mut pubrec = vec![0u8; pubrec_len];
    client.read_exact(&mut pubrec).await.unwrap();
    assert_eq!(
        parse_puback_reason_code(&pubrec),
        0x87,
        "unexpected pubrec bytes: {pubrec:?}"
    );
    assert_eq!(
        parse_puback_reason_string(&pubrec).as_deref(),
        Some("publish denied")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_publish_denied_request_problem_information_false_suppresses_pubrec_reason_string()
{
    let broker = test_broker_with_acl(
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![AclRule {
            decision: AclDecision::Deny,
            action: AclAction::Publish,
            identity: IdentityMatcher {
                tenant_id: "public".to_string(),
                user_id: "mqtt".to_string(),
                client_id: "pub-denied-qos2".to_string(),
            },
            topic_filter: "devices/#".to_string(),
        }])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "pub-denied-qos2",
            true,
            &request_problem_information_property(false),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet_v5_qos2(1, "devices/d1/state", b"denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBREC);
    let pubrec_len = read_remaining_length_for_test(&mut client).await;
    let mut pubrec = vec![0u8; pubrec_len];
    client.read_exact(&mut pubrec).await.unwrap();
    assert_eq!(parse_puback_reason_code(&pubrec), 0x87);
    assert_eq!(parse_puback_reason_string(&pubrec), None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_client_maximum_packet_size_disconnects_oversized_delivery() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut subscriber = TcpStream::connect(bind).await.unwrap();
    subscriber
        .write_all(&connect_packet_v5_with_properties(
            "small-client",
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
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("packet too large")
    );

    server.abort();
}
