use super::*;

#[test]
fn mqtt_v5_subscribe_no_local_option_is_parsed() {
    let packet = subscribe_packet_v5_with_options(1, "devices/+/state", 0b0000_0101, &[]);
    let parsed = parse_packet_frame(&packet, 5).unwrap();
    let Packet::Subscribe(subscribe) = parsed else {
        panic!("expected subscribe packet");
    };
    assert_eq!(subscribe.subscriptions.len(), 1);
    assert!(subscribe.subscriptions[0].no_local);
    assert_eq!(subscribe.subscriptions[0].qos, 1);
}
#[test]
fn mqtt_v5_subscribe_identifier_is_parsed() {
    let packet = subscribe_packet_v5_with_properties(
        1,
        "devices/+/state",
        &subscription_identifier_property(9),
    );
    let parsed = parse_packet_frame(&packet, 5).unwrap();
    let Packet::Subscribe(subscribe) = parsed else {
        panic!("expected subscribe packet");
    };
    assert_eq!(subscribe.subscriptions[0].subscription_identifier, Some(9));
}
#[test]
fn mqtt_v5_invalid_subscribe_property_is_rejected() {
    let packet = subscribe_packet_v5_with_properties(1, "devices/+/state", &[0x01, 0x01]);
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("invalid subscribe property"));
}
#[test]
fn mqtt_v5_empty_subscribe_payload_is_rejected() {
    let packet = empty_subscribe_packet_v5(1);
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("empty subscribe payload"));
}
#[test]
fn mqtt_v5_empty_unsubscribe_payload_is_rejected() {
    let packet = empty_unsubscribe_packet_v5(1);
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("empty unsubscribe payload"));
}
#[test]
fn mqtt_v5_invalid_subscribe_topic_filter_is_rejected() {
    let packet = subscribe_packet_v5(1, "devices/#/state");
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("invalid topic filter"));
}
#[test]
fn mqtt_v5_invalid_shared_unsubscribe_topic_filter_is_rejected() {
    let packet = unsubscribe_packet_v5_with_properties(1, "$share//devices/+/state", &[]);
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("invalid topic filter"));
}
#[test]
fn mqtt_v5_truncated_subscribe_frame_is_rejected() {
    let mut packet = subscribe_packet_v5(1, "devices/+/state");
    packet.pop();
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("malformed frame length"));
}
#[tokio::test]
async fn mqtt_v5_unsubscribe_returns_success_reason_codes() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("v5-unsuback"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut client).await;
    let mut suback = vec![0u8; suback_len];
    client.read_exact(&mut suback).await.unwrap();

    client
        .write_all(&unsubscribe_packet_v5_with_properties(
            2,
            "devices/+/state",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_UNSUBACK);
    let unsuback_len = read_remaining_length_for_test(&mut client).await;
    let mut unsuback = vec![0u8; unsuback_len];
    client.read_exact(&mut unsuback).await.unwrap();
    assert_eq!(parse_unsuback_return_codes(&unsuback, 5), vec![0x00]);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unsubscribe_returns_no_subscription_existed_reason_code() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("v5-unsuback-missing"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&unsubscribe_packet_v5_with_properties(
            2,
            "devices/+/state",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_UNSUBACK);
    let unsuback_len = read_remaining_length_for_test(&mut client).await;
    let mut unsuback = vec![0u8; unsuback_len];
    client.read_exact(&mut unsuback).await.unwrap();
    assert_eq!(parse_unsuback_return_codes(&unsuback, 5), vec![0x11]);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_unsubscribe_returns_per_filter_reason_codes() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("v5-unsuback-mixed"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut client).await;
    let mut suback = vec![0u8; suback_len];
    client.read_exact(&mut suback).await.unwrap();

    client
        .write_all(&unsubscribe_packet_v5_multiple_with_properties(
            2,
            &["devices/+/state", "alerts/#"],
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_UNSUBACK);
    let unsuback_len = read_remaining_length_for_test(&mut client).await;
    let mut unsuback = vec![0u8; unsuback_len];
    client.read_exact(&mut unsuback).await.unwrap();
    assert_eq!(parse_unsuback_return_codes(&unsuback, 5), vec![0x00, 0x11]);

    server.abort();
}
#[tokio::test]
async fn mqtt_subscribe_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&subscribe_packet(1, "devices/+/state"))
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
async fn mqtt_unsubscribe_before_connect_closes_connection() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&unsubscribe_packet(1, "devices/+/state"))
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
async fn mqtt_subscribe_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(
            (subscribe_packet(1, "devices/+/state")).into(),
        ))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_unsubscribe_before_connect_closes_websocket() {
    let broker = test_ws_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_ws(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let ws_url = format!("ws://{}", bind);
    let mut client = connect_ws_with_retry(&ws_url).await.unwrap();
    client
        .send(Message::Binary(
            (unsubscribe_packet(1, "devices/+/state")).into(),
        ))
        .await
        .unwrap();
    assert_ws_connection_closed(&mut client).await;

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_subscribe_denied_returns_not_authorized_suback() {
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("sub-denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut client).await;
    let mut suback = vec![0u8; suback_len];
    client.read_exact(&mut suback).await.unwrap();
    assert_eq!(parse_suback_return_codes(&suback, 5), vec![0x87]);
    assert_eq!(
        parse_suback_reason_string(&suback, 5).as_deref(),
        Some("subscribe denied")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_subscribe_denied_request_problem_information_false_suppresses_suback_reason_string(
) {
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "sub-denied",
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
        .write_all(&subscribe_packet_v5(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut client).await;
    let mut suback = vec![0u8; suback_len];
    client.read_exact(&mut suback).await.unwrap();
    assert_eq!(parse_suback_return_codes(&suback, 5), vec![0x87]);
    assert_eq!(parse_suback_reason_string(&suback, 5), None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v3_subscribe_denied_returns_failure_suback() {
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
        ConfiguredAcl::Static(StaticAclProvider::new(vec![])),
    );
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet("sub-denied"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_SUBACK);
    let suback_len = read_remaining_length_for_test(&mut client).await;
    let mut suback = vec![0u8; suback_len];
    client.read_exact(&mut suback).await.unwrap();
    assert_eq!(parse_suback_return_codes(&suback, 4), vec![0x80]);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscribe_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-subscribe-property"))
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
            &[0x01, 0x01],
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
        Some("invalid subscribe property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscribe_topic_filter_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-subscribe-topic-filter"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&subscribe_packet_v5(1, "devices/#/state"))
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
        Some("invalid topic filter")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_empty_subscribe_payload_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("empty-subscribe-payload"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&empty_subscribe_packet_v5(1))
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
        Some("empty subscribe payload")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_empty_unsubscribe_payload_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("empty-unsubscribe-payload"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&empty_unsubscribe_packet_v5(1))
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
        Some("empty unsubscribe payload")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscribe_request_problem_information_false_suppresses_reason_string() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-subscribe-no-problem-info",
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
    assert_eq!(disconnect.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_subscribe_reserved_bits_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-subscribe-reserved-bits"))
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
            0b1100_0001,
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
async fn mqtt_v5_invalid_unsubscribe_flags_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-unsubscribe-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    let mut packet = unsubscribe_packet(1, "devices/+/state");
    packet[0] = 0xA3;
    client.write_all(&packet).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut client).await;
    let mut disconnect = vec![0u8; disconnect_len];
    client.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid unsubscribe flags")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_property_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-unsubscribe-property"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&unsubscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
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
        Some("invalid unsubscribe property")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_topic_filter_disconnects_protocol_error() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5("invalid-unsubscribe-topic-filter"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&unsubscribe_packet_v5_with_properties(
            1,
            "$share//devices/+/state",
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
        Some("invalid topic filter")
    );

    server.abort();
}
