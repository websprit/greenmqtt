use super::*;

#[tokio::test]
async fn mqtt_v5_message_expiry_interval_drops_expired_replay() {
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
            b"expire-me",
            &message_expiry_interval_property(1),
        ))
        .await
        .unwrap();
    assert_eq!(publisher.read_u8().await.unwrap() >> 4, PACKET_TYPE_PUBACK);
    let puback_len = read_remaining_length_for_test(&mut publisher).await;
    let mut puback = vec![0u8; puback_len];
    publisher.read_exact(&mut puback).await.unwrap();
    sleep(Duration::from_millis(1_200)).await;

    let mut resumed = TcpStream::connect(bind).await.unwrap();
    resumed
        .write_all(&connect_packet_with_clean_start("sub", false))
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
async fn mqtt_tcp_pingreq_pingresp_round_trip() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(50)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&connect_packet("pinger")).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&[0xC0, 0x00]).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_PINGRESP);
    let pingresp_len = read_remaining_length_for_test(&mut client).await;
    assert_eq!(pingresp_len, 0);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_connack_advertises_server_limits_and_capabilities() {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: false,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: Some(9),
        max_packet_size: Some(128),
        response_information: None,
        server_reference: None,
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client.write_all(&connect_packet_v5("caps")).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let properties = parse_v5_connack_packet(&connack);
    assert_eq!(properties.server_keep_alive_secs, Some(9));
    assert_eq!(properties.maximum_packet_size, Some(128));
    assert_eq!(properties.topic_alias_maximum, Some(u16::MAX));
    assert!(properties.retain_available);
    assert!(properties.wildcard_subscription_available);
    assert!(properties.subscription_identifiers_available);
    assert!(properties.shared_subscription_available);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_connack_includes_response_information_when_requested() {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: false,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: None,
        max_packet_size: None,
        response_information: Some("greenmqtt://broker".to_string()),
        server_reference: None,
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "response-info",
            true,
            &request_response_information_property(true),
        ))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let properties = parse_v5_connack_packet(&connack);
    assert_eq!(
        properties.response_information.as_deref(),
        Some("greenmqtt://broker")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_receive_maximum_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-receive-maximum",
            true,
            &receive_maximum_property(0),
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
        Some("invalid receive maximum")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_receive_maximum_includes_server_reference_connack() {
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
        server_reference: Some("greenmqtt://node-3".to_string()),
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-rx-max",
            true,
            &receive_maximum_property(0),
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
        connack.server_reference.as_deref(),
        Some("greenmqtt://node-3")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_receive_maximum_request_problem_information_false_suppresses_connack_reason_string(
) {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = receive_maximum_property(0);
    properties.extend_from_slice(&request_problem_information_property(false));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-receive-maximum-no-problem-info",
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
    assert_eq!(connack.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_maximum_packet_size_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-maximum-packet-size",
            true,
            &maximum_packet_size_property(0),
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
        Some("invalid maximum packet size")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_maximum_packet_size_request_problem_information_false_suppresses_connack_reason_string(
) {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = maximum_packet_size_property(0);
    properties.extend_from_slice(&request_problem_information_property(false));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-maximum-packet-size-no-problem-info",
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
    assert_eq!(connack.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_request_problem_information_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-request-problem-information",
            true,
            &request_problem_information_raw_property(2),
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
        Some("invalid request problem information")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_request_response_information_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-request-response-information",
            true,
            &request_response_information_raw_property(2),
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
        Some("invalid request response information")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_invalid_request_response_information_with_request_problem_information_false_suppresses_connack_reason_string(
) {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = request_problem_information_property(false);
    properties.extend_from_slice(&request_response_information_raw_property(2));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "invalid-request-response-information-no-problem-info",
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
    assert_eq!(connack.reason_string, None);

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_receive_maximum_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = receive_maximum_property(10);
    properties.extend_from_slice(&receive_maximum_property(20));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "duplicate-receive-maximum",
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
        Some("duplicate receive maximum")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_request_problem_information_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = request_problem_information_property(true);
    properties.extend_from_slice(&request_problem_information_property(true));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "duplicate-request-problem-information",
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
        Some("duplicate request problem information")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_request_response_information_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = request_response_information_property(true);
    properties.extend_from_slice(&request_response_information_property(true));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "duplicate-request-response-information",
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
        Some("duplicate request response information")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_duplicate_maximum_packet_size_returns_protocol_error_connack() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut properties = maximum_packet_size_property(1024);
    properties.extend_from_slice(&maximum_packet_size_property(2048));

    let mut client = connect_tcp_with_retry(bind).await.unwrap();
    client
        .write_all(&connect_packet_v5_with_properties(
            "duplicate-maximum-packet-size",
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
        Some("duplicate maximum packet size")
    );

    server.abort();
}
#[tokio::test]
async fn mqtt_v5_zero_length_client_id_receives_assigned_client_identifier() {
    let broker = test_broker();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client.write_all(&connect_packet_v5("")).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();
    let properties = parse_v5_connack_packet(&connack);
    let assigned_client_id = properties
        .assigned_client_identifier
        .expect("assigned client id must be present");
    assert!(assigned_client_id.starts_with("greenmqtt-"));
    assert!(!assigned_client_id.is_empty());

    server.abort();
}
#[tokio::test]
async fn mqtt_tcp_rejects_packets_above_server_maximum_packet_size() {
    let broker = test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: false,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: None,
        max_packet_size: Some(48),
        response_information: None,
        server_reference: None,
        audit_log_path: None,
    });
    let bind = next_test_bind();
    let server = tokio::spawn(serve_tcp(broker, bind));
    sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(bind).await.unwrap();
    client.write_all(&connect_packet("limit")).await.unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&publish_packet("devices/d1/state", &[b'x'; 96]))
        .await
        .unwrap();
    let read_result = tokio::time::timeout(Duration::from_secs(1), client.read_u8()).await;
    assert!(matches!(read_result, Ok(Err(_))));

    server.abort();
}
