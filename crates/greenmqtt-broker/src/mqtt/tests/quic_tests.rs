use super::*;

#[tokio::test]
async fn mqtt_v5_invalid_auth_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-auth-reason"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&auth_packet_v5_with_reason(
        0x17,
        Some("custom"),
        Some(b"client-hello"),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_quic_ingress_shaping_settings_allow_connect_subscribe_publish_flow() {
    let _env = scoped_env_vars(&[
        ("GREENMQTT_INGRESS_READ_RATE_PER_SEC", Some("64")),
        ("GREENMQTT_INGRESS_READ_BURST", Some("64")),
        ("GREENMQTT_INGRESS_WRITE_RATE_PER_SEC", Some("64")),
        ("GREENMQTT_INGRESS_WRITE_BURST", Some("64")),
        ("GREENMQTT_HANDSHAKE_TIMEOUT_MS", Some("2000")),
    ]);
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();

    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let subscriber_conn = connect_quic_with_retry(&endpoint, bind).await;
    let (mut subscriber_tx, mut subscriber_rx) = subscriber_conn.open_bi().await.unwrap();
    subscriber_tx.write_all(&connect_packet("sub")).await.unwrap();
    assert_eq!(
        subscriber_rx.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut subscriber_rx).await;
    let mut connack = vec![0u8; connack_len];
    subscriber_rx.read_exact(&mut connack).await.unwrap();

    subscriber_tx
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(
        subscriber_rx.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_SUBACK
    );
    let suback_len = read_remaining_length_for_test(&mut subscriber_rx).await;
    let mut suback = vec![0u8; suback_len];
    subscriber_rx.read_exact(&mut suback).await.unwrap();

    let publisher_conn = connect_quic_with_retry(&endpoint, bind).await;
    let (mut publisher_tx, mut publisher_rx) = publisher_conn.open_bi().await.unwrap();
    publisher_tx.write_all(&connect_packet("pub")).await.unwrap();
    assert_eq!(
        publisher_rx.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher_rx).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher_rx
        .read_exact(&mut publisher_connack)
        .await
        .unwrap();
    publisher_tx
        .write_all(&publish_packet("devices/d1/state", b"quic-shaped"))
        .await
        .unwrap();

    let header = subscriber_rx.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber_rx).await;
    let mut publish = vec![0u8; publish_len];
    subscriber_rx.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(11).any(|window| window == b"quic-shaped"));

    endpoint.close(0u32.into(), b"done");
    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_auth_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-auth-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&auth_packet_v5_with_properties(
        0x18,
        &receive_maximum_property(10),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_second_connect_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();

    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-connect"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&connect_packet_v5("quic-duplicate-connect"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected connect packet")
    );

    endpoint.close(0u32.into(), b"done");
    server.abort();
}

#[tokio::test]
async fn mqtt_v5_malformed_remaining_length_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-bad-remaining-length"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&malformed_remaining_length_packet())
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("malformed remaining length")
    );

    endpoint.close(0u32.into(), b"done");
    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pingreq_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-bad-pingreq-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pingreq_packet_with_flags(0x01))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pingreq flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_disconnect_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-bad-disconnect-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&disconnect_packet_v5_with_flags(0x01))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unexpected_pubrec_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-unexpected-pubrec"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubrec_client_packet(7)).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubrec packet id")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unexpected_pubrel_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-unexpected-pubrel"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubrel_packet(7)).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubrel packet id")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unexpected_pubcomp_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-unexpected-pubcomp"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubcomp_client_packet(7)).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubcomp packet id")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_auth_method_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-auth-method"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&auth_method_property("custom"));
    tx.write_all(&auth_packet_v5_with_properties(0x18, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth method")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_auth_data_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-auth-data"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&auth_data_property(b"hello"));
    properties.extend_from_slice(&auth_data_property(b"again"));
    tx.write_all(&auth_packet_v5_with_properties(0x18, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth data")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_auth_reason_string_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-auth-reason-string"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&reason_string_property("first"));
    properties.extend_from_slice(&reason_string_property("second"));
    tx.write_all(&auth_packet_v5_with_properties(0x18, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth reason string")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_auth_data_without_method_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-auth-data-without-method"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&auth_packet_v5_with_reason(
        0x18,
        None,
        Some(b"client-hello"),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("auth data without auth method")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_auth_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-auth-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut auth = auth_packet_v5(Some("custom"), Some(b"client-hello"));
    auth[0] = (PACKET_TYPE_AUTH << 4) | 0x01;
    tx.write_all(&auth).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth flags")
    );

    endpoint.close(0u32.into(), b"done");
    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscription_identifier_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-subscription-id"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5_with_properties(
        1,
        "devices/+/state",
        &subscription_identifier_property(0),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscription identifier")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_subscription_identifier_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-subscription-id"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = subscription_identifier_property(7);
    properties.extend_from_slice(&subscription_identifier_property(9));
    tx.write_all(&subscribe_packet_v5_with_properties(
        1,
        "devices/+/state",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate subscription identifier")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_shared_subscription_with_no_local_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-shared-no-local"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5_with_options(
        1,
        "$share/g1/devices/+/state",
        0b0000_0101,
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_payload_format_indicator_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-payload-format-indicator"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &publish_properties(Some(2), None, None, None, None, &[]),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid payload format indicator")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_payload_format_indicator_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5(
        "quic-duplicate-payload-format-indicator",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(Some(1), None, None, None, None, &[]);
    properties.extend_from_slice(&publish_properties(Some(1), None, None, None, None, &[]));
    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate payload format indicator")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_content_type_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-content-type"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, Some("text/plain"), None, None, None, &[]);
    properties.extend_from_slice(&publish_properties(
        None,
        Some("application/json"),
        None,
        None,
        None,
        &[],
    ));
    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate content type")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_message_expiry_interval_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-message-expiry-interval"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, None, Some(10), None, None, &[]);
    properties.extend_from_slice(&publish_properties(None, None, Some(20), None, None, &[]));
    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate message expiry interval")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_response_topic_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-response-topic"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, None, None, Some("devices/replies/1"), None, &[]);
    properties.extend_from_slice(&publish_properties(
        None,
        None,
        None,
        Some("devices/replies/2"),
        None,
        &[],
    ));
    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate response topic")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_correlation_data_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-correlation-data"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = publish_properties(None, None, None, None, Some(b"trace-1"), &[]);
    properties.extend_from_slice(&publish_properties(
        None,
        None,
        None,
        None,
        Some(b"trace-2"),
        &[],
    ));
    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate correlation data")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_disconnect_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-disconnect-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&disconnect_packet_v5_with_properties(
        &receive_maximum_property(10),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_disconnect_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-disconnect-reason-code"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&disconnect_packet_v5_with_reason_and_properties(0x03, &[]))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_disconnect_reason_string_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5(
        "quic-duplicate-disconnect-reason-string",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    tx.write_all(&disconnect_packet_v5_with_properties(&properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate disconnect reason string")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrel_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubrel-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut packet = pubrel_packet(7);
    packet[0] = 0x60;
    tx.write_all(&packet).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrel_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubrel-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubrel_packet_v5_with_properties(
        7,
        &subscription_identifier_property(1),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_pubrel_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-pubrel-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    tx.write_all(&pubrel_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate pubrel property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_puback_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-puback-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&[0x41, 0x02, 0x00, 0x01]).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrec_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubrec-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&[0x51, 0x02, 0x00, 0x01]).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubcomp-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&[0x71, 0x02, 0x00, 0x01]).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_puback_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-puback-reason-code"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&puback_client_packet_v5_with_reason_code(7, 0x02))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_puback_properties_without_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5(
        "quic-puback-properties-without-reason-code",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet(PACKET_TYPE_PUBACK << 4, &body);
    tx.write_all(&packet).await.unwrap();

    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrec_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubrec-reason-code"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubrec_client_packet_v5_with_reason_code(7, 0x02))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_pubrec_properties_without_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5(
        "quic-pubrec-properties-without-reason-code",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet(PACKET_TYPE_PUBREC << 4, &body);
    tx.write_all(&packet).await.unwrap();

    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrec_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubrec-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubrec_client_packet_v5_with_properties(
        7,
        &subscription_identifier_property(1),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_pubrec_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-pubrec-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    tx.write_all(&pubrec_client_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrel_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubrel-reason-code"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubrel_packet_v5_with_reason_code(7, 0x10))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_pubrel_properties_without_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5(
        "quic-pubrel-properties-without-reason-code",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet((PACKET_TYPE_PUBREL << 4) | 0b0010, &body);
    tx.write_all(&packet).await.unwrap();

    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_reason_code_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubcomp-reason-code"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubcomp_client_packet_v5_with_reason_code(7, 0x10))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubcomp reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_puback_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-puback-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&puback_client_packet_v5_with_properties(
        7,
        &subscription_identifier_property(1),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_puback_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-puback-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    tx.write_all(&puback_client_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-pubcomp-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&pubcomp_client_packet_v5_with_properties(
        7,
        &subscription_identifier_property(1),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_pubcomp_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-pubcomp-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    tx.write_all(&pubcomp_client_packet_v5_with_properties(7, &properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_disconnect_session_expiry_interval_disconnects_protocol_error_over_quic()
{
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5(
        "quic-duplicate-disconnect-session-expiry",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = session_expiry_interval_property(30);
    properties.extend_from_slice(&session_expiry_interval_property(60));
    tx.write_all(&disconnect_packet_v5_with_properties(&properties))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate disconnect session expiry interval")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscribe_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-subscribe-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5_with_properties(
        1,
        "devices/+/state",
        &reason_string_property("bad"),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscribe_topic_filter_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-subscribe-topic-filter"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5(1, "devices/#/state"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic filter")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_empty_subscribe_payload_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-empty-subscribe-payload"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&empty_subscribe_packet_v5(1)).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("empty subscribe payload")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_empty_unsubscribe_payload_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-empty-unsubscribe-payload"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&empty_unsubscribe_packet_v5(1)).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("empty unsubscribe payload")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscribe_reserved_bits_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-subscribe-reserved-bits"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5_with_options(
        1,
        "devices/+/state",
        0b1100_0001,
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_flags_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-unsubscribe-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut packet = unsubscribe_packet(1, "devices/+/state");
    packet[0] = 0xA3;
    tx.write_all(&packet).await.unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid unsubscribe flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_property_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-unsubscribe-property"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&unsubscribe_packet_v5_with_properties(
        1,
        "devices/+/state",
        &subscription_identifier_property(7),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid unsubscribe property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_topic_filter_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-unsubscribe-topic-filter"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&unsubscribe_packet_v5_with_properties(
        1,
        "$share//devices/+/state",
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic filter")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_topic_alias_zero_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(50)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-alias-zero"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad-alias-zero",
        &topic_alias_property(0),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic alias")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unknown_topic_alias_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(50)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-alias-unknown"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "",
        b"bad-alias-unknown",
        &topic_alias_property(11),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic alias")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_quic_connect_subscribe_publish_flow() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();

    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let subscriber_conn = connect_quic_with_retry(&endpoint, bind).await;
    let (mut subscriber_tx, mut subscriber_rx) = subscriber_conn.open_bi().await.unwrap();
    subscriber_tx
        .write_all(&connect_packet("sub"))
        .await
        .unwrap();
    assert_eq!(
        subscriber_rx.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let connack_len = read_remaining_length_for_test(&mut subscriber_rx).await;
    let mut connack = vec![0u8; connack_len];
    subscriber_rx.read_exact(&mut connack).await.unwrap();

    subscriber_tx
        .write_all(&subscribe_packet(1, "devices/+/state"))
        .await
        .unwrap();
    assert_eq!(
        subscriber_rx.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_SUBACK
    );
    let suback_len = read_remaining_length_for_test(&mut subscriber_rx).await;
    let mut suback = vec![0u8; suback_len];
    subscriber_rx.read_exact(&mut suback).await.unwrap();

    let publisher_conn = connect_quic_with_retry(&endpoint, bind).await;
    let (mut publisher_tx, mut publisher_rx) = publisher_conn.open_bi().await.unwrap();
    publisher_tx
        .write_all(&connect_packet("pub"))
        .await
        .unwrap();
    assert_eq!(
        publisher_rx.read_u8().await.unwrap() >> 4,
        PACKET_TYPE_CONNACK
    );
    let publisher_connack_len = read_remaining_length_for_test(&mut publisher_rx).await;
    let mut publisher_connack = vec![0u8; publisher_connack_len];
    publisher_rx
        .read_exact(&mut publisher_connack)
        .await
        .unwrap();
    publisher_tx
        .write_all(&publish_packet("devices/d1/state", b"quic-up"))
        .await
        .unwrap();

    let header = subscriber_rx.read_u8().await.unwrap();
    assert_eq!(header >> 4, PACKET_TYPE_PUBLISH);
    let publish_len = read_remaining_length_for_test(&mut subscriber_rx).await;
    let mut publish = vec![0u8; publish_len];
    subscriber_rx.read_exact(&mut publish).await.unwrap();
    assert!(publish.windows(7).any(|window| window == b"quic-up"));

    endpoint.close(0u32.into(), b"done");
    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_retain_handling_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-retain-handling"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5_with_options(
        1,
        "devices/+/state",
        0b0011_0001,
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscription_qos_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-subscription-qos"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&subscribe_packet_v5_with_options(
        1,
        "devices/+/state",
        0b0000_0011,
        &[],
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscription qos")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_publish_qos_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-publish-qos"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&publish_packet_with_flags(
        0b0110,
        "devices/d1/state",
        b"bad",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid publish qos")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_publish_dup_qos0_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-invalid-publish-flags"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&publish_packet_with_flags(
        0b1000,
        "devices/d1/state",
        b"bad",
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid publish flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_publish_topic_alias_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-duplicate-publish-topic-alias"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    let mut properties = topic_alias_property(1);
    properties.extend_from_slice(&topic_alias_property(2));
    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &properties,
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate topic alias")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_publish_subscription_identifier_disconnects_protocol_error_over_quic() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
    let bind = next_test_bind();
    let server = tokio::spawn(serve_quic(
        broker,
        bind,
        cert_path.clone(),
        key_path.clone(),
    ));
    sleep(Duration::from_millis(100)).await;

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = QuinnEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let connection = connect_quic_with_retry(&endpoint, bind).await;
    let (mut tx, mut rx) = connection.open_bi().await.unwrap();
    tx.write_all(&connect_packet_v5("quic-publish-subscription-identifier"))
        .await
        .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut rx).await;
    let mut connack = vec![0u8; connack_len];
    rx.read_exact(&mut connack).await.unwrap();

    tx.write_all(&publish_packet_v5_qos1_with_properties(
        1,
        "devices/d1/state",
        b"bad",
        &subscription_identifier_property(7),
    ))
    .await
    .unwrap();
    assert_eq!(rx.read_u8().await.unwrap() >> 4, PACKET_TYPE_DISCONNECT);
    let disconnect_len = read_remaining_length_for_test(&mut rx).await;
    let mut disconnect = vec![0u8; disconnect_len];
    rx.read_exact(&mut disconnect).await.unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("subscription identifier not allowed on publish")
    );

    server.abort();
}
