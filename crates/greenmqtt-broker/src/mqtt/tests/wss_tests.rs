use super::*;

#[tokio::test]
async fn mqtt_v5_invalid_auth_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-auth-reason",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(auth_packet_v5_with_reason(
            0x17,
            Some("custom"),
            Some(b"client-hello"),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_auth_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-auth-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(auth_packet_v5_with_properties(
            0x18,
            &receive_maximum_property(10),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_second_connect_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());

    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-duplicate-connect")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(connect_packet_v5("wss-duplicate-connect")))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected connect packet")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_malformed_remaining_length_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-bad-remaining-length",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(malformed_remaining_length_packet()))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("malformed remaining length")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pingreq_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-bad-pingreq-flags")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pingreq_packet_with_flags(0x01)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pingreq flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_disconnect_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-bad-disconnect-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(disconnect_packet_v5_with_flags(0x01)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unexpected_pubrec_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-unexpected-pubrec")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubrec_client_packet(7)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubrec packet id")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unexpected_pubrel_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-unexpected-pubrel")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubrel_packet(7)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubrel packet id")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unexpected_pubcomp_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-unexpected-pubcomp")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubcomp_client_packet(7)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("unexpected pubcomp packet id")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_auth_method_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-auth-method",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&auth_method_property("custom"));
    client
        .send(Message::Binary(auth_packet_v5_with_properties(
            0x18,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth method")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_auth_data_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-auth-data",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&auth_data_property(b"hello"));
    properties.extend_from_slice(&auth_data_property(b"again"));
    client
        .send(Message::Binary(auth_packet_v5_with_properties(
            0x18,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth data")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_auth_reason_string_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-auth-reason-string",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = auth_method_property("custom");
    properties.extend_from_slice(&reason_string_property("first"));
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .send(Message::Binary(auth_packet_v5_with_properties(
            0x18,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate auth reason string")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_auth_data_without_method_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-auth-data-without-method",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(auth_packet_v5_with_reason(
            0x18,
            None,
            Some(b"client-hello"),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("auth data without auth method")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_auth_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-invalid-auth-flags")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut auth = auth_packet_v5(Some("custom"), Some(b"client-hello"));
    auth[0] = (PACKET_TYPE_AUTH << 4) | 0x01;
    client.send(Message::Binary(auth)).await.unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid auth flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscription_identifier_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-subscription-id",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(0),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscription identifier")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_subscription_identifier_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-subscription-id",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = subscription_identifier_property(7);
    properties.extend_from_slice(&subscription_identifier_property(9));
    client
        .send(Message::Binary(subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate subscription identifier")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_shared_subscription_with_no_local_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-shared-no-local")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5_with_options(
            1,
            "$share/g1/devices/+/state",
            0b0000_0101,
            &[],
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_payload_format_indicator_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-payload-format-indicator",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &publish_properties(Some(2), None, None, None, None, &[]),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid payload format indicator")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_payload_format_indicator_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-payload-format-indicator",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = publish_properties(Some(1), None, None, None, None, &[]);
    properties.extend_from_slice(&publish_properties(Some(1), None, None, None, None, &[]));
    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate payload format indicator")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_content_type_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-content-type",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

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
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate content type")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_message_expiry_interval_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-message-expiry-interval",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = publish_properties(None, None, Some(10), None, None, &[]);
    properties.extend_from_slice(&publish_properties(None, None, Some(20), None, None, &[]));
    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate message expiry interval")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_response_topic_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-response-topic",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

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
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate response topic")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_correlation_data_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-correlation-data",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

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
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate correlation data")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_disconnect_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-disconnect-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(disconnect_packet_v5_with_properties(
            &receive_maximum_property(10),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_disconnect_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-disconnect-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(
            disconnect_packet_v5_with_reason_and_properties(0x03, &[]),
        ))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid disconnect reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_disconnect_reason_string_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-disconnect-reason-string",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .send(Message::Binary(disconnect_packet_v5_with_properties(
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate disconnect reason string")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrel_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubrel-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut packet = pubrel_packet(7);
    packet[0] = 0x60;
    client.send(Message::Binary(packet)).await.unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrel_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubrel-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubrel_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_pubrel_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-pubrel-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .send(Message::Binary(pubrel_packet_v5_with_properties(
            7,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate pubrel property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_puback_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-puback-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(vec![0x41, 0x02, 0x00, 0x01]))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrec_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubrec-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(vec![0x51, 0x02, 0x00, 0x01]))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubcomp-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(vec![0x71, 0x02, 0x00, 0x01]))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_puback_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-puback-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(puback_client_packet_v5_with_reason_code(
            7, 0x02,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_puback_properties_without_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-puback-properties-without-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet(PACKET_TYPE_PUBACK << 4, &body);
    client.send(Message::Binary(packet)).await.unwrap();

    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrec_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubrec-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubrec_client_packet_v5_with_reason_code(
            7, 0x02,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_pubrec_properties_without_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-pubrec-properties-without-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet(PACKET_TYPE_PUBREC << 4, &body);
    client.send(Message::Binary(packet)).await.unwrap();

    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrec_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubrec-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubrec_client_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_pubrec_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-pubrec-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .send(Message::Binary(pubrec_client_packet_v5_with_properties(
            7,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubrel_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubrel-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubrel_packet_v5_with_reason_code(7, 0x10)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_pubrel_properties_without_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-pubrel-properties-without-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let properties = reason_string_property("out-of-order");
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let packet = crate::mqtt::codec::build_packet((PACKET_TYPE_PUBREL << 4) | 0b0010, &body);
    client.send(Message::Binary(packet)).await.unwrap();

    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubrel reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_reason_code_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubcomp-reason-code",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubcomp_client_packet_v5_with_reason_code(
            7, 0x10,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid pubcomp reason code")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_puback_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-puback-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(puback_client_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_puback_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-puback-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .send(Message::Binary(puback_client_packet_v5_with_properties(
            7,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_pubcomp_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-pubcomp-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(pubcomp_client_packet_v5_with_properties(
            7,
            &subscription_identifier_property(1),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_pubcomp_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-pubcomp-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = reason_string_property("first");
    properties.extend_from_slice(&reason_string_property("second"));
    client
        .send(Message::Binary(pubcomp_client_packet_v5_with_properties(
            7,
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate puback/pubrec/pubcomp property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_disconnect_session_expiry_interval_disconnects_protocol_error_over_wss()
{
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-disconnect-session-expiry",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = session_expiry_interval_property(30);
    properties.extend_from_slice(&session_expiry_interval_property(60));
    client
        .send(Message::Binary(disconnect_packet_v5_with_properties(
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate disconnect session expiry interval")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscribe_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-subscribe-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &reason_string_property("bad"),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscribe_topic_filter_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-subscribe-topic-filter",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5(1, "devices/#/state")))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic filter")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_empty_subscribe_payload_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-empty-subscribe-payload",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(empty_subscribe_packet_v5(1)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("empty subscribe payload")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_empty_unsubscribe_payload_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-empty-unsubscribe-payload",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(empty_unsubscribe_packet_v5(1)))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("empty unsubscribe payload")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscribe_reserved_bits_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-subscribe-reserved-bits",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b1100_0001,
            &[],
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_flags_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-unsubscribe-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut packet = unsubscribe_packet(1, "devices/+/state");
    packet[0] = 0xA3;
    client.send(Message::Binary(packet)).await.unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid unsubscribe flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_property_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-unsubscribe-property",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(unsubscribe_packet_v5_with_properties(
            1,
            "devices/+/state",
            &subscription_identifier_property(7),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid unsubscribe property")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_unsubscribe_topic_filter_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-unsubscribe-topic-filter",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(unsubscribe_packet_v5_with_properties(
            1,
            "$share//devices/+/state",
            &[],
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic filter")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_topic_alias_zero_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(50)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-alias-zero")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad-alias-zero",
            &topic_alias_property(0),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic alias")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_unknown_topic_alias_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(50)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5("wss-alias-unknown")))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "",
            b"bad-alias-unknown",
            &topic_alias_property(11),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid topic alias")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_wss_connect_subscribe_publish_flow() {
    let broker = test_broker();
    let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();

    let bind = next_test_bind();
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let wss_url = format!("wss://localhost:{}", bind.port());
    let subscriber_stream = TcpStream::connect(bind).await.unwrap();
    let subscriber_tls = connector
        .connect(
            ServerName::try_from("localhost").unwrap(),
            subscriber_stream,
        )
        .await
        .unwrap();
    let (mut subscriber, _) = client_async(&wss_url, subscriber_tls).await.unwrap();
    subscriber
        .send(Message::Binary(connect_packet("sub")))
        .await
        .unwrap();
    let connack = subscriber.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    subscriber
        .send(Message::Binary(subscribe_packet(1, "devices/+/state")))
        .await
        .unwrap();
    let suback = subscriber.next().await.unwrap().unwrap().into_data();
    assert_eq!(suback[0] >> 4, PACKET_TYPE_SUBACK);

    let publisher_stream = TcpStream::connect(bind).await.unwrap();
    let publisher_tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), publisher_stream)
        .await
        .unwrap();
    let (mut publisher, _) = client_async(&wss_url, publisher_tls).await.unwrap();
    publisher
        .send(Message::Binary(connect_packet("pub")))
        .await
        .unwrap();
    let publisher_connack = publisher.next().await.unwrap().unwrap().into_data();
    assert_eq!(publisher_connack[0] >> 4, PACKET_TYPE_CONNACK);
    publisher
        .send(Message::Binary(publish_packet(
            "devices/d1/state",
            b"wss-up",
        )))
        .await
        .unwrap();

    let publish = subscriber.next().await.unwrap().unwrap().into_data();
    assert_eq!(publish[0] >> 4, PACKET_TYPE_PUBLISH);
    assert!(publish.windows(6).any(|window| window == b"wss-up"));

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_retain_handling_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-retain-handling",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0011_0001,
            &[],
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscribe options")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_subscription_qos_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-subscription-qos",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(subscribe_packet_v5_with_options(
            1,
            "devices/+/state",
            0b0000_0011,
            &[],
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid subscription qos")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_publish_qos_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-publish-qos",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(publish_packet_with_flags(
            0b0110,
            "devices/d1/state",
            b"bad",
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid publish qos")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_publish_dup_qos0_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-invalid-publish-flags",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(publish_packet_with_flags(
            0b1000,
            "devices/d1/state",
            b"bad",
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("invalid publish flags")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_duplicate_publish_topic_alias_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-duplicate-publish-topic-alias",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    let mut properties = topic_alias_property(1);
    properties.extend_from_slice(&topic_alias_property(2));
    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &properties,
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("duplicate topic alias")
    );

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_publish_subscription_identifier_disconnects_protocol_error_over_wss() {
    let broker = test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: false,
            enable_wss: true,
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
    let server = tokio::spawn(serve_wss(broker, bind, cert_path.clone(), key_path.clone()));
    sleep(Duration::from_millis(100)).await;

    let mut roots = RustlsRootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let wss_url = format!("wss://localhost:{}", bind.port());
    let stream = TokioTcpStream::connect(bind).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap();
    let (mut client, _) = client_async(&wss_url, tls).await.unwrap();
    client
        .send(Message::Binary(connect_packet_v5(
            "wss-publish-subscription-identifier",
        )))
        .await
        .unwrap();
    let connack = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(connack[0] >> 4, PACKET_TYPE_CONNACK);

    client
        .send(Message::Binary(publish_packet_v5_qos1_with_properties(
            1,
            "devices/d1/state",
            b"bad",
            &subscription_identifier_property(7),
        )))
        .await
        .unwrap();
    let disconnect = client.next().await.unwrap().unwrap().into_data();
    assert_eq!(disconnect[0] >> 4, PACKET_TYPE_DISCONNECT);
    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&disconnect, &mut cursor).unwrap();
    let disconnect = parse_v5_disconnect_packet(&disconnect[cursor..]);
    assert_eq!(disconnect.reason_code, 0x82);
    assert_eq!(
        disconnect.reason_string.as_deref(),
        Some("subscription identifier not allowed on publish")
    );

    server.abort();
}
