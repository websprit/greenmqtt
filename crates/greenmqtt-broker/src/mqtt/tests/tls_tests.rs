use super::*;

#[tokio::test]
#[ignore = "flaky local tls startup in CI-like environments"]
async fn mqtt_tls_connection_limit_rejects_second_client() {
    let mut broker = test_broker_with_custom_auth(
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
    Arc::get_mut(&mut broker)
        .expect("broker should be uniquely owned before serve")
        .set_connection_limit(1);
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

    let mut first = None;
    for _ in 0..40 {
        let first_stream = connect_tcp_with_retry(bind).await.unwrap();
        match connector
            .connect(ServerName::try_from("localhost").unwrap(), first_stream)
            .await
        {
            Ok(stream) => {
                first = Some(stream);
                break;
            }
            Err(_) => sleep(Duration::from_millis(75)).await,
        }
    }
    let mut first = first.expect("expected first tls connection to succeed");
    first.write_all(&connect_packet("first")).await.unwrap();
    assert_eq!(first.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut first).await;
    let mut connack = vec![0u8; connack_len];
    first.read_exact(&mut connack).await.unwrap();

    let second_stream = connect_tcp_with_retry(bind).await.unwrap();
    match connector
        .connect(ServerName::try_from("localhost").unwrap(), second_stream)
        .await
    {
        Ok(mut second) => {
            let _ = second.write_all(&connect_packet("second")).await;
            let error = second
                .read_u8()
                .await
                .expect_err("expected second tls client to be rejected");
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ));
        }
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("eof")
                    || message.contains("peer")
                    || message.contains("refused")
                    || message.contains("reset"),
                "unexpected tls error: {message}"
            );
        }
    }

    server.abort();
}

#[tokio::test]
async fn mqtt_v5_invalid_auth_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-auth-reason"))
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
async fn mqtt_v5_invalid_auth_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-auth-property"))
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
async fn mqtt_v5_second_connect_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-connect"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client
        .write_all(&connect_packet_v5("tls-duplicate-connect"))
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
async fn mqtt_v5_malformed_remaining_length_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-bad-remaining-length"))
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
async fn mqtt_v5_invalid_pingreq_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-bad-pingreq-flags"))
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
async fn mqtt_v5_invalid_disconnect_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-bad-disconnect-flags"))
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
async fn mqtt_v5_unexpected_pubrec_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-unexpected-pubrec"))
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
async fn mqtt_v5_unexpected_pubrel_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-unexpected-pubrel"))
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
async fn mqtt_v5_unexpected_pubcomp_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-unexpected-pubcomp"))
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
async fn mqtt_v5_duplicate_auth_method_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-auth-method"))
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
async fn mqtt_v5_duplicate_auth_data_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-auth-data"))
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
async fn mqtt_v5_duplicate_auth_reason_string_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-auth-reason-string"))
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
async fn mqtt_v5_auth_data_without_method_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-auth-data-without-method"))
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
async fn mqtt_v5_invalid_auth_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-auth-flags"))
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
async fn mqtt_v5_invalid_subscription_identifier_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-subscription-id"))
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
async fn mqtt_v5_duplicate_subscription_identifier_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-subscription-id"))
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
async fn mqtt_v5_shared_subscription_with_no_local_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-shared-no-local"))
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
async fn mqtt_v5_invalid_payload_format_indicator_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-payload-format-indicator"))
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
async fn mqtt_v5_duplicate_payload_format_indicator_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-payload-format-indicator"))
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
async fn mqtt_v5_duplicate_content_type_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-content-type"))
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
async fn mqtt_v5_duplicate_message_expiry_interval_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-message-expiry-interval"))
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
async fn mqtt_v5_duplicate_response_topic_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-response-topic"))
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
async fn mqtt_v5_duplicate_correlation_data_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-correlation-data"))
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
async fn mqtt_v5_invalid_disconnect_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-disconnect-property"))
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
async fn mqtt_v5_invalid_disconnect_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-disconnect-reason-code"))
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
async fn mqtt_v5_duplicate_disconnect_reason_string_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-disconnect-reason-string"))
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
async fn mqtt_v5_invalid_pubrel_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubrel-flags"))
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
async fn mqtt_v5_invalid_pubrel_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubrel-property"))
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
async fn mqtt_v5_duplicate_pubrel_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-pubrel-property"))
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
async fn mqtt_v5_invalid_puback_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-puback-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&[0x41, 0x02, 0x00, 0x01]).await.unwrap();
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
async fn mqtt_v5_invalid_pubrec_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubrec-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&[0x51, 0x02, 0x00, 0x01]).await.unwrap();
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
async fn mqtt_v5_invalid_pubcomp_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubcomp-flags"))
        .await
        .unwrap();
    assert_eq!(client.read_u8().await.unwrap() >> 4, PACKET_TYPE_CONNACK);
    let connack_len = read_remaining_length_for_test(&mut client).await;
    let mut connack = vec![0u8; connack_len];
    client.read_exact(&mut connack).await.unwrap();

    client.write_all(&[0x71, 0x02, 0x00, 0x01]).await.unwrap();
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
async fn mqtt_v5_invalid_puback_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-puback-reason-code"))
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
async fn mqtt_v5_puback_properties_without_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5(
            "tls-puback-properties-without-reason-code",
        ))
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
async fn mqtt_v5_invalid_pubrec_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubrec-reason-code"))
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
async fn mqtt_v5_pubrec_properties_without_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5(
            "tls-pubrec-properties-without-reason-code",
        ))
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
async fn mqtt_v5_invalid_pubrec_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubrec-property"))
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
async fn mqtt_v5_duplicate_pubrec_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-pubrec-property"))
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
async fn mqtt_v5_invalid_pubrel_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubrel-reason-code"))
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
async fn mqtt_v5_pubrel_properties_without_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5(
            "tls-pubrel-properties-without-reason-code",
        ))
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
async fn mqtt_v5_invalid_pubcomp_reason_code_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubcomp-reason-code"))
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
async fn mqtt_v5_invalid_puback_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-puback-property"))
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
async fn mqtt_v5_duplicate_puback_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-puback-property"))
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
async fn mqtt_v5_invalid_pubcomp_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-pubcomp-property"))
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
async fn mqtt_v5_duplicate_pubcomp_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-duplicate-pubcomp-property"))
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
async fn mqtt_v5_duplicate_disconnect_session_expiry_interval_disconnects_protocol_error_over_tls()
{
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
        .write_all(&connect_packet_v5(
            "tls-duplicate-disconnect-session-expiry",
        ))
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
async fn mqtt_v5_invalid_subscribe_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-subscribe-property"))
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
            &reason_string_property("bad"),
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
async fn mqtt_v5_invalid_subscribe_topic_filter_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-subscribe-topic-filter"))
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
async fn mqtt_v5_empty_subscribe_payload_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-empty-subscribe-payload"))
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
async fn mqtt_v5_empty_unsubscribe_payload_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-empty-unsubscribe-payload"))
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
async fn mqtt_v5_invalid_subscribe_reserved_bits_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-subscribe-reserved-bits"))
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
async fn mqtt_v5_invalid_unsubscribe_flags_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-unsubscribe-flags"))
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
async fn mqtt_v5_invalid_unsubscribe_property_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-unsubscribe-property"))
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
async fn mqtt_v5_invalid_unsubscribe_topic_filter_disconnects_protocol_error_over_tls() {
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
        .write_all(&connect_packet_v5("tls-invalid-unsubscribe-topic-filter"))
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
