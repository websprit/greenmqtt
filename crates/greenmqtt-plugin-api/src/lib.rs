mod acl;
mod auth;
mod bridge;
mod hook;
mod plugin_api;
mod rewrite;
mod webhook;

pub use acl::{
    AclAction, AclDecision, AclRule, AllowAllAcl, ConfiguredAcl, HttpAclConfig, HttpAclProvider,
    StaticAclProvider,
};
pub use auth::{
    AllowAllAuth, ConfiguredAuth, HttpAuthConfig, HttpAuthProvider, IdentityMatcher,
    StaticAuthProvider, StaticEnhancedAuthProvider,
};
pub use bridge::{BridgeEventHook, BridgeRule};
use greenmqtt_core::PublishProperties;
pub use greenmqtt_core::PublishRequest;
pub use hook::{ConfiguredEventHook, HookTarget, NoopEventHook};
pub use plugin_api::{
    AclProvider, AuthProvider, EnhancedAuthResult, EventHook, BRIDGE_CLIENT_ID_PREFIX,
    BRIDGE_HOP_COUNT_PROPERTY, BRIDGE_TRACE_ID_PROPERTY,
};
pub use rewrite::{TopicRewriteEventHook, TopicRewriteRule};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
pub use webhook::{WebHookConfig, WebHookEventHook};

fn connect_packet_v5(client_id: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(5);
    body.push(0b0000_0010);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    packet(0x10, &body)
}

fn publish_packet_v5(
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    properties: &PublishProperties,
) -> anyhow::Result<Vec<u8>> {
    let qos = qos.min(1);
    let header = 0x30 | ((qos & 0b11) << 1) | u8::from(retain);
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&1u16.to_be_bytes());
    }
    write_publish_properties(&mut body, properties)?;
    body.extend_from_slice(payload);
    Ok(packet(header, &body))
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

fn write_publish_properties(
    out: &mut Vec<u8>,
    properties: &PublishProperties,
) -> anyhow::Result<()> {
    let mut encoded = Vec::new();
    if let Some(payload_format_indicator) = properties.payload_format_indicator {
        encoded.push(0x01);
        encoded.push(payload_format_indicator);
    }
    if let Some(content_type) = &properties.content_type {
        encoded.push(0x03);
        write_utf8(&mut encoded, content_type)?;
    }
    if let Some(message_expiry_interval_secs) = properties.message_expiry_interval_secs {
        encoded.push(0x02);
        encoded.extend_from_slice(&message_expiry_interval_secs.to_be_bytes());
    }
    if let Some(response_topic) = &properties.response_topic {
        encoded.push(0x08);
        write_utf8(&mut encoded, response_topic)?;
    }
    if let Some(correlation_data) = &properties.correlation_data {
        encoded.push(0x09);
        write_binary(&mut encoded, correlation_data)?;
    }
    for property in &properties.user_properties {
        encoded.push(0x26);
        write_utf8(&mut encoded, &property.key)?;
        write_utf8(&mut encoded, &property.value)?;
    }
    encode_remaining_length(out, encoded.len());
    out.extend_from_slice(&encoded);
    Ok(())
}

fn write_utf8(out: &mut Vec<u8>, value: &str) -> anyhow::Result<()> {
    let len = u16::try_from(value.len())?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_binary(out: &mut Vec<u8>, value: &[u8]) -> anyhow::Result<()> {
    let len = u16::try_from(value.len())?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

async fn read_packet_type(stream: &mut TcpStream) -> anyhow::Result<u8> {
    let mut header = [0u8; 1];
    stream.read_exact(&mut header).await?;
    Ok(header[0] >> 4)
}

async fn read_packet_body(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let len = read_remaining_length(stream).await?;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

async fn read_remaining_length(stream: &mut TcpStream) -> anyhow::Result<usize> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        value += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
        anyhow::ensure!(multiplier <= 128 * 128 * 128, "malformed remaining length");
    }
}

fn matches_pattern(pattern: &str, value: &str) -> bool {
    pattern == "*" || pattern == value
}

struct ParsedHttpUrl {
    address: String,
    host_header: String,
    path: String,
}

fn parse_http_url(url: &str) -> anyhow::Result<ParsedHttpUrl> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("only http:// webhook urls are supported"))?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{}", path)),
        None => (rest, "/".to_string()),
    };
    anyhow::ensure!(
        !authority.trim().is_empty(),
        "webhook url authority must not be empty"
    );
    Ok(ParsedHttpUrl {
        address: authority.trim().to_string(),
        host_header: authority.trim().to_string(),
        path,
    })
}

async fn read_http_status_line(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut bytes = Vec::new();
    let mut last_four = [0u8; 4];
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        bytes.push(byte[0]);
        last_four.rotate_left(1);
        last_four[3] = byte[0];
        if last_four == *b"\r\n\r\n" {
            break;
        }
        anyhow::ensure!(bytes.len() <= 8192, "webhook response headers too large");
    }
    let response = String::from_utf8(bytes)?;
    let status_line = response
        .lines()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing webhook status line"))?;
    Ok(status_line.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        read_packet_body, read_packet_type, AclAction, AclDecision, AclProvider, AclRule,
        AuthProvider, BridgeEventHook, BridgeRule, ConfiguredAcl, ConfiguredAuth,
        ConfiguredEventHook, EnhancedAuthResult, EventHook, HookTarget, HttpAclConfig,
        HttpAclProvider, HttpAuthConfig, HttpAuthProvider, IdentityMatcher, StaticAclProvider,
        StaticAuthProvider, StaticEnhancedAuthProvider, TopicRewriteEventHook, TopicRewriteRule,
        WebHookConfig, WebHookEventHook, BRIDGE_CLIENT_ID_PREFIX, BRIDGE_HOP_COUNT_PROPERTY,
        BRIDGE_TRACE_ID_PROPERTY,
    };
    use greenmqtt_core::{
        ClientIdentity, PublishOutcome, PublishProperties, PublishRequest, SessionKind,
        SessionRecord, Subscription, UserProperty,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn bridge_hook_forwards_matching_publish() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<(String, Vec<u8>, PublishProperties)>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 1);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream
                .write_all(&[0x20, 0x03, 0x00, 0x00, 0x00])
                .await
                .unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 3);
            let body = read_packet_body(&mut stream).await.unwrap();
            let (topic, payload, properties) = parse_v5_publish_body(&body);
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 14);
            let _ = read_packet_body(&mut stream).await.unwrap();
            tx.send((topic, payload, properties)).unwrap();
        });

        let hook = BridgeEventHook::new(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
        );

        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"forwarded".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties {
                    content_type: Some("text/plain".into()),
                    message_expiry_interval_secs: Some(30),
                    response_topic: Some("devices/replies".into()),
                    correlation_data: Some(b"trace".to_vec()),
                    user_properties: vec![UserProperty {
                        key: "trace".into(),
                        value: "publisher".into(),
                    }],
                    ..PublishProperties::default()
                },
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();

        let (topic, payload, properties) =
            timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
        assert_eq!(topic, "devices/d1/state");
        assert_eq!(payload, b"forwarded");
        assert_eq!(properties.content_type.as_deref(), Some("text/plain"));
        assert_eq!(properties.message_expiry_interval_secs, Some(30));
        assert_eq!(
            properties.response_topic.as_deref(),
            Some("devices/replies")
        );
        assert_eq!(properties.correlation_data.as_deref(), Some(&b"trace"[..]));
        assert!(properties
            .user_properties
            .iter()
            .any(|property| { property.key == "trace" && property.value == "publisher" }));
        assert!(properties.user_properties.iter().any(|property| {
            property.key == BRIDGE_TRACE_ID_PROPERTY && property.value.starts_with("bridge-test-0")
        }));
        assert!(properties.user_properties.iter().any(|property| {
            property.key == BRIDGE_HOP_COUNT_PROPERTY && property.value == "1"
        }));
    }

    #[tokio::test]
    async fn bridge_hook_rewrites_forwarded_topic_when_rule_requests_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 1);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream
                .write_all(&[0x20, 0x03, 0x00, 0x00, 0x00])
                .await
                .unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 3);
            let body = read_packet_body(&mut stream).await.unwrap();
            let (topic, _, _) = parse_v5_publish_body(&body);
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 14);
            let _ = read_packet_body(&mut stream).await.unwrap();
            tx.send(topic).unwrap();
        });

        let hook = BridgeEventHook::new(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/+/raw".into(),
                rewrite_to: Some("bridge/devices/{1}".into()),
                remote_addr: addr.to_string(),
            }],
        );

        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/raw".into(),
                payload: b"forwarded".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();

        let topic = timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
        assert_eq!(topic, "bridge/devices/d1");
    }

    #[tokio::test]
    async fn bridge_hook_skips_messages_from_bridge_clients() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hook = BridgeEventHook::new(
            format!("{BRIDGE_CLIENT_ID_PREFIX}node1"),
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
        );

        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "bridge".into(),
                client_id: format!("{BRIDGE_CLIENT_ID_PREFIX}peer-1"),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"loop".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();

        let accept = timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(
            accept.is_err(),
            "bridge client publish should not be re-bridged"
        );
    }

    #[tokio::test]
    async fn bridge_hook_skips_messages_with_bridge_trace_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hook = BridgeEventHook::new(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
        );

        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"loop".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties {
                    user_properties: vec![
                        UserProperty {
                            key: BRIDGE_TRACE_ID_PROPERTY.into(),
                            value: "bridge-test-0".into(),
                        },
                        UserProperty {
                            key: BRIDGE_HOP_COUNT_PROPERTY.into(),
                            value: "1".into(),
                        },
                    ],
                    ..PublishProperties::default()
                },
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();

        let accept = timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(
            accept.is_err(),
            "publish carrying bridge trace metadata should not be re-bridged"
        );
    }

    #[tokio::test]
    async fn bridge_hook_fail_open_times_out_without_failing_publish() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let hook = BridgeEventHook::with_options(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
            Duration::from_millis(50),
            true,
            0,
            Duration::from_millis(0),
            16,
        );

        let started = std::time::Instant::now();
        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"forwarded".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();
        assert!(started.elapsed() < std::time::Duration::from_millis(200));
    }

    #[tokio::test]
    async fn bridge_hook_fail_closed_propagates_timeout_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let hook = BridgeEventHook::with_options(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
            Duration::from_millis(50),
            false,
            0,
            Duration::from_millis(0),
            16,
        );

        let error = hook
            .on_publish(
                &ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "publisher".into(),
                },
                &PublishRequest {
                    topic: "devices/d1/state".into(),
                    payload: b"forwarded".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
                &PublishOutcome {
                    matched_routes: 1,
                    online_deliveries: 1,
                    offline_enqueues: 0,
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("deadline") || error.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn bridge_hook_retries_after_transient_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 1);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream
                .write_all(&[0x20, 0x03, 0x00, 0x00, 0x00])
                .await
                .unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 3);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 14);
            let _ = read_packet_body(&mut stream).await.unwrap();
            tx.send(()).unwrap();
        });

        let hook = BridgeEventHook::with_options(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
            Duration::from_millis(250),
            false,
            1,
            Duration::from_millis(25),
            16,
        );

        let started = std::time::Instant::now();
        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"retry".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(25));

        timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn bridge_hook_fail_open_skips_when_inflight_limit_is_reached() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel::<()>();
        let (second_tx, second_rx) = oneshot::channel::<bool>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            accepted_tx.send(()).unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 1);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream
                .write_all(&[0x20, 0x03, 0x00, 0x00, 0x00])
                .await
                .unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 3);
            let _ = read_packet_body(&mut stream).await.unwrap();
            let second_accept = timeout(Duration::from_millis(75), listener.accept()).await;
            second_tx.send(second_accept.is_ok()).unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 14);
            let _ = read_packet_body(&mut stream).await.unwrap();
        });

        let hook = BridgeEventHook::with_options(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
            Duration::from_millis(500),
            true,
            0,
            Duration::from_millis(0),
            1,
        );
        let hook_clone = hook.clone();
        let first = tokio::spawn(async move {
            hook_clone
                .on_publish(
                    &ClientIdentity {
                        tenant_id: "tenant-a".into(),
                        user_id: "alice".into(),
                        client_id: "publisher-1".into(),
                    },
                    &PublishRequest {
                        topic: "devices/d1/state".into(),
                        payload: b"first".to_vec().into(),
                        qos: 1,
                        retain: false,
                        properties: PublishProperties::default(),
                    },
                    &PublishOutcome {
                        matched_routes: 1,
                        online_deliveries: 1,
                        offline_enqueues: 0,
                    },
                )
                .await
        });
        timeout(Duration::from_secs(1), accepted_rx)
            .await
            .unwrap()
            .unwrap();

        let started = std::time::Instant::now();
        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher-2".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"second".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_millis(75));
        assert!(!timeout(Duration::from_secs(1), second_rx)
            .await
            .unwrap()
            .unwrap());
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn bridge_hook_fail_closed_returns_error_when_inflight_limit_is_reached() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            accepted_tx.send(()).unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 1);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream
                .write_all(&[0x20, 0x03, 0x00, 0x00, 0x00])
                .await
                .unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 3);
            let _ = read_packet_body(&mut stream).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 14);
            let _ = read_packet_body(&mut stream).await.unwrap();
        });

        let hook = BridgeEventHook::with_options(
            "bridge-test",
            vec![BridgeRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/#".into(),
                rewrite_to: None,
                remote_addr: addr.to_string(),
            }],
            Duration::from_millis(500),
            false,
            0,
            Duration::from_millis(0),
            1,
        );
        let hook_clone = hook.clone();
        let first = tokio::spawn(async move {
            hook_clone
                .on_publish(
                    &ClientIdentity {
                        tenant_id: "tenant-a".into(),
                        user_id: "alice".into(),
                        client_id: "publisher-1".into(),
                    },
                    &PublishRequest {
                        topic: "devices/d1/state".into(),
                        payload: b"first".to_vec().into(),
                        qos: 1,
                        retain: false,
                        properties: PublishProperties::default(),
                    },
                    &PublishOutcome {
                        matched_routes: 1,
                        online_deliveries: 1,
                        offline_enqueues: 0,
                    },
                )
                .await
        });
        timeout(Duration::from_secs(1), accepted_rx)
            .await
            .unwrap()
            .unwrap();

        let error = hook
            .on_publish(
                &ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "publisher-2".into(),
                },
                &PublishRequest {
                    topic: "devices/d1/state".into(),
                    payload: b"second".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
                &PublishOutcome {
                    matched_routes: 1,
                    online_deliveries: 1,
                    offline_enqueues: 0,
                },
            )
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("bridge inflight limit 1 reached"));
        first.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn webhook_hook_posts_publish_event() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            tx.send(request).unwrap();
        });

        let hook = WebHookEventHook::new(WebHookConfig {
            url: format!("http://{addr}/hook"),
            connect: false,
            disconnect: false,
            subscribe: false,
            unsubscribe: false,
            publish: true,
            offline: false,
            retain: false,
        });
        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"webhook".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();

        let request = timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
        assert!(request.starts_with("POST /hook HTTP/1.1\r\n"));
        assert!(request.contains("\"event\":\"publish\""));
        assert!(request.contains("\"topic\":\"devices/d1/state\""));
    }

    #[tokio::test]
    async fn webhook_hook_posts_lifecycle_events() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await.unwrap();
                stream
                    .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
                tx.send(request).unwrap();
            }
        });

        let hook = WebHookEventHook::new(WebHookConfig {
            url: format!("http://{addr}/events"),
            connect: true,
            disconnect: true,
            subscribe: true,
            unsubscribe: true,
            publish: false,
            offline: false,
            retain: false,
        });
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "cli".into(),
        };
        let session = SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Transient,
            identity: identity.clone(),
            session_expiry_interval_secs: Some(0),
            expires_at_ms: None,
        };
        let subscription = Subscription {
            session_id: "s1".into(),
            tenant_id: "tenant-a".into(),
            topic_filter: "devices/#".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Transient,
        };

        hook.on_connect(&session).await.unwrap();
        hook.on_subscribe(&identity, &subscription).await.unwrap();
        hook.on_unsubscribe(&identity, &subscription).await.unwrap();
        hook.on_disconnect(&session).await.unwrap();

        let mut requests = Vec::new();
        for _ in 0..4 {
            requests.push(
                timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(requests
            .iter()
            .any(|request| request.contains("\"event\":\"connect\"")));
        assert!(requests
            .iter()
            .any(|request| request.contains("\"event\":\"disconnect\"")));
        assert!(requests
            .iter()
            .any(|request| request.contains("\"event\":\"subscribe\"")));
        assert!(requests
            .iter()
            .any(|request| request.contains("\"event\":\"unsubscribe\"")));
    }

    #[tokio::test]
    async fn configured_event_hook_runs_bridge_and_webhook_together() {
        let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mqtt_addr = mqtt_listener.local_addr().unwrap();
        let webhook_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let webhook_addr = webhook_listener.local_addr().unwrap();
        let (mqtt_tx, mqtt_rx) = oneshot::channel::<()>();
        let (webhook_tx, webhook_rx) = oneshot::channel::<String>();

        tokio::spawn(async move {
            let (mut stream, _) = mqtt_listener.accept().await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 1);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 3);
            let _ = read_packet_body(&mut stream).await.unwrap();
            stream.write_all(&[0x40, 0x02, 0x00, 0x01]).await.unwrap();
            assert_eq!(read_packet_type(&mut stream).await.unwrap(), 14);
            let _ = read_packet_body(&mut stream).await.unwrap();
            mqtt_tx.send(()).unwrap();
        });
        tokio::spawn(async move {
            let (mut stream, _) = webhook_listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            webhook_tx.send(request).unwrap();
        });

        let hook = ConfiguredEventHook::new(vec![
            HookTarget::Bridge(BridgeEventHook::new(
                "bridge-test",
                vec![BridgeRule {
                    tenant_id: Some("tenant-a".into()),
                    topic_filter: "devices/#".into(),
                    rewrite_to: None,
                    remote_addr: mqtt_addr.to_string(),
                }],
            )),
            HookTarget::WebHook(WebHookEventHook::new(WebHookConfig {
                url: format!("http://{webhook_addr}/events"),
                connect: false,
                disconnect: false,
                subscribe: false,
                unsubscribe: false,
                publish: true,
                offline: false,
                retain: false,
            })),
        ]);
        hook.on_publish(
            &ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "publisher".into(),
            },
            &PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"combined".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
            &PublishOutcome {
                matched_routes: 1,
                online_deliveries: 1,
                offline_enqueues: 0,
            },
        )
        .await
        .unwrap();

        timeout(Duration::from_secs(2), mqtt_rx)
            .await
            .unwrap()
            .unwrap();
        let request = timeout(Duration::from_secs(2), webhook_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(request.contains("\"event\":\"publish\""));
    }

    #[tokio::test]
    async fn topic_rewrite_hook_rewrites_matching_publish_topics() {
        let hook = TopicRewriteEventHook::new(vec![TopicRewriteRule {
            tenant_id: Some("tenant-a".into()),
            topic_filter: "devices/+/raw".into(),
            rewrite_to: "devices/{1}/normalized".into(),
        }]);
        let rewritten = hook
            .rewrite_publish(
                &ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "publisher".into(),
                },
                &PublishRequest {
                    topic: "devices/d1/raw".into(),
                    payload: b"rewrite".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(rewritten.topic, "devices/d1/normalized");
        assert_eq!(&rewritten.payload[..], b"rewrite");
    }

    #[tokio::test]
    async fn configured_event_hook_chains_multiple_topic_rewrites() {
        let hook = ConfiguredEventHook::new(vec![
            HookTarget::TopicRewrite(TopicRewriteEventHook::new(vec![TopicRewriteRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/+/raw".into(),
                rewrite_to: "devices/{1}/normalized".into(),
            }])),
            HookTarget::TopicRewrite(TopicRewriteEventHook::new(vec![TopicRewriteRule {
                tenant_id: Some("tenant-a".into()),
                topic_filter: "devices/+/normalized".into(),
                rewrite_to: "tenant/devices/{1}".into(),
            }])),
        ]);
        let rewritten = hook
            .rewrite_publish(
                &ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "publisher".into(),
                },
                &PublishRequest {
                    topic: "devices/d1/raw".into(),
                    payload: b"rewrite".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(rewritten.topic, "tenant/devices/d1");
    }

    #[tokio::test]
    async fn static_auth_matches_identity_allowlist() {
        let auth = StaticAuthProvider::new(vec![IdentityMatcher {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "*".into(),
        }]);
        assert!(auth
            .authenticate(&ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "c1".into(),
            })
            .await
            .unwrap());
        assert!(!auth
            .authenticate(&ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "bob".into(),
                client_id: "c1".into(),
            })
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn static_auth_denied_identities_override_allowlist() {
        let auth = StaticAuthProvider::with_denied(
            vec![IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "*".into(),
                client_id: "*".into(),
            }],
            vec![IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "blocked".into(),
                client_id: "*".into(),
            }],
        );
        assert!(auth
            .authenticate(&ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "c1".into(),
            })
            .await
            .unwrap());
        assert!(!auth
            .authenticate(&ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "blocked".into(),
                client_id: "c1".into(),
            })
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn static_enhanced_auth_provider_challenges_then_accepts_expected_response() {
        let auth = StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "*".into(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        );
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "c1".into(),
        };
        let begin = auth
            .begin_enhanced_auth(&identity, "custom", Some(b"client-hello"))
            .await
            .unwrap();
        assert_eq!(
            begin,
            EnhancedAuthResult::Continue {
                auth_data: Some(b"server-challenge".to_vec()),
                reason_string: Some("continue authentication".to_string()),
            }
        );
        let finish = auth
            .continue_enhanced_auth(&identity, "custom", Some(b"client-response"))
            .await
            .unwrap();
        assert_eq!(finish, EnhancedAuthResult::Success);
    }

    #[tokio::test]
    async fn static_enhanced_auth_denied_identity_is_rejected() {
        let auth = StaticEnhancedAuthProvider::with_denied(
            vec![IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "*".into(),
                client_id: "*".into(),
            }],
            vec![IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "blocked".into(),
                client_id: "*".into(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        );
        let error = auth
            .begin_enhanced_auth(
                &ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "blocked".into(),
                    client_id: "c1".into(),
                },
                "custom",
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("authentication failed"));
    }

    #[tokio::test]
    async fn static_acl_default_allow_applies_when_no_rule_matches() {
        let acl = StaticAclProvider::with_default_decision(Vec::new(), true);
        assert!(acl
            .can_publish(
                &ClientIdentity {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "c1".into(),
                },
                "devices/d1/state",
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn configured_enhanced_auth_delegates_to_static_provider() {
        let auth = ConfiguredAuth::EnhancedStatic(StaticEnhancedAuthProvider::new(
            vec![IdentityMatcher {
                tenant_id: "*".into(),
                user_id: "*".into(),
                client_id: "*".into(),
            }],
            "custom",
            b"server-challenge".to_vec(),
            b"client-response".to_vec(),
        ));
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "c1".into(),
        };
        let begin = auth
            .begin_enhanced_auth(&identity, "custom", None)
            .await
            .unwrap();
        assert_eq!(
            begin,
            EnhancedAuthResult::Continue {
                auth_data: Some(b"server-challenge".to_vec()),
                reason_string: Some("continue authentication".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn http_auth_provider_posts_identity_and_returns_decision() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 14\r\n\r\n{\"allow\":true}",
                )
                .await
                .unwrap();
            tx.send(request).unwrap();
        });

        let auth = HttpAuthProvider::new(HttpAuthConfig {
            url: format!("http://{addr}/auth"),
        });
        let allow = auth
            .authenticate(&ClientIdentity {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "cli".into(),
            })
            .await
            .unwrap();
        assert!(allow);

        let request = timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
        assert!(request.starts_with("POST /auth HTTP/1.1\r\n"));
        assert!(request.contains("\"tenant_id\":\"tenant-a\""));
        assert!(request.contains("\"user_id\":\"alice\""));
        assert!(request.contains("\"client_id\":\"cli\""));
    }

    #[tokio::test]
    async fn http_auth_provider_posts_enhanced_auth_requests_and_maps_challenge_flow() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await.unwrap();
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

        let auth = HttpAuthProvider::new(HttpAuthConfig {
            url: format!("http://{addr}/auth"),
        });
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "cli".into(),
        };
        let begin = auth
            .begin_enhanced_auth(&identity, "custom", Some(b"client-hello"))
            .await
            .unwrap();
        assert_eq!(
            begin,
            EnhancedAuthResult::Continue {
                auth_data: Some(b"server".to_vec()),
                reason_string: Some("challenge".to_string()),
            }
        );
        let finish = auth
            .continue_enhanced_auth(&identity, "custom", Some(b"client-response"))
            .await
            .unwrap();
        assert_eq!(finish, EnhancedAuthResult::Success);

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
        assert!(
            begin_request.contains("\"auth_data\":[99,108,105,101,110,116,45,104,101,108,108,111]")
        );
        assert!(continue_request.contains("\"phase\":\"continue\""));
        assert!(continue_request.contains("\"method\":\"custom\""));
        assert!(continue_request
            .contains("\"auth_data\":[99,108,105,101,110,116,45,114,101,115,112,111,110,115,101]"));
    }

    #[tokio::test]
    async fn http_acl_provider_posts_subscribe_and_publish_decisions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await.unwrap();
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 14\r\n\r\n{\"allow\":true}",
                    )
                    .await
                    .unwrap();
                tx.send(request).unwrap();
            }
        });

        let acl = HttpAclProvider::new(HttpAclConfig {
            url: format!("http://{addr}/acl"),
        });
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "cli".into(),
        };
        let subscription = Subscription {
            session_id: "s1".into(),
            tenant_id: "tenant-a".into(),
            topic_filter: "devices/#".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        };
        assert!(acl.can_subscribe(&identity, &subscription).await.unwrap());
        assert!(acl
            .can_publish(&identity, "devices/d1/state")
            .await
            .unwrap());

        let subscribe_request = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let publish_request = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(subscribe_request.starts_with("POST /acl HTTP/1.1\r\n"));
        assert!(subscribe_request.contains("\"action\":\"subscribe\""));
        assert!(subscribe_request.contains("\"topic_filter\":\"devices/#\""));
        assert!(publish_request.starts_with("POST /acl HTTP/1.1\r\n"));
        assert!(publish_request.contains("\"action\":\"publish\""));
        assert!(publish_request.contains("\"topic\":\"devices/d1/state\""));
    }

    #[tokio::test]
    async fn static_acl_applies_first_matching_rule() {
        let acl = StaticAclProvider::new(vec![
            AclRule {
                decision: AclDecision::Deny,
                action: AclAction::Publish,
                identity: IdentityMatcher {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "*".into(),
                },
                topic_filter: "devices/private/#".into(),
            },
            AclRule {
                decision: AclDecision::Allow,
                action: AclAction::Publish,
                identity: IdentityMatcher {
                    tenant_id: "tenant-a".into(),
                    user_id: "alice".into(),
                    client_id: "*".into(),
                },
                topic_filter: "devices/#".into(),
            },
        ]);
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "c1".into(),
        };
        assert!(acl
            .can_publish(&identity, "devices/d1/state")
            .await
            .unwrap());
        assert!(!acl
            .can_publish(&identity, "devices/private/d1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn static_acl_denies_when_no_rule_matches() {
        let acl = StaticAclProvider::new(vec![AclRule {
            decision: AclDecision::Allow,
            action: AclAction::Subscribe,
            identity: IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "alice".into(),
                client_id: "*".into(),
            },
            topic_filter: "devices/public/#".into(),
        }]);
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "c1".into(),
        };
        assert!(!acl
            .can_subscribe(
                &identity,
                &Subscription {
                    session_id: "s1".into(),
                    tenant_id: "tenant-a".into(),
                    topic_filter: "devices/private/#".into(),
                    qos: 1,
                    subscription_identifier: None,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: 0,
                    shared_group: None,
                    kind: SessionKind::Transient,
                },
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn configured_auth_and_acl_delegate_to_static_providers() {
        let auth = ConfiguredAuth::Static(StaticAuthProvider::new(vec![IdentityMatcher {
            tenant_id: "tenant-a".into(),
            user_id: "*".into(),
            client_id: "*".into(),
        }]));
        let acl = ConfiguredAcl::Static(StaticAclProvider::new(vec![AclRule {
            decision: AclDecision::Allow,
            action: AclAction::Publish,
            identity: IdentityMatcher {
                tenant_id: "tenant-a".into(),
                user_id: "*".into(),
                client_id: "*".into(),
            },
            topic_filter: "devices/#".into(),
        }]));
        let identity = ClientIdentity {
            tenant_id: "tenant-a".into(),
            user_id: "alice".into(),
            client_id: "c1".into(),
        };
        assert!(auth.authenticate(&identity).await.unwrap());
        assert!(acl.can_publish(&identity, "devices/d1").await.unwrap());
    }

    async fn read_http_request(stream: &mut TcpStream) -> anyhow::Result<String> {
        let mut buffer = Vec::new();
        let mut header_end = None;
        while header_end.is_none() {
            let mut chunk = [0u8; 256];
            let read = stream.read(&mut chunk).await?;
            anyhow::ensure!(read > 0, "unexpected eof while reading request headers");
            buffer.extend_from_slice(&chunk[..read]);
            header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
        }
        let header_end = header_end.unwrap() + 4;
        let header_text = String::from_utf8(buffer[..header_end].to_vec())?;
        let content_length = header_text
            .lines()
            .find_map(|line| {
                line.strip_prefix("Content-Length: ")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        while buffer.len() < header_end + content_length {
            let mut chunk = [0u8; 256];
            let read = stream.read(&mut chunk).await?;
            anyhow::ensure!(read > 0, "unexpected eof while reading request body");
            buffer.extend_from_slice(&chunk[..read]);
        }
        Ok(String::from_utf8(buffer)?)
    }

    fn parse_v5_publish_body(body: &[u8]) -> (String, Vec<u8>, PublishProperties) {
        let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let topic = String::from_utf8(body[2..2 + topic_len].to_vec()).unwrap();
        let mut cursor = 2 + topic_len;
        let _packet_id = u16::from_be_bytes([body[cursor], body[cursor + 1]]);
        cursor += 2;
        let properties_len = decode_varint_from_slice(body, &mut cursor);
        let properties_end = cursor + properties_len;
        let mut properties = PublishProperties::default();
        while cursor < properties_end {
            match body[cursor] {
                0x01 => {
                    cursor += 1;
                    properties.payload_format_indicator = Some(body[cursor]);
                    cursor += 1;
                }
                0x02 => {
                    cursor += 1;
                    properties.message_expiry_interval_secs = Some(u32::from_be_bytes([
                        body[cursor],
                        body[cursor + 1],
                        body[cursor + 2],
                        body[cursor + 3],
                    ]));
                    cursor += 4;
                }
                0x03 => {
                    cursor += 1;
                    properties.content_type = Some(read_utf8_from_slice(body, &mut cursor));
                }
                0x08 => {
                    cursor += 1;
                    properties.response_topic = Some(read_utf8_from_slice(body, &mut cursor));
                }
                0x09 => {
                    cursor += 1;
                    let len = u16::from_be_bytes([body[cursor], body[cursor + 1]]) as usize;
                    cursor += 2;
                    properties.correlation_data = Some(body[cursor..cursor + len].to_vec());
                    cursor += len;
                }
                0x26 => {
                    cursor += 1;
                    properties.user_properties.push(UserProperty {
                        key: read_utf8_from_slice(body, &mut cursor),
                        value: read_utf8_from_slice(body, &mut cursor),
                    });
                }
                other => panic!("unexpected publish property {other:#x}"),
            }
        }
        (topic, body[cursor..].to_vec(), properties)
    }

    fn decode_varint_from_slice(body: &[u8], cursor: &mut usize) -> usize {
        let mut multiplier = 1usize;
        let mut value = 0usize;
        loop {
            let byte = body[*cursor];
            *cursor += 1;
            value += ((byte & 0x7F) as usize) * multiplier;
            if byte & 0x80 == 0 {
                return value;
            }
            multiplier *= 128;
        }
    }

    fn read_utf8_from_slice(body: &[u8], cursor: &mut usize) -> String {
        let len = u16::from_be_bytes([body[*cursor], body[*cursor + 1]]) as usize;
        *cursor += 2;
        let value = String::from_utf8(body[*cursor..*cursor + len].to_vec()).unwrap();
        *cursor += len;
        value
    }
}
