use crate::InboxTransportService;
use greenmqtt_capnp::{
    decode_inbox_ack_inflight_request, decode_inbox_attach_request, decode_inbox_detach_request,
    decode_inbox_purge_session_request, encode_inbox_ack_inflight_request,
    encode_inbox_attach_request, encode_inbox_detach_request, encode_inbox_purge_session_request,
    InboxAckInflightRequestEnvelope, InboxAttachRequestEnvelope, InboxDetachRequestEnvelope,
    InboxPurgeSessionRequestEnvelope, RpcFrameEnvelope, RpcFrameKind, RpcRequestHeaderEnvelope,
    RpcResponseHeaderEnvelope, RpcServiceKind, RpcStatusCode,
};
use greenmqtt_core::{normalize_service_endpoint, ParsedServiceEndpoint, ServiceEndpointTransport};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const INBOX_ATTACH_METHOD: u16 = 1;
const INBOX_DETACH_METHOD: u16 = 2;
const INBOX_PURGE_SESSION_METHOD: u16 = 3;
const INBOX_ACK_INFLIGHT_METHOD: u16 = 4;

static QUIC_RUSTLS_PROVIDER: Once = Once::new();

pub(crate) struct QuicInboxServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicInboxServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicInboxClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicInboxClient {
    pub(crate) async fn connect_with_client_config(
        endpoint: impl Into<String>,
        server_name: &str,
        client_config: ClientConfig,
    ) -> anyhow::Result<Self> {
        ensure_quic_crypto_provider_installed();
        let normalized =
            normalize_service_endpoint(&endpoint.into(), ServiceEndpointTransport::Quic)?;
        let parsed = ParsedServiceEndpoint::parse(&normalized)?;
        anyhow::ensure!(
            parsed.transport == ServiceEndpointTransport::Quic,
            "Inbox QUIC client requires quic endpoint: {normalized}"
        );
        let addr: SocketAddr = parsed
            .authority
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid quic socket address: {}", parsed.authority))?;
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_config);
        let connection = endpoint.connect(addr, server_name)?.await?;
        Ok(Self {
            _endpoint: endpoint,
            connection,
            next_request_id: AtomicU64::new(1),
        })
    }

    pub(crate) async fn attach(&self, session_id: &str) -> anyhow::Result<()> {
        let payload = encode_inbox_attach_request(&InboxAttachRequestEnvelope {
            session_id: session_id.to_string(),
        })?;
        let _ = self.send_request(INBOX_ATTACH_METHOD, payload).await?;
        Ok(())
    }

    pub(crate) async fn detach(&self, session_id: &str) -> anyhow::Result<()> {
        let payload = encode_inbox_detach_request(&InboxDetachRequestEnvelope {
            session_id: session_id.to_string(),
        })?;
        let _ = self.send_request(INBOX_DETACH_METHOD, payload).await?;
        Ok(())
    }

    pub(crate) async fn purge_session(&self, session_id: &str) -> anyhow::Result<()> {
        let payload = encode_inbox_purge_session_request(&InboxPurgeSessionRequestEnvelope {
            session_id: session_id.to_string(),
        })?;
        let _ = self
            .send_request(INBOX_PURGE_SESSION_METHOD, payload)
            .await?;
        Ok(())
    }

    pub(crate) async fn ack_inflight(
        &self,
        session_id: &str,
        packet_id: u16,
    ) -> anyhow::Result<()> {
        let payload = encode_inbox_ack_inflight_request(&InboxAckInflightRequestEnvelope {
            session_id: session_id.to_string(),
            packet_id,
        })?;
        let _ = self
            .send_request(INBOX_ACK_INFLIGHT_METHOD, payload)
            .await?;
        Ok(())
    }

    async fn send_request(
        &self,
        method_id: u16,
        payload: Vec<u8>,
    ) -> anyhow::Result<(RpcResponseHeaderEnvelope, Vec<u8>)> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request_bytes = RpcFrameEnvelope {
            kind: RpcFrameKind::RequestBody,
            request_header: Some(RpcRequestHeaderEnvelope {
                service: RpcServiceKind::Inbox,
                method_id,
                protocol_version: QUIC_RPC_PROTOCOL_VERSION,
                request_id,
                timeout_ms: 0,
                trace_id: String::new(),
                expected_epoch: 0,
                fencing_token: 0,
            }),
            response_header: None,
            payload,
        }
        .encode()?;
        let (mut send, mut recv) = self.connection.open_bi().await?;
        send.write_all(&request_bytes).await?;
        send.finish()?;
        let response_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
        let response_frame = RpcFrameEnvelope::decode(&response_bytes)?;
        let response_header = response_frame
            .response_header
            .ok_or_else(|| anyhow::anyhow!("missing quic rpc response header"))?;
        match response_header.status {
            RpcStatusCode::Ok => Ok((response_header, response_frame.payload)),
            status => anyhow::bail!(
                "quic/rpc status={status:?} message={}",
                response_header.error_message
            ),
        }
    }
}

pub(crate) fn spawn_inbox_quic_server(
    service: Arc<dyn InboxTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicInboxServerHandle> {
    let endpoint = Endpoint::server(
        crate::quic_range_admin::load_quic_server_config(cert_path.as_ref(), key_path.as_ref())?,
        bind,
    )?;
    let local_addr = endpoint.local_addr()?;
    let task = tokio::spawn(async move {
        while let Some(connecting) = endpoint.accept().await {
            let service = service.clone();
            tokio::spawn(async move {
                let connection = match connecting.await {
                    Ok(connection) => connection,
                    Err(error) => {
                        eprintln!("greenmqtt inbox quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_inbox_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt inbox quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicInboxServerHandle { local_addr, task })
}

async fn handle_inbox_stream(
    service: &dyn InboxTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_inbox_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_inbox_request(
    service: &dyn InboxTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::Inbox,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        INBOX_ATTACH_METHOD => {
            let request = decode_inbox_attach_request(&request.payload)?;
            service.attach_session(&request.session_id).await?;
            Ok(Vec::new())
        }
        INBOX_DETACH_METHOD => {
            let request = decode_inbox_detach_request(&request.payload)?;
            service.detach_session(&request.session_id).await?;
            Ok(Vec::new())
        }
        INBOX_PURGE_SESSION_METHOD => {
            let request = decode_inbox_purge_session_request(&request.payload)?;
            service.purge_session_state(&request.session_id).await?;
            Ok(Vec::new())
        }
        INBOX_ACK_INFLIGHT_METHOD => {
            let request = decode_inbox_ack_inflight_request(&request.payload)?;
            service
                .ack_inflight_packet(&request.session_id, request.packet_id)
                .await?;
            Ok(Vec::new())
        }
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported inbox method id {}", header.method_id),
            );
        }
    };
    match result {
        Ok(payload) => encode_response_ok(payload),
        Err(error) => encode_response_error(RpcStatusCode::Internal, error.to_string()),
    }
}

fn encode_response_ok(payload: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    RpcFrameEnvelope {
        kind: RpcFrameKind::ResponseBody,
        request_header: None,
        response_header: Some(RpcResponseHeaderEnvelope {
            status: RpcStatusCode::Ok,
            retryable: false,
            retry_after_ms: 0,
            error_message: String::new(),
        }),
        payload,
    }
    .encode()
}

fn encode_response_error(status: RpcStatusCode, message: String) -> anyhow::Result<Vec<u8>> {
    RpcFrameEnvelope {
        kind: RpcFrameKind::ResponseBody,
        request_header: None,
        response_header: Some(RpcResponseHeaderEnvelope {
            status,
            retryable: false,
            retry_after_ms: 0,
            error_message: message,
        }),
        payload: Vec::new(),
    }
    .encode()
}

fn ensure_quic_crypto_provider_installed() {
    QUIC_RUSTLS_PROVIDER.call_once(|| {
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InboxRpc;
    use async_trait::async_trait;
    use greenmqtt_core::{PublishProperties, PublishRequest};
    use greenmqtt_inbox::{
        DelayedLwtPublish, DelayedLwtSink, InboxHandle, InboxLwtResult, InboxService,
    };
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[derive(Clone, Default)]
    struct RecordingLwtSink {
        publishes: Arc<Mutex<Vec<DelayedLwtPublish>>>,
    }

    #[async_trait]
    impl DelayedLwtSink for RecordingLwtSink {
        async fn send_lwt(&self, publish: &DelayedLwtPublish) -> anyhow::Result<()> {
            self.publishes.lock().unwrap().push(publish.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn inbox_quic_attach_detach_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let inbox = Arc::new(InboxHandle::default());
        let service: Arc<dyn crate::InboxTransportService> = Arc::new(InboxRpc {
            inner: inbox.clone(),
            assignment_registry: None,
            lwt_sink: None,
        });
        let server = spawn_inbox_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicInboxClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();
        let sink = RecordingLwtSink::default();

        client.attach("s1").await.unwrap();
        inbox
            .register_delayed_lwt(
                1,
                DelayedLwtPublish {
                    tenant_id: "t1".into(),
                    session_id: "s1".into(),
                    publish: PublishRequest {
                        topic: "devices/d1/lwt".into(),
                        payload: b"bye".to_vec().into(),
                        qos: 1,
                        retain: false,
                        properties: PublishProperties::default(),
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(
            inbox
                .send_delayed_lwt(&"s1".to_string(), 1, &sink)
                .await
                .unwrap(),
            InboxLwtResult::NoDetach
        );

        client.detach("s1").await.unwrap();
        assert_eq!(
            inbox
                .send_delayed_lwt(&"s1".to_string(), 1, &sink)
                .await
                .unwrap(),
            InboxLwtResult::Ok
        );
        assert_eq!(sink.publishes.lock().unwrap().len(), 1);

        server.abort();
    }

    #[tokio::test]
    async fn inbox_quic_purge_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let inbox = Arc::new(InboxHandle::default());
        inbox
            .subscribe(greenmqtt_core::Subscription {
                session_id: "s1".into(),
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                qos: 1,
                subscription_identifier: Some(1),
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: None,
                kind: greenmqtt_core::SessionKind::Persistent,
            })
            .await
            .unwrap();
        inbox
            .enqueue(greenmqtt_core::OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
        inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                packet_id: 7,
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();

        let service: Arc<dyn crate::InboxTransportService> = Arc::new(InboxRpc {
            inner: inbox.clone(),
            assignment_registry: None,
            lwt_sink: None,
        });
        let server = spawn_inbox_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicInboxClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        client.purge_session("s1").await.unwrap();
        assert!(inbox
            .list_subscriptions(&"s1".to_string())
            .await
            .unwrap()
            .is_empty());
        assert!(inbox.peek(&"s1".to_string()).await.unwrap().is_empty());
        assert!(inbox
            .fetch_inflight(&"s1".to_string())
            .await
            .unwrap()
            .is_empty());

        server.abort();
    }

    #[tokio::test]
    async fn inbox_quic_ack_inflight_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let inbox = Arc::new(InboxHandle::default());
        inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                packet_id: 7,
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();

        let service: Arc<dyn crate::InboxTransportService> = Arc::new(InboxRpc {
            inner: inbox.clone(),
            assignment_registry: None,
            lwt_sink: None,
        });
        let server = spawn_inbox_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicInboxClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        client.ack_inflight("s1", 7).await.unwrap();
        assert!(inbox
            .fetch_inflight(&"s1".to_string())
            .await
            .unwrap()
            .is_empty());

        server.abort();
    }

    fn write_self_signed_tls_material() -> (
        TempDir,
        PathBuf,
        PathBuf,
        rcgen::CertifiedKey<rcgen::KeyPair>,
    ) {
        let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();
        let tempdir = tempfile::tempdir().unwrap();
        let cert_path = tempdir.path().join("server.crt");
        let key_path = tempdir.path().join("server.key");
        std::fs::write(&cert_path, cert_pem.as_bytes()).unwrap();
        std::fs::write(&key_path, key_pem.as_bytes()).unwrap();
        (tempdir, cert_path, key_path, cert)
    }
}
