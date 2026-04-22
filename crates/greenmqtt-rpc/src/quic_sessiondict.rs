use crate::SessionDictTransportService;
use greenmqtt_capnp::{
    decode_list_sessions_reply, decode_list_sessions_request, decode_lookup_session_by_id_request,
    decode_lookup_session_reply, decode_lookup_session_request, decode_register_session_reply,
    decode_register_session_request, decode_unregister_session_request, encode_list_sessions_reply,
    encode_list_sessions_request, encode_lookup_session_by_id_request, encode_lookup_session_reply,
    encode_lookup_session_request, encode_register_session_reply, encode_register_session_request,
    encode_unregister_session_request, ListSessionsReplyEnvelope, ListSessionsRequestEnvelope,
    LookupSessionByIdRequestEnvelope, LookupSessionReplyEnvelope, LookupSessionRequestEnvelope,
    RegisterSessionReplyEnvelope, RegisterSessionRequestEnvelope, RpcFrameEnvelope, RpcFrameKind,
    RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope, RpcServiceKind, RpcStatusCode,
    UnregisterSessionRequestEnvelope,
};
use greenmqtt_core::{
    normalize_service_endpoint, ClientIdentity, ParsedServiceEndpoint, ServiceEndpointTransport,
    SessionRecord,
};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const SESSIONDICT_REGISTER_METHOD: u16 = 1;
const SESSIONDICT_UNREGISTER_METHOD: u16 = 2;
const SESSIONDICT_LOOKUP_IDENTITY_METHOD: u16 = 3;
const SESSIONDICT_LOOKUP_BY_ID_METHOD: u16 = 4;
const SESSIONDICT_LIST_METHOD: u16 = 5;

static QUIC_RUSTLS_PROVIDER: Once = Once::new();

pub(crate) struct QuicSessionDictServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicSessionDictServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicSessionDictClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicSessionDictClient {
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
            "SessionDict QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn register(
        &self,
        record: SessionRecord,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let payload = encode_register_session_request(&RegisterSessionRequestEnvelope { record })?;
        let (_, payload) = self
            .send_request(SESSIONDICT_REGISTER_METHOD, payload)
            .await?;
        Ok(decode_register_session_reply(&payload)?.replaced)
    }

    pub(crate) async fn unregister(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let payload = encode_unregister_session_request(&UnregisterSessionRequestEnvelope {
            session_id: session_id.to_string(),
        })?;
        let (_, payload) = self
            .send_request(SESSIONDICT_UNREGISTER_METHOD, payload)
            .await?;
        Ok(decode_register_session_reply(&payload)?.replaced)
    }

    pub(crate) async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let payload = encode_lookup_session_request(&LookupSessionRequestEnvelope {
            identity: identity.clone(),
        })?;
        let (_, payload) = self
            .send_request(SESSIONDICT_LOOKUP_IDENTITY_METHOD, payload)
            .await?;
        Ok(decode_lookup_session_reply(&payload)?.record)
    }

    pub(crate) async fn lookup_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let payload = encode_lookup_session_by_id_request(&LookupSessionByIdRequestEnvelope {
            session_id: session_id.to_string(),
        })?;
        let (_, payload) = self
            .send_request(SESSIONDICT_LOOKUP_BY_ID_METHOD, payload)
            .await?;
        Ok(decode_lookup_session_reply(&payload)?.record)
    }

    pub(crate) async fn list_sessions(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<SessionRecord>> {
        let payload = encode_list_sessions_request(&ListSessionsRequestEnvelope {
            tenant_id: tenant_id.map(str::to_string),
        })?;
        let (_, payload) = self.send_request(SESSIONDICT_LIST_METHOD, payload).await?;
        Ok(decode_list_sessions_reply(&payload)?.records)
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
                service: RpcServiceKind::SessionDict,
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

pub(crate) fn spawn_sessiondict_quic_server(
    service: Arc<dyn SessionDictTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicSessionDictServerHandle> {
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
                        eprintln!("greenmqtt sessiondict quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_sessiondict_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt sessiondict quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicSessionDictServerHandle { local_addr, task })
}

async fn handle_sessiondict_stream(
    service: &dyn SessionDictTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_sessiondict_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_sessiondict_request(
    service: &dyn SessionDictTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::SessionDict,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        SESSIONDICT_REGISTER_METHOD => {
            let request = decode_register_session_request(&request.payload)?;
            Ok(encode_register_session_reply(
                &RegisterSessionReplyEnvelope {
                    replaced: service.register_session_record(request.record).await?,
                },
            )?)
        }
        SESSIONDICT_UNREGISTER_METHOD => {
            let request = decode_unregister_session_request(&request.payload)?;
            Ok(encode_register_session_reply(
                &RegisterSessionReplyEnvelope {
                    replaced: service
                        .unregister_session_record(&request.session_id)
                        .await?,
                },
            )?)
        }
        SESSIONDICT_LOOKUP_IDENTITY_METHOD => {
            let request = decode_lookup_session_request(&request.payload)?;
            Ok(encode_lookup_session_reply(&LookupSessionReplyEnvelope {
                record: service
                    .lookup_session_record_by_identity(&request.identity)
                    .await?,
            })?)
        }
        SESSIONDICT_LOOKUP_BY_ID_METHOD => {
            let request = decode_lookup_session_by_id_request(&request.payload)?;
            Ok(encode_lookup_session_reply(&LookupSessionReplyEnvelope {
                record: service
                    .lookup_session_record_by_id(&request.session_id)
                    .await?,
            })?)
        }
        SESSIONDICT_LIST_METHOD => {
            let request = decode_list_sessions_request(&request.payload)?;
            Ok(encode_list_sessions_reply(&ListSessionsReplyEnvelope {
                records: service
                    .list_session_records(request.tenant_id.as_deref())
                    .await?,
            })?)
        }
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported sessiondict method id {}", header.method_id),
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
    use crate::SessionDictRpc;
    use greenmqtt_core::{ClientIdentity, SessionKind, SessionRecord};
    use greenmqtt_sessiondict::{SessionDictHandle, SessionDirectory};
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn sessiondict_quic_read_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let sessiondict = Arc::new(SessionDictHandle::default());
        sessiondict
            .register(SessionRecord {
                session_id: "s-quic-1".into(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: Some(60),
                expires_at_ms: None,
            })
            .await
            .unwrap();
        sessiondict
            .register(SessionRecord {
                session_id: "s-quic-2".into(),
                node_id: 9,
                kind: SessionKind::Transient,
                identity: ClientIdentity {
                    tenant_id: "t2".into(),
                    user_id: "u2".into(),
                    client_id: "c2".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let service: Arc<dyn crate::SessionDictTransportService> = Arc::new(SessionDictRpc {
            inner: sessiondict,
            assignment_registry: None,
        });
        let server = spawn_sessiondict_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicSessionDictClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let looked_up = client
            .lookup_identity(&ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(looked_up.session_id, "s-quic-1");

        let by_id = client.lookup_session("s-quic-2").await.unwrap().unwrap();
        assert_eq!(by_id.identity.tenant_id, "t2");
        assert!(client.lookup_session("missing").await.unwrap().is_none());

        let listed = client.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id, "s-quic-1");

        server.abort();
    }

    #[tokio::test]
    async fn sessiondict_quic_register_unregister_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let sessiondict = Arc::new(SessionDictHandle::default());
        let service: Arc<dyn crate::SessionDictTransportService> = Arc::new(SessionDictRpc {
            inner: sessiondict,
            assignment_registry: None,
        });
        let server = spawn_sessiondict_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicSessionDictClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let replaced = client
            .register(SessionRecord {
                session_id: "s-quic-mut".into(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: Some(60),
                expires_at_ms: None,
            })
            .await
            .unwrap();
        assert!(replaced.is_none());

        let removed = client.unregister("s-quic-mut").await.unwrap().unwrap();
        assert_eq!(removed.identity.user_id, "u1");
        assert!(client.unregister("s-quic-mut").await.unwrap().is_none());

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
