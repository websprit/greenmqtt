use crate::RetainTransportService;
use greenmqtt_capnp::{
    decode_retain_match_reply, decode_retain_match_request, decode_retain_write_request,
    encode_retain_match_reply, encode_retain_match_request, encode_retain_write_request,
    RetainMatchReplyEnvelope, RetainMatchRequestEnvelope, RetainWriteRequestEnvelope,
    RpcFrameEnvelope, RpcFrameKind, RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope,
    RpcServiceKind, RpcStatusCode,
};
use greenmqtt_core::{
    normalize_service_endpoint, ParsedServiceEndpoint, RetainedMessage, ServiceEndpointTransport,
};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const RETAIN_WRITE_METHOD: u16 = 1;
const RETAIN_MATCH_METHOD: u16 = 2;

pub(crate) struct QuicRetainServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicRetainServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicRetainClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicRetainClient {
    pub(crate) async fn connect_with_client_config(
        endpoint: impl Into<String>,
        server_name: &str,
        client_config: ClientConfig,
    ) -> anyhow::Result<Self> {
        let normalized =
            normalize_service_endpoint(&endpoint.into(), ServiceEndpointTransport::Quic)?;
        let parsed = ParsedServiceEndpoint::parse(&normalized)?;
        anyhow::ensure!(
            parsed.transport == ServiceEndpointTransport::Quic,
            "Retain QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        let payload = encode_retain_write_request(&RetainWriteRequestEnvelope { message })?;
        let _ = self.send_request(RETAIN_WRITE_METHOD, payload).await?;
        Ok(())
    }

    pub(crate) async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let payload = encode_retain_match_request(&RetainMatchRequestEnvelope {
            tenant_id: tenant_id.to_string(),
            topic_filter: topic_filter.to_string(),
        })?;
        let (_, payload) = self.send_request(RETAIN_MATCH_METHOD, payload).await?;
        Ok(decode_retain_match_reply(&payload)?.messages)
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
                service: RpcServiceKind::Retain,
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

pub(crate) fn spawn_retain_quic_server(
    service: Arc<dyn RetainTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicRetainServerHandle> {
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
                        eprintln!("greenmqtt retain quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_retain_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt retain quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicRetainServerHandle { local_addr, task })
}

async fn handle_retain_stream(
    service: &dyn RetainTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_retain_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_retain_request(
    service: &dyn RetainTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::Retain,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        RETAIN_WRITE_METHOD => {
            let request = decode_retain_write_request(&request.payload)?;
            service.retain_message(request.message).await?;
            Ok(Vec::new())
        }
        RETAIN_MATCH_METHOD => {
            let request = decode_retain_match_request(&request.payload)?;
            Ok(encode_retain_match_reply(&RetainMatchReplyEnvelope {
                messages: service
                    .match_retained(&request.tenant_id, &request.topic_filter)
                    .await?,
            })?)
        }
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported retain method id {}", header.method_id),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetainRpc;
    use greenmqtt_core::RetainedMessage;
    use greenmqtt_retain::RetainHandle;
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn retain_quic_write_and_match_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let retain = Arc::new(RetainHandle::default());
        let service: Arc<dyn crate::RetainTransportService> = Arc::new(RetainRpc {
            inner: retain.clone(),
        });
        let server = spawn_retain_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRetainClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        client
            .retain(RetainedMessage {
                tenant_id: "demo".into(),
                topic: "devices/d1/state".into(),
                payload: b"retained".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let messages = client.match_topic("demo", "#").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].topic, "devices/d1/state");

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
