use crate::DistTransportService;
use greenmqtt_capnp::{
    decode_add_route_request, decode_list_routes_reply, decode_list_routes_request,
    decode_list_session_routes_reply, decode_list_session_routes_request, decode_match_topic_reply,
    decode_match_topic_request, decode_remove_route_request, encode_add_route_request,
    encode_list_routes_reply, encode_list_routes_request, encode_list_session_routes_reply,
    encode_list_session_routes_request, encode_match_topic_reply, encode_match_topic_request,
    encode_remove_route_request, AddRouteRequestEnvelope, ListRoutesReplyEnvelope,
    ListRoutesRequestEnvelope, ListSessionRoutesReplyEnvelope, ListSessionRoutesRequestEnvelope,
    MatchTopicReplyEnvelope, MatchTopicRequestEnvelope, RemoveRouteRequestEnvelope,
    RpcFrameEnvelope, RpcFrameKind, RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope,
    RpcServiceKind, RpcStatusCode,
};
use greenmqtt_core::{
    normalize_service_endpoint, ParsedServiceEndpoint, RouteRecord, ServiceEndpointTransport,
};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const DIST_ADD_ROUTE_METHOD: u16 = 1;
const DIST_REMOVE_ROUTE_METHOD: u16 = 2;
const DIST_LIST_SESSION_ROUTES_METHOD: u16 = 3;
const DIST_MATCH_TOPIC_METHOD: u16 = 4;
const DIST_LIST_ROUTES_METHOD: u16 = 5;

static QUIC_RUSTLS_PROVIDER: Once = Once::new();

pub(crate) struct QuicDistServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicDistServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicDistClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicDistClient {
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
            "Dist QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        let payload = encode_add_route_request(&AddRouteRequestEnvelope { route })?;
        let _ = self.send_request(DIST_ADD_ROUTE_METHOD, payload).await?;
        Ok(())
    }

    pub(crate) async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let payload = encode_remove_route_request(&RemoveRouteRequestEnvelope {
            route: route.clone(),
        })?;
        let _ = self.send_request(DIST_REMOVE_ROUTE_METHOD, payload).await?;
        Ok(())
    }

    pub(crate) async fn list_session_routes(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let payload = encode_list_session_routes_request(&ListSessionRoutesRequestEnvelope {
            session_id: session_id.to_string(),
        })?;
        let (_, payload) = self
            .send_request(DIST_LIST_SESSION_ROUTES_METHOD, payload)
            .await?;
        Ok(decode_list_session_routes_reply(&payload)?.routes)
    }

    pub(crate) async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let payload = encode_match_topic_request(&MatchTopicRequestEnvelope {
            tenant_id: tenant_id.to_string(),
            topic: topic.to_string(),
        })?;
        let (_, payload) = self.send_request(DIST_MATCH_TOPIC_METHOD, payload).await?;
        Ok(decode_match_topic_reply(&payload)?.routes)
    }

    pub(crate) async fn list_routes(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let payload = encode_list_routes_request(&ListRoutesRequestEnvelope {
            tenant_id: tenant_id.map(str::to_string),
        })?;
        let (_, payload) = self.send_request(DIST_LIST_ROUTES_METHOD, payload).await?;
        Ok(decode_list_routes_reply(&payload)?.routes)
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
                service: RpcServiceKind::Dist,
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

pub(crate) fn spawn_dist_quic_server(
    service: Arc<dyn DistTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicDistServerHandle> {
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
                        eprintln!("greenmqtt dist quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_dist_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt dist quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicDistServerHandle { local_addr, task })
}

async fn handle_dist_stream(
    service: &dyn DistTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_dist_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_dist_request(
    service: &dyn DistTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::Dist,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        DIST_ADD_ROUTE_METHOD => {
            let request = decode_add_route_request(&request.payload)?;
            service.add_route_record(request.route).await?;
            Ok(Vec::new())
        }
        DIST_REMOVE_ROUTE_METHOD => {
            let request = decode_remove_route_request(&request.payload)?;
            service.remove_route_record(&request.route).await?;
            Ok(Vec::new())
        }
        DIST_LIST_SESSION_ROUTES_METHOD => {
            let request = decode_list_session_routes_request(&request.payload)?;
            Ok(encode_list_session_routes_reply(
                &ListSessionRoutesReplyEnvelope {
                    routes: service
                        .list_session_route_records(&request.session_id)
                        .await?,
                },
            )?)
        }
        DIST_MATCH_TOPIC_METHOD => {
            let request = decode_match_topic_request(&request.payload)?;
            Ok(encode_match_topic_reply(&MatchTopicReplyEnvelope {
                routes: service
                    .match_route_records(&request.tenant_id, &request.topic)
                    .await?,
            })?)
        }
        DIST_LIST_ROUTES_METHOD => {
            let request = decode_list_routes_request(&request.payload)?;
            Ok(encode_list_routes_reply(&ListRoutesReplyEnvelope {
                routes: service
                    .list_route_records(request.tenant_id.as_deref())
                    .await?,
            })?)
        }
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported dist method id {}", header.method_id),
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
    use crate::DistRpc;
    use greenmqtt_core::{RouteRecord, SessionKind};
    use greenmqtt_dist::{DistHandle, DistRouter};
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn dist_quic_read_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let dist = Arc::new(DistHandle::default());
        dist.add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
        dist.add_route(RouteRecord {
            tenant_id: "t2".into(),
            topic_filter: "devices/+/status".into(),
            session_id: "s2".into(),
            node_id: 9,
            subscription_identifier: None,
            no_local: true,
            retain_as_published: true,
            shared_group: Some("g1".into()),
            kind: SessionKind::Transient,
        })
        .await
        .unwrap();

        let service: Arc<dyn crate::DistTransportService> = Arc::new(DistRpc {
            inner: dist,
            assignment_registry: None,
        });
        let server = spawn_dist_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicDistClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let session_routes = client.list_session_routes("s1").await.unwrap();
        assert_eq!(session_routes.len(), 1);
        assert_eq!(session_routes[0].tenant_id, "t1");

        let matched = client.match_topic("t1", "devices/d1/state").await.unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].session_id, "s1");

        let listed = client.list_routes(Some("t1")).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].topic_filter, "devices/+/state");

        server.abort();
    }

    #[tokio::test]
    async fn dist_quic_add_remove_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let dist = Arc::new(DistHandle::default());
        let service: Arc<dyn crate::DistTransportService> = Arc::new(DistRpc {
            inner: dist,
            assignment_registry: None,
        });
        let server = spawn_dist_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicDistClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let route = RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        };
        client.add_route(route.clone()).await.unwrap();
        assert_eq!(client.list_routes(Some("t1")).await.unwrap().len(), 1);
        client.remove_route(&route).await.unwrap();
        assert!(client.list_routes(Some("t1")).await.unwrap().is_empty());

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
