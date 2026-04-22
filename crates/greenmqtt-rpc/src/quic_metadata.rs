use crate::MetadataTransportService;
use greenmqtt_capnp::{
    decode_member_list_reply, decode_member_lookup_request, decode_member_record_reply,
    decode_range_list_reply, decode_range_list_request, decode_range_lookup_request,
    decode_range_record_reply, decode_route_range_request, encode_member_list_reply,
    encode_member_lookup_request, encode_member_record_reply, encode_range_list_reply,
    encode_range_list_request, encode_range_lookup_request, encode_range_record_reply,
    MemberListReplyEnvelope, MemberLookupRequestEnvelope, MemberRecordReplyEnvelope,
    RangeListReplyEnvelope, RangeListRequestEnvelope, RangeLookupRequestEnvelope,
    RangeRecordReplyEnvelope, RouteRangeRequestEnvelope, RpcFrameEnvelope, RpcFrameKind,
    RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope, RpcServiceKind, RpcStatusCode,
};
use greenmqtt_core::{normalize_service_endpoint, ParsedServiceEndpoint, ServiceEndpointTransport};
use greenmqtt_core::{
    ClusterNodeMembership, ReplicatedRangeDescriptor, ServiceShardKey, ServiceShardKind,
};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const METADATA_LOOKUP_MEMBER_METHOD: u16 = 1;
const METADATA_LIST_MEMBERS_METHOD: u16 = 2;
const METADATA_LOOKUP_RANGE_METHOD: u16 = 3;
const METADATA_LIST_RANGES_METHOD: u16 = 4;
const METADATA_ROUTE_RANGE_METHOD: u16 = 5;
static QUIC_RUSTLS_PROVIDER: Once = Once::new();

pub(crate) struct QuicMetadataServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicMetadataServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicMetadataClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicMetadataClient {
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
            "Metadata QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn lookup_member(
        &self,
        node_id: u64,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let payload = encode_member_lookup_request(&MemberLookupRequestEnvelope { node_id })?;
        let (_, payload) = self
            .send_request(METADATA_LOOKUP_MEMBER_METHOD, payload)
            .await?;
        Ok(decode_member_record_reply(&payload)?.member)
    }

    pub(crate) async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let (_, payload) = self
            .send_request(METADATA_LIST_MEMBERS_METHOD, Vec::new())
            .await?;
        Ok(decode_member_list_reply(&payload)?.members)
    }

    pub(crate) async fn lookup_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let payload = encode_range_lookup_request(&RangeLookupRequestEnvelope {
            range_id: range_id.to_string(),
        })?;
        let (_, payload) = self
            .send_request(METADATA_LOOKUP_RANGE_METHOD, payload)
            .await?;
        Ok(decode_range_record_reply(&payload)?.descriptor)
    }

    pub(crate) async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
        tenant_id: Option<&str>,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let payload = encode_range_list_request(&RangeListRequestEnvelope {
            shard_kind,
            tenant_id: tenant_id.map(str::to_string),
            scope: scope.map(str::to_string),
        })?;
        let (_, payload) = self
            .send_request(METADATA_LIST_RANGES_METHOD, payload)
            .await?;
        Ok(decode_range_list_reply(&payload)?.descriptors)
    }

    pub(crate) async fn route_range(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let payload = greenmqtt_capnp::encode_route_range_request(&RouteRangeRequestEnvelope {
            shard: shard.clone(),
            key: key.to_vec(),
        })?;
        let (_, payload) = self
            .send_request(METADATA_ROUTE_RANGE_METHOD, payload)
            .await?;
        Ok(decode_range_record_reply(&payload)?.descriptor)
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
                service: RpcServiceKind::Metadata,
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

pub(crate) fn spawn_metadata_quic_server(
    service: Arc<dyn MetadataTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicMetadataServerHandle> {
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
                        eprintln!("greenmqtt metadata quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_metadata_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt metadata quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicMetadataServerHandle { local_addr, task })
}

async fn handle_metadata_stream(
    service: &dyn MetadataTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_metadata_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_metadata_request(
    service: &dyn MetadataTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::Metadata,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        METADATA_LOOKUP_MEMBER_METHOD => {
            let request = decode_member_lookup_request(&request.payload)?;
            Ok(encode_member_record_reply(&MemberRecordReplyEnvelope {
                member: service.lookup_member_record(request.node_id).await?,
            })?)
        }
        METADATA_LIST_MEMBERS_METHOD => Ok(encode_member_list_reply(&MemberListReplyEnvelope {
            members: service.list_member_records().await?,
        })?),
        METADATA_LOOKUP_RANGE_METHOD => {
            let request = decode_range_lookup_request(&request.payload)?;
            Ok(encode_range_record_reply(&RangeRecordReplyEnvelope {
                descriptor: service.lookup_range_record(&request.range_id).await?,
            })?)
        }
        METADATA_LIST_RANGES_METHOD => {
            let request = decode_range_list_request(&request.payload)?;
            Ok(encode_range_list_reply(&RangeListReplyEnvelope {
                descriptors: service
                    .list_range_records(
                        request.shard_kind,
                        request.tenant_id.as_deref(),
                        request.scope.as_deref(),
                    )
                    .await?,
            })?)
        }
        METADATA_ROUTE_RANGE_METHOD => {
            let request = decode_route_range_request(&request.payload)?;
            Ok(encode_range_record_reply(&RangeRecordReplyEnvelope {
                descriptor: service
                    .route_range_record(&request.shard, &request.key)
                    .await?,
            })?)
        }
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported metadata method id {}", header.method_id),
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
    use crate::{MetadataRpc, StaticServiceEndpointRegistry};
    use greenmqtt_core::{
        ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership, RangeBoundary,
        RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ReplicatedRangeRegistry, ServiceEndpoint, ServiceKind, ServiceShardKey, ServiceShardKind,
        ServiceShardLifecycle,
    };
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn metadata_quic_member_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        registry
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(
                    ServiceKind::Retain,
                    7,
                    "http://127.0.0.1:50062",
                )],
            ))
            .await
            .unwrap();
        let service: Arc<dyn crate::MetadataTransportService> = Arc::new(MetadataRpc {
            inner: Some(registry),
        });
        let server = spawn_metadata_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicMetadataClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let looked_up = client.lookup_member(7).await.unwrap().unwrap();
        assert_eq!(looked_up.node_id, 7);
        let missing = client.lookup_member(999).await.unwrap();
        assert!(missing.is_none());
        let listed = client.list_members().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].node_id, 7);

        server.abort();
    }

    #[tokio::test]
    async fn metadata_quic_range_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let registry = Arc::new(StaticServiceEndpointRegistry::default());
        let shard = ServiceShardKey::retain("t1");
        registry
            .upsert_range(ReplicatedRangeDescriptor::new(
                "range-quic",
                shard.clone(),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
                1,
                2,
                Some(7),
                vec![
                    RangeReplica::new(7, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                    RangeReplica::new(9, ReplicaRole::Learner, ReplicaSyncState::Snapshotting),
                ],
                11,
                10,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let service: Arc<dyn crate::MetadataTransportService> = Arc::new(MetadataRpc {
            inner: Some(registry),
        });
        let server = spawn_metadata_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicMetadataClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let looked_up = client.lookup_range("range-quic").await.unwrap().unwrap();
        assert_eq!(looked_up.id, "range-quic");
        let listed = client
            .list_ranges(Some(ServiceShardKind::Retain), Some("t1"), None)
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        let routed = client.route_range(&shard, b"pear").await.unwrap().unwrap();
        assert_eq!(routed.id, "range-quic");

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
