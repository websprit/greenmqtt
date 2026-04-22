use crate::RangeAdminTransportService;
use greenmqtt_capnp::{
    decode_range_debug_reply, decode_range_health_list_reply, decode_range_health_reply,
    decode_range_health_request, encode_range_debug_reply, encode_range_health_list_reply,
    encode_range_health_reply, encode_range_health_request, RaftRoleEnvelope,
    RangeDebugReplyEnvelope, RangeHealthEnvelope, RangeHealthListReplyEnvelope,
    RangeHealthReplyEnvelope, RangeHealthRequestEnvelope, ReplicaLagEnvelope, RpcFrameEnvelope,
    RpcFrameKind, RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope, RpcServiceKind,
    RpcStatusCode,
};
use greenmqtt_core::{
    normalize_service_endpoint, ParsedServiceEndpoint, RangeReconfigurationState,
    ServiceEndpointTransport,
};
use greenmqtt_kv_server::RangeHealthSnapshot;
use quinn::{ClientConfig, Connection, Endpoint, ServerConfig};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const RANGE_ADMIN_LIST_RANGE_HEALTH_METHOD: u16 = 1;
const RANGE_ADMIN_GET_RANGE_HEALTH_METHOD: u16 = 2;
const RANGE_ADMIN_DEBUG_DUMP_METHOD: u16 = 3;

static QUIC_RUSTLS_PROVIDER: Once = Once::new();

pub(crate) struct QuicRangeAdminServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicRangeAdminServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicRangeAdminClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicRangeAdminClient {
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
            "RangeAdmin QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn list_range_health_snapshots(
        &self,
    ) -> anyhow::Result<Vec<RangeHealthSnapshot>> {
        let (_, payload) = self
            .send_request(RANGE_ADMIN_LIST_RANGE_HEALTH_METHOD, Vec::new())
            .await?;
        Ok(decode_range_health_list_reply(&payload)?
            .entries
            .into_iter()
            .map(from_quic_range_health)
            .collect())
    }

    pub(crate) async fn get_range_health_snapshot(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeHealthSnapshot>> {
        let payload = encode_range_health_request(&RangeHealthRequestEnvelope {
            range_id: range_id.to_string(),
        })?;
        match self
            .send_request(RANGE_ADMIN_GET_RANGE_HEALTH_METHOD, payload)
            .await
        {
            Ok((_, payload)) => Ok(decode_range_health_reply(&payload)?
                .health
                .map(from_quic_range_health)),
            Err(error) if error.to_string().contains("status=NotFound") => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn debug_dump(&self) -> anyhow::Result<String> {
        let (_, payload) = self
            .send_request(RANGE_ADMIN_DEBUG_DUMP_METHOD, Vec::new())
            .await?;
        Ok(decode_range_debug_reply(&payload)?.text)
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
                service: RpcServiceKind::RangeAdmin,
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

pub(crate) fn spawn_range_admin_quic_server(
    service: Arc<dyn RangeAdminTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicRangeAdminServerHandle> {
    let endpoint = Endpoint::server(
        load_quic_server_config(cert_path.as_ref(), key_path.as_ref())?,
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
                        eprintln!("greenmqtt range-admin quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_range_admin_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt range-admin quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicRangeAdminServerHandle { local_addr, task })
}

pub(crate) fn load_quic_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<ServerConfig> {
    ensure_quic_crypto_provider_installed();
    let (certs, key) = load_tls_material(cert_path, key_path)?;
    Ok(ServerConfig::with_single_cert(certs, key)?)
}

fn ensure_quic_crypto_provider_installed() {
    QUIC_RUSTLS_PROVIDER.call_once(|| {
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_tls_material(
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<(
    Vec<quinn::rustls::pki_types::CertificateDer<'static>>,
    quinn::rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;
    let mut cert_reader = std::io::BufReader::new(cert_pem.as_slice());
    let mut key_reader = std::io::BufReader::new(key_pem.as_slice());
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, io::Error>>()?;
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow::anyhow!("missing private key"))?;
    Ok((certs, key))
}

async fn handle_range_admin_stream(
    service: &dyn RangeAdminTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_range_admin_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_range_admin_request(
    service: &dyn RangeAdminTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::RangeAdmin,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        RANGE_ADMIN_LIST_RANGE_HEALTH_METHOD => {
            let entries = service
                .list_range_health_snapshots()
                .await?
                .into_iter()
                .map(to_quic_range_health)
                .collect();
            Ok(encode_range_health_list_reply(
                &RangeHealthListReplyEnvelope { entries },
            )?)
        }
        RANGE_ADMIN_GET_RANGE_HEALTH_METHOD => {
            let request = decode_range_health_request(&request.payload)?;
            let Some(health) = service.get_range_health_snapshot(&request.range_id).await? else {
                return encode_response_error(
                    RpcStatusCode::NotFound,
                    format!("range not found: {}", request.range_id),
                );
            };
            Ok(encode_range_health_reply(&RangeHealthReplyEnvelope {
                health: Some(to_quic_range_health(health)),
            })?)
        }
        RANGE_ADMIN_DEBUG_DUMP_METHOD => Ok(encode_range_debug_reply(&RangeDebugReplyEnvelope {
            text: service.debug_dump_text().await?,
        })?),
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported range-admin method id {}", header.method_id),
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

fn to_quic_role(role: greenmqtt_kv_raft::RaftNodeRole) -> RaftRoleEnvelope {
    match role {
        greenmqtt_kv_raft::RaftNodeRole::Follower => RaftRoleEnvelope::Follower,
        greenmqtt_kv_raft::RaftNodeRole::Candidate => RaftRoleEnvelope::Candidate,
        greenmqtt_kv_raft::RaftNodeRole::Leader => RaftRoleEnvelope::Leader,
    }
}

fn from_quic_role(role: RaftRoleEnvelope) -> greenmqtt_kv_raft::RaftNodeRole {
    match role {
        RaftRoleEnvelope::Follower => greenmqtt_kv_raft::RaftNodeRole::Follower,
        RaftRoleEnvelope::Candidate => greenmqtt_kv_raft::RaftNodeRole::Candidate,
        RaftRoleEnvelope::Leader => greenmqtt_kv_raft::RaftNodeRole::Leader,
    }
}

fn to_quic_range_health(health: RangeHealthSnapshot) -> RangeHealthEnvelope {
    RangeHealthEnvelope {
        range_id: health.range_id,
        lifecycle: health.lifecycle,
        role: to_quic_role(health.role),
        current_term: health.current_term,
        leader_node_id: health.leader_node_id,
        commit_index: health.commit_index,
        applied_index: health.applied_index,
        latest_snapshot_index: health.latest_snapshot_index,
        replica_lag: health
            .replica_lag
            .into_iter()
            .map(|replica| ReplicaLagEnvelope {
                node_id: replica.node_id,
                lag: replica.lag,
                match_index: replica.match_index,
                next_index: replica.next_index,
            })
            .collect(),
        reconfiguration: health.reconfiguration,
    }
}

fn from_quic_range_health(health: RangeHealthEnvelope) -> RangeHealthSnapshot {
    RangeHealthSnapshot {
        range_id: health.range_id,
        lifecycle: health.lifecycle,
        role: from_quic_role(health.role),
        current_term: health.current_term,
        leader_node_id: health.leader_node_id,
        commit_index: health.commit_index,
        applied_index: health.applied_index,
        latest_snapshot_index: health.latest_snapshot_index,
        replica_lag: health
            .replica_lag
            .into_iter()
            .map(|replica| greenmqtt_kv_server::ReplicaLagSnapshot {
                node_id: replica.node_id,
                lag: replica.lag,
                match_index: replica.match_index,
                next_index: replica.next_index,
            })
            .collect(),
        reconfiguration: RangeReconfigurationState {
            range_id: health.reconfiguration.range_id,
            old_voters: health.reconfiguration.old_voters,
            old_learners: health.reconfiguration.old_learners,
            current_voters: health.reconfiguration.current_voters,
            current_learners: health.reconfiguration.current_learners,
            joint_voters: health.reconfiguration.joint_voters,
            joint_learners: health.reconfiguration.joint_learners,
            pending_voters: health.reconfiguration.pending_voters,
            pending_learners: health.reconfiguration.pending_learners,
            phase: health.reconfiguration.phase,
            blocked_on_catch_up: health.reconfiguration.blocked_on_catch_up,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RangeAdminRpc;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_engine::{KvEngine, KvRangeBootstrap, MemoryKvEngine};
    use greenmqtt_kv_raft::{MemoryRaftNode, RaftNode};
    use greenmqtt_kv_server::{HostedRange, KvRangeHost, MemoryKvRangeHost};
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn range_admin_quic_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-admin-quic".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let range = engine.open_range("range-admin-quic").await.unwrap();
        let host = Arc::new(MemoryKvRangeHost::default());
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-admin-quic"));
        raft_impl.recover().await.unwrap();
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-admin-quic",
                ServiceShardKey::retain("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            raft,
            space: Arc::from(range),
        })
        .await
        .unwrap();

        let service: Arc<dyn crate::RangeAdminTransportService> = Arc::new(RangeAdminRpc {
            host: Some(host),
            runtime: None,
        });
        let server = spawn_range_admin_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeAdminClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let listed = client.list_range_health_snapshots().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].range_id, "range-admin-quic");

        let fetched = client
            .get_range_health_snapshot("range-admin-quic")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.range_id, "range-admin-quic");

        let missing = client.get_range_health_snapshot("missing").await.unwrap();
        assert!(missing.is_none());

        let dump = client.debug_dump().await.unwrap();
        assert!(dump.contains("range=range-admin-quic"));

        server.abort();
    }

    fn write_self_signed_tls_material() -> (
        TempDir,
        PathBuf,
        PathBuf,
        rcgen::CertifiedKey<rcgen::KeyPair>,
    ) {
        ensure_quic_crypto_provider_installed();
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
