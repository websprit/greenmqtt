use crate::RangeControlTransportService;
use greenmqtt_capnp::{
    decode_range_bootstrap_reply, decode_range_bootstrap_request,
    decode_range_change_replicas_request, decode_range_drain_request, decode_range_merge_reply,
    decode_range_merge_request, decode_range_recover_request, decode_range_retire_request,
    decode_range_split_reply, decode_range_split_request, decode_range_transfer_leadership_request,
    encode_range_bootstrap_reply, encode_range_bootstrap_request,
    encode_range_change_replicas_request, encode_range_drain_request, encode_range_merge_reply,
    encode_range_merge_request, encode_range_recover_request, encode_range_retire_request,
    encode_range_split_reply, encode_range_split_request, encode_range_transfer_leadership_request,
    RangeBootstrapReplyEnvelope, RangeBootstrapRequestEnvelope, RangeChangeReplicasRequestEnvelope,
    RangeDrainRequestEnvelope, RangeMergeReplyEnvelope, RangeMergeRequestEnvelope,
    RangeRecoverRequestEnvelope, RangeRetireRequestEnvelope, RangeSplitReplyEnvelope,
    RangeSplitRequestEnvelope, RangeTransferLeadershipRequestEnvelope, RpcFrameEnvelope,
    RpcFrameKind, RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope, RpcServiceKind,
    RpcStatusCode,
};
use greenmqtt_core::{
    normalize_service_endpoint, ParsedServiceEndpoint, ReplicatedRangeDescriptor,
    ServiceEndpointTransport,
};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const RANGE_CONTROL_BOOTSTRAP_METHOD: u16 = 1;
const RANGE_CONTROL_CHANGE_REPLICAS_METHOD: u16 = 2;
const RANGE_CONTROL_TRANSFER_LEADERSHIP_METHOD: u16 = 3;
const RANGE_CONTROL_RECOVER_METHOD: u16 = 4;
const RANGE_CONTROL_SPLIT_METHOD: u16 = 5;
const RANGE_CONTROL_MERGE_METHOD: u16 = 6;
const RANGE_CONTROL_DRAIN_METHOD: u16 = 7;
const RANGE_CONTROL_RETIRE_METHOD: u16 = 8;

pub(crate) struct QuicRangeControlServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicRangeControlServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicRangeControlClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicRangeControlClient {
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
            "RangeControl QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn bootstrap_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        let payload =
            encode_range_bootstrap_request(&RangeBootstrapRequestEnvelope { descriptor })?;
        let (_, payload) = self
            .send_request(RANGE_CONTROL_BOOTSTRAP_METHOD, payload)
            .await?;
        Ok(decode_range_bootstrap_reply(&payload)?.range_id)
    }

    pub(crate) async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> anyhow::Result<()> {
        let payload = encode_range_change_replicas_request(&RangeChangeReplicasRequestEnvelope {
            range_id: range_id.to_string(),
            voters,
            learners,
        })?;
        let _ = self
            .send_request(RANGE_CONTROL_CHANGE_REPLICAS_METHOD, payload)
            .await?;
        Ok(())
    }

    pub(crate) async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: u64,
    ) -> anyhow::Result<()> {
        let payload =
            encode_range_transfer_leadership_request(&RangeTransferLeadershipRequestEnvelope {
                range_id: range_id.to_string(),
                target_node_id,
            })?;
        let _ = self
            .send_request(RANGE_CONTROL_TRANSFER_LEADERSHIP_METHOD, payload)
            .await?;
        Ok(())
    }

    pub(crate) async fn recover_range(
        &self,
        range_id: &str,
        new_leader_node_id: u64,
    ) -> anyhow::Result<()> {
        let payload = encode_range_recover_request(&RangeRecoverRequestEnvelope {
            range_id: range_id.to_string(),
            new_leader_node_id,
        })?;
        let _ = self
            .send_request(RANGE_CONTROL_RECOVER_METHOD, payload)
            .await?;
        Ok(())
    }

    pub(crate) async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<(String, String)> {
        let payload = encode_range_split_request(&RangeSplitRequestEnvelope {
            range_id: range_id.to_string(),
            split_key,
        })?;
        let (_, payload) = self
            .send_request(RANGE_CONTROL_SPLIT_METHOD, payload)
            .await?;
        let reply = decode_range_split_reply(&payload)?;
        Ok((reply.left_range_id, reply.right_range_id))
    }

    pub(crate) async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        let payload = encode_range_merge_request(&RangeMergeRequestEnvelope {
            left_range_id: left_range_id.to_string(),
            right_range_id: right_range_id.to_string(),
        })?;
        let (_, payload) = self
            .send_request(RANGE_CONTROL_MERGE_METHOD, payload)
            .await?;
        Ok(decode_range_merge_reply(&payload)?.range_id)
    }

    pub(crate) async fn drain_range(&self, range_id: &str) -> anyhow::Result<()> {
        let payload = encode_range_drain_request(&RangeDrainRequestEnvelope {
            range_id: range_id.to_string(),
        })?;
        let _ = self
            .send_request(RANGE_CONTROL_DRAIN_METHOD, payload)
            .await?;
        Ok(())
    }

    pub(crate) async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        let payload = encode_range_retire_request(&RangeRetireRequestEnvelope {
            range_id: range_id.to_string(),
        })?;
        let _ = self
            .send_request(RANGE_CONTROL_RETIRE_METHOD, payload)
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
                service: RpcServiceKind::RangeControl,
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

pub(crate) fn spawn_range_control_quic_server(
    service: Arc<dyn RangeControlTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicRangeControlServerHandle> {
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
                        eprintln!("greenmqtt range-control quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_range_control_stream(service.as_ref(), &mut send, &mut recv)
                                .await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt range-control quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicRangeControlServerHandle { local_addr, task })
}

async fn handle_range_control_stream(
    service: &dyn RangeControlTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_range_control_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_range_control_request(
    service: &dyn RangeControlTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::RangeControl,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result: anyhow::Result<Vec<u8>> = match header.method_id {
        RANGE_CONTROL_BOOTSTRAP_METHOD => {
            let request = decode_range_bootstrap_request(&request.payload)?;
            let range_id = service.bootstrap_range_action(request.descriptor).await?;
            Ok(encode_range_bootstrap_reply(
                &RangeBootstrapReplyEnvelope { range_id },
            )?)
        }
        RANGE_CONTROL_CHANGE_REPLICAS_METHOD => {
            let request = decode_range_change_replicas_request(&request.payload)?;
            service
                .change_replicas_action(&request.range_id, request.voters, request.learners)
                .await?;
            Ok(Vec::new())
        }
        RANGE_CONTROL_TRANSFER_LEADERSHIP_METHOD => {
            let request = decode_range_transfer_leadership_request(&request.payload)?;
            service
                .transfer_leadership_action(&request.range_id, request.target_node_id)
                .await?;
            Ok(Vec::new())
        }
        RANGE_CONTROL_RECOVER_METHOD => {
            let request = decode_range_recover_request(&request.payload)?;
            service
                .recover_range_action(&request.range_id, request.new_leader_node_id)
                .await?;
            Ok(Vec::new())
        }
        RANGE_CONTROL_SPLIT_METHOD => {
            let request = decode_range_split_request(&request.payload)?;
            let (left_range_id, right_range_id) = service
                .split_range_action(&request.range_id, request.split_key)
                .await?;
            Ok(encode_range_split_reply(&RangeSplitReplyEnvelope {
                left_range_id,
                right_range_id,
            })?)
        }
        RANGE_CONTROL_MERGE_METHOD => {
            let request = decode_range_merge_request(&request.payload)?;
            let range_id = service
                .merge_ranges_action(&request.left_range_id, &request.right_range_id)
                .await?;
            Ok(encode_range_merge_reply(&RangeMergeReplyEnvelope {
                range_id,
            })?)
        }
        RANGE_CONTROL_DRAIN_METHOD => {
            let request = decode_range_drain_request(&request.payload)?;
            service.drain_range_action(&request.range_id).await?;
            Ok(Vec::new())
        }
        RANGE_CONTROL_RETIRE_METHOD => {
            let request = decode_range_retire_request(&request.payload)?;
            service.retire_range_action(&request.range_id).await?;
            Ok(Vec::new())
        }
        _ => {
            return encode_response_error(
                RpcStatusCode::NotSupported,
                format!("unsupported range-control method id {}", header.method_id),
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
    use crate::RangeControlRpc;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_engine::{KvEngine, KvRangeBootstrap, MemoryKvEngine};
    use greenmqtt_kv_raft::{MemoryRaftNode, RaftNode};
    use greenmqtt_kv_server::{
        HostedRange, MemoryKvRangeHost, RangeLifecycleManager, ReplicaRuntime,
    };
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[derive(Clone)]
    struct LocalRangeLifecycleManager {
        engine: MemoryKvEngine,
    }

    #[async_trait::async_trait]
    impl RangeLifecycleManager for LocalRangeLifecycleManager {
        async fn create_range(
            &self,
            descriptor: ReplicatedRangeDescriptor,
        ) -> anyhow::Result<HostedRange> {
            self.engine
                .bootstrap(KvRangeBootstrap {
                    range_id: descriptor.id.clone(),
                    boundary: descriptor.boundary.clone(),
                })
                .await?;
            let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
                self.engine.open_range(&descriptor.id).await?,
            );
            let raft = Arc::new(MemoryRaftNode::single_node(
                descriptor.leader_node_id.unwrap_or(1),
                &descriptor.id,
            ));
            raft.recover().await?;
            Ok(HostedRange {
                descriptor,
                raft,
                space,
            })
        }

        async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
            self.engine.destroy_range(range_id).await
        }
    }

    #[tokio::test]
    async fn range_control_quic_bootstrap_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-bootstrap",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-bootstrap");
        assert!(runtime
            .health_snapshot()
            .await
            .unwrap()
            .into_iter()
            .any(|entry| entry.range_id == "range-quic-bootstrap"));

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_change_replicas_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-change",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-change");

        client
            .change_replicas("range-quic-change", vec![1, 2], Vec::new())
            .await
            .unwrap();

        let status = runtime
            .health_snapshot()
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.range_id == "range-quic-change")
            .unwrap();
        assert_eq!(status.reconfiguration.current_voters, vec![1]);
        assert!(
            status.reconfiguration.pending_voters == vec![1, 2]
                || status.reconfiguration.phase.is_some()
        );

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_transfer_leadership_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-transfer",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-transfer");

        client
            .transfer_leadership("range-quic-transfer", 1)
            .await
            .unwrap();
        let status = runtime
            .health_snapshot()
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.range_id == "range-quic-transfer")
            .unwrap();
        assert_eq!(status.leader_node_id, Some(1));

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_recover_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-recover",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-recover");

        client.recover_range("range-quic-recover", 1).await.unwrap();
        let status = runtime
            .health_snapshot()
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.range_id == "range-quic-recover")
            .unwrap();
        assert_eq!(status.leader_node_id, Some(1));

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_split_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-split",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-split");

        let (left, right) = client
            .split_range("range-quic-split", b"m".to_vec())
            .await
            .unwrap();
        assert_ne!(left, right);

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_merge_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-merge",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-merge");

        let (left, right) = client
            .split_range("range-quic-merge", b"m".to_vec())
            .await
            .unwrap();
        let merged = client.merge_ranges(&left, &right).await.unwrap();
        assert!(merged.contains(&left));

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_drain_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-drain",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-drain");

        client.drain_range("range-quic-drain").await.unwrap();
        let status = runtime
            .health_snapshot()
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.range_id == "range-quic-drain")
            .unwrap();
        assert_eq!(status.lifecycle, ServiceShardLifecycle::Draining);

        server.abort();
    }

    #[tokio::test]
    async fn range_control_quic_retire_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        let host = Arc::new(MemoryKvRangeHost::default());
        let runtime = Arc::new(ReplicaRuntime::with_config(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
            Some(Arc::new(LocalRangeLifecycleManager {
                engine: engine.clone(),
            })),
            Duration::from_secs(1),
            64,
            64,
            64,
        ));
        let service: Arc<dyn crate::RangeControlTransportService> = Arc::new(RangeControlRpc {
            inner: Some(runtime.clone()),
            registry: None,
        });
        let server = spawn_range_control_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicRangeControlClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let range_id = client
            .bootstrap_range(ReplicatedRangeDescriptor::new(
                "range-quic-retire",
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
                ServiceShardLifecycle::Bootstrapping,
            ))
            .await
            .unwrap();
        assert_eq!(range_id, "range-quic-retire");

        client.retire_range("range-quic-retire").await.unwrap();
        assert!(!runtime
            .health_snapshot()
            .await
            .unwrap()
            .into_iter()
            .any(|entry| entry.range_id == "range-quic-retire"));

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
