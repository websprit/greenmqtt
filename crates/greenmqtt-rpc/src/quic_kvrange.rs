use crate::{KvRangeTransportError, KvRangeTransportService};
use bytes::Bytes;
use greenmqtt_capnp::{
    decode_kv_range_apply_request, decode_kv_range_checkpoint_reply,
    decode_kv_range_checkpoint_request, decode_kv_range_get_reply, decode_kv_range_get_request,
    decode_kv_range_scan_reply, decode_kv_range_scan_request, decode_kv_range_snapshot_reply,
    decode_kv_range_snapshot_request, encode_kv_range_apply_request,
    encode_kv_range_checkpoint_reply, encode_kv_range_checkpoint_request,
    encode_kv_range_get_reply, encode_kv_range_get_request, encode_kv_range_scan_reply,
    encode_kv_range_scan_request, encode_kv_range_snapshot_reply, encode_kv_range_snapshot_request,
    KvEntryEnvelope, KvMutationEnvelope, KvRangeApplyRequestEnvelope,
    KvRangeCheckpointReplyEnvelope, KvRangeCheckpointRequestEnvelope, KvRangeGetReplyEnvelope,
    KvRangeGetRequestEnvelope, KvRangeScanReplyEnvelope, KvRangeScanRequestEnvelope,
    KvRangeSnapshotReplyEnvelope, KvRangeSnapshotRequestEnvelope, RpcFrameEnvelope, RpcFrameKind,
    RpcRequestHeaderEnvelope, RpcResponseHeaderEnvelope, RpcServiceKind, RpcStatusCode,
};
use greenmqtt_core::{
    normalize_service_endpoint, ParsedServiceEndpoint, RangeBoundary, ServiceEndpointTransport,
};
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
use quinn::{ClientConfig, Connection, Endpoint};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use tokio::task::JoinHandle;

const QUIC_RPC_PROTOCOL_VERSION: u16 = 1;
const QUIC_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const KVRANGE_GET_METHOD: u16 = 1;
const KVRANGE_SCAN_METHOD: u16 = 2;
const KVRANGE_APPLY_METHOD: u16 = 3;
const KVRANGE_CHECKPOINT_METHOD: u16 = 4;
const KVRANGE_SNAPSHOT_METHOD: u16 = 5;

static QUIC_RUSTLS_PROVIDER: Once = Once::new();

pub(crate) struct QuicKvRangeServerHandle {
    local_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl QuicKvRangeServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn abort(&self) {
        self.task.abort();
    }
}

pub(crate) struct QuicKvRangeClient {
    _endpoint: Endpoint,
    connection: Connection,
    next_request_id: AtomicU64,
}

impl QuicKvRangeClient {
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
            "KvRange QUIC client requires quic endpoint: {normalized}"
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

    pub(crate) async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.get_fenced(range_id, key, 0).await
    }

    pub(crate) async fn get_fenced(
        &self,
        range_id: &str,
        key: &[u8],
        expected_epoch: u64,
    ) -> anyhow::Result<Option<Bytes>> {
        let payload = encode_kv_range_get_request(&KvRangeGetRequestEnvelope {
            range_id: range_id.to_string(),
            key: key.to_vec(),
            expected_epoch,
        })?;
        let (_, payload) = self.send_request(KVRANGE_GET_METHOD, payload).await?;
        let reply = decode_kv_range_get_reply(&payload)?;
        Ok(reply.found.then_some(Bytes::from(reply.value)))
    }

    pub(crate) async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        self.scan_fenced(range_id, boundary, limit, 0).await
    }

    pub(crate) async fn scan_fenced(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
        expected_epoch: u64,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let payload = encode_kv_range_scan_request(&KvRangeScanRequestEnvelope {
            range_id: range_id.to_string(),
            boundary,
            limit: limit as u32,
            expected_epoch,
        })?;
        let (_, payload) = self.send_request(KVRANGE_SCAN_METHOD, payload).await?;
        Ok(decode_kv_range_scan_reply(&payload)?
            .entries
            .into_iter()
            .map(|entry| (Bytes::from(entry.key), Bytes::from(entry.value)))
            .collect())
    }

    pub(crate) async fn apply(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
    ) -> anyhow::Result<()> {
        self.apply_fenced(range_id, mutations, 0).await
    }

    pub(crate) async fn apply_fenced(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        let payload = encode_kv_range_apply_request(&KvRangeApplyRequestEnvelope {
            range_id: range_id.to_string(),
            mutations: mutations
                .into_iter()
                .map(|mutation| KvMutationEnvelope {
                    key: mutation.key.to_vec(),
                    value: mutation.value.map(|value| value.to_vec()),
                })
                .collect(),
            expected_epoch,
        })?;
        let _ = self.send_request(KVRANGE_APPLY_METHOD, payload).await?;
        Ok(())
    }

    pub(crate) async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        self.checkpoint_fenced(range_id, checkpoint_id, 0).await
    }

    pub(crate) async fn checkpoint_fenced(
        &self,
        range_id: &str,
        checkpoint_id: &str,
        expected_epoch: u64,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let payload = encode_kv_range_checkpoint_request(&KvRangeCheckpointRequestEnvelope {
            range_id: range_id.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            expected_epoch,
        })?;
        let (_, payload) = self
            .send_request(KVRANGE_CHECKPOINT_METHOD, payload)
            .await?;
        let reply = decode_kv_range_checkpoint_reply(&payload)?;
        Ok(KvRangeCheckpoint {
            range_id: reply.range_id,
            checkpoint_id: reply.checkpoint_id,
            path: reply.path,
        })
    }

    pub(crate) async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        self.snapshot_fenced(range_id, 0).await
    }

    pub(crate) async fn snapshot_fenced(
        &self,
        range_id: &str,
        expected_epoch: u64,
    ) -> anyhow::Result<KvRangeSnapshot> {
        let payload = encode_kv_range_snapshot_request(&KvRangeSnapshotRequestEnvelope {
            range_id: range_id.to_string(),
            expected_epoch,
        })?;
        let (_, payload) = self.send_request(KVRANGE_SNAPSHOT_METHOD, payload).await?;
        let reply = decode_kv_range_snapshot_reply(&payload)?;
        Ok(KvRangeSnapshot {
            range_id: reply.range_id,
            boundary: reply.boundary,
            term: reply.term,
            index: reply.index,
            checksum: reply.checksum,
            layout_version: reply.layout_version,
            data_path: reply.data_path,
        })
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
                service: RpcServiceKind::KvRange,
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

pub(crate) fn spawn_kvrange_quic_server(
    service: Arc<dyn KvRangeTransportService>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<QuicKvRangeServerHandle> {
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
                        eprintln!("greenmqtt kvrange quic handshake error: {error:#}");
                        return;
                    }
                };
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_kvrange_stream(service.as_ref(), &mut send, &mut recv).await
                        {
                            let _ = send.reset(0u32.into());
                            eprintln!("greenmqtt kvrange quic stream error: {error:#}");
                        }
                    });
                }
            });
        }
        Ok(())
    });
    Ok(QuicKvRangeServerHandle { local_addr, task })
}

async fn handle_kvrange_stream(
    service: &dyn KvRangeTransportService,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> anyhow::Result<()> {
    let request_bytes = recv.read_to_end(QUIC_RPC_MAX_FRAME_BYTES).await?;
    let response = handle_kvrange_request(service, &request_bytes).await?;
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

async fn handle_kvrange_request(
    service: &dyn KvRangeTransportService,
    request_bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let request = RpcFrameEnvelope::decode(request_bytes)?;
    let header = request
        .request_header
        .ok_or_else(|| anyhow::anyhow!("missing quic rpc request header"))?;
    anyhow::ensure!(
        header.service == RpcServiceKind::KvRange,
        "unexpected quic rpc service: {:?}",
        header.service
    );
    let result = match header.method_id {
        KVRANGE_GET_METHOD => {
            let request = decode_kv_range_get_request(&request.payload)?;
            match service
                .get_value(&request.range_id, &request.key, request.expected_epoch)
                .await
            {
                Ok(value) => {
                    encode_response_ok(encode_kv_range_get_reply(&KvRangeGetReplyEnvelope {
                        value: value.clone().unwrap_or_default().to_vec(),
                        found: value.is_some(),
                    })?)
                }
                Err(KvRangeTransportError::Unavailable(message)) => {
                    encode_response_error(RpcStatusCode::Unavailable, message)
                }
                Err(KvRangeTransportError::NotFound(message)) => {
                    encode_response_error(RpcStatusCode::NotFound, message)
                }
                Err(KvRangeTransportError::DeadlineExceeded(message)) => {
                    encode_response_error(RpcStatusCode::DeadlineExceeded, message)
                }
                Err(KvRangeTransportError::Internal(error)) => {
                    encode_response_error(RpcStatusCode::Internal, error.to_string())
                }
            }
        }
        KVRANGE_SCAN_METHOD => {
            let request = decode_kv_range_scan_request(&request.payload)?;
            match service
                .scan_entries(
                    &request.range_id,
                    request.boundary,
                    request.limit as usize,
                    request.expected_epoch,
                )
                .await
            {
                Ok(entries) => {
                    encode_response_ok(encode_kv_range_scan_reply(&KvRangeScanReplyEnvelope {
                        entries: entries
                            .into_iter()
                            .map(|(key, value)| KvEntryEnvelope {
                                key: key.to_vec(),
                                value: value.to_vec(),
                            })
                            .collect(),
                    })?)
                }
                Err(KvRangeTransportError::Unavailable(message)) => {
                    encode_response_error(RpcStatusCode::Unavailable, message)
                }
                Err(KvRangeTransportError::NotFound(message)) => {
                    encode_response_error(RpcStatusCode::NotFound, message)
                }
                Err(KvRangeTransportError::DeadlineExceeded(message)) => {
                    encode_response_error(RpcStatusCode::DeadlineExceeded, message)
                }
                Err(KvRangeTransportError::Internal(error)) => {
                    encode_response_error(RpcStatusCode::Internal, error.to_string())
                }
            }
        }
        KVRANGE_APPLY_METHOD => {
            let request = decode_kv_range_apply_request(&request.payload)?;
            match service
                .apply_mutations(
                    &request.range_id,
                    request
                        .mutations
                        .into_iter()
                        .map(|mutation| KvMutation {
                            key: Bytes::from(mutation.key),
                            value: mutation.value.map(Bytes::from),
                        })
                        .collect(),
                    request.expected_epoch,
                )
                .await
            {
                Ok(()) => encode_response_ok(Vec::new()),
                Err(KvRangeTransportError::Unavailable(message)) => {
                    encode_response_error(RpcStatusCode::Unavailable, message)
                }
                Err(KvRangeTransportError::NotFound(message)) => {
                    encode_response_error(RpcStatusCode::NotFound, message)
                }
                Err(KvRangeTransportError::DeadlineExceeded(message)) => {
                    encode_response_error(RpcStatusCode::DeadlineExceeded, message)
                }
                Err(KvRangeTransportError::Internal(error)) => {
                    encode_response_error(RpcStatusCode::Internal, error.to_string())
                }
            }
        }
        KVRANGE_CHECKPOINT_METHOD => {
            let request = decode_kv_range_checkpoint_request(&request.payload)?;
            match service
                .checkpoint_range(
                    &request.range_id,
                    &request.checkpoint_id,
                    request.expected_epoch,
                )
                .await
            {
                Ok(checkpoint) => encode_response_ok(encode_kv_range_checkpoint_reply(
                    &KvRangeCheckpointReplyEnvelope {
                        range_id: checkpoint.range_id,
                        checkpoint_id: checkpoint.checkpoint_id,
                        path: checkpoint.path,
                    },
                )?),
                Err(KvRangeTransportError::Unavailable(message)) => {
                    encode_response_error(RpcStatusCode::Unavailable, message)
                }
                Err(KvRangeTransportError::NotFound(message)) => {
                    encode_response_error(RpcStatusCode::NotFound, message)
                }
                Err(KvRangeTransportError::DeadlineExceeded(message)) => {
                    encode_response_error(RpcStatusCode::DeadlineExceeded, message)
                }
                Err(KvRangeTransportError::Internal(error)) => {
                    encode_response_error(RpcStatusCode::Internal, error.to_string())
                }
            }
        }
        KVRANGE_SNAPSHOT_METHOD => {
            let request = decode_kv_range_snapshot_request(&request.payload)?;
            match service
                .snapshot_metadata(&request.range_id, request.expected_epoch)
                .await
            {
                Ok(snapshot) => encode_response_ok(encode_kv_range_snapshot_reply(
                    &KvRangeSnapshotReplyEnvelope {
                        range_id: snapshot.range_id,
                        boundary: snapshot.boundary,
                        term: snapshot.term,
                        index: snapshot.index,
                        checksum: snapshot.checksum,
                        layout_version: snapshot.layout_version,
                        data_path: snapshot.data_path,
                    },
                )?),
                Err(KvRangeTransportError::Unavailable(message)) => {
                    encode_response_error(RpcStatusCode::Unavailable, message)
                }
                Err(KvRangeTransportError::NotFound(message)) => {
                    encode_response_error(RpcStatusCode::NotFound, message)
                }
                Err(KvRangeTransportError::DeadlineExceeded(message)) => {
                    encode_response_error(RpcStatusCode::DeadlineExceeded, message)
                }
                Err(KvRangeTransportError::Internal(error)) => {
                    encode_response_error(RpcStatusCode::Internal, error.to_string())
                }
            }
        }
        _ => encode_response_error(
            RpcStatusCode::NotSupported,
            format!("unsupported kvrange method id {}", header.method_id),
        ),
    }?;
    Ok(result)
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
    use crate::KvRangeRpc;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_engine::{KvEngine, KvMutation, KvRangeBootstrap, MemoryKvEngine};
    use greenmqtt_kv_raft::{MemoryRaftNode, RaftNode};
    use greenmqtt_kv_server::{HostedRange, KvRangeHost, MemoryKvRangeHost};
    use quinn::rustls::RootCertStore;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn kvrange_quic_get_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-quic-get".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let host = Arc::new(MemoryKvRangeHost::default());
        let range = engine.open_range("range-quic-get").await.unwrap();
        range
            .writer()
            .apply(vec![KvMutation {
                key: b"mango".as_slice().into(),
                value: Some(b"yellow".as_slice().into()),
            }])
            .await
            .unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-quic-get"));
        raft_impl.recover().await.unwrap();
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-quic-get",
                ServiceShardKey::retain("t1"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
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

        let service: Arc<dyn crate::KvRangeTransportService> =
            Arc::new(KvRangeRpc { inner: Some(host) });
        let server = spawn_kvrange_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicKvRangeClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let mango = client
            .get("range-quic-get", b"mango")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&mango[..], b"yellow");
        assert!(client
            .get("range-quic-get", b"missing")
            .await
            .unwrap()
            .is_none());
        let missing_range = client.get("missing-range", b"mango").await.unwrap_err();
        assert!(missing_range.to_string().contains("status=NotFound"));

        server.abort();
    }

    #[tokio::test]
    async fn kvrange_quic_scan_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-quic-scan".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let host = Arc::new(MemoryKvRangeHost::default());
        let range = engine.open_range("range-quic-scan").await.unwrap();
        range
            .writer()
            .apply(vec![
                KvMutation {
                    key: b"mango".as_slice().into(),
                    value: Some(b"yellow".as_slice().into()),
                },
                KvMutation {
                    key: b"pear".as_slice().into(),
                    value: Some(b"green".as_slice().into()),
                },
                KvMutation {
                    key: b"plum".as_slice().into(),
                    value: Some(b"purple".as_slice().into()),
                },
            ])
            .await
            .unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-quic-scan"));
        raft_impl.recover().await.unwrap();
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-quic-scan",
                ServiceShardKey::retain("t1"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
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

        let service: Arc<dyn crate::KvRangeTransportService> =
            Arc::new(KvRangeRpc { inner: Some(host) });
        let server = spawn_kvrange_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicKvRangeClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let scanned = client
            .scan(
                "range-quic-scan",
                Some(RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec()))),
                2,
            )
            .await
            .unwrap();
        assert_eq!(scanned.len(), 2);
        assert_eq!(&scanned[0].0[..], b"mango");
        assert_eq!(&scanned[1].0[..], b"pear");

        server.abort();
    }

    #[tokio::test]
    async fn kvrange_quic_apply_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-quic-apply".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let host = Arc::new(MemoryKvRangeHost::default());
        let range = engine.open_range("range-quic-apply").await.unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-quic-apply"));
        raft_impl.recover().await.unwrap();
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-quic-apply",
                ServiceShardKey::retain("t1"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
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

        let service: Arc<dyn crate::KvRangeTransportService> =
            Arc::new(KvRangeRpc { inner: Some(host) });
        let server = spawn_kvrange_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicKvRangeClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        client
            .apply(
                "range-quic-apply",
                vec![KvMutation {
                    key: b"mango".as_slice().into(),
                    value: Some(b"yellow".as_slice().into()),
                }],
            )
            .await
            .unwrap();
        let mango = client
            .get("range-quic-apply", b"mango")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&mango[..], b"yellow");

        server.abort();
    }

    #[tokio::test]
    async fn kvrange_quic_checkpoint_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-quic-checkpoint".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let host = Arc::new(MemoryKvRangeHost::default());
        let range = engine.open_range("range-quic-checkpoint").await.unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-quic-checkpoint"));
        raft_impl.recover().await.unwrap();
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-quic-checkpoint",
                ServiceShardKey::retain("t1"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
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

        let service: Arc<dyn crate::KvRangeTransportService> =
            Arc::new(KvRangeRpc { inner: Some(host) });
        let server = spawn_kvrange_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicKvRangeClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let checkpoint = client
            .checkpoint("range-quic-checkpoint", "cp-1")
            .await
            .unwrap();
        assert_eq!(checkpoint.range_id, "range-quic-checkpoint");
        assert_eq!(checkpoint.checkpoint_id, "cp-1");
        assert!(checkpoint.path.contains("cp-1"));

        server.abort();
    }

    #[tokio::test]
    async fn kvrange_quic_snapshot_round_trip_works() {
        let (_tempdir, cert_path, key_path, cert) = write_self_signed_tls_material();
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);

        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-quic-snapshot".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let host = Arc::new(MemoryKvRangeHost::default());
        let range = engine.open_range("range-quic-snapshot").await.unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-quic-snapshot"));
        raft_impl.recover().await.unwrap();
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-quic-snapshot",
                ServiceShardKey::retain("t1"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
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

        let service: Arc<dyn crate::KvRangeTransportService> =
            Arc::new(KvRangeRpc { inner: Some(host) });
        let server = spawn_kvrange_quic_server(service, bind, &cert_path, &key_path).unwrap();
        sleep(Duration::from_millis(50)).await;

        let mut roots = RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_config = QuinnClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let client = QuicKvRangeClient::connect_with_client_config(
            format!("quic://{}", server.local_addr()),
            "localhost",
            client_config,
        )
        .await
        .unwrap();

        let snapshot = client.snapshot("range-quic-snapshot").await.unwrap();
        assert_eq!(snapshot.range_id, "range-quic-snapshot");
        assert_eq!(snapshot.boundary.start_key.as_deref(), Some(&b"a"[..]));
        assert_eq!(snapshot.boundary.end_key.as_deref(), Some(&b"z"[..]));

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
