use crate::*;

pub(crate) fn range_engine_root(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("range-engine")
}

fn legacy_rocksdb_store_dirs(data_dir: &std::path::Path) -> [PathBuf; 6] {
    [
        data_dir.join("sessions"),
        data_dir.join("routes"),
        data_dir.join("subscriptions"),
        data_dir.join("inbox"),
        data_dir.join("inflight"),
        data_dir.join("retain"),
    ]
}

fn parse_store_raft_peers(node_id: u64) -> anyhow::Result<BTreeMap<u64, String>> {
    let mut peers = BTreeMap::new();
    let Some(raw) = std::env::var("GREENMQTT_STORE_RAFT_PEERS")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        peers.insert(node_id, String::new());
        return Ok(peers);
    };
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (node_id, endpoint) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid GREENMQTT_STORE_RAFT_PEERS entry: {item}"))?;
        peers.insert(node_id.parse()?, endpoint.trim().to_string());
    }
    peers.entry(node_id).or_insert_with(String::new);
    Ok(peers)
}

fn store_range_descriptors(voter_ids: &[u64]) -> Vec<ReplicatedRangeDescriptor> {
    let leader_node_id = voter_ids.first().copied();
    let replicas = voter_ids
        .iter()
        .map(|node_id| {
            RangeReplica::new(*node_id, ReplicaRole::Voter, ReplicaSyncState::Replicating)
        })
        .collect::<Vec<_>>();
    let metadata_layout = metadata_plane_layout_from_env();
    let mut descriptors: Vec<(String, ServiceShardKey)> = vec![
        (
            "__sessiondict".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__dist".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Dist,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__retain".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Retain,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__inbox".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Inbox,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__inflight".to_string(),
            ServiceShardKey {
                kind: ServiceShardKind::Inflight,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
    ];
    descriptors.extend([
        (
            metadata_layout.assignments_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "assignments".into(),
            },
        ),
        (
            metadata_layout.members_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "members".into(),
            },
        ),
        (
            metadata_layout.ranges_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "ranges".into(),
            },
        ),
        (
            metadata_layout.balancers_range_id,
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "__metadata".into(),
                scope: "balancers".into(),
            },
        ),
    ]);
    descriptors
        .into_iter()
        .map(|(range_id, shard)| {
            ReplicatedRangeDescriptor::new(
                range_id,
                shard,
                RangeBoundary::full(),
                1,
                1,
                leader_node_id,
                replicas.clone(),
                0,
                0,
                ServiceShardLifecycle::Serving,
            )
        })
        .collect()
}

#[derive(Clone, Default)]
struct StaticStoreReplicaTransport {
    endpoints: Arc<RwLock<BTreeMap<u64, String>>>,
    clients: Arc<tokio::sync::Mutex<HashMap<u64, RaftTransportGrpcClient>>>,
}

impl StaticStoreReplicaTransport {
    fn new(endpoints: BTreeMap<u64, String>) -> Self {
        Self {
            endpoints: Arc::new(RwLock::new(endpoints)),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn client_for(&self, target_node_id: u64) -> anyhow::Result<RaftTransportGrpcClient> {
        if let Some(client) = self.clients.lock().await.get(&target_node_id).cloned() {
            return Ok(client);
        }
        let endpoint = self
            .endpoints
            .read()
            .expect("store transport endpoints poisoned")
            .get(&target_node_id)
            .cloned()
            .filter(|endpoint| !endpoint.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("no store raft endpoint configured for node {target_node_id}")
            })?;
        let client = RaftTransportGrpcClient::connect(endpoint).await?;
        self.clients
            .lock()
            .await
            .insert(target_node_id, client.clone());
        Ok(client)
    }
}

#[async_trait]
impl greenmqtt_kv_server::ReplicaTransport for StaticStoreReplicaTransport {
    async fn send(
        &self,
        from_node_id: u64,
        target_node_id: u64,
        range_id: &str,
        message: &greenmqtt_kv_raft::RaftMessage,
    ) -> anyhow::Result<()> {
        let client = self.client_for(target_node_id).await?;
        client.send(range_id, from_node_id, message).await
    }
}

async fn store_membership_registry(
    peer_endpoints: &BTreeMap<u64, String>,
) -> anyhow::Result<Arc<StaticServiceEndpointRegistry>> {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    for (node_id, endpoint) in peer_endpoints {
        if endpoint.is_empty() {
            continue;
        }
        registry
            .upsert_member(ClusterNodeMembership::new(
                *node_id,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(ServiceKind::SessionDict, *node_id, endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Dist, *node_id, endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Inbox, *node_id, endpoint.clone()),
                    ServiceEndpoint::new(ServiceKind::Retain, *node_id, endpoint.clone()),
                ],
            ))
            .await?;
    }
    Ok(registry)
}

pub(crate) fn metadata_plane_layout_from_env() -> MetadataPlaneLayout {
    let range_base =
        std::env::var("GREENMQTT_METADATA_RANGE_ID").unwrap_or_else(|_| "__metadata".to_string());
    MetadataPlaneLayout {
        assignments_range_id: std::env::var("GREENMQTT_METADATA_ASSIGNMENTS_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.assignments")),
        members_range_id: std::env::var("GREENMQTT_METADATA_MEMBERS_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.members")),
        ranges_range_id: std::env::var("GREENMQTT_METADATA_RANGES_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.ranges")),
        balancers_range_id: std::env::var("GREENMQTT_METADATA_BALANCERS_RANGE_ID")
            .unwrap_or_else(|_| format!("{range_base}.balancers")),
    }
}

pub(crate) fn local_state_serve_metadata_registry(
    range_host: Option<Arc<dyn KvRangeHost>>,
    storage_backend: &str,
) -> Option<Arc<dyn MetadataRegistry>> {
    if storage_backend != "rocksdb" {
        return None;
    };
    range_host.map(|host| {
        Arc::new(ReplicatedMetadataRegistry::with_layout(
            Arc::new(HostedKvRangeExecutor { host }),
            metadata_plane_layout_from_env(),
        )) as Arc<dyn MetadataRegistry>
    })
}

pub(crate) async fn range_scoped_rocksdb_stack(
    data_dir: PathBuf,
    node_id: u64,
) -> anyhow::Result<LocalRangeRuntimeStack> {
    let root = range_engine_root(&data_dir);
    let has_legacy_layout = legacy_rocksdb_store_dirs(&data_dir)
        .into_iter()
        .any(|path| path.exists());
    anyhow::ensure!(
        root.exists() || !has_legacy_layout,
        "legacy RocksDB store layout detected in {}; migrate to range-engine layout before using GREENMQTT_STORAGE_BACKEND=rocksdb",
        data_dir.display()
    );

    let engine = Arc::new(RocksDbKvEngine::open(&root)?);
    let peer_endpoints = parse_store_raft_peers(node_id)?;
    let mut voter_ids = peer_endpoints.keys().copied().collect::<Vec<_>>();
    voter_ids.sort_unstable();
    let descriptors = store_range_descriptors(&voter_ids);
    let host = Arc::new(MemoryKvRangeHost::default());
    for descriptor in &descriptors {
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::from(engine.open_range(&descriptor.id).await?);
        let raft_impl = Arc::new(MemoryRaftNode::new(
            node_id,
            &descriptor.id,
            greenmqtt_kv_raft::RaftClusterConfig {
                voters: voter_ids.clone(),
                learners: Vec::new(),
            },
        ));
        raft_impl.recover().await?;
        if let Some(leader_node_id) = descriptor.leader_node_id {
            raft_impl.transfer_leadership(leader_node_id).await?;
        }
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: descriptor.clone(),
            raft,
            space,
        })
        .await?;
    }
    let router: Arc<dyn KvRangeRouter> =
        Arc::new(HostedStoreRangeRouter::new(host.clone(), descriptors));
    let executor: Arc<dyn KvRangeExecutor> = if voter_ids.len() > 1 {
        let membership = store_membership_registry(&peer_endpoints).await?;
        Arc::new(LeaderRoutedKvRangeExecutor::new(
            router.clone(),
            membership,
            Arc::new(HostedKvRangeExecutor { host: host.clone() }),
            Arc::new(KvRangeGrpcExecutorFactory),
        ))
    } else {
        Arc::new(HostedKvRangeExecutor { host: host.clone() })
    };
    let transport: Arc<dyn greenmqtt_kv_server::ReplicaTransport> = if voter_ids.len() > 1 {
        Arc::new(StaticStoreReplicaTransport::new(peer_endpoints))
    } else {
        Arc::new(greenmqtt_rpc::NoopReplicaTransport)
    };
    let range_runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        transport,
        Some(Arc::new(RocksRangeLifecycleManager {
            engine: engine.clone(),
            node_id,
        })),
        Duration::from_secs(1),
        1024,
        64,
        64,
    ));
    Ok(LocalRangeRuntimeStack {
        sessiondict: Arc::new(ReplicatedSessionDictHandle::new(Arc::new(
            RoutedRangeDataClient::new(router.clone(), executor.clone()),
        ))),
        dist: Arc::new(ReplicatedDistHandle::new(Arc::new(RoutedRangeDataClient::new(
            router.clone(),
            executor.clone(),
        )))),
        inbox: Arc::new(ReplicatedInboxHandle::new(Arc::new(RoutedRangeDataClient::new(
            router.clone(),
            executor.clone(),
        )))),
        retain: Arc::new(ReplicatedRetainHandle::new(Arc::new(
            RoutedRangeDataClient::new(router, executor),
        ))),
        range_host: host,
        range_runtime,
    })
}

#[derive(Clone)]
pub(crate) struct RocksRangeLifecycleManager {
    engine: Arc<RocksDbKvEngine>,
    node_id: u64,
}

#[async_trait]
impl RangeLifecycleManager for RocksRangeLifecycleManager {
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
        let space = Arc::from(self.engine.open_range(&descriptor.id).await?);
        let raft_impl = Arc::new(MemoryRaftNode::new(
            self.node_id,
            &descriptor.id,
            greenmqtt_kv_raft::RaftClusterConfig {
                voters: descriptor
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == ReplicaRole::Voter)
                    .map(|replica| replica.node_id)
                    .collect(),
                learners: descriptor
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == ReplicaRole::Learner)
                    .map(|replica| replica.node_id)
                    .collect(),
            },
        ));
        raft_impl.recover().await?;
        if let Some(leader_node_id) = descriptor.leader_node_id {
            raft_impl.transfer_leadership(leader_node_id).await?;
        }
        let raft: Arc<dyn RaftNode> = raft_impl;
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
