use crate::*;

#[derive(Clone)]
pub(crate) struct HostedKvRangeExecutor {
    pub(crate) host: Arc<dyn KvRangeHost>,
}

#[async_trait]
impl KvRangeExecutor for HostedKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<bytes::Bytes>> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.reader().get(key).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted
            .space
            .reader()
            .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
            .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.writer().apply(mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.checkpoint(checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let hosted = self
            .host
            .open_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range not found"))?;
        hosted.space.snapshot().await
    }
}

#[derive(Clone)]
pub(crate) struct HostedStoreRangeRouter {
    host: Arc<dyn KvRangeHost>,
    descriptors: Arc<RwLock<BTreeMap<String, ReplicatedRangeDescriptor>>>,
}

impl HostedStoreRangeRouter {
    pub(crate) fn new(
        host: Arc<dyn KvRangeHost>,
        descriptors: impl IntoIterator<Item = ReplicatedRangeDescriptor>,
    ) -> Self {
        let router = Self {
            host,
            descriptors: Arc::new(RwLock::new(BTreeMap::new())),
        };
        {
            let mut guard = router
                .descriptors
                .write()
                .expect("hosted store router poisoned");
            for descriptor in descriptors {
                guard.insert(descriptor.id.clone(), descriptor);
            }
        }
        router
    }

    fn refresh_descriptor_from_status(
        &self,
        status: greenmqtt_kv_server::HostedRangeStatus,
    ) -> ReplicatedRangeDescriptor {
        let mut descriptor = status.descriptor;
        descriptor.leader_node_id = status.raft.leader_node_id;
        let mut replicas = status.raft.cluster_config.replicas();
        for replica in &mut replicas {
            replica.sync_state = status
                .raft
                .replica_progress
                .iter()
                .find(|progress| progress.node_id == replica.node_id)
                .map(|progress| {
                    if progress.match_index >= status.raft.commit_index {
                        ReplicaSyncState::Replicating
                    } else {
                        ReplicaSyncState::Probing
                    }
                })
                .unwrap_or(ReplicaSyncState::Offline);
        }
        descriptor.replicas = replicas;
        descriptor.commit_index = status.raft.commit_index;
        descriptor.applied_index = status.raft.applied_index;
        descriptor
    }

    async fn current_descriptors(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let statuses = self.host.list_ranges().await?;
        if statuses.is_empty() {
            return Ok(self
                .descriptors
                .read()
                .expect("hosted store router poisoned")
                .values()
                .cloned()
                .collect());
        }
        Ok(statuses
            .into_iter()
            .map(|status| self.refresh_descriptor_from_status(status))
            .collect())
    }

    fn shard_matches(descriptor: &ReplicatedRangeDescriptor, shard: &ServiceShardKey) -> bool {
        descriptor.shard.kind == shard.kind
            && (descriptor.shard.tenant_id == shard.tenant_id || descriptor.shard.tenant_id == "*")
            && (descriptor.shard.scope == shard.scope || descriptor.shard.scope == "*")
    }

    fn shard_specificity(descriptor: &ReplicatedRangeDescriptor, shard: &ServiceShardKey) -> usize {
        usize::from(descriptor.shard.tenant_id == shard.tenant_id) * 2
            + usize::from(descriptor.shard.scope == shard.scope)
    }
}

#[async_trait]
impl KvRangeRouter for HostedStoreRangeRouter {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        self.descriptors
            .write()
            .expect("hosted store router poisoned")
            .insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .descriptors
            .write()
            .expect("hosted store router poisoned")
            .remove(range_id))
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.current_descriptors().await
    }

    async fn by_id(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .current_descriptors()
            .await?
            .into_iter()
            .find(|descriptor| descriptor.id == range_id)
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor))
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .current_descriptors()
            .await?
            .into_iter()
            .filter(|descriptor| {
                Self::shard_matches(descriptor, shard) && descriptor.contains_key(key)
            })
            .max_by_key(|descriptor| Self::shard_specificity(descriptor, shard))
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor))
    }

    async fn route_shard(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Vec<greenmqtt_kv_client::RangeRoute>> {
        let mut descriptors = self
            .current_descriptors()
            .await?
            .into_iter()
            .filter(|descriptor| Self::shard_matches(descriptor, shard))
            .collect::<Vec<_>>();
        descriptors.sort_by_key(|descriptor| {
            std::cmp::Reverse(Self::shard_specificity(descriptor, shard))
        });
        Ok(descriptors
            .into_iter()
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor)
            .collect())
    }
}
