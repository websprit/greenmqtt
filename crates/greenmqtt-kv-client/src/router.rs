use async_trait::async_trait;
use greenmqtt_core::{NodeId, ReplicatedRangeDescriptor, ServiceShardKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeRoute {
    pub descriptor: ReplicatedRangeDescriptor,
    pub leader_node_id: Option<NodeId>,
    pub query_ready_nodes: Vec<NodeId>,
}

impl RangeRoute {
    pub fn from_descriptor(descriptor: ReplicatedRangeDescriptor) -> Self {
        let leader_node_id = descriptor.leader_node_id;
        let query_ready_nodes = descriptor
            .replicas
            .iter()
            .filter(|replica| replica.is_query_ready())
            .map(|replica| replica.node_id)
            .collect();
        Self {
            descriptor,
            leader_node_id,
            query_ready_nodes,
        }
    }
}

#[async_trait]
pub trait KvRangeRouter: Send + Sync {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()>;
    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>>;
    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>>;
    async fn by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>>;
    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>>;
    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>>;
    async fn invalidate_range(&self, _range_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn invalidate_key(&self, _shard: &ServiceShardKey, _key: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn invalidate_shard(&self, _shard: &ServiceShardKey) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryKvRangeRouter {
    descriptors: Arc<RwLock<BTreeMap<String, ReplicatedRangeDescriptor>>>,
}

#[async_trait]
impl KvRangeRouter for MemoryKvRangeRouter {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        self.descriptors
            .write()
            .expect("router poisoned")
            .insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .descriptors
            .write()
            .expect("router poisoned")
            .remove(range_id))
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        Ok(self
            .descriptors
            .read()
            .expect("router poisoned")
            .values()
            .cloned()
            .collect())
    }

    async fn by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        Ok(self
            .descriptors
            .read()
            .expect("router poisoned")
            .get(range_id)
            .cloned()
            .map(RangeRoute::from_descriptor))
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        Ok(self
            .descriptors
            .read()
            .expect("router poisoned")
            .values()
            .find(|descriptor| descriptor.shard == *shard && descriptor.contains_key(key))
            .cloned()
            .map(RangeRoute::from_descriptor))
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        Ok(self
            .descriptors
            .read()
            .expect("router poisoned")
            .values()
            .filter(|descriptor| descriptor.shard == *shard)
            .cloned()
            .map(RangeRoute::from_descriptor)
            .collect())
    }
}
