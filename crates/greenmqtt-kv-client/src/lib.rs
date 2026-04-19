use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{NodeId, ReplicatedRangeDescriptor, ServiceShardKey};
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetryDirective {
    RetryWithLeader(NodeId),
    RefreshRouteCache,
    FailFast,
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
}

#[async_trait]
pub trait KvRangeExecutor: Send + Sync {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>>;
    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>>;
    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()>;
    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint>;
    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot>;
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

#[derive(Debug, Clone, Default)]
pub struct QuerySequencer {
    cursor: Arc<RwLock<usize>>,
}

impl QuerySequencer {
    pub fn select(&self, route: &RangeRoute) -> Option<NodeId> {
        if route.query_ready_nodes.is_empty() {
            return None;
        }
        let mut cursor = self.cursor.write().expect("sequencer poisoned");
        let node = route.query_ready_nodes[*cursor % route.query_ready_nodes.len()];
        *cursor += 1;
        Some(node)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationRedirect {
    pub range_id: String,
    pub directive: RetryDirective,
}

impl MutationRedirect {
    pub fn from_route(route: &RangeRoute) -> Self {
        let directive = route
            .leader_node_id
            .map(RetryDirective::RetryWithLeader)
            .unwrap_or(RetryDirective::RefreshRouteCache);
        Self {
            range_id: route.descriptor.id.clone(),
            directive,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutedMutation {
    pub target_node_id: NodeId,
    pub range_id: String,
    pub payload: Bytes,
}

impl RoutedMutation {
    pub fn new(target_node_id: NodeId, range_id: impl Into<String>, payload: Bytes) -> Self {
        Self {
            target_node_id,
            range_id: range_id.into(),
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KvRangeRouter, MemoryKvRangeRouter, MutationRedirect, QuerySequencer, RetryDirective};
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };

    fn descriptor(id: &str, shard: ServiceShardKey, start: &[u8], end: &[u8], leader: u64) -> ReplicatedRangeDescriptor {
        ReplicatedRangeDescriptor::new(
            id,
            shard,
            RangeBoundary::new(Some(start.to_vec()), Some(end.to_vec())),
            1,
            1,
            Some(leader),
            vec![
                RangeReplica::new(leader, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(leader + 1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
            ],
            0,
            0,
            ServiceShardLifecycle::Serving,
        )
    }

    #[tokio::test]
    async fn memory_router_routes_keys_to_matching_range_and_leader() {
        let router = MemoryKvRangeRouter::default();
        let shard = ServiceShardKey::retain("tenant-a");
        router
            .upsert(descriptor("range-1", shard.clone(), b"a", b"m", 1))
            .await
            .unwrap();
        router
            .upsert(descriptor("range-2", shard.clone(), b"m", b"z", 3))
            .await
            .unwrap();

        let route = router.route_key(&shard, b"melon").await.unwrap().unwrap();
        assert_eq!(route.descriptor.id, "range-2");
        assert_eq!(route.leader_node_id, Some(3));
        assert_eq!(route.query_ready_nodes, vec![3, 4]);
    }

    #[tokio::test]
    async fn router_lists_ranges_by_shard_and_supports_redirect_hints() {
        let router = MemoryKvRangeRouter::default();
        let shard = ServiceShardKey::dist("tenant-a");
        router
            .upsert(descriptor("range-1", shard.clone(), b"a", b"z", 7))
            .await
            .unwrap();
        let routes = router.route_shard(&shard).await.unwrap();
        assert_eq!(routes.len(), 1);
        let redirect = MutationRedirect::from_route(&routes[0]);
        assert_eq!(redirect.range_id, "range-1");
        assert_eq!(redirect.directive, RetryDirective::RetryWithLeader(7));
    }

    #[test]
    fn query_sequencer_round_robins_query_ready_replicas() {
        let route = super::RangeRoute {
            descriptor: descriptor("range-1", ServiceShardKey::retain("tenant-a"), b"a", b"z", 1),
            leader_node_id: Some(1),
            query_ready_nodes: vec![1, 2],
        };
        let sequencer = QuerySequencer::default();
        assert_eq!(sequencer.select(&route), Some(1));
        assert_eq!(sequencer.select(&route), Some(2));
        assert_eq!(sequencer.select(&route), Some(1));
    }
}
