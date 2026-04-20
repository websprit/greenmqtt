use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    ClusterMembershipRegistry, NodeId, ReplicatedRangeDescriptor, ServiceKind, ServiceShardKey,
};
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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

#[async_trait]
pub trait KvRangeExecutorFactory: Send + Sync {
    async fn connect(&self, endpoint: &str) -> anyhow::Result<Arc<dyn KvRangeExecutor>>;
}

#[derive(Clone)]
pub struct LeaderRoutedKvRangeExecutor {
    router: Arc<dyn KvRangeRouter>,
    membership: Arc<dyn ClusterMembershipRegistry>,
    fallback: Arc<dyn KvRangeExecutor>,
    factory: Arc<dyn KvRangeExecutorFactory>,
    query_sequencer: QuerySequencer,
    executors: Arc<RwLock<HashMap<String, Arc<dyn KvRangeExecutor>>>>,
}

impl LeaderRoutedKvRangeExecutor {
    pub fn new(
        router: Arc<dyn KvRangeRouter>,
        membership: Arc<dyn ClusterMembershipRegistry>,
        fallback: Arc<dyn KvRangeExecutor>,
        factory: Arc<dyn KvRangeExecutorFactory>,
    ) -> Self {
        Self {
            router,
            membership,
            fallback,
            factory,
            query_sequencer: QuerySequencer::default(),
            executors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn route_by_id(&self, range_id: &str) -> anyhow::Result<RangeRoute> {
        self.router
            .by_id(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no route registered for range {range_id}"))
    }

    fn endpoint_kind(route: &RangeRoute) -> ServiceKind {
        route.descriptor.shard.service_kind()
    }

    async fn executor_for_endpoint(
        &self,
        endpoint: &str,
    ) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
        if let Some(executor) = self
            .executors
            .read()
            .expect("kv client executor cache poisoned")
            .get(endpoint)
            .cloned()
        {
            return Ok(executor);
        }
        let executor = self.factory.connect(endpoint).await?;
        self.executors
            .write()
            .expect("kv client executor cache poisoned")
            .insert(endpoint.to_string(), executor.clone());
        Ok(executor)
    }

    async fn routed_executor_for_node(
        &self,
        route: &RangeRoute,
        node_id: NodeId,
    ) -> anyhow::Result<Option<Arc<dyn KvRangeExecutor>>> {
        let Some(member) = self.membership.resolve_member(node_id).await? else {
            return Ok(None);
        };
        let Some(endpoint) = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == Self::endpoint_kind(route))
            .map(|endpoint| endpoint.endpoint.clone())
        else {
            return Ok(None);
        };
        Ok(Some(self.executor_for_endpoint(&endpoint).await?))
    }

    async fn leader_executor(
        &self,
        route: &RangeRoute,
    ) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
        if let Some(node_id) = route.leader_node_id {
            if let Some(executor) = self.routed_executor_for_node(route, node_id).await? {
                return Ok(executor);
            }
        }
        Ok(self.fallback.clone())
    }

    async fn query_executor(&self, route: &RangeRoute) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
        if let Some(node_id) = self.query_sequencer.select(route).or(route.leader_node_id) {
            if let Some(executor) = self.routed_executor_for_node(route, node_id).await? {
                return Ok(executor);
            }
        }
        Ok(self.fallback.clone())
    }
}

#[async_trait]
impl KvRangeExecutor for LeaderRoutedKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let route = self.route_by_id(range_id).await?;
        let executor = self.query_executor(&route).await?;
        match executor.get(range_id, key).await {
            Ok(value) => Ok(value),
            Err(_) => {
                let refreshed = self.route_by_id(range_id).await?;
                self.query_executor(&refreshed)
                    .await?
                    .get(range_id, key)
                    .await
            }
        }
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let route = self.route_by_id(range_id).await?;
        let executor = self.query_executor(&route).await?;
        match executor.scan(range_id, boundary.clone(), limit).await {
            Ok(entries) => Ok(entries),
            Err(_) => {
                let refreshed = self.route_by_id(range_id).await?;
                self.query_executor(&refreshed)
                    .await?
                    .scan(range_id, boundary, limit)
                    .await
            }
        }
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let route = self.route_by_id(range_id).await?;
        let executor = self.leader_executor(&route).await?;
        match executor.apply(range_id, mutations.clone()).await {
            Ok(()) => Ok(()),
            Err(_) => {
                let refreshed = self.route_by_id(range_id).await?;
                self.leader_executor(&refreshed)
                    .await?
                    .apply(range_id, mutations)
                    .await
            }
        }
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let route = self.route_by_id(range_id).await?;
        let executor = self.leader_executor(&route).await?;
        match executor.checkpoint(range_id, checkpoint_id).await {
            Ok(checkpoint) => Ok(checkpoint),
            Err(_) => {
                let refreshed = self.route_by_id(range_id).await?;
                self.leader_executor(&refreshed)
                    .await?
                    .checkpoint(range_id, checkpoint_id)
                    .await
            }
        }
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let route = self.route_by_id(range_id).await?;
        let executor = self.leader_executor(&route).await?;
        match executor.snapshot(range_id).await {
            Ok(snapshot) => Ok(snapshot),
            Err(_) => {
                let refreshed = self.route_by_id(range_id).await?;
                self.leader_executor(&refreshed)
                    .await?
                    .snapshot(range_id)
                    .await
            }
        }
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
    use super::{
        KvRangeExecutor, KvRangeExecutorFactory, KvRangeRouter, LeaderRoutedKvRangeExecutor,
        MemoryKvRangeRouter, MutationRedirect, QuerySequencer, RetryDirective,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use greenmqtt_core::{
        ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership, RangeBoundary,
        RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, ServiceEndpoint,
        ServiceKind, ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, RwLock};

    fn descriptor(
        id: &str,
        shard: ServiceShardKey,
        start: &[u8],
        end: &[u8],
        leader: u64,
    ) -> ReplicatedRangeDescriptor {
        ReplicatedRangeDescriptor::new(
            id,
            shard,
            RangeBoundary::new(Some(start.to_vec()), Some(end.to_vec())),
            1,
            1,
            Some(leader),
            vec![
                RangeReplica::new(leader, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(
                    leader + 1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                ),
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
            descriptor: descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                1,
            ),
            leader_node_id: Some(1),
            query_ready_nodes: vec![1, 2],
        };
        let sequencer = QuerySequencer::default();
        assert_eq!(sequencer.select(&route), Some(1));
        assert_eq!(sequencer.select(&route), Some(2));
        assert_eq!(sequencer.select(&route), Some(1));
    }

    #[derive(Default)]
    struct TestMembershipRegistry {
        members: Arc<RwLock<BTreeMap<u64, ClusterNodeMembership>>>,
    }

    #[async_trait]
    impl ClusterMembershipRegistry for TestMembershipRegistry {
        async fn upsert_member(
            &self,
            member: ClusterNodeMembership,
        ) -> anyhow::Result<Option<ClusterNodeMembership>> {
            Ok(self
                .members
                .write()
                .expect("test membership poisoned")
                .insert(member.node_id, member))
        }

        async fn resolve_member(
            &self,
            node_id: u64,
        ) -> anyhow::Result<Option<ClusterNodeMembership>> {
            Ok(self
                .members
                .read()
                .expect("test membership poisoned")
                .get(&node_id)
                .cloned())
        }

        async fn remove_member(
            &self,
            node_id: u64,
        ) -> anyhow::Result<Option<ClusterNodeMembership>> {
            Ok(self
                .members
                .write()
                .expect("test membership poisoned")
                .remove(&node_id))
        }

        async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
            Ok(self
                .members
                .read()
                .expect("test membership poisoned")
                .values()
                .cloned()
                .collect())
        }
    }

    #[derive(Default)]
    struct TestExecutor {
        writes: Arc<RwLock<HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>>>,
    }

    #[async_trait]
    impl KvRangeExecutor for TestExecutor {
        async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            Ok(self
                .writes
                .read()
                .expect("test executor poisoned")
                .get(range_id)
                .and_then(|entries| entries.get(key).cloned())
                .map(Bytes::from))
        }

        async fn scan(
            &self,
            range_id: &str,
            _boundary: Option<greenmqtt_core::RangeBoundary>,
            limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            Ok(self
                .writes
                .read()
                .expect("test executor poisoned")
                .get(range_id)
                .into_iter()
                .flat_map(|entries| entries.iter().take(limit))
                .map(|(key, value)| (Bytes::from(key.clone()), Bytes::from(value.clone())))
                .collect())
        }

        async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
            let mut guard = self.writes.write().expect("test executor poisoned");
            let entries = guard.entry(range_id.to_string()).or_default();
            for mutation in mutations {
                if let Some(value) = mutation.value {
                    entries.insert(mutation.key.to_vec(), value.to_vec());
                } else {
                    entries.remove(mutation.key.as_ref());
                }
            }
            Ok(())
        }

        async fn checkpoint(
            &self,
            range_id: &str,
            checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            Ok(KvRangeCheckpoint {
                range_id: range_id.to_string(),
                checkpoint_id: checkpoint_id.to_string(),
                path: format!("/tmp/{range_id}-{checkpoint_id}"),
            })
        }

        async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            Ok(KvRangeSnapshot {
                range_id: range_id.to_string(),
                boundary: RangeBoundary::full(),
                data_path: format!("/tmp/{range_id}-snapshot"),
            })
        }
    }

    #[derive(Default)]
    struct TestExecutorFactory {
        executors: Arc<RwLock<HashMap<String, Arc<dyn KvRangeExecutor>>>>,
    }

    impl TestExecutorFactory {
        fn register(&self, endpoint: &str, executor: Arc<dyn KvRangeExecutor>) {
            self.executors
                .write()
                .expect("test executor factory poisoned")
                .insert(endpoint.to_string(), executor);
        }
    }

    #[async_trait]
    impl KvRangeExecutorFactory for TestExecutorFactory {
        async fn connect(&self, endpoint: &str) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
            self.executors
                .read()
                .expect("test executor factory poisoned")
                .get(endpoint)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no executor registered for endpoint {endpoint}"))
        }
    }

    #[tokio::test]
    async fn leader_routed_executor_sends_mutations_to_current_leader() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback_impl = Arc::new(TestExecutor::default());
        let node7_impl = Arc::new(TestExecutor::default());
        let node9_impl = Arc::new(TestExecutor::default());
        let fallback: Arc<dyn KvRangeExecutor> = fallback_impl.clone();
        let factory = Arc::new(TestExecutorFactory::default());
        factory.register("node7", node7_impl.clone());
        factory.register("node9", node9_impl.clone());
        membership
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(ServiceKind::Retain, 7, "node7")],
            ))
            .await
            .unwrap();
        membership
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(ServiceKind::Retain, 9, "node9")],
            ))
            .await
            .unwrap();
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                7,
            ))
            .await
            .unwrap();
        let executor =
            LeaderRoutedKvRangeExecutor::new(router.clone(), membership.clone(), fallback, factory);

        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"apple"),
                    value: Some(Bytes::from_static(b"red")),
                }],
            )
            .await
            .unwrap();

        assert_eq!(
            node7_impl.get("range-1", b"apple").await.unwrap().unwrap(),
            Bytes::from_static(b"red")
        );
        assert!(node9_impl.get("range-1", b"apple").await.unwrap().is_none());
        assert!(fallback_impl
            .get("range-1", b"apple")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn leader_routed_executor_follows_leader_changes() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback: Arc<dyn KvRangeExecutor> = Arc::new(TestExecutor::default());
        let node7_impl = Arc::new(TestExecutor::default());
        let node9_impl = Arc::new(TestExecutor::default());
        let factory = Arc::new(TestExecutorFactory::default());
        factory.register("node7", node7_impl.clone());
        factory.register("node9", node9_impl.clone());
        for (node_id, endpoint) in [(7, "node7"), (9, "node9")] {
            membership
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(ServiceKind::Retain, node_id, endpoint)],
                ))
                .await
                .unwrap();
        }
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                7,
            ))
            .await
            .unwrap();
        let executor =
            LeaderRoutedKvRangeExecutor::new(router.clone(), membership, fallback, factory);

        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"alpha"),
                    value: Some(Bytes::from_static(b"one")),
                }],
            )
            .await
            .unwrap();
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                9,
            ))
            .await
            .unwrap();
        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"beta"),
                    value: Some(Bytes::from_static(b"two")),
                }],
            )
            .await
            .unwrap();

        assert!(node7_impl.get("range-1", b"alpha").await.unwrap().is_some());
        assert!(node7_impl.get("range-1", b"beta").await.unwrap().is_none());
        assert!(node9_impl.get("range-1", b"alpha").await.unwrap().is_none());
        assert_eq!(
            node9_impl.get("range-1", b"beta").await.unwrap().unwrap(),
            Bytes::from_static(b"two")
        );
    }
}
