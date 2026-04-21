use crate::consistency::{RangeReadOptions, ReadConsistency};
use crate::error::{classify_kv_error, should_refresh_route, KvRangeErrorKind, RetryDirective};
use crate::executor::{KvRangeExecutor, KvRangeExecutorFactory};
use crate::router::{KvRangeRouter, RangeRoute};
use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    ClusterMembershipRegistry, NodeId, ReplicatedRangeDescriptor, ServiceKind, ServiceShardKey,
};
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[async_trait]
pub trait RangeDataClient: Send + Sync {
    async fn list_ranges(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>>;
    async fn route_by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>>;
    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>>;
    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>>;
    async fn get_with_options(
        &self,
        range_id: &str,
        key: &[u8],
        options: RangeReadOptions,
    ) -> anyhow::Result<Option<Bytes>>;
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.get_with_options(range_id, key, RangeReadOptions::default())
            .await
    }
    async fn scan_with_options(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
        options: RangeReadOptions,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>>;
    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        self.scan_with_options(range_id, boundary, limit, RangeReadOptions::default())
            .await
    }
    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()>;
    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint>;
    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot>;
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

#[derive(Clone)]
pub struct RoutedRangeDataClient {
    router: Arc<dyn KvRangeRouter>,
    executor: Arc<dyn KvRangeExecutor>,
}

impl RoutedRangeDataClient {
    pub fn new(router: Arc<dyn KvRangeRouter>, executor: Arc<dyn KvRangeExecutor>) -> Self {
        Self { router, executor }
    }
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

    async fn load_route_by_id(&self, range_id: &str) -> anyhow::Result<RangeRoute> {
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

    fn candidate_nodes(route: &RangeRoute) -> Vec<NodeId> {
        let mut candidates = Vec::new();
        if let Some(leader_node_id) = route.leader_node_id {
            candidates.push(leader_node_id);
        }
        for node_id in &route.query_ready_nodes {
            if !candidates.contains(node_id) {
                candidates.push(*node_id);
            }
        }
        for replica in &route.descriptor.replicas {
            if !candidates.contains(&replica.node_id) {
                candidates.push(replica.node_id);
            }
        }
        candidates
    }

    async fn execute_query_with_retry<T, F, Fut>(
        &self,
        range_id: &str,
        route: RangeRoute,
        op: F,
    ) -> anyhow::Result<T>
    where
        F: Fn(Arc<dyn KvRangeExecutor>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let mut current = route;
        for attempt in 0..3 {
            let preferred = self.query_sequencer.select(&current);
            let mut candidates = Self::candidate_nodes(&current);
            if let Some(preferred) = preferred {
                candidates.retain(|node_id| *node_id != preferred);
                candidates.insert(0, preferred);
            }
            let mut last_error = None;
            for node_id in candidates {
                let executor = if let Some(executor) =
                    self.routed_executor_for_node(&current, node_id).await?
                {
                    executor
                } else if Some(node_id) == current.leader_node_id {
                    self.fallback.clone()
                } else {
                    continue;
                };
                match op(executor).await {
                    Ok(value) => return Ok(value),
                    Err(error) => match classify_kv_error(&error) {
                        KvRangeErrorKind::NotLeader(Some(node_id)) => {
                            current.leader_node_id = Some(node_id);
                            last_error = Some(error);
                            break;
                        }
                        KvRangeErrorKind::EpochMismatch | KvRangeErrorKind::RangeNotFound => {
                            current = self.load_route_by_id(range_id).await?;
                            last_error = Some(error);
                            break;
                        }
                        KvRangeErrorKind::ConfigChanging | KvRangeErrorKind::SnapshotInProgress => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            current = self.load_route_by_id(range_id).await?;
                            last_error = Some(error);
                            break;
                        }
                        KvRangeErrorKind::TransportUnavailable => {
                            last_error = Some(error);
                        }
                        KvRangeErrorKind::NotLeader(None) | KvRangeErrorKind::Other => {
                            last_error = Some(error);
                        }
                        KvRangeErrorKind::RetryExhausted => {
                            return Err(error);
                        }
                    },
                }
            }
            if attempt == 2 {
                if let Some(error) = last_error {
                    return Err(error);
                }
                anyhow::bail!("kv/retry-exhausted range={range_id}");
            }
            if last_error.is_none() {
                current = self.load_route_by_id(range_id).await?;
            } else if let Some(error) = &last_error {
                if should_refresh_route(&classify_kv_error(error)) {
                    self.router.invalidate_range(range_id).await?;
                    current = self.load_route_by_id(range_id).await?;
                }
            }
        }
        anyhow::bail!("kv/retry-exhausted range={range_id}")
    }

    async fn execute_leader_with_retry<T, F, Fut>(
        &self,
        range_id: &str,
        route: RangeRoute,
        op: F,
    ) -> anyhow::Result<T>
    where
        F: Fn(Arc<dyn KvRangeExecutor>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let mut current = route;
        for attempt in 0..3 {
            let mut last_error = None;
            for node_id in Self::candidate_nodes(&current) {
                let executor = if let Some(executor) =
                    self.routed_executor_for_node(&current, node_id).await?
                {
                    executor
                } else if Some(node_id) == current.leader_node_id {
                    self.fallback.clone()
                } else {
                    continue;
                };
                match op(executor).await {
                    Ok(value) => return Ok(value),
                    Err(error) => match classify_kv_error(&error) {
                        KvRangeErrorKind::NotLeader(Some(node_id)) => {
                            current.leader_node_id = Some(node_id);
                            last_error = Some(error);
                            break;
                        }
                        KvRangeErrorKind::EpochMismatch | KvRangeErrorKind::RangeNotFound => {
                            current = self.load_route_by_id(range_id).await?;
                            last_error = Some(error);
                            break;
                        }
                        KvRangeErrorKind::ConfigChanging | KvRangeErrorKind::SnapshotInProgress => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            current = self.load_route_by_id(range_id).await?;
                            last_error = Some(error);
                            break;
                        }
                        KvRangeErrorKind::TransportUnavailable => {
                            last_error = Some(error);
                        }
                        KvRangeErrorKind::NotLeader(None) | KvRangeErrorKind::Other => {
                            last_error = Some(error);
                        }
                        KvRangeErrorKind::RetryExhausted => {
                            return Err(error);
                        }
                    },
                }
            }
            if attempt == 2 {
                if let Some(error) = last_error {
                    return Err(error);
                }
                anyhow::bail!("kv/retry-exhausted range={range_id}");
            }
            if last_error.is_none() {
                current = self.load_route_by_id(range_id).await?;
            } else if let Some(error) = &last_error {
                if should_refresh_route(&classify_kv_error(error)) {
                    self.router.invalidate_range(range_id).await?;
                    current = self.load_route_by_id(range_id).await?;
                }
            }
        }
        anyhow::bail!("kv/retry-exhausted range={range_id}")
    }
}

#[async_trait]
impl KvRangeExecutor for LeaderRoutedKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.get_with_options(range_id, key, RangeReadOptions::default())
            .await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        self.scan_with_options(range_id, boundary, limit, RangeReadOptions::default())
            .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let route = self.load_route_by_id(range_id).await?;
        let expected_epoch = route.descriptor.epoch;
        self.execute_leader_with_retry(range_id, route, |executor| {
            let mutations = mutations.clone();
            async move {
                executor
                    .apply_fenced(range_id, mutations, Some(expected_epoch))
                    .await
            }
        })
        .await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let route = self.load_route_by_id(range_id).await?;
        let expected_epoch = route.descriptor.epoch;
        self.execute_leader_with_retry(range_id, route, |executor| async move {
            executor
                .checkpoint_fenced(range_id, checkpoint_id, Some(expected_epoch))
                .await
        })
        .await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let route = self.load_route_by_id(range_id).await?;
        let expected_epoch = route.descriptor.epoch;
        self.execute_leader_with_retry(range_id, route, |executor| async move {
            executor
                .snapshot_fenced(range_id, Some(expected_epoch))
                .await
        })
        .await
    }
}

#[async_trait]
impl RangeDataClient for LeaderRoutedKvRangeExecutor {
    async fn list_ranges(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.router.list().await
    }

    async fn route_by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        self.router.by_id(range_id).await
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        self.router.route_key(shard, key).await
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        self.router.route_shard(shard).await
    }

    async fn get_with_options(
        &self,
        range_id: &str,
        key: &[u8],
        options: RangeReadOptions,
    ) -> anyhow::Result<Option<Bytes>> {
        let route = self.load_route_by_id(range_id).await?;
        let expected_epoch = options.expected_epoch.or(Some(route.descriptor.epoch));
        self.execute_query_with_retry(range_id, route, |executor| async move {
            match options.consistency {
                ReadConsistency::LocalMaybeStale => executor.get(range_id, key).await,
                ReadConsistency::LeaderReadIndex | ReadConsistency::LeaderWriteAck => {
                    executor.get_fenced(range_id, key, expected_epoch).await
                }
            }
        })
        .await
    }

    async fn scan_with_options(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
        options: RangeReadOptions,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let route = self.load_route_by_id(range_id).await?;
        let expected_epoch = options.expected_epoch.or(Some(route.descriptor.epoch));
        self.execute_query_with_retry(range_id, route, |executor| {
            let boundary = boundary.clone();
            async move {
                match options.consistency {
                    ReadConsistency::LocalMaybeStale => {
                        executor.scan(range_id, boundary, limit).await
                    }
                    ReadConsistency::LeaderReadIndex | ReadConsistency::LeaderWriteAck => {
                        executor
                            .scan_fenced(range_id, boundary, limit, expected_epoch)
                            .await
                    }
                }
            }
        })
        .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        <Self as KvRangeExecutor>::apply(self, range_id, mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        <Self as KvRangeExecutor>::checkpoint(self, range_id, checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        <Self as KvRangeExecutor>::snapshot(self, range_id).await
    }
}

#[async_trait]
impl RangeDataClient for RoutedRangeDataClient {
    async fn list_ranges(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.router.list().await
    }

    async fn route_by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        self.router.by_id(range_id).await
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        self.router.route_key(shard, key).await
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        self.router.route_shard(shard).await
    }

    async fn get_with_options(
        &self,
        range_id: &str,
        key: &[u8],
        options: RangeReadOptions,
    ) -> anyhow::Result<Option<Bytes>> {
        match options.consistency {
            ReadConsistency::LocalMaybeStale => self.executor.get(range_id, key).await,
            ReadConsistency::LeaderReadIndex | ReadConsistency::LeaderWriteAck => {
                self.executor
                    .get_fenced(range_id, key, options.expected_epoch)
                    .await
            }
        }
    }

    async fn scan_with_options(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
        options: RangeReadOptions,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        match options.consistency {
            ReadConsistency::LocalMaybeStale => self.executor.scan(range_id, boundary, limit).await,
            ReadConsistency::LeaderReadIndex | ReadConsistency::LeaderWriteAck => {
                self.executor
                    .scan_fenced(range_id, boundary, limit, options.expected_epoch)
                    .await
            }
        }
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        self.executor.apply(range_id, mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        self.executor.checkpoint(range_id, checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        self.executor.snapshot(range_id).await
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
