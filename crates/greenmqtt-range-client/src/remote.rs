use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{RangeBoundary, ServiceShardKey};
use greenmqtt_kv_client::{
    CachedKvRangeRouter, KvRangeExecutor, KvRangeExecutorFactory, KvRangeRouter,
    LeaderRoutedKvRangeExecutor, RangeDataClient, RangeReadOptions, RangeRoute, RouteCacheConfig,
};
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
use greenmqtt_rpc::{KvRangeGrpcClient, KvRangeGrpcExecutorFactory, MetadataGrpcClient};
use std::sync::Arc;

#[derive(Clone)]
pub struct RemoteRangeDataClient {
    inner: Arc<LeaderRoutedKvRangeExecutor>,
}

impl RemoteRangeDataClient {
    pub async fn connect(
        metadata_endpoint: impl Into<String>,
        range_endpoint: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Self::connect_with_cache_config(
            metadata_endpoint,
            range_endpoint,
            RouteCacheConfig::default(),
        )
        .await
    }

    pub async fn connect_with_cache_config(
        metadata_endpoint: impl Into<String>,
        range_endpoint: impl Into<String>,
        route_cache_config: RouteCacheConfig,
    ) -> anyhow::Result<Self> {
        let metadata = Arc::new(MetadataGrpcClient::connect(metadata_endpoint.into()).await?);
        let router: Arc<dyn KvRangeRouter> = Arc::new(CachedKvRangeRouter::new(
            metadata.clone(),
            route_cache_config,
        ));
        let fallback = Arc::new(KvRangeGrpcClient::connect(range_endpoint.into()).await?);
        let factory: Arc<dyn KvRangeExecutorFactory> =
            Arc::new(KvRangeGrpcExecutorFactory::default());
        let inner = Arc::new(LeaderRoutedKvRangeExecutor::new(
            router,
            metadata,
            fallback as Arc<dyn KvRangeExecutor>,
            factory,
        ));
        Ok(Self { inner })
    }

    pub fn from_inner(inner: Arc<LeaderRoutedKvRangeExecutor>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> Arc<LeaderRoutedKvRangeExecutor> {
        self.inner.clone()
    }
}

#[async_trait]
impl RangeDataClient for RemoteRangeDataClient {
    async fn list_ranges(&self) -> anyhow::Result<Vec<greenmqtt_core::ReplicatedRangeDescriptor>> {
        self.inner.list_ranges().await
    }

    async fn route_by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        self.inner.route_by_id(range_id).await
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        RangeDataClient::route_key(&*self.inner, shard, key).await
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        RangeDataClient::route_shard(&*self.inner, shard).await
    }

    async fn get_with_options(
        &self,
        range_id: &str,
        key: &[u8],
        options: RangeReadOptions,
    ) -> anyhow::Result<Option<Bytes>> {
        RangeDataClient::get_with_options(&*self.inner, range_id, key, options).await
    }

    async fn scan_with_options(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
        options: RangeReadOptions,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        RangeDataClient::scan_with_options(&*self.inner, range_id, boundary, limit, options).await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        RangeDataClient::apply(&*self.inner, range_id, mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        RangeDataClient::checkpoint(&*self.inner, range_id, checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        RangeDataClient::snapshot(&*self.inner, range_id).await
    }
}
