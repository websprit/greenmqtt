use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::RangeBoundary;
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
use std::sync::Arc;

#[async_trait]
pub trait KvRangeExecutor: Send + Sync {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>>;
    async fn get_fenced(
        &self,
        range_id: &str,
        key: &[u8],
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<Option<Bytes>> {
        let _ = expected_epoch;
        self.get(range_id, key).await
    }
    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>>;
    async fn scan_fenced(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let _ = expected_epoch;
        self.scan(range_id, boundary, limit).await
    }
    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()>;
    async fn apply_fenced(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<()> {
        let _ = expected_epoch;
        self.apply(range_id, mutations).await
    }
    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint>;
    async fn checkpoint_fenced(
        &self,
        range_id: &str,
        checkpoint_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let _ = expected_epoch;
        self.checkpoint(range_id, checkpoint_id).await
    }
    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot>;
    async fn snapshot_fenced(
        &self,
        range_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<KvRangeSnapshot> {
        let _ = expected_epoch;
        self.snapshot(range_id).await
    }
}

#[async_trait]
pub trait KvRangeExecutorFactory: Send + Sync {
    async fn connect(&self, endpoint: &str) -> anyhow::Result<Arc<dyn KvRangeExecutor>>;
}

