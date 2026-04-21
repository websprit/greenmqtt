use async_trait::async_trait;
use greenmqtt_core::{NodeId, ReplicatedRangeDescriptor};
use greenmqtt_kv_client::{RangeControlClient, RangeSplitResult};
use greenmqtt_rpc::{RangeControlGrpcClient, RoutedRangeControlGrpcClient, ServiceTrafficGovernor};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct RemoteRangeControlClient {
    inner: RoutedRangeControlGrpcClient,
}

#[derive(Clone)]
pub struct DirectRangeControlClient {
    inner: RangeControlGrpcClient,
}

impl RemoteRangeControlClient {
    pub async fn connect(
        metadata_endpoint: impl Into<String>,
        fallback_endpoint: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: RoutedRangeControlGrpcClient::connect(metadata_endpoint, fallback_endpoint)
                .await?,
        })
    }

    pub async fn connect_with_backoff(
        metadata_endpoint: impl Into<String>,
        fallback_endpoint: impl Into<String>,
        overload_backoff: Duration,
        service_governor: Option<Arc<ServiceTrafficGovernor>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: RoutedRangeControlGrpcClient::connect_with_backoff(
                metadata_endpoint,
                fallback_endpoint,
                overload_backoff,
                service_governor,
            )
            .await?,
        })
    }

    pub fn inner(&self) -> &RoutedRangeControlGrpcClient {
        &self.inner
    }
}

impl DirectRangeControlClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: RangeControlGrpcClient::connect(endpoint).await?,
        })
    }

    pub fn inner(&self) -> &RangeControlGrpcClient {
        &self.inner
    }
}

#[async_trait]
impl RangeControlClient for RemoteRangeControlClient {
    async fn bootstrap_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        self.inner.bootstrap_range(descriptor).await
    }

    async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        self.inner.change_replicas(range_id, voters, learners).await
    }

    async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.inner
            .transfer_leadership(range_id, target_node_id)
            .await
    }

    async fn recover_range(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.inner
            .recover_range(range_id, new_leader_node_id)
            .await
    }

    async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<RangeSplitResult> {
        let (left_range_id, right_range_id) = self.inner.split_range(range_id, split_key).await?;
        Ok(RangeSplitResult {
            left_range_id,
            right_range_id,
        })
    }

    async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        self.inner.merge_ranges(left_range_id, right_range_id).await
    }

    async fn drain_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.inner.drain_range(range_id).await
    }

    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.inner.retire_range(range_id).await
    }
}

#[async_trait]
impl RangeControlClient for DirectRangeControlClient {
    async fn bootstrap_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        self.inner.bootstrap_range(descriptor).await
    }

    async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        self.inner.change_replicas(range_id, voters, learners).await
    }

    async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.inner
            .transfer_leadership(range_id, target_node_id)
            .await
    }

    async fn recover_range(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.inner
            .recover_range(range_id, new_leader_node_id)
            .await
    }

    async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<RangeSplitResult> {
        let (left_range_id, right_range_id) = self.inner.split_range(range_id, split_key).await?;
        Ok(RangeSplitResult {
            left_range_id,
            right_range_id,
        })
    }

    async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        self.inner.merge_ranges(left_range_id, right_range_id).await
    }

    async fn drain_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.inner.drain_range(range_id).await
    }

    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.inner.retire_range(range_id).await
    }
}
