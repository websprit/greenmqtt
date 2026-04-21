use crate::{RemoteRangeControlClient, RemoteRangeDataClient};
use greenmqtt_kv_client::RouteCacheConfig;
use greenmqtt_rpc::ServiceTrafficGovernor;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Default)]
pub struct RemoteRangeClientBuilder {
    metadata_endpoint: Option<String>,
    range_endpoint: Option<String>,
    control_fallback_endpoint: Option<String>,
    overload_backoff: Option<Duration>,
    service_governor: Option<Arc<ServiceTrafficGovernor>>,
    route_cache_config: RouteCacheConfig,
}

impl RemoteRangeClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metadata_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self
    }

    pub fn range_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.range_endpoint = Some(endpoint.into());
        self
    }

    pub fn control_fallback_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.control_fallback_endpoint = Some(endpoint.into());
        self
    }

    pub fn overload_backoff(mut self, backoff: Duration) -> Self {
        self.overload_backoff = Some(backoff);
        self
    }

    pub fn service_governor(mut self, governor: Arc<ServiceTrafficGovernor>) -> Self {
        self.service_governor = Some(governor);
        self
    }

    pub fn route_cache_config(mut self, config: RouteCacheConfig) -> Self {
        self.route_cache_config = config;
        self
    }

    pub async fn build_data(&self) -> anyhow::Result<RemoteRangeDataClient> {
        let metadata_endpoint = self
            .metadata_endpoint
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing metadata endpoint"))?;
        let range_endpoint = self
            .range_endpoint
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing range endpoint"))?;
        RemoteRangeDataClient::connect_with_cache_config(
            metadata_endpoint,
            range_endpoint,
            self.route_cache_config.clone(),
        )
        .await
    }

    pub async fn build_control(&self) -> anyhow::Result<RemoteRangeControlClient> {
        let metadata_endpoint = self
            .metadata_endpoint
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing metadata endpoint"))?;
        let fallback_endpoint = self
            .control_fallback_endpoint
            .clone()
            .or_else(|| self.range_endpoint.clone())
            .ok_or_else(|| anyhow::anyhow!("missing control fallback endpoint"))?;
        match self.overload_backoff {
            Some(overload_backoff) => {
                RemoteRangeControlClient::connect_with_backoff(
                    metadata_endpoint,
                    fallback_endpoint,
                    overload_backoff,
                    self.service_governor.clone(),
                )
                .await
            }
            None => RemoteRangeControlClient::connect(metadata_endpoint, fallback_endpoint).await,
        }
    }
}
