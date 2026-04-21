use async_trait::async_trait;
use greenmqtt_retain::{RetainRangeStore, RetainService as RetainStoreService};
use greenmqtt_rpc::RetainGrpcClient;

#[derive(Clone)]
pub struct RemoteRetainRangeStore {
    inner: RetainGrpcClient,
}

impl RemoteRetainRangeStore {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: RetainGrpcClient::connect(endpoint).await?,
        })
    }

    pub fn inner(&self) -> &RetainGrpcClient {
        &self.inner
    }
}

#[async_trait]
impl RetainRangeStore for RemoteRetainRangeStore {
    async fn write_retain(&self, message: greenmqtt_core::RetainedMessage) -> anyhow::Result<()> {
        RetainStoreService::retain(&self.inner, message).await
    }

    async fn list_tenant_retained(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_core::RetainedMessage>> {
        RetainStoreService::list_tenant_retained(&self.inner, tenant_id).await
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::RetainedMessage>> {
        RetainStoreService::lookup_topic(&self.inner, tenant_id, topic).await
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        RetainStoreService::retained_count(&self.inner).await
    }
}
