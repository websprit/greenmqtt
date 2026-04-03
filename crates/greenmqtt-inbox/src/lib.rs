use async_trait::async_trait;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, ServiceShardKey, ServiceShardKind, SessionId, Subscription,
};

#[async_trait]
pub trait InboxService: Send + Sync {
    async fn attach(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn detach(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn purge_session(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn purge_session_subscriptions_only(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<usize> {
        let subscriptions = self.list_subscriptions(session_id).await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let subscriptions = self.list_tenant_subscriptions(tenant_id).await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn count_session_subscriptions(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.list_subscriptions(session_id).await?.len())
    }
    async fn list_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_subscriptions(session_id)
            .await?
            .into_iter()
            .filter(|subscription| subscription.topic_filter == topic_filter)
            .collect())
    }
    async fn count_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await?
            .len())
    }
    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_subscriptions(tenant_id).await?.len())
    }
    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_shared_subscriptions(tenant_id, shared_group)
            .await?
            .len())
    }
    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?
            .len())
    }
    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await?
            .len())
    }
    async fn purge_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.fetch(session_id).await?.len())
    }
    async fn purge_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let messages = self.list_tenant_offline(tenant_id).await?;
        let removed = messages.len();
        let session_ids: std::collections::BTreeSet<_> = messages
            .into_iter()
            .map(|message| message.session_id)
            .collect();
        for session_id in session_ids {
            let _ = self.purge_offline(&session_id).await?;
        }
        Ok(removed)
    }
    async fn count_session_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.peek(session_id).await?.len())
    }
    async fn count_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_offline(tenant_id).await?.len())
    }
    async fn purge_inflight_session(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        let messages = self.fetch_inflight(session_id).await?;
        let removed = messages.len();
        for message in messages {
            self.ack_inflight(session_id, message.packet_id).await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let messages = self.list_tenant_inflight(tenant_id).await?;
        let removed = messages.len();
        let session_ids: std::collections::BTreeSet<_> = messages
            .into_iter()
            .map(|message| message.session_id)
            .collect();
        for session_id in session_ids {
            let _ = self.purge_inflight_session(&session_id).await?;
        }
        Ok(removed)
    }
    async fn count_session_inflight(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.fetch_inflight(session_id).await?.len())
    }
    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_inflight(tenant_id).await?.len())
    }
    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()>;
    async fn lookup_subscription(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        Ok(self
            .list_subscriptions(session_id)
            .await?
            .into_iter()
            .find(|subscription| {
                subscription.topic_filter == topic_filter
                    && subscription.shared_group.as_deref() == shared_group
            }))
    }
    async fn unsubscribe(&self, session_id: &SessionId, topic_filter: &str)
        -> anyhow::Result<bool>;
    async fn unsubscribe_shared(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool>;
    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()>;
    async fn enqueue_batch(&self, messages: Vec<OfflineMessage>) -> anyhow::Result<()> {
        for message in messages {
            self.enqueue(message).await?;
        }
        Ok(())
    }
    async fn list_subscriptions(&self, session_id: &SessionId)
        -> anyhow::Result<Vec<Subscription>>;
    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_all_subscriptions()
            .await?
            .into_iter()
            .filter(|subscription| subscription.tenant_id == tenant_id)
            .collect())
    }
    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_tenant_subscriptions(tenant_id)
            .await?
            .into_iter()
            .filter(|subscription| subscription.shared_group.as_deref() == shared_group)
            .collect())
    }
    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_tenant_subscriptions(tenant_id)
            .await?
            .into_iter()
            .filter(|subscription| subscription.topic_filter == topic_filter)
            .collect())
    }
    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?
            .into_iter()
            .filter(|subscription| subscription.shared_group.as_deref() == shared_group)
            .collect())
    }
    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>>;
    async fn peek(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn list_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .list_all_offline()
            .await?
            .into_iter()
            .filter(|message| message.tenant_id == tenant_id)
            .collect())
    }
    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn fetch(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()>;
    async fn stage_inflight_batch(&self, messages: Vec<InflightMessage>) -> anyhow::Result<()> {
        for message in messages {
            self.stage_inflight(message).await?;
        }
        Ok(())
    }
    async fn ack_inflight(&self, session_id: &SessionId, packet_id: u16) -> anyhow::Result<()>;
    async fn ack_inflight_batch(
        &self,
        session_id: &SessionId,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        for packet_id in packet_ids {
            self.ack_inflight(session_id, *packet_id).await?;
        }
        Ok(())
    }
    async fn fetch_inflight(&self, session_id: &SessionId) -> anyhow::Result<Vec<InflightMessage>>;
    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        Ok(self
            .list_all_inflight()
            .await?
            .into_iter()
            .filter(|message| message.tenant_id == tenant_id)
            .collect())
    }
    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>>;
    async fn subscription_count(&self) -> anyhow::Result<usize>;
    async fn offline_count(&self) -> anyhow::Result<usize>;
    async fn inflight_count(&self) -> anyhow::Result<usize>;
}

pub fn inbox_session_shard(tenant_id: &str, session_id: &SessionId) -> ServiceShardKey {
    ServiceShardKey::inbox(tenant_id.to_string(), session_id.clone())
}

pub fn inflight_session_shard(tenant_id: &str, session_id: &SessionId) -> ServiceShardKey {
    ServiceShardKey::inflight(tenant_id.to_string(), session_id.clone())
}

pub fn subscription_shard(subscription: &Subscription) -> ServiceShardKey {
    inbox_session_shard(&subscription.tenant_id, &subscription.session_id)
}

pub fn inbox_tenant_scan_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::Inbox,
        tenant_id: tenant_id.to_string(),
        scope: "*".into(),
    }
}

pub fn inflight_tenant_scan_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::Inflight,
        tenant_id: tenant_id.to_string(),
        scope: "*".into(),
    }
}

mod handle;
mod state;

pub use handle::{InboxHandle, PersistentInboxHandle};

#[cfg(test)]
mod tests;
