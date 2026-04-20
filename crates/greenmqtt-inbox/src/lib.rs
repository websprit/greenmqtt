use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, ServiceShardKey, ServiceShardKind, SessionId, Subscription,
};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter};
use greenmqtt_kv_engine::KvMutation;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

#[derive(Clone)]
pub struct ReplicatedInboxHandle {
    router: Arc<dyn KvRangeRouter>,
    executor: Arc<dyn KvRangeExecutor>,
    next_seq: Arc<AtomicU64>,
}

impl ReplicatedInboxHandle {
    pub fn new(router: Arc<dyn KvRangeRouter>, executor: Arc<dyn KvRangeExecutor>) -> Self {
        Self {
            router,
            executor,
            next_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn inbox_range_id_for_tenant(&self, tenant_id: &str) -> anyhow::Result<String> {
        let shard = inbox_tenant_scan_shard(tenant_id);
        self.router
            .route_shard(&shard)
            .await?
            .into_iter()
            .next()
            .map(|route| route.descriptor.id)
            .ok_or_else(|| anyhow::anyhow!("no inbox range available for tenant {tenant_id}"))
    }

    async fn inflight_range_id_for_tenant(&self, tenant_id: &str) -> anyhow::Result<String> {
        let shard = inflight_tenant_scan_shard(tenant_id);
        self.router
            .route_shard(&shard)
            .await?
            .into_iter()
            .next()
            .map(|route| route.descriptor.id)
            .ok_or_else(|| anyhow::anyhow!("no inflight range available for tenant {tenant_id}"))
    }

    async fn scan_inbox_kind(&self, kind: ServiceShardKind) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut entries = Vec::new();
        for descriptor in self.router.list().await? {
            if descriptor.shard.kind != kind {
                continue;
            }
            entries.extend(self.executor.scan(&descriptor.id, None, usize::MAX).await?);
        }
        Ok(entries)
    }

    async fn next_offline_seq(&self) -> anyhow::Result<u64> {
        let current = self.next_seq.load(Ordering::Relaxed);
        if current > 0 {
            return Ok(self.next_seq.fetch_add(1, Ordering::Relaxed));
        }
        let mut max_seq = 0u64;
        for (key, _) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Some(seq) = offline_seq_from_key(&key) {
                max_seq = max_seq.max(seq);
            }
        }
        let next = max_seq.saturating_add(1);
        let _ = self
            .next_seq
            .compare_exchange(0, next, Ordering::SeqCst, Ordering::SeqCst);
        Ok(self.next_seq.fetch_add(1, Ordering::Relaxed))
    }
}

const SUBSCRIPTION_KEY_PREFIX: &[u8] = b"sub\0";
const OFFLINE_KEY_PREFIX: &[u8] = b"offline\0";
const INFLIGHT_KEY_PREFIX: &[u8] = b"inflight\0";

fn subscription_key(
    session_id: &SessionId,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> Bytes {
    let mut key = Vec::with_capacity(
        SUBSCRIPTION_KEY_PREFIX.len()
            + session_id.len()
            + topic_filter.len()
            + shared_group.unwrap_or_default().len()
            + 2,
    );
    key.extend_from_slice(SUBSCRIPTION_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(shared_group.unwrap_or_default().as_bytes());
    key.push(0);
    key.extend_from_slice(topic_filter.as_bytes());
    Bytes::from(key)
}

fn offline_key(session_id: &SessionId, seq: u64) -> Bytes {
    let mut key = Vec::with_capacity(OFFLINE_KEY_PREFIX.len() + session_id.len() + 9);
    key.extend_from_slice(OFFLINE_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&seq.to_be_bytes());
    Bytes::from(key)
}

fn offline_seq_from_key(key: &[u8]) -> Option<u64> {
    if !key.starts_with(OFFLINE_KEY_PREFIX) || key.len() < 8 {
        return None;
    }
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&key[key.len().saturating_sub(8)..]);
    Some(u64::from_be_bytes(raw))
}

fn inflight_key(session_id: &SessionId, packet_id: u16) -> Bytes {
    let mut key = Vec::with_capacity(INFLIGHT_KEY_PREFIX.len() + session_id.len() + 3);
    key.extend_from_slice(INFLIGHT_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&packet_id.to_be_bytes());
    Bytes::from(key)
}

fn encode_subscription(subscription: &Subscription) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(subscription)?))
}

fn decode_subscription(value: &[u8]) -> anyhow::Result<Subscription> {
    Ok(bincode::deserialize(value)?)
}

fn encode_offline(message: &OfflineMessage) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(message)?))
}

fn decode_offline(value: &[u8]) -> anyhow::Result<OfflineMessage> {
    Ok(bincode::deserialize(value)?)
}

fn encode_inflight(message: &InflightMessage) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(message)?))
}

fn decode_inflight(value: &[u8]) -> anyhow::Result<InflightMessage> {
    Ok(bincode::deserialize(value)?)
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

#[async_trait]
impl InboxService for ReplicatedInboxHandle {
    async fn attach(&self, _session_id: &SessionId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn detach(&self, _session_id: &SessionId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn purge_session(&self, session_id: &SessionId) -> anyhow::Result<()> {
        let subscriptions = self.list_subscriptions(session_id).await?;
        let offline = self.peek(session_id).await?;
        let inflight = self.fetch_inflight(session_id).await?;

        for subscription in subscriptions {
            let range_id = self
                .inbox_range_id_for_tenant(&subscription.tenant_id)
                .await?;
            self.executor
                .apply(
                    &range_id,
                    vec![KvMutation {
                        key: subscription_key(
                            &subscription.session_id,
                            &subscription.topic_filter,
                            subscription.shared_group.as_deref(),
                        ),
                        value: None,
                    }],
                )
                .await?;
        }

        for message in offline {
            let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
            let entries = self.executor.scan(&range_id, None, usize::MAX).await?;
            let mut removals = Vec::new();
            for (key, value) in entries {
                if !key.starts_with(OFFLINE_KEY_PREFIX) {
                    continue;
                }
                let candidate = decode_offline(&value)?;
                if candidate.session_id == *session_id
                    && candidate.topic == message.topic
                    && candidate.from_session_id == message.from_session_id
                {
                    removals.push(KvMutation { key, value: None });
                }
            }
            if !removals.is_empty() {
                self.executor.apply(&range_id, removals).await?;
            }
        }

        for message in inflight {
            let range_id = self
                .inflight_range_id_for_tenant(&message.tenant_id)
                .await?;
            self.executor
                .apply(
                    &range_id,
                    vec![KvMutation {
                        key: inflight_key(session_id, message.packet_id),
                        value: None,
                    }],
                )
                .await?;
        }
        Ok(())
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        let range_id = self
            .inbox_range_id_for_tenant(&subscription.tenant_id)
            .await?;
        self.executor
            .apply(
                &range_id,
                vec![KvMutation {
                    key: subscription_key(
                        &subscription.session_id,
                        &subscription.topic_filter,
                        subscription.shared_group.as_deref(),
                    ),
                    value: Some(encode_subscription(&subscription)?),
                }],
            )
            .await
    }

    async fn unsubscribe(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<bool> {
        self.unsubscribe_shared(session_id, topic_filter, None)
            .await
    }

    async fn unsubscribe_shared(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let Some(subscription) = self
            .lookup_subscription(session_id, topic_filter, shared_group)
            .await?
        else {
            return Ok(false);
        };
        let range_id = self
            .inbox_range_id_for_tenant(&subscription.tenant_id)
            .await?;
        self.executor
            .apply(
                &range_id,
                vec![KvMutation {
                    key: subscription_key(session_id, topic_filter, shared_group),
                    value: None,
                }],
            )
            .await?;
        Ok(true)
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
        let seq = self.next_offline_seq().await?;
        self.executor
            .apply(
                &range_id,
                vec![KvMutation {
                    key: offline_key(&message.session_id, seq),
                    value: Some(encode_offline(&message)?),
                }],
            )
            .await
    }

    async fn list_subscriptions(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Ok(subscription) = decode_subscription(&value) {
                if subscription.session_id == *session_id {
                    subscriptions.push(subscription);
                }
            }
        }
        subscriptions.sort_by(|left, right| {
            left.topic_filter
                .cmp(&right.topic_filter)
                .then(left.shared_group.cmp(&right.shared_group))
        });
        Ok(subscriptions)
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Ok(subscription) = decode_subscription(&value) {
                subscriptions.push(subscription);
            }
        }
        subscriptions.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn peek(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Ok(message) = decode_offline(&value) {
                if message.session_id == *session_id {
                    messages.push(message);
                }
            }
        }
        Ok(messages)
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Ok(message) = decode_offline(&value) {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    async fn fetch(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        let messages = self.peek(session_id).await?;
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        for message in &messages {
            let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
            let entries = self.executor.scan(&range_id, None, usize::MAX).await?;
            let mut removals = Vec::new();
            for (key, value) in entries {
                if !key.starts_with(OFFLINE_KEY_PREFIX) {
                    continue;
                }
                let candidate = decode_offline(&value)?;
                if candidate.session_id == *session_id
                    && candidate.topic == message.topic
                    && candidate.from_session_id == message.from_session_id
                {
                    removals.push(KvMutation { key, value: None });
                }
            }
            if !removals.is_empty() {
                self.executor.apply(&range_id, removals).await?;
            }
        }
        Ok(messages)
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        let range_id = self
            .inflight_range_id_for_tenant(&message.tenant_id)
            .await?;
        self.executor
            .apply(
                &range_id,
                vec![KvMutation {
                    key: inflight_key(&message.session_id, message.packet_id),
                    value: Some(encode_inflight(&message)?),
                }],
            )
            .await
    }

    async fn ack_inflight(&self, session_id: &SessionId, packet_id: u16) -> anyhow::Result<()> {
        let Some(message) = self
            .fetch_inflight(session_id)
            .await?
            .into_iter()
            .find(|message| message.packet_id == packet_id)
        else {
            return Ok(());
        };
        let range_id = self
            .inflight_range_id_for_tenant(&message.tenant_id)
            .await?;
        self.executor
            .apply(
                &range_id,
                vec![KvMutation {
                    key: inflight_key(session_id, packet_id),
                    value: None,
                }],
            )
            .await
    }

    async fn fetch_inflight(&self, session_id: &SessionId) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inflight).await? {
            if let Ok(message) = decode_inflight(&value) {
                if message.session_id == *session_id {
                    messages.push(message);
                }
            }
        }
        messages.sort_by_key(|message| message.packet_id);
        Ok(messages)
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inflight).await? {
            if let Ok(message) = decode_inflight(&value) {
                messages.push(message);
            }
        }
        messages.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.packet_id.cmp(&right.packet_id))
        });
        Ok(messages)
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_all_subscriptions().await?.len())
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_all_offline().await?.len())
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_all_inflight().await?.len())
    }
}

mod handle;
mod state;

pub use handle::{InboxHandle, PersistentInboxHandle};

#[cfg(test)]
mod tests;
