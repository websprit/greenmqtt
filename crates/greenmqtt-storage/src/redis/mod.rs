use super::{InboxStore, InflightStore, RetainStore, RouteStore, SessionStore, SubscriptionStore};
use crate::*;
use ::redis::{cmd, AsyncCommands, Client};
use async_trait::async_trait;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, SessionRecord, Subscription,
};

#[derive(Clone)]
pub struct RedisSessionStore {
    client: Client,
}

#[derive(Clone)]
pub struct RedisSubscriptionStore {
    client: Client,
}

#[derive(Clone)]
pub struct RedisInboxStore {
    client: Client,
}

#[derive(Clone)]
pub struct RedisInflightStore {
    client: Client,
}

#[derive(Clone)]
pub struct RedisRetainStore {
    client: Client,
}

#[derive(Clone)]
pub struct RedisRouteStore {
    client: Client,
}

impl RedisSessionStore {
    pub fn open(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::open(url)?,
        })
    }
}

impl RedisSubscriptionStore {
    pub fn open(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::open(url)?,
        })
    }
}

impl RedisInboxStore {
    pub fn open(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::open(url)?,
        })
    }
}

impl RedisInflightStore {
    pub fn open(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::open(url)?,
        })
    }
}

impl RedisRetainStore {
    pub fn open(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::open(url)?,
        })
    }
}

impl RedisRouteStore {
    pub fn open(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: Client::open(url)?,
        })
    }
}

#[async_trait]
impl SessionStore for RedisSessionStore {
    async fn save_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        let previous = self
            .load_session(&session.identity.tenant_id, &session.identity.client_id)
            .await?;
        let is_new = previous.is_none();
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        if let Some(previous) = previous {
            let _: usize = conn.del(redis_session_id_key(&previous.session_id)).await?;
        }
        let _: () = conn
            .set(
                redis_session_key(&session.identity.tenant_id, &session.identity.client_id),
                serde_json::to_string(session)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_session_id_key(&session.session_id),
                serde_json::to_string(session)?,
            )
            .await?;
        if is_new {
            let _: isize = conn.incr(redis_session_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn load_session(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value: Option<String> = conn.get(redis_session_key(tenant_id, client_id)).await?;
        Ok(value
            .map(|value| serde_json::from_str(&value))
            .transpose()?)
    }

    async fn load_session_by_session_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value: Option<String> = conn.get(redis_session_id_key(session_id)).await?;
        Ok(value
            .map(|value| serde_json::from_str(&value))
            .transpose()?)
    }

    async fn delete_session(&self, tenant_id: &str, client_id: &str) -> anyhow::Result<()> {
        let existing = self.load_session(tenant_id, client_id).await?;
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let _: usize = conn.del(redis_session_key(tenant_id, client_id)).await?;
        if let Some(existing) = existing {
            let _: usize = conn.del(redis_session_id_key(&existing.session_id)).await?;
            let _: isize = conn.decr(redis_session_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn list_sessions(&self) -> anyhow::Result<Vec<SessionRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_prefix_pattern("session"))
            .query_async(&mut conn)
            .await?;
        let mut sessions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            sessions.push(serde_json::from_str(&value)?);
        }
        Ok(sessions)
    }

    async fn list_tenant_sessions(&self, tenant_id: &str) -> anyhow::Result<Vec<SessionRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_session_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut sessions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            sessions.push(serde_json::from_str(&value)?);
        }
        Ok(sessions)
    }

    async fn count_sessions(&self) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: Option<usize> = conn.get(redis_session_count_key()).await?;
        Ok(count.unwrap_or(0))
    }
}

#[async_trait]
impl SubscriptionStore for RedisSubscriptionStore {
    async fn save_subscription(&self, subscription: &Subscription) -> anyhow::Result<()> {
        let is_new = self
            .load_subscription(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?
            .is_none();
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let _: () = conn
            .set(
                redis_subscription_key(
                    &subscription.session_id,
                    subscription.shared_group.as_deref(),
                    &subscription.topic_filter,
                ),
                serde_json::to_string(subscription)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_subscription_tenant_key(subscription),
                serde_json::to_string(subscription)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_subscription_tenant_shared_key(subscription),
                serde_json::to_string(subscription)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_subscription_tenant_topic_key(subscription),
                serde_json::to_string(subscription)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_subscription_tenant_topic_shared_key(subscription),
                serde_json::to_string(subscription)?,
            )
            .await?;
        if is_new {
            let _: isize = conn.incr(redis_subscription_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value: Option<String> = conn
            .get(redis_subscription_key(
                session_id,
                shared_group,
                topic_filter,
            ))
            .await?;
        value
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(Into::into)
    }

    async fn delete_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let removed_subscription = self
            .load_subscription(session_id, topic_filter, shared_group)
            .await?;
        let removed: usize = conn
            .del(redis_subscription_key(
                session_id,
                shared_group,
                topic_filter,
            ))
            .await?;
        if let Some(subscription) = removed_subscription {
            let _: usize = conn
                .del(redis_subscription_tenant_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_shared_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_topic_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_topic_shared_key(&subscription))
                .await?;
            let _: isize = conn.decr(redis_subscription_count_key(), 1).await?;
        }
        Ok(removed > 0)
    }

    async fn list_subscriptions(&self, session_id: &str) -> anyhow::Result<Vec<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let mut subscriptions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            subscriptions.push(serde_json::from_str(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &str,
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
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await?
            .len())
    }

    async fn purge_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let removed = keys.len();
        if !keys.is_empty() {
            let mut tenant_keys = Vec::with_capacity(keys.len());
            for key in &keys {
                let value: String = conn.get(key).await?;
                let subscription: Subscription = serde_json::from_str(&value)?;
                tenant_keys.push(redis_subscription_tenant_key(&subscription));
                tenant_keys.push(redis_subscription_tenant_shared_key(&subscription));
                tenant_keys.push(redis_subscription_tenant_topic_key(&subscription));
                tenant_keys.push(redis_subscription_tenant_topic_shared_key(&subscription));
            }
            let _: usize = conn.del(keys).await?;
            let _: usize = conn.del(tenant_keys).await?;
            let _: isize = conn
                .decr(redis_subscription_count_key(), removed as isize)
                .await?;
        }
        Ok(removed)
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let removed = keys.len();
        for key in keys {
            let value: String = conn.get(&key).await?;
            let subscription: Subscription = serde_json::from_str(&value)?;
            let _: usize = conn.del(&key).await?;
            let _: usize = conn
                .del(redis_subscription_key(
                    &subscription.session_id,
                    subscription.shared_group.as_deref(),
                    &subscription.topic_filter,
                ))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_shared_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_topic_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_topic_shared_key(&subscription))
                .await?;
        }
        if removed > 0 {
            let _: isize = conn
                .decr(redis_subscription_count_key(), removed as isize)
                .await?;
        }
        Ok(removed)
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_topic_pattern(
                tenant_id,
                topic_filter,
            ))
            .query_async(&mut conn)
            .await?;
        let removed = keys.len();
        for key in keys {
            let value: String = conn.get(&key).await?;
            let subscription: Subscription = serde_json::from_str(&value)?;
            let _: usize = conn.del(&key).await?;
            let _: usize = conn
                .del(redis_subscription_key(
                    &subscription.session_id,
                    subscription.shared_group.as_deref(),
                    &subscription.topic_filter,
                ))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_shared_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_topic_shared_key(&subscription))
                .await?;
        }
        if removed > 0 {
            let _: isize = conn
                .decr(redis_subscription_count_key(), removed as isize)
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
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_topic_shared_pattern(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        let removed = keys.len();
        for key in keys {
            let value: String = conn.get(&key).await?;
            let subscription: Subscription = serde_json::from_str(&value)?;
            let _: usize = conn.del(&key).await?;
            let _: usize = conn
                .del(redis_subscription_key(
                    &subscription.session_id,
                    subscription.shared_group.as_deref(),
                    &subscription.topic_filter,
                ))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_shared_key(&subscription))
                .await?;
            let _: usize = conn
                .del(redis_subscription_tenant_topic_key(&subscription))
                .await?;
        }
        if removed > 0 {
            let _: isize = conn
                .decr(redis_subscription_count_key(), removed as isize)
                .await?;
        }
        Ok(removed)
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut subscriptions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            subscriptions.push(serde_json::from_str(&value)?);
        }
        Ok(subscriptions)
    }

    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_shared_pattern(
                tenant_id,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        let mut subscriptions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            subscriptions.push(serde_json::from_str(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_shared_pattern(
                tenant_id,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_topic_pattern(
                tenant_id,
                topic_filter,
            ))
            .query_async(&mut conn)
            .await?;
        let mut subscriptions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            subscriptions.push(serde_json::from_str(&value)?);
        }
        Ok(subscriptions)
    }

    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_topic_shared_pattern(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        let mut subscriptions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            subscriptions.push(serde_json::from_str(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_topic_pattern(
                tenant_id,
                topic_filter,
            ))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_subscription_tenant_topic_shared_pattern(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_prefix_pattern("subscription"))
            .query_async(&mut conn)
            .await?;
        let mut subscriptions = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            subscriptions.push(serde_json::from_str(&value)?);
        }
        subscriptions.sort_by(|left: &Subscription, right: &Subscription| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn count_subscriptions(&self) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: Option<usize> = conn.get(redis_subscription_count_key()).await?;
        Ok(count.unwrap_or(0))
    }
}

#[async_trait]
impl InboxStore for RedisInboxStore {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value = serde_json::to_string(message)?;
        let _: usize = conn
            .rpush(redis_inbox_key(&message.session_id), value.clone())
            .await?;
        let _: usize = conn
            .rpush(redis_inbox_tenant_key(&message.tenant_id), value)
            .await?;
        let _: isize = conn.incr(redis_inbox_count_key(), 1).await?;
        Ok(())
    }

    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let values: Vec<String> = conn.lrange(redis_inbox_key(session_id), 0, -1).await?;
        values
            .into_iter()
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .collect()
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_prefix_pattern("inbox"))
            .query_async(&mut conn)
            .await?;
        let mut messages = Vec::new();
        for key in keys {
            let values: Vec<String> = conn.lrange(key, 0, -1).await?;
            for value in values {
                messages.push(serde_json::from_str(&value)?);
            }
        }
        messages.sort_by(|left: &OfflineMessage, right: &OfflineMessage| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic.cmp(&right.topic))
        });
        Ok(messages)
    }

    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = redis_inbox_key(session_id);
        let values: Vec<String> = conn.lrange(&key, 0, -1).await?;
        let _: usize = conn.del(key).await?;
        let mut messages = Vec::with_capacity(values.len());
        for value in values {
            let message: OfflineMessage = serde_json::from_str(&value)?;
            let _: usize = conn
                .lrem(redis_inbox_tenant_key(&message.tenant_id), 1, value)
                .await?;
            messages.push(message);
        }
        if !messages.is_empty() {
            let _: isize = conn
                .decr(redis_inbox_count_key(), messages.len() as isize)
                .await?;
        }
        Ok(messages)
    }

    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = redis_inbox_key(session_id);
        let values: Vec<String> = conn.lrange(&key, 0, -1).await?;
        let removed = values.len();
        let _: usize = conn.del(key).await?;
        for value in values {
            let message: OfflineMessage = serde_json::from_str(&value)?;
            let _: usize = conn
                .lrem(redis_inbox_tenant_key(&message.tenant_id), 1, value)
                .await?;
        }
        if removed > 0 {
            let _: isize = conn.decr(redis_inbox_count_key(), removed as isize).await?;
        }
        Ok(removed)
    }

    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let tenant_key = redis_inbox_tenant_key(tenant_id);
        let values: Vec<String> = conn.lrange(&tenant_key, 0, -1).await?;
        let removed = values.len();
        let _: usize = conn.del(tenant_key).await?;
        for value in values {
            let message: OfflineMessage = serde_json::from_str(&value)?;
            let _: usize = conn
                .lrem(redis_inbox_key(&message.session_id), 1, value)
                .await?;
        }
        if removed > 0 {
            let _: isize = conn.decr(redis_inbox_count_key(), removed as isize).await?;
        }
        Ok(removed)
    }

    async fn count_session_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: usize = conn.llen(redis_inbox_key(session_id)).await?;
        Ok(count)
    }

    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: usize = conn.llen(redis_inbox_tenant_key(tenant_id)).await?;
        Ok(count)
    }

    async fn list_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let values: Vec<String> = conn
            .lrange(redis_inbox_tenant_key(tenant_id), 0, -1)
            .await?;
        values
            .into_iter()
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .collect()
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: Option<usize> = conn.get(redis_inbox_count_key()).await?;
        Ok(count.unwrap_or(0))
    }
}

#[async_trait]
impl InflightStore for RedisInflightStore {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let primary_key = redis_inflight_key(&message.session_id, message.packet_id);
        let previous: Option<String> = conn.get(&primary_key).await?;
        let is_new = previous.is_none();
        if let Some(previous) = previous {
            let previous: InflightMessage = serde_json::from_str(&previous)?;
            let _: usize = conn
                .del(redis_inflight_tenant_key(
                    &previous.tenant_id,
                    &previous.session_id,
                    previous.packet_id,
                ))
                .await?;
        }
        let value = serde_json::to_string(message)?;
        let _: () = conn.set(&primary_key, value.clone()).await?;
        let _: () = conn
            .set(
                redis_inflight_tenant_key(
                    &message.tenant_id,
                    &message.session_id,
                    message.packet_id,
                ),
                value,
            )
            .await?;
        if is_new {
            let _: isize = conn.incr(redis_inflight_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_inflight_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let mut messages = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            messages.push(serde_json::from_str(&value)?);
        }
        Ok(messages)
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_prefix_pattern("inflight"))
            .query_async(&mut conn)
            .await?;
        let mut messages = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            messages.push(serde_json::from_str(&value)?);
        }
        messages.sort_by(|left: &InflightMessage, right: &InflightMessage| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.packet_id.cmp(&right.packet_id))
        });
        Ok(messages)
    }

    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_inflight_tenant_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut messages = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            messages.push(serde_json::from_str(&value)?);
        }
        messages.sort_by(|left: &InflightMessage, right: &InflightMessage| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.packet_id.cmp(&right.packet_id))
        });
        Ok(messages)
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let primary_key = redis_inflight_key(session_id, packet_id);
        let removed: Option<String> = conn.get(&primary_key).await?;
        let _: usize = conn.del(primary_key).await?;
        if let Some(removed) = removed {
            let removed: InflightMessage = serde_json::from_str(&removed)?;
            let _: usize = conn
                .del(redis_inflight_tenant_key(
                    &removed.tenant_id,
                    &removed.session_id,
                    removed.packet_id,
                ))
                .await?;
            let _: isize = conn.decr(redis_inflight_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_inflight_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let removed = keys.len();
        for key in keys {
            let value: String = conn.get(&key).await?;
            let message: InflightMessage = serde_json::from_str(&value)?;
            let _: usize = conn.del(key).await?;
            let _: usize = conn
                .del(redis_inflight_tenant_key(
                    &message.tenant_id,
                    &message.session_id,
                    message.packet_id,
                ))
                .await?;
        }
        if removed > 0 {
            let _: isize = conn
                .decr(redis_inflight_count_key(), removed as isize)
                .await?;
        }
        Ok(removed)
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_inflight_tenant_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let removed = keys.len();
        for key in keys {
            let value: String = conn.get(&key).await?;
            let message: InflightMessage = serde_json::from_str(&value)?;
            let _: usize = conn.del(&key).await?;
            let _: usize = conn
                .del(redis_inflight_key(&message.session_id, message.packet_id))
                .await?;
        }
        if removed > 0 {
            let _: isize = conn
                .decr(redis_inflight_count_key(), removed as isize)
                .await?;
        }
        Ok(removed)
    }

    async fn count_session_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_inflight_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_inflight_tenant_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: Option<usize> = conn.get(redis_inflight_count_key()).await?;
        Ok(count.unwrap_or(0))
    }
}

#[async_trait]
impl RetainStore for RedisRetainStore {
    async fn put_retain(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = redis_retain_key(&message.tenant_id, &message.topic);
        let exists: bool = conn.exists(&key).await?;
        if message.payload.is_empty() {
            if exists {
                let _: usize = conn.del(key).await?;
                let _: isize = conn.decr(redis_retain_count_key(), 1).await?;
            }
        } else {
            let _: () = conn.set(key, serde_json::to_string(message)?).await?;
            if !exists {
                let _: isize = conn.incr(redis_retain_count_key(), 1).await?;
            }
        }
        Ok(())
    }

    async fn load_retain(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value: Option<String> = conn.get(redis_retain_key(tenant_id, topic)).await?;
        Ok(value
            .map(|value| serde_json::from_str(&value))
            .transpose()?)
    }

    async fn match_retain(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_retain_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut messages = Vec::new();
        for key in keys {
            let value: String = conn.get(key).await?;
            let message: RetainedMessage = serde_json::from_str(&value)?;
            if greenmqtt_core::topic_matches(topic_filter, &message.topic) {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_retain_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut messages = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            messages.push(serde_json::from_str(&value)?);
        }
        Ok(messages)
    }

    async fn count_retained(&self) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: Option<usize> = conn.get(redis_retain_count_key()).await?;
        Ok(count.unwrap_or(0))
    }
}

#[async_trait]
impl RouteStore for RedisRouteStore {
    async fn save_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let exists: bool = conn.exists(redis_route_key(route)).await?;
        let _: () = conn
            .set(redis_route_key(route), serde_json::to_string(route)?)
            .await?;
        let _: () = conn
            .set(
                redis_route_session_key(route),
                serde_json::to_string(route)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_route_session_topic_shared_key(route),
                serde_json::to_string(route)?,
            )
            .await?;
        let _: () = conn
            .set(
                redis_route_tenant_shared_key(route),
                serde_json::to_string(route)?,
            )
            .await?;
        let _: () = conn
            .set(redis_route_filter_key(route), serde_json::to_string(route)?)
            .await?;
        let _: () = conn
            .set(
                redis_route_filter_shared_key(route),
                serde_json::to_string(route)?,
            )
            .await?;
        let route_json = serde_json::to_string(route)?;
        if route_topic_filter_is_exact(&route.topic_filter) {
            let _: () = conn.set(redis_route_exact_key(route), route_json).await?;
        } else {
            let _: () = conn
                .set(redis_route_wildcard_key(route), route_json)
                .await?;
        }
        if !exists {
            let _: isize = conn.incr(redis_route_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn delete_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let removed: usize = conn.del(redis_route_key(route)).await?;
        let _: usize = conn.del(redis_route_session_key(route)).await?;
        let _: usize = conn
            .del(redis_route_session_topic_shared_key(route))
            .await?;
        let _: usize = conn.del(redis_route_tenant_shared_key(route)).await?;
        let _: usize = conn.del(redis_route_filter_key(route)).await?;
        let _: usize = conn.del(redis_route_filter_shared_key(route)).await?;
        if route_topic_filter_is_exact(&route.topic_filter) {
            let _: usize = conn.del(redis_route_exact_key(route)).await?;
        } else {
            let _: usize = conn.del(redis_route_wildcard_key(route)).await?;
        }
        if removed > 0 {
            let _: isize = conn.decr(redis_route_count_key(), 1).await?;
        }
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_session_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str::<RouteRecord>(&value)?);
        }
        drop(conn);
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str::<RouteRecord>(&value)?);
        }
        drop(conn);
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_tenant_shared_pattern(tenant_id, shared_group))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str::<RouteRecord>(&value)?);
        }
        drop(conn);
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn load_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let value: Option<String> = conn
            .get(redis_route_session_topic_shared_key_from_parts(
                session_id,
                topic_filter,
                shared_group,
            ))
            .await?;
        value
            .map(|value| serde_json::from_str::<RouteRecord>(&value))
            .transpose()
            .map_err(Into::into)
    }

    async fn remove_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let Some(route) = self
            .load_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await?
        else {
            return Ok(0);
        };
        self.delete_route(&route).await?;
        Ok(1)
    }

    async fn remove_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_filter_pattern(tenant_id, topic_filter))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str::<RouteRecord>(&value)?);
        }
        drop(conn);
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_filter_shared_pattern(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str::<RouteRecord>(&value)?);
        }
        drop(conn);
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_session_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str::<RouteRecord>(&value)?);
        }
        drop(conn);
        for route in &routes {
            let mut updated = route.clone();
            updated.node_id = node_id;
            self.save_route(&updated).await?;
        }
        Ok(routes.len())
    }

    async fn list_routes(&self) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_prefix_pattern("route"))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn list_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_tenant_shared_pattern(tenant_id, shared_group))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_tenant_shared_pattern(tenant_id, shared_group))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_filter_pattern(tenant_id, topic_filter))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_filter_shared_pattern(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_filter_pattern(tenant_id, topic_filter))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_filter_shared_pattern(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_exact_pattern(tenant_id, topic_filter))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn list_tenant_wildcard_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_wildcard_pattern(tenant_id))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_session_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        let mut routes = Vec::with_capacity(keys.len());
        for key in keys {
            let value: String = conn.get(key).await?;
            routes.push(serde_json::from_str(&value)?);
        }
        Ok(routes)
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_session_routes(session_id)
            .await?
            .into_iter()
            .filter(|route| route.topic_filter == topic_filter)
            .collect())
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = cmd("KEYS")
            .arg(redis_route_session_pattern(session_id))
            .query_async(&mut conn)
            .await?;
        Ok(keys.len())
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await?
            .len())
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let exists: bool = conn
            .exists(redis_route_session_topic_shared_key_from_parts(
                session_id,
                topic_filter,
                shared_group,
            ))
            .await?;
        Ok(usize::from(exists))
    }

    async fn count_routes(&self) -> anyhow::Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let count: Option<usize> = conn.get(redis_route_count_key()).await?;
        Ok(count.unwrap_or(0))
    }
}
