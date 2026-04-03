use super::{InboxStore, InflightStore, RetainStore, RouteStore, SessionStore, SubscriptionStore};
use crate::{
    inbox_key, inbox_prefix, inbox_tenant_index_key, inbox_tenant_prefix, inflight_key,
    inflight_prefix, inflight_tenant_index_key, inflight_tenant_prefix, is_inbox_internal_key,
    is_inflight_internal_key, is_route_internal_key, is_session_internal_key,
    is_subscription_internal_key, next_inbox_seq, open_rocks_db, read_rocks_count, retain_key,
    retain_prefix, route_exact_index_key, route_exact_index_prefix, route_filter_index_key,
    route_filter_index_prefix, route_filter_shared_index_key, route_filter_shared_index_prefix,
    route_key, route_prefix, route_session_index_key, route_session_index_prefix,
    route_session_topic_shared_index_key, route_session_topic_shared_index_key_from_parts,
    route_tenant_shared_index_key, route_tenant_shared_index_prefix, route_topic_filter_is_exact,
    route_wildcard_index_key, route_wildcard_index_prefix, session_id_index_key, session_key,
    session_prefix, subscription_key, subscription_prefix, subscription_tenant_index_key,
    subscription_tenant_prefix, subscription_tenant_shared_index_key,
    subscription_tenant_shared_prefix, subscription_tenant_topic_index_key,
    subscription_tenant_topic_prefix, subscription_tenant_topic_shared_index_key,
    subscription_tenant_topic_shared_prefix, trailing_u64, update_rocks_count, INBOX_COUNT_KEY,
    INFLIGHT_COUNT_KEY, RETAIN_COUNT_KEY, ROUTE_COUNT_KEY, SESSION_COUNT_KEY,
    SUBSCRIPTION_COUNT_KEY,
};
use ::rocksdb::{Direction, IteratorMode, DB};
use async_trait::async_trait;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, SessionRecord, Subscription,
};
use std::path::Path;
use std::sync::atomic::AtomicU64;

pub struct RocksSessionStore {
    db: DB,
}

pub struct RocksSubscriptionStore {
    db: DB,
}

pub struct RocksInboxStore {
    db: DB,
    seq: AtomicU64,
}

pub struct RocksInflightStore {
    db: DB,
}

pub struct RocksRetainStore {
    db: DB,
}

pub struct RocksRouteStore {
    db: DB,
}

impl RocksSessionStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: open_rocks_db(path)?,
        })
    }
}

impl RocksSubscriptionStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: open_rocks_db(path)?,
        })
    }
}

impl RocksInboxStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: open_rocks_db(path)?,
            seq: AtomicU64::new(0),
        })
    }
}

impl RocksInflightStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: open_rocks_db(path)?,
        })
    }
}

impl RocksRetainStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: open_rocks_db(path)?,
        })
    }
}

impl RocksRouteStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: open_rocks_db(path)?,
        })
    }
}

#[async_trait]
impl SessionStore for RocksSessionStore {
    async fn save_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        let previous = self
            .load_session(&session.identity.tenant_id, &session.identity.client_id)
            .await?;
        let is_new = previous.is_none();
        if let Some(previous) = previous {
            self.db.delete(session_id_index_key(&previous.session_id))?;
        }
        self.db.put(
            session_key(&session.identity.tenant_id, &session.identity.client_id),
            serde_json::to_vec(session)?,
        )?;
        self.db.put(
            session_id_index_key(&session.session_id),
            serde_json::to_vec(session)?,
        )?;
        if is_new {
            update_rocks_count(&self.db, SESSION_COUNT_KEY, 1)?;
        }
        self.db.flush()?;
        Ok(())
    }

    async fn load_session(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .db
            .get(session_key(tenant_id, client_id))?
            .map(|value| serde_json::from_slice(&value))
            .transpose()?)
    }

    async fn load_session_by_session_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .db
            .get(session_id_index_key(session_id))?
            .map(|value| serde_json::from_slice(&value))
            .transpose()?)
    }

    async fn delete_session(&self, tenant_id: &str, client_id: &str) -> anyhow::Result<()> {
        if let Some(existing) = self.load_session(tenant_id, client_id).await? {
            self.db.delete(session_id_index_key(&existing.session_id))?;
            update_rocks_count(&self.db, SESSION_COUNT_KEY, -1)?;
        }
        self.db.delete(session_key(tenant_id, client_id))?;
        self.db.flush()?;
        Ok(())
    }

    async fn list_sessions(&self) -> anyhow::Result<Vec<SessionRecord>> {
        let mut sessions = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            if is_session_internal_key(key.as_ref()) {
                continue;
            }
            sessions.push(serde_json::from_slice(&value)?);
        }
        Ok(sessions)
    }

    async fn list_tenant_sessions(&self, tenant_id: &str) -> anyhow::Result<Vec<SessionRecord>> {
        let mut sessions = Vec::new();
        let prefix = session_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            sessions.push(serde_json::from_slice(&value)?);
        }
        Ok(sessions)
    }

    async fn count_sessions(&self) -> anyhow::Result<usize> {
        read_rocks_count(&self.db, SESSION_COUNT_KEY)
    }
}

#[async_trait]
impl SubscriptionStore for RocksSubscriptionStore {
    async fn save_subscription(&self, subscription: &Subscription) -> anyhow::Result<()> {
        let key = subscription_key(
            &subscription.session_id,
            subscription.shared_group.as_deref(),
            &subscription.topic_filter,
        );
        let is_new = self.db.get_pinned(&key)?.is_none();
        self.db.put(key, serde_json::to_vec(subscription)?)?;
        self.db.put(
            subscription_tenant_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        self.db.put(
            subscription_tenant_shared_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        self.db.put(
            subscription_tenant_topic_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        self.db.put(
            subscription_tenant_topic_shared_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        if is_new {
            update_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY, 1)?;
        }
        self.db.flush()?;
        Ok(())
    }

    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        Ok(self
            .db
            .get(subscription_key(session_id, shared_group, topic_filter))?
            .map(|value| serde_json::from_slice(&value))
            .transpose()?)
    }

    async fn delete_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let removed_subscription = self
            .load_subscription(session_id, topic_filter, shared_group)
            .await?;
        let key = subscription_key(session_id, shared_group, topic_filter);
        let removed = self.db.get_pinned(&key)?.is_some();
        self.db.delete(key)?;
        if let Some(subscription) = removed_subscription {
            self.db
                .delete(subscription_tenant_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_shared_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_shared_index_key(&subscription))?;
            update_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY, -1)?;
        }
        self.db.flush()?;
        Ok(removed)
    }

    async fn list_subscriptions(&self, session_id: &str) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        let prefix = subscription_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = subscription_prefix(session_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        let prefix = subscription_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let subscription: Subscription = serde_json::from_slice(&value)?;
            if subscription.topic_filter == topic_filter {
                subscriptions.push(subscription);
            }
        }
        Ok(subscriptions)
    }

    async fn count_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let prefix = subscription_prefix(session_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let subscription: Subscription = serde_json::from_slice(&value)?;
            if subscription.topic_filter == topic_filter {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn purge_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = subscription_prefix(session_id);
        let mut entries = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            entries.push((key, value));
        }
        let removed = entries.len();
        for (key, value) in entries {
            let subscription: Subscription = serde_json::from_slice(&value)?;
            self.db.delete(key)?;
            self.db
                .delete(subscription_tenant_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_shared_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_prefix(tenant_id);
        let mut subscriptions = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let subscription: Subscription = serde_json::from_slice(&value)?;
            subscriptions.push(subscription);
        }
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.db.delete(subscription_key(
                &subscription.session_id,
                subscription.shared_group.as_deref(),
                &subscription.topic_filter,
            ))?;
            self.db
                .delete(subscription_tenant_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_shared_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_topic_shared_prefix(tenant_id, topic_filter, shared_group);
        let mut subscriptions = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let subscription: Subscription = serde_json::from_slice(&value)?;
            subscriptions.push(subscription);
        }
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.db.delete(subscription_key(
                &subscription.session_id,
                subscription.shared_group.as_deref(),
                &subscription.topic_filter,
            ))?;
            self.db
                .delete(subscription_tenant_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_shared_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_topic_prefix(tenant_id, topic_filter);
        let mut subscriptions = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let subscription: Subscription = serde_json::from_slice(&value)?;
            subscriptions.push(subscription);
        }
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.db.delete(subscription_key(
                &subscription.session_id,
                subscription.shared_group.as_deref(),
                &subscription.topic_filter,
            ))?;
            self.db
                .delete(subscription_tenant_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_shared_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_index_key(&subscription))?;
            self.db
                .delete(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        let prefix = subscription_tenant_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        let prefix = subscription_tenant_shared_prefix(tenant_id, shared_group);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_prefix(tenant_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_shared_prefix(tenant_id, shared_group);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        let prefix = subscription_tenant_topic_prefix(tenant_id, topic_filter);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        let prefix = subscription_tenant_topic_shared_prefix(tenant_id, topic_filter, shared_group);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_topic_prefix(tenant_id, topic_filter);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let prefix = subscription_tenant_topic_shared_prefix(tenant_id, topic_filter, shared_group);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            if is_subscription_internal_key(key.as_ref()) {
                continue;
            }
            subscriptions.push(serde_json::from_slice(&value)?);
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
        read_rocks_count(&self.db, SUBSCRIPTION_COUNT_KEY)
    }
}

#[async_trait]
impl InboxStore for RocksInboxStore {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        let seq = next_inbox_seq(&self.seq);
        let value = serde_json::to_vec(message)?;
        self.db
            .put(inbox_key(&message.session_id, seq), value.clone())?;
        self.db.put(
            inbox_tenant_index_key(&message.tenant_id, &message.session_id, seq),
            value,
        )?;
        update_rocks_count(&self.db, INBOX_COUNT_KEY, 1)?;
        self.db.flush()?;
        Ok(())
    }

    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        let prefix = inbox_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            messages.push(serde_json::from_slice(&value)?);
        }
        Ok(messages)
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            if is_inbox_internal_key(key.as_ref()) {
                continue;
            }
            messages.push(serde_json::from_slice(&value)?);
        }
        messages.sort_by(|left: &OfflineMessage, right: &OfflineMessage| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic.cmp(&right.topic))
        });
        Ok(messages)
    }

    async fn list_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        let prefix = inbox_tenant_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            messages.push(serde_json::from_slice(&value)?);
        }
        messages.sort_by(|left: &OfflineMessage, right: &OfflineMessage| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.topic.cmp(&right.topic))
        });
        Ok(messages)
    }

    async fn count_session_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = inbox_prefix(session_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = inbox_tenant_prefix(tenant_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        let mut keys = Vec::new();
        let prefix = inbox_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message: OfflineMessage = serde_json::from_slice(&value)?;
            let seq = trailing_u64(key.as_ref());
            keys.push((key, message.tenant_id.clone(), seq));
            messages.push(message);
        }
        for (key, tenant_id, seq) in keys {
            self.db.delete(key)?;
            if let Some(seq) = seq {
                self.db
                    .delete(inbox_tenant_index_key(&tenant_id, session_id, seq))?;
            }
        }
        update_rocks_count(&self.db, INBOX_COUNT_KEY, -(messages.len() as isize))?;
        self.db.flush()?;
        Ok(messages)
    }

    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut removed = 0usize;
        let mut keys = Vec::new();
        let prefix = inbox_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message: OfflineMessage = serde_json::from_slice(&value)?;
            let seq = trailing_u64(key.as_ref());
            keys.push((key, message.tenant_id, seq));
        }
        for (key, tenant_id, seq) in keys {
            self.db.delete(key)?;
            if let Some(seq) = seq {
                self.db
                    .delete(inbox_tenant_index_key(&tenant_id, session_id, seq))?;
            }
            removed += 1;
        }
        update_rocks_count(&self.db, INBOX_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = inbox_tenant_prefix(tenant_id);
        let mut keys = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message: OfflineMessage = serde_json::from_slice(&value)?;
            let seq = trailing_u64(key.as_ref());
            keys.push((key, message.session_id, seq));
        }
        let removed = keys.len();
        for (tenant_key, session_id, seq) in keys {
            self.db.delete(tenant_key)?;
            if let Some(seq) = seq {
                self.db.delete(inbox_key(&session_id, seq))?;
            }
        }
        update_rocks_count(&self.db, INBOX_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        read_rocks_count(&self.db, INBOX_COUNT_KEY)
    }
}

#[async_trait]
impl InflightStore for RocksInflightStore {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()> {
        let key = inflight_key(&message.session_id, message.packet_id);
        if let Some(previous) = self.db.get(&key)? {
            let previous: InflightMessage = serde_json::from_slice(&previous)?;
            self.db.delete(inflight_tenant_index_key(
                &previous.tenant_id,
                &previous.session_id,
                previous.packet_id,
            ))?;
        } else {
            update_rocks_count(&self.db, INFLIGHT_COUNT_KEY, 1)?;
        }
        let value = serde_json::to_vec(message)?;
        self.db.put(&key, value.clone())?;
        self.db.put(
            inflight_tenant_index_key(&message.tenant_id, &message.session_id, message.packet_id),
            value,
        )?;
        self.db.flush()?;
        Ok(())
    }

    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        let prefix = inflight_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            messages.push(serde_json::from_slice(&value)?);
        }
        Ok(messages)
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            if is_inflight_internal_key(key.as_ref()) {
                continue;
            }
            messages.push(serde_json::from_slice(&value)?);
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
        let mut messages = Vec::new();
        let prefix = inflight_tenant_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            messages.push(serde_json::from_slice(&value)?);
        }
        messages.sort_by(|left: &InflightMessage, right: &InflightMessage| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.packet_id.cmp(&right.packet_id))
        });
        Ok(messages)
    }

    async fn count_session_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = inflight_prefix(session_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = inflight_tenant_prefix(tenant_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        let key = inflight_key(session_id, packet_id);
        if let Some(removed) = self.db.get(&key)? {
            let removed: InflightMessage = serde_json::from_slice(&removed)?;
            self.db.delete(inflight_tenant_index_key(
                &removed.tenant_id,
                &removed.session_id,
                removed.packet_id,
            ))?;
            update_rocks_count(&self.db, INFLIGHT_COUNT_KEY, -1)?;
        }
        self.db.delete(key)?;
        self.db.flush()?;
        Ok(())
    }

    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = inflight_prefix(session_id);
        let mut keys = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message: InflightMessage = serde_json::from_slice(&value)?;
            keys.push((key, message.tenant_id, message.packet_id));
        }
        let removed = keys.len();
        for (key, tenant_id, packet_id) in keys {
            self.db.delete(key)?;
            self.db
                .delete(inflight_tenant_index_key(&tenant_id, session_id, packet_id))?;
        }
        update_rocks_count(&self.db, INFLIGHT_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = inflight_tenant_prefix(tenant_id);
        let mut keys = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message: InflightMessage = serde_json::from_slice(&value)?;
            keys.push((key, message.session_id, message.packet_id));
        }
        let removed = keys.len();
        for (tenant_key, session_id, packet_id) in keys {
            self.db.delete(tenant_key)?;
            self.db.delete(inflight_key(&session_id, packet_id))?;
        }
        update_rocks_count(&self.db, INFLIGHT_COUNT_KEY, -(removed as isize))?;
        self.db.flush()?;
        Ok(removed)
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        read_rocks_count(&self.db, INFLIGHT_COUNT_KEY)
    }
}

#[async_trait]
impl RetainStore for RocksRetainStore {
    async fn put_retain(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        let key = retain_key(&message.tenant_id, &message.topic);
        let existing = self.db.get_pinned(&key)?.is_some();
        if message.payload.is_empty() {
            if existing {
                self.db.delete(key)?;
                update_rocks_count(&self.db, RETAIN_COUNT_KEY, -1)?;
            }
        } else {
            self.db.put(key, serde_json::to_vec(message)?)?;
            if !existing {
                update_rocks_count(&self.db, RETAIN_COUNT_KEY, 1)?;
            }
        }
        self.db.flush()?;
        Ok(())
    }

    async fn load_retain(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self
            .db
            .get(retain_key(tenant_id, topic))?
            .map(|value| serde_json::from_slice(&value))
            .transpose()?)
    }

    async fn match_retain(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut messages = Vec::new();
        let prefix = retain_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message: RetainedMessage = serde_json::from_slice(&value)?;
            if greenmqtt_core::topic_matches(topic_filter, &message.topic) {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut messages = Vec::new();
        let prefix = retain_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            messages.push(serde_json::from_slice(&value)?);
        }
        Ok(messages)
    }

    async fn count_retained(&self) -> anyhow::Result<usize> {
        read_rocks_count(&self.db, RETAIN_COUNT_KEY)
    }
}

#[async_trait]
impl RouteStore for RocksRouteStore {
    async fn save_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let key = route_key(route);
        let is_new = self.db.get_pinned(&key)?.is_none();
        self.db.put(key, serde_json::to_vec(route)?)?;
        self.db
            .put(route_session_index_key(route), serde_json::to_vec(route)?)?;
        self.db.put(
            route_session_topic_shared_index_key(route),
            serde_json::to_vec(route)?,
        )?;
        self.db.put(
            route_tenant_shared_index_key(route),
            serde_json::to_vec(route)?,
        )?;
        self.db
            .put(route_filter_index_key(route), serde_json::to_vec(route)?)?;
        self.db.put(
            route_filter_shared_index_key(route),
            serde_json::to_vec(route)?,
        )?;
        if route_topic_filter_is_exact(&route.topic_filter) {
            self.db
                .put(route_exact_index_key(route), serde_json::to_vec(route)?)?;
        } else {
            self.db
                .put(route_wildcard_index_key(route), serde_json::to_vec(route)?)?;
        }
        if is_new {
            update_rocks_count(&self.db, ROUTE_COUNT_KEY, 1)?;
        }
        self.db.flush()?;
        Ok(())
    }

    async fn delete_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let removed = self.db.get_pinned(route_key(route))?.is_some();
        self.db.delete(route_key(route))?;
        self.db.delete(route_session_index_key(route))?;
        self.db
            .delete(route_session_topic_shared_index_key(route))?;
        self.db.delete(route_tenant_shared_index_key(route))?;
        self.db.delete(route_filter_index_key(route))?;
        self.db.delete(route_filter_shared_index_key(route))?;
        if route_topic_filter_is_exact(&route.topic_filter) {
            self.db.delete(route_exact_index_key(route))?;
        } else {
            self.db.delete(route_wildcard_index_key(route))?;
        }
        if removed {
            update_rocks_count(&self.db, ROUTE_COUNT_KEY, -1)?;
        }
        self.db.flush()?;
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = route_session_index_prefix(session_id);
        let mut routes = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = route_prefix(tenant_id);
        let mut routes = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
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
        let prefix = route_tenant_shared_index_prefix(tenant_id, shared_group);
        let mut routes = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
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
        Ok(self
            .db
            .get_pinned(route_session_topic_shared_index_key_from_parts(
                session_id,
                topic_filter,
                shared_group,
            ))?
            .map(|value| serde_json::from_slice::<RouteRecord>(value.as_ref()))
            .transpose()?)
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
        let prefix = route_filter_index_prefix(tenant_id, topic_filter);
        let mut routes = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
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
        let prefix = route_filter_shared_index_prefix(tenant_id, topic_filter, shared_group);
        let mut routes = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
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
        let prefix = route_session_index_prefix(session_id);
        let mut routes = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
        for route in &routes {
            let mut updated = route.clone();
            updated.node_id = node_id;
            self.save_route(&updated).await?;
        }
        Ok(routes.len())
    }

    async fn list_routes(&self) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            if is_route_internal_key(key.as_ref()) {
                continue;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let prefix = route_prefix(tenant_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_tenant_shared_index_prefix(tenant_id, shared_group);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let prefix = route_tenant_shared_index_prefix(tenant_id, shared_group);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_filter_index_prefix(tenant_id, topic_filter);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_filter_shared_index_prefix(tenant_id, topic_filter, shared_group);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let prefix = route_filter_index_prefix(tenant_id, topic_filter);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let prefix = route_filter_shared_index_prefix(tenant_id, topic_filter, shared_group);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_exact_index_prefix(tenant_id, topic_filter);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_tenant_wildcard_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_wildcard_index_prefix(tenant_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_session_index_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        let prefix = route_session_index_prefix(session_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let route: RouteRecord = serde_json::from_slice(&value)?;
            if route.topic_filter == topic_filter {
                routes.push(route);
            }
        }
        Ok(routes)
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let prefix = route_session_index_prefix(session_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let prefix = route_session_index_prefix(session_id);
        let mut count = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let route: RouteRecord = serde_json::from_slice(&value)?;
            if route.topic_filter == topic_filter {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(usize::from(
            self.db
                .get_pinned(route_session_topic_shared_index_key_from_parts(
                    session_id,
                    topic_filter,
                    shared_group,
                ))?
                .is_some(),
        ))
    }

    async fn count_routes(&self) -> anyhow::Result<usize> {
        read_rocks_count(&self.db, ROUTE_COUNT_KEY)
    }
}
