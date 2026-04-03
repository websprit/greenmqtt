use super::{InboxStore, InflightStore, RetainStore, RouteStore, SessionStore, SubscriptionStore};
use crate::{
    inbox_key, inbox_prefix, inbox_tenant_index_key, inbox_tenant_prefix, inflight_key,
    inflight_prefix, inflight_tenant_index_key, inflight_tenant_prefix, is_inbox_internal_key,
    is_inflight_internal_key, is_route_internal_key, is_session_internal_key,
    is_subscription_internal_key, read_sled_count, retain_key, retain_prefix,
    route_exact_index_key, route_exact_index_prefix, route_filter_index_key,
    route_filter_index_prefix, route_filter_shared_index_key, route_filter_shared_index_prefix,
    route_key, route_prefix, route_session_index_key, route_session_index_prefix,
    route_session_topic_shared_index_key, route_session_topic_shared_index_key_from_parts,
    route_tenant_shared_index_key, route_tenant_shared_index_prefix, route_topic_filter_is_exact,
    route_wildcard_index_key, route_wildcard_index_prefix, session_id_index_key, session_key,
    session_prefix, subscription_key, subscription_prefix, subscription_tenant_index_key,
    subscription_tenant_prefix, subscription_tenant_shared_index_key,
    subscription_tenant_shared_prefix, subscription_tenant_topic_index_key,
    subscription_tenant_topic_prefix, subscription_tenant_topic_shared_index_key,
    subscription_tenant_topic_shared_prefix, trailing_u64, update_sled_count, INBOX_COUNT_KEY,
    INFLIGHT_COUNT_KEY, RETAIN_COUNT_KEY, ROUTE_COUNT_KEY, SESSION_COUNT_KEY,
    SUBSCRIPTION_COUNT_KEY,
};
use ::sled::{Db, Tree};
use async_trait::async_trait;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, SessionRecord, Subscription,
};
use std::path::Path;

#[derive(Clone)]
pub struct SledSessionStore {
    tree: Tree,
}

#[derive(Clone)]
pub struct SledSubscriptionStore {
    tree: Tree,
}

#[derive(Clone)]
pub struct SledInboxStore {
    db: Db,
    tree: Tree,
}

#[derive(Clone)]
pub struct SledInflightStore {
    tree: Tree,
}

#[derive(Clone)]
pub struct SledRetainStore {
    tree: Tree,
}

#[derive(Clone)]
pub struct SledRouteStore {
    tree: Tree,
}

impl SledSessionStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            tree: db.open_tree("sessions")?,
        })
    }
}

impl SledSubscriptionStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            tree: db.open_tree("subscriptions")?,
        })
    }
}

impl SledInboxStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        let tree = db.open_tree("inbox")?;
        Ok(Self { db, tree })
    }
}

impl SledInflightStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            tree: db.open_tree("inflight")?,
        })
    }
}

impl SledRetainStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            tree: db.open_tree("retain")?,
        })
    }
}

impl SledRouteStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            tree: db.open_tree("routes")?,
        })
    }
}

#[async_trait]
impl SessionStore for SledSessionStore {
    async fn save_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        let previous = self
            .load_session(&session.identity.tenant_id, &session.identity.client_id)
            .await?;
        let is_new = previous.is_none();
        if let Some(previous) = previous {
            self.tree
                .remove(session_id_index_key(&previous.session_id))?;
        }
        self.tree.insert(
            session_key(&session.identity.tenant_id, &session.identity.client_id),
            serde_json::to_vec(session)?,
        )?;
        self.tree.insert(
            session_id_index_key(&session.session_id),
            serde_json::to_vec(session)?,
        )?;
        if is_new {
            update_sled_count(&self.tree, SESSION_COUNT_KEY, 1)?;
        }
        self.tree.flush()?;
        Ok(())
    }

    async fn load_session(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .tree
            .get(session_key(tenant_id, client_id))?
            .map(|value| serde_json::from_slice(&value))
            .transpose()?)
    }

    async fn load_session_by_session_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .tree
            .get(session_id_index_key(session_id))?
            .map(|value| serde_json::from_slice(&value))
            .transpose()?)
    }

    async fn delete_session(&self, tenant_id: &str, client_id: &str) -> anyhow::Result<()> {
        if let Some(existing) = self.load_session(tenant_id, client_id).await? {
            self.tree
                .remove(session_id_index_key(&existing.session_id))?;
            update_sled_count(&self.tree, SESSION_COUNT_KEY, -1)?;
        }
        self.tree.remove(session_key(tenant_id, client_id))?;
        self.tree.flush()?;
        Ok(())
    }

    async fn list_sessions(&self) -> anyhow::Result<Vec<SessionRecord>> {
        let mut sessions = Vec::new();
        for item in self.tree.iter() {
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
        for item in self.tree.scan_prefix(session_prefix(tenant_id)) {
            let (_, value) = item?;
            sessions.push(serde_json::from_slice(&value)?);
        }
        Ok(sessions)
    }

    async fn count_sessions(&self) -> anyhow::Result<usize> {
        read_sled_count(&self.tree, SESSION_COUNT_KEY)
    }
}

#[async_trait]
impl SubscriptionStore for SledSubscriptionStore {
    async fn save_subscription(&self, subscription: &Subscription) -> anyhow::Result<()> {
        let key = subscription_key(
            &subscription.session_id,
            subscription.shared_group.as_deref(),
            &subscription.topic_filter,
        );
        let is_new = self.tree.get(&key)?.is_none();
        self.tree.insert(key, serde_json::to_vec(subscription)?)?;
        self.tree.insert(
            subscription_tenant_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        self.tree.insert(
            subscription_tenant_shared_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        self.tree.insert(
            subscription_tenant_topic_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        self.tree.insert(
            subscription_tenant_topic_shared_index_key(subscription),
            serde_json::to_vec(subscription)?,
        )?;
        if is_new {
            update_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY, 1)?;
        }
        self.tree.flush()?;
        Ok(())
    }

    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        Ok(self
            .tree
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
        let removed = self
            .tree
            .remove(subscription_key(session_id, shared_group, topic_filter))?
            .is_some();
        if let Some(subscription) = removed_subscription {
            self.tree
                .remove(subscription_tenant_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_shared_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_shared_index_key(&subscription))?;
            update_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY, -1)?;
        }
        self.tree.flush()?;
        Ok(removed)
    }

    async fn list_subscriptions(&self, session_id: &str) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for item in self.tree.scan_prefix(subscription_prefix(session_id)) {
            let (_, value) = item?;
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(subscription_prefix(session_id))
            .count())
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for item in self.tree.scan_prefix(subscription_prefix(session_id)) {
            let (_, value) = item?;
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
        let mut count = 0usize;
        for item in self.tree.scan_prefix(subscription_prefix(session_id)) {
            let (_, value) = item?;
            let subscription: Subscription = serde_json::from_slice(&value)?;
            if subscription.topic_filter == topic_filter {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn purge_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let entries: Vec<_> = self
            .tree
            .scan_prefix(subscription_prefix(session_id))
            .collect::<Result<_, _>>()?;
        let removed = entries.len();
        for (key, value) in entries {
            let subscription: Subscription = serde_json::from_slice(&value)?;
            self.tree.remove(key)?;
            self.tree
                .remove(subscription_tenant_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_shared_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let entries: Vec<_> = self
            .tree
            .scan_prefix(subscription_tenant_prefix(tenant_id))
            .collect::<Result<_, _>>()?;
        let removed = entries.len();
        for (_, value) in entries {
            let subscription: Subscription = serde_json::from_slice(&value)?;
            self.tree.remove(subscription_key(
                &subscription.session_id,
                subscription.shared_group.as_deref(),
                &subscription.topic_filter,
            ))?;
            self.tree
                .remove(subscription_tenant_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_shared_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let entries: Vec<_> = self
            .tree
            .scan_prefix(subscription_tenant_topic_shared_prefix(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .collect::<Result<_, _>>()?;
        let removed = entries.len();
        for (_, value) in entries {
            let subscription: Subscription = serde_json::from_slice(&value)?;
            self.tree.remove(subscription_key(
                &subscription.session_id,
                subscription.shared_group.as_deref(),
                &subscription.topic_filter,
            ))?;
            self.tree
                .remove(subscription_tenant_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_shared_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let entries: Vec<_> = self
            .tree
            .scan_prefix(subscription_tenant_topic_prefix(tenant_id, topic_filter))
            .collect::<Result<_, _>>()?;
        let removed = entries.len();
        for (_, value) in entries {
            let subscription: Subscription = serde_json::from_slice(&value)?;
            self.tree.remove(subscription_key(
                &subscription.session_id,
                subscription.shared_group.as_deref(),
                &subscription.topic_filter,
            ))?;
            self.tree
                .remove(subscription_tenant_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_shared_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_index_key(&subscription))?;
            self.tree
                .remove(subscription_tenant_topic_shared_index_key(&subscription))?;
        }
        update_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for item in self.tree.scan_prefix(subscription_tenant_prefix(tenant_id)) {
            let (_, value) = item?;
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
        for item in self
            .tree
            .scan_prefix(subscription_tenant_shared_prefix(tenant_id, shared_group))
        {
            let (_, value) = item?;
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(subscription_tenant_prefix(tenant_id))
            .count())
    }

    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(subscription_tenant_shared_prefix(tenant_id, shared_group))
            .count())
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for item in self
            .tree
            .scan_prefix(subscription_tenant_topic_prefix(tenant_id, topic_filter))
        {
            let (_, value) = item?;
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
        for item in self
            .tree
            .scan_prefix(subscription_tenant_topic_shared_prefix(
                tenant_id,
                topic_filter,
                shared_group,
            ))
        {
            let (_, value) = item?;
            subscriptions.push(serde_json::from_slice(&value)?);
        }
        Ok(subscriptions)
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(subscription_tenant_topic_prefix(tenant_id, topic_filter))
            .count())
    }

    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(subscription_tenant_topic_shared_prefix(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .count())
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for item in self.tree.iter() {
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
        read_sled_count(&self.tree, SUBSCRIPTION_COUNT_KEY)
    }
}

#[async_trait]
impl InboxStore for SledInboxStore {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        let seq = self.db.generate_id()?;
        let value = serde_json::to_vec(message)?;
        self.tree
            .insert(inbox_key(&message.session_id, seq), value.clone())?;
        self.tree.insert(
            inbox_tenant_index_key(&message.tenant_id, &message.session_id, seq),
            value,
        )?;
        update_sled_count(&self.tree, INBOX_COUNT_KEY, 1)?;
        self.tree.flush()?;
        Ok(())
    }

    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for item in self.tree.scan_prefix(inbox_prefix(session_id)) {
            let (_, value) = item?;
            messages.push(serde_json::from_slice(&value)?);
        }
        Ok(messages)
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for item in self.tree.iter() {
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
        for item in self.tree.scan_prefix(inbox_tenant_prefix(tenant_id)) {
            let (_, value) = item?;
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
        Ok(self.tree.scan_prefix(inbox_prefix(session_id)).count())
    }

    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(inbox_tenant_prefix(tenant_id))
            .count())
    }

    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        let mut keys = Vec::new();
        for item in self.tree.scan_prefix(inbox_prefix(session_id)) {
            let (key, value) = item?;
            let message: OfflineMessage = serde_json::from_slice(&value)?;
            let seq = trailing_u64(key.as_ref());
            keys.push((key, message.tenant_id.clone(), seq));
            messages.push(message);
        }
        for (key, tenant_id, seq) in keys {
            self.tree.remove(key)?;
            if let Some(seq) = seq {
                self.tree
                    .remove(inbox_tenant_index_key(&tenant_id, session_id, seq))?;
            }
        }
        update_sled_count(&self.tree, INBOX_COUNT_KEY, -(messages.len() as isize))?;
        self.tree.flush()?;
        Ok(messages)
    }

    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut removed = 0usize;
        let mut keys = Vec::new();
        for item in self.tree.scan_prefix(inbox_prefix(session_id)) {
            let (key, value) = item?;
            let message: OfflineMessage = serde_json::from_slice(&value)?;
            let seq = trailing_u64(key.as_ref());
            keys.push((key, message.tenant_id, seq));
        }
        for (key, tenant_id, seq) in keys {
            self.tree.remove(key)?;
            if let Some(seq) = seq {
                self.tree
                    .remove(inbox_tenant_index_key(&tenant_id, session_id, seq))?;
            }
            removed += 1;
        }
        update_sled_count(&self.tree, INBOX_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut keys = Vec::new();
        for item in self.tree.scan_prefix(inbox_tenant_prefix(tenant_id)) {
            let (key, value) = item?;
            let message: OfflineMessage = serde_json::from_slice(&value)?;
            let seq = trailing_u64(key.as_ref());
            keys.push((key, message.session_id, seq));
        }
        let removed = keys.len();
        for (tenant_key, session_id, seq) in keys {
            self.tree.remove(tenant_key)?;
            if let Some(seq) = seq {
                self.tree.remove(inbox_key(&session_id, seq))?;
            }
        }
        update_sled_count(&self.tree, INBOX_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        read_sled_count(&self.tree, INBOX_COUNT_KEY)
    }
}

#[async_trait]
impl InflightStore for SledInflightStore {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()> {
        let key = inflight_key(&message.session_id, message.packet_id);
        let value = serde_json::to_vec(message)?;
        let previous = self.tree.insert(key, value.clone())?;
        if let Some(previous) = previous {
            let previous: InflightMessage = serde_json::from_slice(&previous)?;
            self.tree.remove(inflight_tenant_index_key(
                &previous.tenant_id,
                &previous.session_id,
                previous.packet_id,
            ))?;
        } else {
            update_sled_count(&self.tree, INFLIGHT_COUNT_KEY, 1)?;
        }
        self.tree.insert(
            inflight_tenant_index_key(&message.tenant_id, &message.session_id, message.packet_id),
            value,
        )?;
        self.tree.flush()?;
        Ok(())
    }

    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for item in self.tree.scan_prefix(inflight_prefix(session_id)) {
            let (_, value) = item?;
            messages.push(serde_json::from_slice(&value)?);
        }
        Ok(messages)
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for item in self.tree.iter() {
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
        for item in self.tree.scan_prefix(inflight_tenant_prefix(tenant_id)) {
            let (_, value) = item?;
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
        Ok(self.tree.scan_prefix(inflight_prefix(session_id)).count())
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(inflight_tenant_prefix(tenant_id))
            .count())
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        if let Some(removed) = self.tree.remove(inflight_key(session_id, packet_id))? {
            let removed: InflightMessage = serde_json::from_slice(&removed)?;
            self.tree.remove(inflight_tenant_index_key(
                &removed.tenant_id,
                &removed.session_id,
                removed.packet_id,
            ))?;
            update_sled_count(&self.tree, INFLIGHT_COUNT_KEY, -1)?;
        }
        self.tree.flush()?;
        Ok(())
    }

    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut keys = Vec::new();
        for item in self.tree.scan_prefix(inflight_prefix(session_id)) {
            let (key, value) = item?;
            let message: InflightMessage = serde_json::from_slice(&value)?;
            keys.push((key, message.tenant_id, message.packet_id));
        }
        let removed = keys.len();
        for (key, tenant_id, packet_id) in keys {
            self.tree.remove(key)?;
            self.tree
                .remove(inflight_tenant_index_key(&tenant_id, session_id, packet_id))?;
        }
        update_sled_count(&self.tree, INFLIGHT_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut keys = Vec::new();
        for item in self.tree.scan_prefix(inflight_tenant_prefix(tenant_id)) {
            let (key, value) = item?;
            let message: InflightMessage = serde_json::from_slice(&value)?;
            keys.push((key, message.session_id, message.packet_id));
        }
        let removed = keys.len();
        for (tenant_key, session_id, packet_id) in keys {
            self.tree.remove(tenant_key)?;
            self.tree.remove(inflight_key(&session_id, packet_id))?;
        }
        update_sled_count(&self.tree, INFLIGHT_COUNT_KEY, -(removed as isize))?;
        self.tree.flush()?;
        Ok(removed)
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        read_sled_count(&self.tree, INFLIGHT_COUNT_KEY)
    }
}

#[async_trait]
impl RetainStore for SledRetainStore {
    async fn put_retain(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        let key = retain_key(&message.tenant_id, &message.topic);
        let existing = self.tree.get(&key)?.is_some();
        if message.payload.is_empty() {
            if existing {
                self.tree.remove(key)?;
                update_sled_count(&self.tree, RETAIN_COUNT_KEY, -1)?;
            }
        } else {
            self.tree.insert(key, serde_json::to_vec(message)?)?;
            if !existing {
                update_sled_count(&self.tree, RETAIN_COUNT_KEY, 1)?;
            }
        }
        self.tree.flush()?;
        Ok(())
    }

    async fn load_retain(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self
            .tree
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
        for item in self.tree.scan_prefix(retain_prefix(tenant_id)) {
            let (_, value) = item?;
            let message: RetainedMessage = serde_json::from_slice(&value)?;
            if greenmqtt_core::topic_matches(topic_filter, &message.topic) {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut messages = Vec::new();
        for item in self.tree.scan_prefix(retain_prefix(tenant_id)) {
            let (_, value) = item?;
            messages.push(serde_json::from_slice(&value)?);
        }
        Ok(messages)
    }

    async fn count_retained(&self) -> anyhow::Result<usize> {
        read_sled_count(&self.tree, RETAIN_COUNT_KEY)
    }
}

#[async_trait]
impl RouteStore for SledRouteStore {
    async fn save_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let key = route_key(route);
        let is_new = self.tree.get(&key)?.is_none();
        self.tree.insert(key, serde_json::to_vec(route)?)?;
        self.tree
            .insert(route_session_index_key(route), serde_json::to_vec(route)?)?;
        self.tree.insert(
            route_session_topic_shared_index_key(route),
            serde_json::to_vec(route)?,
        )?;
        self.tree.insert(
            route_tenant_shared_index_key(route),
            serde_json::to_vec(route)?,
        )?;
        self.tree
            .insert(route_filter_index_key(route), serde_json::to_vec(route)?)?;
        self.tree.insert(
            route_filter_shared_index_key(route),
            serde_json::to_vec(route)?,
        )?;
        if route_topic_filter_is_exact(&route.topic_filter) {
            self.tree
                .insert(route_exact_index_key(route), serde_json::to_vec(route)?)?;
        } else {
            self.tree
                .insert(route_wildcard_index_key(route), serde_json::to_vec(route)?)?;
        }
        if is_new {
            update_sled_count(&self.tree, ROUTE_COUNT_KEY, 1)?;
        }
        self.tree.flush()?;
        Ok(())
    }

    async fn delete_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let removed = self.tree.remove(route_key(route))?.is_some();
        self.tree.remove(route_session_index_key(route))?;
        self.tree
            .remove(route_session_topic_shared_index_key(route))?;
        self.tree.remove(route_tenant_shared_index_key(route))?;
        self.tree.remove(route_filter_index_key(route))?;
        self.tree.remove(route_filter_shared_index_key(route))?;
        if route_topic_filter_is_exact(&route.topic_filter) {
            self.tree.remove(route_exact_index_key(route))?;
        } else {
            self.tree.remove(route_wildcard_index_key(route))?;
        }
        if removed {
            update_sled_count(&self.tree, ROUTE_COUNT_KEY, -1)?;
        }
        self.tree.flush()?;
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_session_index_prefix(session_id))
        {
            let (_, value) = item?;
            routes.push(serde_json::from_slice::<RouteRecord>(&value)?);
        }
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut routes = Vec::new();
        for item in self.tree.scan_prefix(route_prefix(tenant_id)) {
            let (_, value) = item?;
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
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_tenant_shared_index_prefix(tenant_id, shared_group))
        {
            let (_, value) = item?;
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
            .tree
            .get(route_session_topic_shared_index_key_from_parts(
                session_id,
                topic_filter,
                shared_group,
            ))?
            .map(|value| serde_json::from_slice::<RouteRecord>(&value))
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
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_filter_index_prefix(tenant_id, topic_filter))
        {
            let (_, value) = item?;
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
        let mut routes = Vec::new();
        for item in self.tree.scan_prefix(route_filter_shared_index_prefix(
            tenant_id,
            topic_filter,
            shared_group,
        )) {
            let (_, value) = item?;
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
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_session_index_prefix(session_id))
        {
            let (_, value) = item?;
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
        for item in self.tree.iter() {
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
        for item in self.tree.scan_prefix(route_prefix(tenant_id)) {
            let (_, value) = item?;
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.tree.scan_prefix(route_prefix(tenant_id)).count())
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_tenant_shared_index_prefix(tenant_id, shared_group))
        {
            let (_, value) = item?;
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(route_tenant_shared_index_prefix(tenant_id, shared_group))
            .count())
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_filter_index_prefix(tenant_id, topic_filter))
        {
            let (_, value) = item?;
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
        for item in self.tree.scan_prefix(route_filter_shared_index_prefix(
            tenant_id,
            topic_filter,
            shared_group,
        )) {
            let (_, value) = item?;
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(route_filter_index_prefix(tenant_id, topic_filter))
            .count())
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(route_filter_shared_index_prefix(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .count())
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_exact_index_prefix(tenant_id, topic_filter))
        {
            let (_, value) = item?;
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_tenant_wildcard_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_wildcard_index_prefix(tenant_id))
        {
            let (_, value) = item?;
            routes.push(serde_json::from_slice(&value)?);
        }
        Ok(routes)
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = Vec::new();
        for item in self
            .tree
            .scan_prefix(route_session_index_prefix(session_id))
        {
            let (_, value) = item?;
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
        for item in self
            .tree
            .scan_prefix(route_session_index_prefix(session_id))
        {
            let (_, value) = item?;
            let route: RouteRecord = serde_json::from_slice(&value)?;
            if route.topic_filter == topic_filter {
                routes.push(route);
            }
        }
        Ok(routes)
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .tree
            .scan_prefix(route_session_index_prefix(session_id))
            .count())
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut count = 0usize;
        for item in self
            .tree
            .scan_prefix(route_session_index_prefix(session_id))
        {
            let (_, value) = item?;
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
            self.tree
                .get(route_session_topic_shared_index_key_from_parts(
                    session_id,
                    topic_filter,
                    shared_group,
                ))?
                .is_some(),
        ))
    }

    async fn count_routes(&self) -> anyhow::Result<usize> {
        read_sled_count(&self.tree, ROUTE_COUNT_KEY)
    }
}
