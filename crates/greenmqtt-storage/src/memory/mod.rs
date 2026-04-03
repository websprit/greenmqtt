use super::{InboxStore, InflightStore, RetainStore, RouteStore, SessionStore, SubscriptionStore};
use crate::{
    route_filter_shared_identity, route_session_topic_shared_identity,
    route_tenant_shared_identity, route_topic_filter_is_exact, subscription_identity,
    subscription_tenant_topic_shared_identity,
};
use async_trait::async_trait;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, SessionRecord, Subscription,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct MemorySessionState {
    by_identity: HashMap<(String, String), SessionRecord>,
    by_session: HashMap<String, SessionRecord>,
    by_tenant: HashMap<String, HashMap<String, SessionRecord>>,
}

#[derive(Clone, Default)]
pub struct MemorySessionStore {
    inner: Arc<RwLock<MemorySessionState>>,
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn save_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("session store poisoned");
        let key = (
            session.identity.tenant_id.clone(),
            session.identity.client_id.clone(),
        );
        if let Some(previous) = guard.by_identity.insert(key, session.clone()) {
            guard.by_session.remove(&previous.session_id);
            if let Some(sessions) = guard.by_tenant.get_mut(&previous.identity.tenant_id) {
                sessions.remove(&previous.session_id);
                if sessions.is_empty() {
                    guard.by_tenant.remove(&previous.identity.tenant_id);
                }
            }
        }
        guard
            .by_session
            .insert(session.session_id.clone(), session.clone());
        guard
            .by_tenant
            .entry(session.identity.tenant_id.clone())
            .or_default()
            .insert(session.session_id.clone(), session.clone());
        Ok(())
    }

    async fn load_session(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .inner
            .read()
            .expect("session store poisoned")
            .by_identity
            .get(&(tenant_id.to_string(), client_id.to_string()))
            .cloned())
    }

    async fn load_session_by_session_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .inner
            .read()
            .expect("session store poisoned")
            .by_session
            .get(session_id)
            .cloned())
    }

    async fn delete_session(&self, tenant_id: &str, client_id: &str) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("session store poisoned");
        if let Some(removed) = guard
            .by_identity
            .remove(&(tenant_id.to_string(), client_id.to_string()))
        {
            guard.by_session.remove(&removed.session_id);
            if let Some(sessions) = guard.by_tenant.get_mut(&removed.identity.tenant_id) {
                sessions.remove(&removed.session_id);
                if sessions.is_empty() {
                    guard.by_tenant.remove(&removed.identity.tenant_id);
                }
            }
        }
        Ok(())
    }

    async fn list_sessions(&self) -> anyhow::Result<Vec<SessionRecord>> {
        Ok(self
            .inner
            .read()
            .expect("session store poisoned")
            .by_identity
            .values()
            .cloned()
            .collect())
    }

    async fn list_tenant_sessions(&self, tenant_id: &str) -> anyhow::Result<Vec<SessionRecord>> {
        Ok(self
            .inner
            .read()
            .expect("session store poisoned")
            .by_tenant
            .get(tenant_id)
            .into_iter()
            .flat_map(|records| records.values())
            .cloned()
            .collect())
    }

    async fn count_sessions(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("session store poisoned")
            .by_session
            .len())
    }
}

#[derive(Default)]
struct MemorySubscriptionState {
    by_session: HashMap<String, Vec<Subscription>>,
    by_tenant: HashMap<String, Vec<Subscription>>,
    by_tenant_shared: HashMap<(String, String), Vec<Subscription>>,
    by_tenant_topic: HashMap<(String, String), Vec<Subscription>>,
    by_tenant_topic_shared: HashMap<(String, String, String), Vec<Subscription>>,
    by_key: HashMap<(String, String, String), Subscription>,
}

#[derive(Clone, Default)]
pub struct MemorySubscriptionStore {
    inner: Arc<RwLock<MemorySubscriptionState>>,
}

fn remove_tenant_subscription_index(
    subscriptions_by_tenant: &mut HashMap<String, Vec<Subscription>>,
    subscriptions_by_tenant_shared: &mut HashMap<(String, String), Vec<Subscription>>,
    subscriptions_by_tenant_topic: &mut HashMap<(String, String), Vec<Subscription>>,
    subscriptions_by_tenant_topic_shared: &mut HashMap<(String, String, String), Vec<Subscription>>,
    subscription: &Subscription,
) {
    if let Some(subscriptions) = subscriptions_by_tenant.get_mut(&subscription.tenant_id) {
        subscriptions.retain(|current| {
            !(current.session_id == subscription.session_id
                && current.topic_filter == subscription.topic_filter
                && current.shared_group == subscription.shared_group)
        });
        if subscriptions.is_empty() {
            subscriptions_by_tenant.remove(&subscription.tenant_id);
        }
    }
    let tenant_shared_key = (
        subscription.tenant_id.clone(),
        subscription
            .shared_group
            .as_deref()
            .unwrap_or_default()
            .to_string(),
    );
    if let Some(subscriptions) = subscriptions_by_tenant_shared.get_mut(&tenant_shared_key) {
        subscriptions.retain(|current| {
            !(current.session_id == subscription.session_id
                && current.topic_filter == subscription.topic_filter)
        });
        if subscriptions.is_empty() {
            subscriptions_by_tenant_shared.remove(&tenant_shared_key);
        }
    }
    let topic_key = (
        subscription.tenant_id.clone(),
        subscription.topic_filter.clone(),
    );
    if let Some(subscriptions) = subscriptions_by_tenant_topic.get_mut(&topic_key) {
        subscriptions.retain(|current| {
            !(current.session_id == subscription.session_id
                && current.topic_filter == subscription.topic_filter
                && current.shared_group == subscription.shared_group)
        });
        if subscriptions.is_empty() {
            subscriptions_by_tenant_topic.remove(&topic_key);
        }
    }
    let shared_key = subscription_tenant_topic_shared_identity(
        &subscription.tenant_id,
        &subscription.topic_filter,
        subscription.shared_group.as_deref(),
    );
    if let Some(subscriptions) = subscriptions_by_tenant_topic_shared.get_mut(&shared_key) {
        subscriptions.retain(|current| current.session_id != subscription.session_id);
        if subscriptions.is_empty() {
            subscriptions_by_tenant_topic_shared.remove(&shared_key);
        }
    }
}

#[async_trait]
impl SubscriptionStore for MemorySubscriptionStore {
    async fn save_subscription(&self, subscription: &Subscription) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("subscription store poisoned");
        let existing = guard
            .by_key
            .get(&subscription_identity(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ))
            .cloned();
        let subscriptions = guard
            .by_session
            .entry(subscription.session_id.clone())
            .or_default();
        if let Some(existing) = subscriptions.iter_mut().find(|existing| {
            existing.topic_filter == subscription.topic_filter
                && existing.shared_group == subscription.shared_group
        }) {
            *existing = subscription.clone();
        } else {
            subscriptions.push(subscription.clone());
        }
        if let Some(existing) = existing {
            let state = &mut *guard;
            remove_tenant_subscription_index(
                &mut state.by_tenant,
                &mut state.by_tenant_shared,
                &mut state.by_tenant_topic,
                &mut state.by_tenant_topic_shared,
                &existing,
            );
        }
        guard
            .by_tenant
            .entry(subscription.tenant_id.clone())
            .or_default()
            .push(subscription.clone());
        guard
            .by_tenant_shared
            .entry((
                subscription.tenant_id.clone(),
                subscription
                    .shared_group
                    .as_deref()
                    .unwrap_or_default()
                    .to_string(),
            ))
            .or_default()
            .push(subscription.clone());
        guard
            .by_tenant_topic
            .entry((
                subscription.tenant_id.clone(),
                subscription.topic_filter.clone(),
            ))
            .or_default()
            .push(subscription.clone());
        guard
            .by_tenant_topic_shared
            .entry(subscription_tenant_topic_shared_identity(
                &subscription.tenant_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ))
            .or_default()
            .push(subscription.clone());
        guard.by_key.insert(
            subscription_identity(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ),
            subscription.clone(),
        );
        Ok(())
    }

    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_key
            .get(&subscription_identity(
                session_id,
                topic_filter,
                shared_group,
            ))
            .cloned())
    }

    async fn delete_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let mut guard = self.inner.write().expect("subscription store poisoned");
        let removed_record = guard
            .by_key
            .get(&subscription_identity(
                session_id,
                topic_filter,
                shared_group,
            ))
            .cloned();
        let removed = if let Some(subscriptions) = guard.by_session.get_mut(session_id) {
            let before = subscriptions.len();
            subscriptions.retain(|subscription| {
                !(subscription.topic_filter == topic_filter
                    && subscription.shared_group.as_deref() == shared_group)
            });
            subscriptions.len() != before
        } else {
            false
        };
        if removed {
            guard.by_key.remove(&subscription_identity(
                session_id,
                topic_filter,
                shared_group,
            ));
            if let Some(removed_record) = removed_record {
                let state = &mut *guard;
                remove_tenant_subscription_index(
                    &mut state.by_tenant,
                    &mut state.by_tenant_shared,
                    &mut state.by_tenant_topic,
                    &mut state.by_tenant_topic_shared,
                    &removed_record,
                );
            }
        }
        Ok(removed)
    }

    async fn list_subscriptions(&self, session_id: &str) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_session
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn count_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_session
            .get(session_id)
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_session
            .get(session_id)
            .into_iter()
            .flat_map(|subscriptions| subscriptions.iter())
            .filter(|subscription| subscription.topic_filter == topic_filter)
            .cloned()
            .collect())
    }

    async fn count_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_session
            .get(session_id)
            .map(|subscriptions| {
                subscriptions
                    .iter()
                    .filter(|subscription| subscription.topic_filter == topic_filter)
                    .count()
            })
            .unwrap_or(0))
    }

    async fn purge_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("subscription store poisoned");
        let removed_records = guard.by_session.remove(session_id).unwrap_or_default();
        let removed = removed_records.len();
        guard
            .by_key
            .retain(|(current_session_id, _, _), _| current_session_id != session_id);
        for removed_record in removed_records {
            let state = &mut *guard;
            remove_tenant_subscription_index(
                &mut state.by_tenant,
                &mut state.by_tenant_shared,
                &mut state.by_tenant_topic,
                &mut state.by_tenant_topic_shared,
                &removed_record,
            );
        }
        Ok(removed)
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("subscription store poisoned");
        let removed_records = guard.by_tenant.remove(tenant_id).unwrap_or_default();
        let removed = removed_records.len();
        for removed_record in removed_records {
            if let Some(subscriptions) = guard.by_session.get_mut(&removed_record.session_id) {
                subscriptions.retain(|subscription| {
                    !(subscription.topic_filter == removed_record.topic_filter
                        && subscription.shared_group == removed_record.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.by_session.remove(&removed_record.session_id);
                }
            }
            guard.by_key.remove(&subscription_identity(
                &removed_record.session_id,
                &removed_record.topic_filter,
                removed_record.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription_index(
                &mut state.by_tenant,
                &mut state.by_tenant_shared,
                &mut state.by_tenant_topic,
                &mut state.by_tenant_topic_shared,
                &removed_record,
            );
        }
        Ok(removed)
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("subscription store poisoned");
        let topic_key = (tenant_id.to_string(), topic_filter.to_string());
        let removed_records = guard.by_tenant_topic.remove(&topic_key).unwrap_or_default();
        let removed = removed_records.len();
        for removed_record in removed_records {
            if let Some(subscriptions) = guard.by_session.get_mut(&removed_record.session_id) {
                subscriptions.retain(|subscription| {
                    !(subscription.topic_filter == removed_record.topic_filter
                        && subscription.shared_group == removed_record.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.by_session.remove(&removed_record.session_id);
                }
            }
            if let Some(subscriptions) = guard.by_tenant.get_mut(&removed_record.tenant_id) {
                subscriptions.retain(|subscription| {
                    !(subscription.session_id == removed_record.session_id
                        && subscription.topic_filter == removed_record.topic_filter
                        && subscription.shared_group == removed_record.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.by_tenant.remove(&removed_record.tenant_id);
                }
            }
            let shared_key = subscription_tenant_topic_shared_identity(
                &removed_record.tenant_id,
                &removed_record.topic_filter,
                removed_record.shared_group.as_deref(),
            );
            if let Some(subscriptions) = guard.by_tenant_topic_shared.get_mut(&shared_key) {
                subscriptions
                    .retain(|subscription| subscription.session_id != removed_record.session_id);
                if subscriptions.is_empty() {
                    guard.by_tenant_topic_shared.remove(&shared_key);
                }
            }
            guard.by_key.remove(&subscription_identity(
                &removed_record.session_id,
                &removed_record.topic_filter,
                removed_record.shared_group.as_deref(),
            ));
        }
        Ok(removed)
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant_shared
            .get(&(
                tenant_id.to_string(),
                shared_group.unwrap_or_default().to_string(),
            ))
            .cloned()
            .unwrap_or_default())
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant
            .get(tenant_id)
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant_shared
            .get(&(
                tenant_id.to_string(),
                shared_group.unwrap_or_default().to_string(),
            ))
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant_topic
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant_topic_shared
            .get(&subscription_tenant_topic_shared_identity(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .cloned()
            .unwrap_or_default())
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant_topic
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_tenant_topic_shared
            .get(&subscription_tenant_topic_shared_identity(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("subscription store poisoned");
        let shared_key =
            subscription_tenant_topic_shared_identity(tenant_id, topic_filter, shared_group);
        let removed_records = guard
            .by_tenant_topic_shared
            .remove(&shared_key)
            .unwrap_or_default();
        let removed = removed_records.len();
        for removed_record in removed_records {
            if let Some(subscriptions) = guard.by_session.get_mut(&removed_record.session_id) {
                subscriptions.retain(|subscription| {
                    !(subscription.topic_filter == removed_record.topic_filter
                        && subscription.shared_group == removed_record.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.by_session.remove(&removed_record.session_id);
                }
            }
            if let Some(subscriptions) = guard.by_tenant_topic.get_mut(&(
                removed_record.tenant_id.clone(),
                removed_record.topic_filter.clone(),
            )) {
                subscriptions.retain(|subscription| {
                    !(subscription.session_id == removed_record.session_id
                        && subscription.shared_group == removed_record.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.by_tenant_topic.remove(&(
                        removed_record.tenant_id.clone(),
                        removed_record.topic_filter.clone(),
                    ));
                }
            }
            if let Some(subscriptions) = guard.by_tenant.get_mut(&removed_record.tenant_id) {
                subscriptions.retain(|subscription| {
                    !(subscription.session_id == removed_record.session_id
                        && subscription.topic_filter == removed_record.topic_filter
                        && subscription.shared_group == removed_record.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.by_tenant.remove(&removed_record.tenant_id);
                }
            }
            guard.by_key.remove(&subscription_identity(
                &removed_record.session_id,
                &removed_record.topic_filter,
                removed_record.shared_group.as_deref(),
            ));
        }
        Ok(removed)
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions: Vec<_> = self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_key
            .values()
            .cloned()
            .collect();
        subscriptions.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn count_subscriptions(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("subscription store poisoned")
            .by_key
            .len())
    }
}

#[derive(Default)]
struct MemoryInboxState {
    by_session: HashMap<String, Vec<OfflineMessage>>,
    by_tenant: HashMap<String, Vec<OfflineMessage>>,
    total_messages: usize,
}

#[derive(Clone, Default)]
pub struct MemoryInboxStore {
    inner: Arc<RwLock<MemoryInboxState>>,
}

#[derive(Default)]
struct MemoryInflightState {
    by_session: HashMap<String, HashMap<u16, InflightMessage>>,
    by_tenant: HashMap<String, Vec<InflightMessage>>,
    total_inflight: usize,
}

#[derive(Clone, Default)]
pub struct MemoryInflightStore {
    inner: Arc<RwLock<MemoryInflightState>>,
}

#[async_trait]
impl InboxStore for MemoryInboxStore {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox store poisoned");
        guard
            .by_session
            .entry(message.session_id.clone())
            .or_default()
            .push(message.clone());
        guard
            .by_tenant
            .entry(message.tenant_id.clone())
            .or_default()
            .push(message.clone());
        guard.total_messages += 1;
        Ok(())
    }

    async fn append_messages(&self, messages: &[OfflineMessage]) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox store poisoned");
        for message in messages {
            guard
                .by_session
                .entry(message.session_id.clone())
                .or_default()
                .push(message.clone());
            guard
                .by_tenant
                .entry(message.tenant_id.clone())
                .or_default()
                .push(message.clone());
            guard.total_messages += 1;
        }
        Ok(())
    }

    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inbox store poisoned")
            .by_session
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn count_session_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox store poisoned")
            .by_session
            .get(session_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages: Vec<_> = self
            .inner
            .read()
            .expect("inbox store poisoned")
            .by_tenant
            .values()
            .flat_map(|messages| messages.iter().cloned())
            .collect();
        messages.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic.cmp(&right.topic))
        });
        Ok(messages)
    }

    async fn list_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inbox store poisoned")
            .by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox store poisoned")
            .by_tenant
            .get(tenant_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut guard = self.inner.write().expect("inbox store poisoned");
        let messages = guard.by_session.remove(session_id).unwrap_or_default();
        guard.total_messages -= messages.len();
        for message in &messages {
            if let Some(tenant_messages) = guard.by_tenant.get_mut(&message.tenant_id) {
                tenant_messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.topic == message.topic
                        && current.payload == message.payload
                        && current.qos == message.qos
                        && current.retain == message.retain
                        && current.from_session_id == message.from_session_id)
                });
                if tenant_messages.is_empty() {
                    guard.by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(messages)
    }

    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox store poisoned");
        let removed_messages = guard.by_session.remove(session_id).unwrap_or_default();
        let removed = removed_messages.len();
        guard.total_messages -= removed;
        for message in &removed_messages {
            if let Some(tenant_messages) = guard.by_tenant.get_mut(&message.tenant_id) {
                tenant_messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.topic == message.topic
                        && current.payload == message.payload
                        && current.qos == message.qos
                        && current.retain == message.retain
                        && current.from_session_id == message.from_session_id)
                });
                if tenant_messages.is_empty() {
                    guard.by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(removed)
    }

    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox store poisoned");
        let removed_messages = guard.by_tenant.remove(tenant_id).unwrap_or_default();
        let removed = removed_messages.len();
        guard.total_messages -= removed;
        for message in &removed_messages {
            if let Some(session_messages) = guard.by_session.get_mut(&message.session_id) {
                session_messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.topic == message.topic
                        && current.payload == message.payload
                        && current.qos == message.qos
                        && current.retain == message.retain
                        && current.from_session_id == message.from_session_id)
                });
                if session_messages.is_empty() {
                    guard.by_session.remove(&message.session_id);
                }
            }
        }
        Ok(removed)
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox store poisoned")
            .total_messages)
    }
}

#[async_trait]
impl InflightStore for MemoryInflightStore {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inflight store poisoned");
        let replaced = {
            guard
                .by_session
                .entry(message.session_id.clone())
                .or_default()
                .insert(message.packet_id, message.clone())
        };
        if let Some(ref existing) = replaced {
            if let Some(tenant_messages) = guard.by_tenant.get_mut(&existing.tenant_id) {
                tenant_messages.retain(|current| {
                    !(current.session_id == existing.session_id
                        && current.packet_id == existing.packet_id)
                });
                if tenant_messages.is_empty() {
                    guard.by_tenant.remove(&existing.tenant_id);
                }
            }
        }
        if replaced.is_none() {
            guard.total_inflight += 1;
        }
        guard
            .by_tenant
            .entry(message.tenant_id.clone())
            .or_default()
            .push(message.clone());
        Ok(())
    }

    async fn save_inflight_batch(&self, messages: &[InflightMessage]) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inflight store poisoned");
        for message in messages {
            let replaced = guard
                .by_session
                .entry(message.session_id.clone())
                .or_default()
                .insert(message.packet_id, message.clone());
            if let Some(existing) = replaced {
                if let Some(tenant_messages) = guard.by_tenant.get_mut(&existing.tenant_id) {
                    tenant_messages.retain(|current| {
                        !(current.session_id == existing.session_id
                            && current.packet_id == existing.packet_id)
                    });
                    if tenant_messages.is_empty() {
                        guard.by_tenant.remove(&existing.tenant_id);
                    }
                }
            } else {
                guard.total_inflight += 1;
            }
            guard
                .by_tenant
                .entry(message.tenant_id.clone())
                .or_default()
                .push(message.clone());
        }
        Ok(())
    }

    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inflight store poisoned")
            .by_session
            .get(session_id)
            .map(|messages| messages.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn count_session_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inflight store poisoned")
            .by_session
            .get(session_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages: Vec<_> = self
            .inner
            .read()
            .expect("inflight store poisoned")
            .by_tenant
            .values()
            .flat_map(|messages| messages.iter().cloned())
            .collect();
        messages.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.packet_id.cmp(&right.packet_id))
        });
        Ok(messages)
    }

    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inflight store poisoned")
            .by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inflight store poisoned")
            .by_tenant
            .get(tenant_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inflight store poisoned");
        let removed = {
            let Some(messages) = guard.by_session.get_mut(session_id) else {
                return Ok(());
            };
            let removed = messages.remove(&packet_id);
            let remove_session_entry = messages.is_empty();
            (removed, remove_session_entry)
        };
        if let Some(message) = removed.0 {
            guard.total_inflight -= 1;
            if let Some(tenant_messages) = guard.by_tenant.get_mut(&message.tenant_id) {
                tenant_messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.packet_id == message.packet_id)
                });
                if tenant_messages.is_empty() {
                    guard.by_tenant.remove(&message.tenant_id);
                }
            }
        }
        if removed.1 {
            guard.by_session.remove(session_id);
        }
        Ok(())
    }

    async fn delete_inflight_batch(
        &self,
        session_id: &str,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inflight store poisoned");
        for packet_id in packet_ids {
            let removed = {
                let Some(messages) = guard.by_session.get_mut(session_id) else {
                    return Ok(());
                };
                let removed = messages.remove(packet_id);
                let remove_session_entry = messages.is_empty();
                (removed, remove_session_entry)
            };
            if let Some(message) = removed.0 {
                guard.total_inflight -= 1;
                if let Some(tenant_messages) = guard.by_tenant.get_mut(&message.tenant_id) {
                    tenant_messages.retain(|current| {
                        !(current.session_id == message.session_id
                            && current.packet_id == message.packet_id)
                    });
                    if tenant_messages.is_empty() {
                        guard.by_tenant.remove(&message.tenant_id);
                    }
                }
            }
            if removed.1 {
                guard.by_session.remove(session_id);
            }
        }
        Ok(())
    }

    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inflight store poisoned");
        let removed_messages = guard.by_session.remove(session_id).unwrap_or_default();
        let removed = removed_messages.len();
        guard.total_inflight -= removed;
        for (_, message) in removed_messages {
            if let Some(tenant_messages) = guard.by_tenant.get_mut(&message.tenant_id) {
                tenant_messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.packet_id == message.packet_id)
                });
                if tenant_messages.is_empty() {
                    guard.by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(removed)
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inflight store poisoned");
        let removed_messages = guard.by_tenant.remove(tenant_id).unwrap_or_default();
        let removed = removed_messages.len();
        guard.total_inflight -= removed;
        for message in &removed_messages {
            if let Some(session_messages) = guard.by_session.get_mut(&message.session_id) {
                session_messages.remove(&message.packet_id);
                if session_messages.is_empty() {
                    guard.by_session.remove(&message.session_id);
                }
            }
        }
        Ok(removed)
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inflight store poisoned")
            .total_inflight)
    }
}

#[derive(Default)]
struct MemoryRetainState {
    by_tenant: HashMap<String, HashMap<String, RetainedMessage>>,
    total_retained: usize,
}

#[derive(Clone, Default)]
pub struct MemoryRetainStore {
    inner: Arc<RwLock<MemoryRetainState>>,
}

#[derive(Default)]
struct MemoryRouteState {
    by_tenant: HashMap<String, Vec<RouteRecord>>,
    by_tenant_shared: HashMap<(String, String), Vec<RouteRecord>>,
    by_filter: HashMap<(String, String), Vec<RouteRecord>>,
    by_filter_shared: HashMap<(String, String, String), Vec<RouteRecord>>,
    by_exact: HashMap<String, HashMap<String, Vec<RouteRecord>>>,
    by_wildcard: HashMap<String, Vec<RouteRecord>>,
    by_session: HashMap<String, Vec<RouteRecord>>,
    by_session_topic_shared: HashMap<(String, String, String), RouteRecord>,
    total_routes: usize,
}

#[derive(Clone, Default)]
pub struct MemoryRouteStore {
    inner: Arc<RwLock<MemoryRouteState>>,
}

fn same_route(left: &RouteRecord, right: &RouteRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.session_id == right.session_id
        && left.topic_filter == right.topic_filter
        && left.shared_group == right.shared_group
}

fn insert_memory_route_indexes(state: &mut MemoryRouteState, route: RouteRecord) {
    state
        .by_tenant_shared
        .entry(route_tenant_shared_identity(
            &route.tenant_id,
            route.shared_group.as_deref(),
        ))
        .or_default()
        .push(route.clone());
    state
        .by_filter
        .entry((route.tenant_id.clone(), route.topic_filter.clone()))
        .or_default()
        .push(route.clone());
    state
        .by_filter_shared
        .entry(route_filter_shared_identity(
            &route.tenant_id,
            &route.topic_filter,
            route.shared_group.as_deref(),
        ))
        .or_default()
        .push(route.clone());
    state.by_session_topic_shared.insert(
        route_session_topic_shared_identity(
            &route.session_id,
            &route.topic_filter,
            route.shared_group.as_deref(),
        ),
        route.clone(),
    );
    if route_topic_filter_is_exact(&route.topic_filter) {
        state
            .by_exact
            .entry(route.tenant_id.clone())
            .or_default()
            .entry(route.topic_filter.clone())
            .or_default()
            .push(route);
    } else {
        state
            .by_wildcard
            .entry(route.tenant_id.clone())
            .or_default()
            .push(route);
    }
}

fn remove_memory_route_indexes(state: &mut MemoryRouteState, route: &RouteRecord) {
    let tenant_shared_key =
        route_tenant_shared_identity(&route.tenant_id, route.shared_group.as_deref());
    if let Some(routes) = state.by_tenant_shared.get_mut(&tenant_shared_key) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            state.by_tenant_shared.remove(&tenant_shared_key);
        }
    }
    let topic_key = (route.tenant_id.clone(), route.topic_filter.clone());
    if let Some(routes) = state.by_filter.get_mut(&topic_key) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            state.by_filter.remove(&topic_key);
        }
    }
    let filter_shared_key = route_filter_shared_identity(
        &route.tenant_id,
        &route.topic_filter,
        route.shared_group.as_deref(),
    );
    if let Some(routes) = state.by_filter_shared.get_mut(&filter_shared_key) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            state.by_filter_shared.remove(&filter_shared_key);
        }
    }
    state
        .by_session_topic_shared
        .remove(&route_session_topic_shared_identity(
            &route.session_id,
            &route.topic_filter,
            route.shared_group.as_deref(),
        ));
    if route_topic_filter_is_exact(&route.topic_filter) {
        if let Some(routes_by_topic) = state.by_exact.get_mut(&route.tenant_id) {
            if let Some(routes) = routes_by_topic.get_mut(&route.topic_filter) {
                routes.retain(|existing| !same_route(existing, route));
                if routes.is_empty() {
                    routes_by_topic.remove(&route.topic_filter);
                }
            }
            if routes_by_topic.is_empty() {
                state.by_exact.remove(&route.tenant_id);
            }
        }
    } else if let Some(routes) = state.by_wildcard.get_mut(&route.tenant_id) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            state.by_wildcard.remove(&route.tenant_id);
        }
    }
}

fn rewrite_memory_route_node(routes: &mut [RouteRecord], route: &RouteRecord, node_id: u64) {
    for existing in routes {
        if same_route(existing, route) {
            existing.node_id = node_id;
        }
    }
}

#[async_trait]
impl RetainStore for MemoryRetainStore {
    async fn put_retain(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("retain store poisoned");
        let count_delta = {
            let messages = guard
                .by_tenant
                .entry(message.tenant_id.clone())
                .or_default();
            if message.payload.is_empty() {
                if messages.remove(&message.topic).is_some() {
                    -1
                } else {
                    0
                }
            } else if messages
                .insert(message.topic.clone(), message.clone())
                .is_none()
            {
                1
            } else {
                0
            }
        };
        if count_delta > 0 {
            guard.total_retained += count_delta as usize;
        } else {
            guard.total_retained -= (-count_delta) as usize;
        }
        Ok(())
    }

    async fn load_retain(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self
            .inner
            .read()
            .expect("retain store poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|messages| messages.get(topic))
            .cloned())
    }

    async fn match_retain(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let guard = self.inner.read().expect("retain store poisoned");
        let matched = guard
            .by_tenant
            .get(tenant_id)
            .into_iter()
            .flat_map(|messages| messages.values())
            .filter(|message| greenmqtt_core::topic_matches(topic_filter, &message.topic))
            .cloned()
            .collect();
        Ok(matched)
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        Ok(self
            .inner
            .read()
            .expect("retain store poisoned")
            .by_tenant
            .get(tenant_id)
            .into_iter()
            .flat_map(|messages| messages.values())
            .cloned()
            .collect())
    }

    async fn count_retained(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("retain store poisoned")
            .total_retained)
    }
}

#[async_trait]
impl RouteStore for MemoryRouteStore {
    async fn save_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let previous = {
            let routes = guard.by_tenant.entry(route.tenant_id.clone()).or_default();
            if let Some(existing) = routes.iter_mut().find(|existing| {
                existing.session_id == route.session_id
                    && existing.topic_filter == route.topic_filter
                    && existing.shared_group == route.shared_group
            }) {
                let previous = existing.clone();
                *existing = route.clone();
                Some(previous)
            } else {
                routes.push(route.clone());
                None
            }
        };
        if let Some(previous) = previous.as_ref() {
            remove_memory_route_indexes(&mut guard, previous);
        }
        insert_memory_route_indexes(&mut guard, route.clone());
        if let Some(previous) = previous {
            if let Some(session_routes) = guard.by_session.get_mut(&previous.session_id) {
                if let Some(existing) = session_routes.iter_mut().find(|existing| {
                    existing.tenant_id == previous.tenant_id
                        && existing.topic_filter == previous.topic_filter
                        && existing.shared_group == previous.shared_group
                }) {
                    *existing = route.clone();
                }
            }
        } else {
            guard
                .by_session
                .entry(route.session_id.clone())
                .or_default()
                .push(route.clone());
            guard.total_routes += 1;
        }
        Ok(())
    }

    async fn delete_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let mut removed = 0usize;
        let mut remove_tenant_entry = false;
        if let Some(routes) = guard.by_tenant.get_mut(&route.tenant_id) {
            let before = routes.len();
            routes.retain(|existing| {
                !(existing.session_id == route.session_id
                    && existing.topic_filter == route.topic_filter
                    && existing.shared_group == route.shared_group)
            });
            removed = before - routes.len();
            remove_tenant_entry = routes.is_empty();
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(&route.tenant_id);
        }
        if removed > 0 {
            remove_memory_route_indexes(&mut guard, route);
        }
        let mut remove_session_entry = false;
        if let Some(routes) = guard.by_session.get_mut(&route.session_id) {
            routes.retain(|existing| {
                !(existing.tenant_id == route.tenant_id
                    && existing.topic_filter == route.topic_filter
                    && existing.shared_group == route.shared_group)
            });
            remove_session_entry = routes.is_empty();
        }
        if remove_session_entry {
            guard.by_session.remove(&route.session_id);
        }
        if removed > 0 {
            guard.total_routes -= removed;
        }
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let Some(routes) = guard.by_session.remove(session_id) else {
            return Ok(0);
        };
        for route in &routes {
            let mut remove_tenant_entry = false;
            if let Some(tenant_routes) = guard.by_tenant.get_mut(&route.tenant_id) {
                tenant_routes.retain(|existing| !same_route(existing, route));
                remove_tenant_entry = tenant_routes.is_empty();
            }
            remove_memory_route_indexes(&mut guard, route);
            if remove_tenant_entry {
                guard.by_tenant.remove(&route.tenant_id);
            }
        }
        let removed = routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let Some(routes) = guard.by_tenant.remove(tenant_id) else {
            return Ok(0);
        };
        for route in &routes {
            let mut remove_session_entry = false;
            if let Some(session_routes) = guard.by_session.get_mut(&route.session_id) {
                session_routes.retain(|existing| !same_route(existing, route));
                remove_session_entry = session_routes.is_empty();
            }
            remove_memory_route_indexes(&mut guard, route);
            if remove_session_entry {
                guard.by_session.remove(&route.session_id);
            }
        }
        let removed = routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let Some(removed_routes) = guard
            .by_tenant_shared
            .remove(&route_tenant_shared_identity(tenant_id, shared_group))
        else {
            return Ok(0);
        };
        let mut remove_tenant_entry = false;
        if let Some(tenant_routes) = guard.by_tenant.get_mut(tenant_id) {
            for route in &removed_routes {
                tenant_routes.retain(|existing| !same_route(existing, route));
            }
            remove_tenant_entry = tenant_routes.is_empty();
        }
        for route in &removed_routes {
            remove_memory_route_indexes(&mut guard, route);
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(tenant_id);
        }
        for route in &removed_routes {
            let mut remove_session_entry = false;
            if let Some(session_routes) = guard.by_session.get_mut(&route.session_id) {
                session_routes.retain(|existing| !same_route(existing, route));
                remove_session_entry = session_routes.is_empty();
            }
            if remove_session_entry {
                guard.by_session.remove(&route.session_id);
            }
        }
        let removed = removed_routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn load_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_session_topic_shared
            .get(&route_session_topic_shared_identity(
                session_id,
                topic_filter,
                shared_group,
            ))
            .cloned())
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
        let mut guard = self.inner.write().expect("route store poisoned");
        let Some(removed_routes) = guard
            .by_filter
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .cloned()
        else {
            return Ok(0);
        };
        let mut remove_tenant_entry = false;
        if let Some(tenant_routes) = guard.by_tenant.get_mut(tenant_id) {
            for route in &removed_routes {
                tenant_routes.retain(|existing| !same_route(existing, route));
            }
            remove_tenant_entry = tenant_routes.is_empty();
        }
        for route in &removed_routes {
            remove_memory_route_indexes(&mut guard, route);
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(tenant_id);
        }
        for route in &removed_routes {
            let mut remove_session_entry = false;
            if let Some(session_routes) = guard.by_session.get_mut(&route.session_id) {
                session_routes.retain(|existing| !same_route(existing, route));
                remove_session_entry = session_routes.is_empty();
            }
            if remove_session_entry {
                guard.by_session.remove(&route.session_id);
            }
        }
        let removed = removed_routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let Some(removed_routes) = guard
            .by_filter_shared
            .get(&route_filter_shared_identity(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .cloned()
        else {
            return Ok(0);
        };
        let mut remove_tenant_entry = false;
        if let Some(tenant_routes) = guard.by_tenant.get_mut(tenant_id) {
            for route in &removed_routes {
                tenant_routes.retain(|existing| !same_route(existing, route));
            }
            remove_tenant_entry = tenant_routes.is_empty();
        }
        for route in &removed_routes {
            remove_memory_route_indexes(&mut guard, route);
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(tenant_id);
        }
        for route in &removed_routes {
            let mut remove_session_entry = false;
            if let Some(session_routes) = guard.by_session.get_mut(&route.session_id) {
                session_routes.retain(|existing| !same_route(existing, route));
                remove_session_entry = session_routes.is_empty();
            }
            if remove_session_entry {
                guard.by_session.remove(&route.session_id);
            }
        }
        let removed = removed_routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("route store poisoned");
        let previous = guard
            .by_session
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        if previous.is_empty() {
            return Ok(0);
        }
        if let Some(routes) = guard.by_session.get_mut(session_id) {
            for route in routes {
                route.node_id = node_id;
            }
        }
        for route in &previous {
            if let Some(tenant_routes) = guard.by_tenant.get_mut(&route.tenant_id) {
                rewrite_memory_route_node(tenant_routes, route, node_id);
            }
            if let Some(routes) = guard
                .by_tenant_shared
                .get_mut(&route_tenant_shared_identity(
                    &route.tenant_id,
                    route.shared_group.as_deref(),
                ))
            {
                rewrite_memory_route_node(routes, route, node_id);
            }
            if let Some(routes) = guard
                .by_filter_shared
                .get_mut(&route_filter_shared_identity(
                    &route.tenant_id,
                    &route.topic_filter,
                    route.shared_group.as_deref(),
                ))
            {
                rewrite_memory_route_node(routes, route, node_id);
            }
            if let Some(current) =
                guard
                    .by_session_topic_shared
                    .get_mut(&route_session_topic_shared_identity(
                        &route.session_id,
                        &route.topic_filter,
                        route.shared_group.as_deref(),
                    ))
            {
                current.node_id = node_id;
            }
            if let Some(routes) = guard
                .by_filter
                .get_mut(&(route.tenant_id.clone(), route.topic_filter.clone()))
            {
                rewrite_memory_route_node(routes, route, node_id);
            }
            if route_topic_filter_is_exact(&route.topic_filter) {
                if let Some(routes_by_topic) = guard.by_exact.get_mut(&route.tenant_id) {
                    if let Some(routes) = routes_by_topic.get_mut(&route.topic_filter) {
                        rewrite_memory_route_node(routes, route, node_id);
                    }
                }
            } else if let Some(routes) = guard.by_wildcard.get_mut(&route.tenant_id) {
                rewrite_memory_route_node(routes, route, node_id);
            }
        }
        Ok(previous.len())
    }

    async fn list_routes(&self) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_tenant
            .values()
            .flat_map(|routes| routes.iter().cloned())
            .collect())
    }

    async fn list_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_tenant
            .get(tenant_id)
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_tenant_shared
            .get(&route_tenant_shared_identity(tenant_id, shared_group))
            .cloned()
            .unwrap_or_default())
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_tenant_shared
            .get(&route_tenant_shared_identity(tenant_id, shared_group))
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_filter
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_filter_shared
            .get(&route_filter_shared_identity(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .cloned()
            .unwrap_or_default())
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_filter
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_filter_shared
            .get(&route_filter_shared_identity(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_exact
            .get(tenant_id)
            .and_then(|routes| routes.get(topic_filter))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_tenant_wildcard_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_wildcard
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_session
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_session
            .get(session_id)
            .into_iter()
            .flat_map(|routes| routes.iter())
            .filter(|route| route.topic_filter == topic_filter)
            .cloned()
            .collect())
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_session
            .get(session_id)
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .by_session
            .get(session_id)
            .map(|routes| {
                routes
                    .iter()
                    .filter(|route| route.topic_filter == topic_filter)
                    .count()
            })
            .unwrap_or(0))
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(usize::from(
            self.inner
                .read()
                .expect("route store poisoned")
                .by_session_topic_shared
                .contains_key(&route_session_topic_shared_identity(
                    session_id,
                    topic_filter,
                    shared_group,
                )),
        ))
    }

    async fn count_routes(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("route store poisoned")
            .total_routes)
    }
}
