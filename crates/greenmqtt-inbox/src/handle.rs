use crate::state::{
    offline_message_matches, remove_cached_tenant_inflight, remove_cached_tenant_offline,
    remove_session_topic_subscription, remove_tenant_subscription, subscription_index_key,
    tenant_topic_shared_subscription_key, InboxState,
};
use crate::InboxService;
use async_trait::async_trait;
use greenmqtt_core::{InflightMessage, Lifecycle, OfflineMessage, SessionId, Subscription};
use greenmqtt_storage::{InboxStore, InflightStore, SubscriptionStore};
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct InboxHandle {
    inner: Arc<RwLock<InboxState>>,
}
#[derive(Clone)]
pub struct PersistentInboxHandle {
    subscription_store: Arc<dyn SubscriptionStore>,
    inbox_store: Arc<dyn InboxStore>,
    inflight_store: Arc<dyn InflightStore>,
    inner: Arc<RwLock<InboxState>>,
}

impl PersistentInboxHandle {
    pub fn open(
        subscription_store: Arc<dyn SubscriptionStore>,
        inbox_store: Arc<dyn InboxStore>,
        inflight_store: Arc<dyn InflightStore>,
    ) -> Self {
        Self {
            subscription_store,
            inbox_store,
            inflight_store,
            inner: Arc::new(RwLock::new(InboxState::default())),
        }
    }
}
#[async_trait]
impl InboxService for InboxHandle {
    async fn attach(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.inner
            .write()
            .expect("inbox poisoned")
            .active_sessions
            .insert(session_id.clone(), true);
        Ok(())
    }

    async fn detach(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.inner
            .write()
            .expect("inbox poisoned")
            .active_sessions
            .insert(session_id.clone(), false);
        Ok(())
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let existing = guard
            .subscription_index
            .get(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ))
            .cloned();
        let index_key = subscription_index_key(
            &subscription.session_id,
            &subscription.topic_filter,
            subscription.shared_group.as_deref(),
        );
        let subscriptions = guard
            .subscriptions
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
        if let Some(existing) = existing.as_ref() {
            let state = &mut *guard;
            remove_session_topic_subscription(&mut state.subscriptions_by_session_topic, existing);
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                existing,
            );
        }
        guard
            .subscriptions_by_session_topic
            .entry((
                subscription.session_id.clone(),
                subscription.topic_filter.clone(),
            ))
            .or_default()
            .push(subscription.clone());
        guard
            .subscriptions_by_tenant
            .entry(subscription.tenant_id.clone())
            .or_default()
            .push(subscription.clone());
        guard
            .subscriptions_by_tenant_shared
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
            .subscriptions_by_tenant_topic
            .entry((
                subscription.tenant_id.clone(),
                subscription.topic_filter.clone(),
            ))
            .or_default()
            .push(subscription.clone());
        guard
            .subscriptions_by_tenant_topic_shared
            .entry(tenant_topic_shared_subscription_key(
                &subscription.tenant_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ))
            .or_default()
            .push(subscription.clone());
        guard.subscription_index.insert(index_key, subscription);
        Ok(())
    }

    async fn lookup_subscription(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscription_index
            .get(&subscription_index_key(
                session_id,
                topic_filter,
                shared_group,
            ))
            .cloned())
    }

    async fn unsubscribe(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<bool> {
        self.unsubscribe_shared(session_id, topic_filter, None)
            .await
    }

    async fn purge_session(&self, session_id: &SessionId) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        guard.active_sessions.remove(session_id);
        let removed = guard.subscriptions.remove(session_id).unwrap_or_default();
        guard
            .subscriptions_by_session_topic
            .retain(|(current_session_id, _), _| current_session_id != session_id);
        guard
            .subscription_index
            .retain(|(current_session_id, _, _), _| current_session_id != session_id);
        for subscription in &removed {
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        let removed_offline = guard
            .offline_messages
            .remove(session_id)
            .unwrap_or_default();
        guard.total_offline_messages -= removed_offline.len();
        for message in &removed_offline {
            if let Some(messages) = guard.offline_messages_by_tenant.get_mut(&message.tenant_id) {
                messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.topic == message.topic
                        && current.payload == message.payload
                        && current.qos == message.qos
                        && current.retain == message.retain
                        && current.from_session_id == message.from_session_id)
                });
                if messages.is_empty() {
                    guard.offline_messages_by_tenant.remove(&message.tenant_id);
                }
            }
        }
        let removed_inflight = guard
            .inflight_messages
            .remove(session_id)
            .unwrap_or_default();
        guard.total_inflight_messages -= removed_inflight.len();
        for (_, message) in removed_inflight {
            if let Some(messages) = guard
                .inflight_messages_by_tenant
                .get_mut(&message.tenant_id)
            {
                messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.packet_id == message.packet_id)
                });
                if messages.is_empty() {
                    guard.inflight_messages_by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(())
    }

    async fn purge_session_subscriptions_only(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed = guard.subscriptions.remove(session_id).unwrap_or_default();
        let removed_count = removed.len();
        guard
            .subscriptions_by_session_topic
            .retain(|(current_session_id, _), _| current_session_id != session_id);
        guard
            .subscription_index
            .retain(|(current_session_id, _, _), _| current_session_id != session_id);
        for subscription in &removed {
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        Ok(removed_count)
    }

    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed = guard
            .subscriptions
            .get(session_id)
            .map(|subscriptions| {
                subscriptions
                    .iter()
                    .filter(|subscription| subscription.topic_filter == topic_filter)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let removed_count = removed.len();
        if removed_count == 0 {
            return Ok(0);
        }
        let mut remove_session_entry = false;
        if let Some(subscriptions) = guard.subscriptions.get_mut(session_id) {
            subscriptions.retain(|subscription| subscription.topic_filter != topic_filter);
            remove_session_entry = subscriptions.is_empty();
        }
        if remove_session_entry {
            guard.subscriptions.remove(session_id);
        }
        for subscription in &removed {
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        Ok(removed_count)
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed = guard
            .subscriptions_by_tenant
            .remove(tenant_id)
            .unwrap_or_default();
        let removed_count = removed.len();
        for subscription in &removed {
            if let Some(subscriptions) = guard.subscriptions.get_mut(&subscription.session_id) {
                subscriptions.retain(|current| {
                    !(current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions.remove(&subscription.session_id);
                }
            }
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        Ok(removed_count)
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed = guard
            .subscriptions_by_tenant_topic_shared
            .remove(&tenant_topic_shared_subscription_key(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .unwrap_or_default();
        let removed_count = removed.len();
        for subscription in &removed {
            if let Some(subscriptions) = guard.subscriptions.get_mut(&subscription.session_id) {
                subscriptions.retain(|current| {
                    !(current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions.remove(&subscription.session_id);
                }
            }
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            if let Some(subscriptions) = guard.subscriptions_by_tenant_topic.get_mut(&(
                subscription.tenant_id.clone(),
                subscription.topic_filter.clone(),
            )) {
                subscriptions.retain(|current| {
                    !(current.session_id == subscription.session_id
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions_by_tenant_topic.remove(&(
                        subscription.tenant_id.clone(),
                        subscription.topic_filter.clone(),
                    ));
                }
            }
            if let Some(subscriptions) = guard.subscriptions_by_tenant.get_mut(tenant_id) {
                subscriptions.retain(|current| {
                    !(current.session_id == subscription.session_id
                        && current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions_by_tenant.remove(tenant_id);
                }
            }
        }
        Ok(removed_count)
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed = guard
            .subscriptions_by_tenant_topic
            .remove(&(tenant_id.to_string(), topic_filter.to_string()))
            .unwrap_or_default();
        let removed_count = removed.len();
        for subscription in &removed {
            if let Some(subscriptions) = guard.subscriptions.get_mut(&subscription.session_id) {
                subscriptions.retain(|current| {
                    !(current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions.remove(&subscription.session_id);
                }
            }
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            if let Some(subscriptions) = guard.subscriptions_by_tenant.get_mut(tenant_id) {
                subscriptions.retain(|current| {
                    !(current.session_id == subscription.session_id
                        && current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions_by_tenant.remove(tenant_id);
                }
            }
        }
        Ok(removed_count)
    }

    async fn count_session_subscriptions(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions
            .get(session_id)
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions_by_session_topic
            .get(&(session_id.clone(), topic_filter.to_string()))
            .into_iter()
            .flat_map(|subscriptions| subscriptions.iter())
            .cloned()
            .collect())
    }

    async fn count_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions_by_session_topic
            .get(&(session_id.clone(), topic_filter.to_string()))
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions_by_tenant
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
            .expect("inbox poisoned")
            .subscriptions_by_tenant_shared
            .get(&(
                tenant_id.to_string(),
                shared_group.unwrap_or_default().to_string(),
            ))
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions_by_tenant_topic
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
            .expect("inbox poisoned")
            .subscriptions_by_tenant_topic_shared
            .get(&tenant_topic_shared_subscription_key(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .map(|subscriptions| subscriptions.len())
            .unwrap_or(0))
    }

    async fn purge_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed_offline = guard
            .offline_messages
            .remove(session_id)
            .unwrap_or_default();
        let removed = removed_offline.len();
        guard.total_offline_messages -= removed;
        for message in &removed_offline {
            if let Some(messages) = guard.offline_messages_by_tenant.get_mut(&message.tenant_id) {
                messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.topic == message.topic
                        && current.payload == message.payload
                        && current.qos == message.qos
                        && current.retain == message.retain
                        && current.from_session_id == message.from_session_id)
                });
                if messages.is_empty() {
                    guard.offline_messages_by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(removed)
    }

    async fn purge_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(remove_cached_tenant_offline(
            &mut self.inner.write().expect("inbox poisoned"),
            tenant_id,
        ))
    }

    async fn count_session_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .offline_messages
            .get(session_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn count_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .offline_messages_by_tenant
            .get(tenant_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn purge_inflight_session(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed_inflight = guard
            .inflight_messages
            .remove(session_id)
            .unwrap_or_default();
        let removed = removed_inflight.len();
        guard.total_inflight_messages -= removed;
        for (_, message) in removed_inflight {
            if let Some(messages) = guard
                .inflight_messages_by_tenant
                .get_mut(&message.tenant_id)
            {
                messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.packet_id == message.packet_id)
                });
                if messages.is_empty() {
                    guard.inflight_messages_by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(removed)
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(remove_cached_tenant_inflight(
            &mut self.inner.write().expect("inbox poisoned"),
            tenant_id,
        ))
    }

    async fn count_session_inflight(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .inflight_messages
            .get(session_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .inflight_messages_by_tenant
            .get(tenant_id)
            .map(|messages| messages.len())
            .unwrap_or(0))
    }

    async fn unsubscribe_shared(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed_subscription = guard.subscription_index.remove(&subscription_index_key(
            session_id,
            topic_filter,
            shared_group,
        ));
        if let Some(subscriptions) = guard.subscriptions.get_mut(session_id) {
            let before = subscriptions.len();
            subscriptions.retain(|subscription| {
                !(subscription.topic_filter == topic_filter
                    && subscription.shared_group.as_deref() == shared_group)
            });
            let removed = subscriptions.len() != before;
            if removed {
                if let Some(removed_subscription) = removed_subscription.as_ref() {
                    let state = &mut *guard;
                    remove_session_topic_subscription(
                        &mut state.subscriptions_by_session_topic,
                        removed_subscription,
                    );
                    remove_tenant_subscription(
                        &mut state.subscriptions_by_tenant,
                        &mut state.subscriptions_by_tenant_shared,
                        &mut state.subscriptions_by_tenant_topic,
                        &mut state.subscriptions_by_tenant_topic_shared,
                        removed_subscription,
                    );
                }
            }
            return Ok(removed);
        }
        Ok(false)
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        guard
            .offline_messages
            .entry(message.session_id.clone())
            .or_default()
            .push(message.clone());
        guard
            .offline_messages_by_tenant
            .entry(message.tenant_id.clone())
            .or_default()
            .push(message);
        guard.total_offline_messages += 1;
        Ok(())
    }

    async fn enqueue_batch(&self, messages: Vec<OfflineMessage>) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        for message in messages {
            guard
                .offline_messages
                .entry(message.session_id.clone())
                .or_default()
                .push(message.clone());
            guard
                .offline_messages_by_tenant
                .entry(message.tenant_id.clone())
                .or_default()
                .push(message);
            guard.total_offline_messages += 1;
        }
        Ok(())
    }

    async fn list_subscriptions(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        let guard = self.inner.read().expect("inbox poisoned");
        Ok(guard
            .subscriptions
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions_by_tenant
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
            .expect("inbox poisoned")
            .subscriptions_by_tenant_shared
            .get(&(
                tenant_id.to_string(),
                shared_group.unwrap_or_default().to_string(),
            ))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscriptions_by_tenant_topic
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .into_iter()
            .flat_map(|subscriptions| subscriptions.iter())
            .cloned()
            .collect())
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
            .expect("inbox poisoned")
            .subscriptions_by_tenant_topic_shared
            .get(&tenant_topic_shared_subscription_key(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .into_iter()
            .flat_map(|subscriptions| subscriptions.iter())
            .cloned()
            .collect())
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let guard = self.inner.read().expect("inbox poisoned");
        let mut subscriptions: Vec<_> = guard.subscription_index.values().cloned().collect();
        subscriptions.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn peek(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .offline_messages
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let guard = self.inner.read().expect("inbox poisoned");
        let mut messages: Vec<_> = guard
            .offline_messages_by_tenant
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

    async fn list_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .offline_messages_by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed = guard
            .offline_messages
            .remove(session_id)
            .unwrap_or_default();
        guard.total_offline_messages -= removed.len();
        for message in &removed {
            if let Some(messages) = guard.offline_messages_by_tenant.get_mut(&message.tenant_id) {
                messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.topic == message.topic
                        && current.payload == message.payload
                        && current.qos == message.qos
                        && current.retain == message.retain
                        && current.from_session_id == message.from_session_id)
                });
                if messages.is_empty() {
                    guard.offline_messages_by_tenant.remove(&message.tenant_id);
                }
            }
        }
        Ok(removed)
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        let replaced = guard
            .inflight_messages
            .entry(message.session_id.clone())
            .or_default()
            .insert(message.packet_id, message.clone());
        if let Some(existing) = replaced {
            if let Some(messages) = guard
                .inflight_messages_by_tenant
                .get_mut(&existing.tenant_id)
            {
                messages.retain(|current| {
                    !(current.session_id == existing.session_id
                        && current.packet_id == existing.packet_id)
                });
                if messages.is_empty() {
                    guard
                        .inflight_messages_by_tenant
                        .remove(&existing.tenant_id);
                }
            }
        } else {
            guard.total_inflight_messages += 1;
        }
        guard
            .inflight_messages_by_tenant
            .entry(message.tenant_id.clone())
            .or_default()
            .push(message);
        Ok(())
    }

    async fn stage_inflight_batch(&self, messages: Vec<InflightMessage>) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        for message in messages {
            let replaced = guard
                .inflight_messages
                .entry(message.session_id.clone())
                .or_default()
                .insert(message.packet_id, message.clone());
            if let Some(existing) = replaced {
                if let Some(tenant_messages) = guard
                    .inflight_messages_by_tenant
                    .get_mut(&existing.tenant_id)
                {
                    tenant_messages.retain(|current| {
                        !(current.session_id == existing.session_id
                            && current.packet_id == existing.packet_id)
                    });
                    if tenant_messages.is_empty() {
                        guard
                            .inflight_messages_by_tenant
                            .remove(&existing.tenant_id);
                    }
                }
            } else {
                guard.total_inflight_messages += 1;
            }
            guard
                .inflight_messages_by_tenant
                .entry(message.tenant_id.clone())
                .or_default()
                .push(message);
        }
        Ok(())
    }

    async fn ack_inflight(&self, session_id: &SessionId, packet_id: u16) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        if let Some(messages) = guard.inflight_messages.get_mut(session_id) {
            if let Some(removed) = messages.remove(&packet_id) {
                guard.total_inflight_messages -= 1;
                if let Some(tenant_messages) = guard
                    .inflight_messages_by_tenant
                    .get_mut(&removed.tenant_id)
                {
                    tenant_messages.retain(|current| {
                        !(current.session_id == removed.session_id
                            && current.packet_id == removed.packet_id)
                    });
                    if tenant_messages.is_empty() {
                        guard.inflight_messages_by_tenant.remove(&removed.tenant_id);
                    }
                }
            }
        }
        Ok(())
    }

    async fn ack_inflight_batch(
        &self,
        session_id: &SessionId,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("inbox poisoned");
        for packet_id in packet_ids {
            let removed = {
                let Some(messages) = guard.inflight_messages.get_mut(session_id) else {
                    break;
                };
                let removed = messages.remove(packet_id);
                let remove_session_entry = messages.is_empty();
                (removed, remove_session_entry)
            };
            let missing = removed.0.is_none();
            if let Some(message) = removed.0 {
                guard.total_inflight_messages -= 1;
                if let Some(tenant_messages) = guard
                    .inflight_messages_by_tenant
                    .get_mut(&message.tenant_id)
                {
                    tenant_messages.retain(|current| {
                        !(current.session_id == message.session_id
                            && current.packet_id == message.packet_id)
                    });
                    if tenant_messages.is_empty() {
                        guard.inflight_messages_by_tenant.remove(&message.tenant_id);
                    }
                }
            }
            if removed.1 {
                guard.inflight_messages.remove(session_id);
            }
            if missing && !removed.1 {
                break;
            }
        }
        Ok(())
    }

    async fn fetch_inflight(&self, session_id: &SessionId) -> anyhow::Result<Vec<InflightMessage>> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .inflight_messages
            .get(session_id)
            .map(|messages| messages.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let guard = self.inner.read().expect("inbox poisoned");
        let mut messages: Vec<_> = guard
            .inflight_messages_by_tenant
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
            .expect("inbox poisoned")
            .inflight_messages_by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .subscription_index
            .len())
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .total_offline_messages)
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("inbox poisoned")
            .total_inflight_messages)
    }
}

#[async_trait]
impl InboxService for PersistentInboxHandle {
    async fn attach(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.inner
            .write()
            .expect("inbox poisoned")
            .active_sessions
            .insert(session_id.clone(), true);
        Ok(())
    }

    async fn detach(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.inner
            .write()
            .expect("inbox poisoned")
            .active_sessions
            .insert(session_id.clone(), false);
        Ok(())
    }

    async fn purge_session(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.subscription_store
            .purge_session_subscriptions(session_id)
            .await?;
        self.inbox_store.purge_messages(session_id).await?;
        self.inflight_store.purge_inflight(session_id).await?;

        let mut guard = self.inner.write().expect("inbox poisoned");
        guard.active_sessions.remove(session_id);
        let removed = guard.subscriptions.remove(session_id).unwrap_or_default();
        guard
            .subscriptions_by_session_topic
            .retain(|(current_session_id, _), _| current_session_id != session_id);
        guard
            .subscription_index
            .retain(|(current_session_id, _, _), _| current_session_id != session_id);
        for subscription in &removed {
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        guard.offline_messages.remove(session_id);
        guard.inflight_messages.remove(session_id);
        Ok(())
    }

    async fn purge_session_subscriptions_only(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<usize> {
        let removed = self
            .subscription_store
            .purge_session_subscriptions(session_id)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let cached = guard.subscriptions.remove(session_id).unwrap_or_default();
        guard
            .subscriptions_by_session_topic
            .retain(|(current_session_id, _), _| current_session_id != session_id);
        guard
            .subscription_index
            .retain(|(current_session_id, _, _), _| current_session_id != session_id);
        for subscription in &cached {
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        Ok(removed.max(cached.len()))
    }

    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let removed = self
            .subscription_store
            .purge_session_topic_subscriptions(session_id, topic_filter)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let affected = guard
            .subscriptions
            .get(session_id)
            .map(|subscriptions| {
                subscriptions
                    .iter()
                    .filter(|subscription| subscription.topic_filter == topic_filter)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut remove_session_entry = false;
        if let Some(subscriptions) = guard.subscriptions.get_mut(session_id) {
            subscriptions.retain(|subscription| subscription.topic_filter != topic_filter);
            remove_session_entry = subscriptions.is_empty();
        }
        if remove_session_entry {
            guard.subscriptions.remove(session_id);
        }
        for subscription in &affected {
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                subscription,
            );
        }
        Ok(removed.max(affected.len()))
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let removed = self
            .subscription_store
            .purge_tenant_subscriptions(tenant_id)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let affected = guard
            .subscriptions_by_tenant
            .get(tenant_id)
            .cloned()
            .unwrap_or_default();
        for subscription in affected {
            if let Some(subscriptions) = guard.subscriptions.get_mut(&subscription.session_id) {
                subscriptions.retain(|current| {
                    !(current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions.remove(&subscription.session_id);
                }
            }
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                &subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                &subscription,
            );
        }
        Ok(removed)
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let removed = self
            .subscription_store
            .purge_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let affected = guard
            .subscriptions_by_tenant_topic
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .cloned()
            .unwrap_or_default();
        for subscription in affected {
            if let Some(subscriptions) = guard.subscriptions.get_mut(&subscription.session_id) {
                subscriptions.retain(|current| {
                    !(current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions.remove(&subscription.session_id);
                }
            }
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                &subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                &subscription,
            );
        }
        Ok(removed)
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let removed = self
            .subscription_store
            .purge_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let affected = guard
            .subscriptions_by_tenant_topic_shared
            .get(&tenant_topic_shared_subscription_key(
                tenant_id,
                topic_filter,
                shared_group,
            ))
            .cloned()
            .unwrap_or_default();
        for subscription in affected {
            if let Some(subscriptions) = guard.subscriptions.get_mut(&subscription.session_id) {
                subscriptions.retain(|current| {
                    !(current.topic_filter == subscription.topic_filter
                        && current.shared_group == subscription.shared_group)
                });
                if subscriptions.is_empty() {
                    guard.subscriptions.remove(&subscription.session_id);
                }
            }
            remove_session_topic_subscription(
                &mut guard.subscriptions_by_session_topic,
                &subscription,
            );
            guard.subscription_index.remove(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ));
            let state = &mut *guard;
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                &subscription,
            );
        }
        Ok(removed)
    }

    async fn count_session_subscriptions(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        self.subscription_store
            .count_session_subscriptions(session_id)
            .await
    }

    async fn list_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        {
            let guard = self.inner.read().expect("inbox poisoned");
            if let Some(subscriptions) = guard
                .subscriptions_by_session_topic
                .get(&(session_id.clone(), topic_filter.to_string()))
            {
                return Ok(subscriptions.clone());
            }
        }
        self.subscription_store
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await
    }

    async fn count_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        {
            let guard = self.inner.read().expect("inbox poisoned");
            if let Some(subscriptions) = guard
                .subscriptions_by_session_topic
                .get(&(session_id.clone(), topic_filter.to_string()))
            {
                return Ok(subscriptions.len());
            }
        }
        self.subscription_store
            .count_session_topic_subscriptions(session_id, topic_filter)
            .await
    }

    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.subscription_store
            .count_tenant_subscriptions(tenant_id)
            .await
    }

    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.subscription_store
            .count_tenant_shared_subscriptions(tenant_id, shared_group)
            .await
    }

    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.subscription_store
            .count_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await
    }

    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.subscription_store
            .count_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn purge_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        let removed = self.inbox_store.purge_messages(session_id).await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let cached = guard
            .offline_messages
            .remove(session_id)
            .unwrap_or_default();
        guard.total_offline_messages = guard.total_offline_messages.saturating_sub(cached.len());
        for message in &cached {
            let remove_tenant = if let Some(messages) =
                guard.offline_messages_by_tenant.get_mut(&message.tenant_id)
            {
                messages.retain(|current| !offline_message_matches(current, message));
                messages.is_empty()
            } else {
                false
            };
            if remove_tenant {
                guard.offline_messages_by_tenant.remove(&message.tenant_id);
            }
        }
        Ok(removed)
    }

    async fn purge_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let removed = self.inbox_store.purge_tenant_messages(tenant_id).await?;
        remove_cached_tenant_offline(&mut self.inner.write().expect("inbox poisoned"), tenant_id);
        Ok(removed)
    }

    async fn count_session_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        self.inbox_store.count_session_messages(session_id).await
    }

    async fn count_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.inbox_store.count_tenant_messages(tenant_id).await
    }

    async fn purge_inflight_session(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        let removed = self.inflight_store.purge_inflight(session_id).await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let cached = guard
            .inflight_messages
            .remove(session_id)
            .unwrap_or_default();
        guard.total_inflight_messages = guard.total_inflight_messages.saturating_sub(cached.len());
        for (_, message) in cached {
            let remove_tenant = if let Some(messages) = guard
                .inflight_messages_by_tenant
                .get_mut(&message.tenant_id)
            {
                messages.retain(|current| {
                    !(current.session_id == message.session_id
                        && current.packet_id == message.packet_id)
                });
                messages.is_empty()
            } else {
                false
            };
            if remove_tenant {
                guard.inflight_messages_by_tenant.remove(&message.tenant_id);
            }
        }
        Ok(removed)
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let removed = self.inflight_store.purge_tenant_inflight(tenant_id).await?;
        remove_cached_tenant_inflight(&mut self.inner.write().expect("inbox poisoned"), tenant_id);
        Ok(removed)
    }

    async fn count_session_inflight(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        self.inflight_store.count_session_inflight(session_id).await
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.inflight_store.count_tenant_inflight(tenant_id).await
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        self.subscription_store
            .save_subscription(&subscription)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let existing = guard
            .subscription_index
            .get(&subscription_index_key(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ))
            .cloned();
        let index_key = subscription_index_key(
            &subscription.session_id,
            &subscription.topic_filter,
            subscription.shared_group.as_deref(),
        );
        let subscriptions = guard
            .subscriptions
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
        if let Some(existing) = existing.as_ref() {
            let state = &mut *guard;
            remove_session_topic_subscription(&mut state.subscriptions_by_session_topic, existing);
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                existing,
            );
        }
        guard
            .subscriptions_by_session_topic
            .entry((
                subscription.session_id.clone(),
                subscription.topic_filter.clone(),
            ))
            .or_default()
            .push(subscription.clone());
        guard
            .subscriptions_by_tenant
            .entry(subscription.tenant_id.clone())
            .or_default()
            .push(subscription.clone());
        guard
            .subscriptions_by_tenant_shared
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
            .subscriptions_by_tenant_topic
            .entry((
                subscription.tenant_id.clone(),
                subscription.topic_filter.clone(),
            ))
            .or_default()
            .push(subscription.clone());
        guard
            .subscriptions_by_tenant_topic_shared
            .entry(tenant_topic_shared_subscription_key(
                &subscription.tenant_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            ))
            .or_default()
            .push(subscription.clone());
        guard.subscription_index.insert(index_key, subscription);
        Ok(())
    }

    async fn lookup_subscription(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        {
            let guard = self.inner.read().expect("inbox poisoned");
            if let Some(subscription) = guard.subscription_index.get(&subscription_index_key(
                session_id,
                topic_filter,
                shared_group,
            )) {
                return Ok(Some(subscription.clone()));
            }
        }
        let subscription = self
            .subscription_store
            .load_subscription(session_id, topic_filter, shared_group)
            .await?;
        if let Some(subscription) = &subscription {
            self.inner
                .write()
                .expect("inbox poisoned")
                .subscription_index
                .insert(
                    subscription_index_key(session_id, topic_filter, shared_group),
                    subscription.clone(),
                );
        }
        Ok(subscription)
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
        let removed = self
            .subscription_store
            .delete_subscription(session_id, topic_filter, shared_group)
            .await?;
        let mut guard = self.inner.write().expect("inbox poisoned");
        let removed_subscription = guard.subscription_index.remove(&subscription_index_key(
            session_id,
            topic_filter,
            shared_group,
        ));
        if let Some(subscriptions) = guard.subscriptions.get_mut(session_id) {
            subscriptions.retain(|subscription| {
                !(subscription.topic_filter == topic_filter
                    && subscription.shared_group.as_deref() == shared_group)
            });
        }
        if let Some(removed_subscription) = removed_subscription.as_ref() {
            let state = &mut *guard;
            remove_session_topic_subscription(
                &mut state.subscriptions_by_session_topic,
                removed_subscription,
            );
            remove_tenant_subscription(
                &mut state.subscriptions_by_tenant,
                &mut state.subscriptions_by_tenant_shared,
                &mut state.subscriptions_by_tenant_topic,
                &mut state.subscriptions_by_tenant_topic_shared,
                removed_subscription,
            );
        }
        Ok(removed)
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        self.inbox_store.append_message(&message).await
    }

    async fn enqueue_batch(&self, messages: Vec<OfflineMessage>) -> anyhow::Result<()> {
        self.inbox_store.append_messages(&messages).await
    }

    async fn list_subscriptions(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        {
            let guard = self.inner.read().expect("inbox poisoned");
            if let Some(subscriptions) = guard.subscriptions.get(session_id) {
                return Ok(subscriptions.clone());
            }
        }
        let subscriptions = self
            .subscription_store
            .list_subscriptions(session_id)
            .await?;
        if !subscriptions.is_empty() {
            let mut guard = self.inner.write().expect("inbox poisoned");
            guard
                .subscriptions
                .insert(session_id.clone(), subscriptions.clone());
            for subscription in &subscriptions {
                guard
                    .subscriptions_by_session_topic
                    .entry((
                        subscription.session_id.clone(),
                        subscription.topic_filter.clone(),
                    ))
                    .or_default()
                    .push(subscription.clone());
                guard
                    .subscriptions_by_tenant
                    .entry(subscription.tenant_id.clone())
                    .or_default()
                    .push(subscription.clone());
                guard
                    .subscriptions_by_tenant_shared
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
                    .subscriptions_by_tenant_topic
                    .entry((
                        subscription.tenant_id.clone(),
                        subscription.topic_filter.clone(),
                    ))
                    .or_default()
                    .push(subscription.clone());
                guard
                    .subscriptions_by_tenant_topic_shared
                    .entry(tenant_topic_shared_subscription_key(
                        &subscription.tenant_id,
                        &subscription.topic_filter,
                        subscription.shared_group.as_deref(),
                    ))
                    .or_default()
                    .push(subscription.clone());
                guard.subscription_index.insert(
                    subscription_index_key(
                        &subscription.session_id,
                        &subscription.topic_filter,
                        subscription.shared_group.as_deref(),
                    ),
                    subscription.clone(),
                );
            }
        }
        Ok(subscriptions)
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = self
            .subscription_store
            .list_tenant_subscriptions(tenant_id)
            .await?;
        subscriptions.sort_by(|left, right| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = self
            .subscription_store
            .list_tenant_shared_subscriptions(tenant_id, shared_group)
            .await?;
        subscriptions.sort_by(|left, right| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = self
            .subscription_store
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?;
        subscriptions.sort_by(|left, right| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = self
            .subscription_store
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await?;
        subscriptions.sort_by(|left, right| {
            left.session_id
                .cmp(&right.session_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = self.subscription_store.list_all_subscriptions().await?;
        subscriptions.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn peek(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inbox_store.peek_messages(session_id).await
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inbox_store.list_all_messages().await
    }

    async fn list_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inbox_store.list_tenant_messages(tenant_id).await
    }

    async fn fetch(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inbox_store.load_messages(session_id).await
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        self.inflight_store.save_inflight(&message).await
    }

    async fn stage_inflight_batch(&self, messages: Vec<InflightMessage>) -> anyhow::Result<()> {
        self.inflight_store.save_inflight_batch(&messages).await
    }

    async fn ack_inflight(&self, session_id: &SessionId, packet_id: u16) -> anyhow::Result<()> {
        self.inflight_store
            .delete_inflight(session_id, packet_id)
            .await
    }

    async fn ack_inflight_batch(
        &self,
        session_id: &SessionId,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        self.inflight_store
            .delete_inflight_batch(session_id, packet_ids)
            .await
    }

    async fn fetch_inflight(&self, session_id: &SessionId) -> anyhow::Result<Vec<InflightMessage>> {
        self.inflight_store.load_inflight(session_id).await
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        self.inflight_store.list_all_inflight().await
    }

    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        self.inflight_store.list_tenant_inflight(tenant_id).await
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        self.subscription_store.count_subscriptions().await
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        self.inbox_store.count_messages().await
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        self.inflight_store.count_inflight().await
    }
}

#[async_trait]
impl Lifecycle for InboxHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Lifecycle for PersistentInboxHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
