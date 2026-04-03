use greenmqtt_core::{InflightMessage, OfflineMessage, SessionId, Subscription};
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct InboxState {
    pub(crate) active_sessions: HashMap<String, bool>,
    pub(crate) subscriptions: HashMap<String, Vec<Subscription>>,
    pub(crate) subscriptions_by_session_topic: HashMap<(String, String), Vec<Subscription>>,
    pub(crate) subscriptions_by_tenant: HashMap<String, Vec<Subscription>>,
    pub(crate) subscriptions_by_tenant_shared: HashMap<(String, String), Vec<Subscription>>,
    pub(crate) subscriptions_by_tenant_topic: HashMap<(String, String), Vec<Subscription>>,
    pub(crate) subscriptions_by_tenant_topic_shared:
        HashMap<(String, String, String), Vec<Subscription>>,
    pub(crate) subscription_index: HashMap<(String, String, String), Subscription>,
    pub(crate) offline_messages: HashMap<String, Vec<OfflineMessage>>,
    pub(crate) offline_messages_by_tenant: HashMap<String, Vec<OfflineMessage>>,
    pub(crate) total_offline_messages: usize,
    pub(crate) inflight_messages: HashMap<String, HashMap<u16, InflightMessage>>,
    pub(crate) inflight_messages_by_tenant: HashMap<String, Vec<InflightMessage>>,
    pub(crate) total_inflight_messages: usize,
}
pub(crate) fn subscription_index_key(
    session_id: &SessionId,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        session_id.clone(),
        shared_group.unwrap_or_default().to_string(),
        topic_filter.to_string(),
    )
}

pub(crate) fn tenant_topic_shared_subscription_key(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        tenant_id.to_string(),
        topic_filter.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

pub(crate) fn remove_tenant_subscription(
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
    let shared_key = tenant_topic_shared_subscription_key(
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

pub(crate) fn remove_session_topic_subscription(
    subscriptions_by_session_topic: &mut HashMap<(String, String), Vec<Subscription>>,
    subscription: &Subscription,
) {
    let topic_key = (
        subscription.session_id.clone(),
        subscription.topic_filter.clone(),
    );
    if let Some(subscriptions) = subscriptions_by_session_topic.get_mut(&topic_key) {
        subscriptions.retain(|current| {
            !(current.tenant_id == subscription.tenant_id
                && current.shared_group == subscription.shared_group)
        });
        if subscriptions.is_empty() {
            subscriptions_by_session_topic.remove(&topic_key);
        }
    }
}

pub(crate) fn offline_message_matches(left: &OfflineMessage, right: &OfflineMessage) -> bool {
    left.session_id == right.session_id
        && left.topic == right.topic
        && left.payload == right.payload
        && left.qos == right.qos
        && left.retain == right.retain
        && left.from_session_id == right.from_session_id
}

pub(crate) fn remove_cached_tenant_offline(state: &mut InboxState, tenant_id: &str) -> usize {
    let removed = state
        .offline_messages_by_tenant
        .remove(tenant_id)
        .unwrap_or_default();
    let removed_count = removed.len();
    state.total_offline_messages = state.total_offline_messages.saturating_sub(removed_count);
    for message in &removed {
        let remove_session =
            if let Some(messages) = state.offline_messages.get_mut(&message.session_id) {
                messages.retain(|current| !offline_message_matches(current, message));
                messages.is_empty()
            } else {
                false
            };
        if remove_session {
            state.offline_messages.remove(&message.session_id);
        }
    }
    removed_count
}

pub(crate) fn remove_cached_tenant_inflight(state: &mut InboxState, tenant_id: &str) -> usize {
    let removed = state
        .inflight_messages_by_tenant
        .remove(tenant_id)
        .unwrap_or_default();
    let removed_count = removed.len();
    state.total_inflight_messages = state.total_inflight_messages.saturating_sub(removed_count);
    for message in removed {
        let remove_session =
            if let Some(messages) = state.inflight_messages.get_mut(&message.session_id) {
                messages.remove(&message.packet_id);
                messages.is_empty()
            } else {
                false
            };
        if remove_session {
            state.inflight_messages.remove(&message.session_id);
        }
    }
    removed_count
}
