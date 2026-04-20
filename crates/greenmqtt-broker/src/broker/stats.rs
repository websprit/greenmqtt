use crate::*;
use greenmqtt_core::{Delivery, PublishProperties, SessionRecord};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};

impl<A, C, H> BrokerRuntime<A, C, H>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    pub(crate) fn note_local_session_added(
        &self,
        state: &LocalSessionState,
        pending_deliveries: usize,
    ) {
        let mut stats = self.local_stats.write().expect("broker poisoned");
        stats.online_sessions += 1;
        match state.record.kind {
            SessionKind::Persistent => stats.persistent_sessions += 1,
            SessionKind::Transient => stats.transient_sessions += 1,
        }
        stats.pending_deliveries += pending_deliveries;
    }

    pub(crate) fn note_local_session_removed(
        &self,
        state: &LocalSessionState,
        pending_deliveries: usize,
    ) {
        let mut stats = self.local_stats.write().expect("broker poisoned");
        stats.online_sessions -= 1;
        match state.record.kind {
            SessionKind::Persistent => stats.persistent_sessions -= 1,
            SessionKind::Transient => stats.transient_sessions -= 1,
        }
        stats.pending_deliveries -= pending_deliveries;
    }

    pub(crate) fn note_pending_deliveries_delta(&self, delta: isize) {
        let mut stats = self.local_stats.write().expect("broker poisoned");
        if delta >= 0 {
            stats.pending_deliveries += delta as usize;
        } else {
            stats.pending_deliveries -= (-delta) as usize;
        }
    }

    pub fn pending_delayed_will_count(&self) -> usize {
        self.pending_will_generations.total_entries()
    }

    pub fn local_hot_state_breakdown(&self) -> LocalHotStateBreakdown {
        let stats = self.local_stats();
        LocalHotStateBreakdown {
            live_connections: stats.local_online_sessions,
            transient_send_queue_entries: stats.local_pending_deliveries,
            short_lived_tracking_entries: self.pending_delayed_will_count()
                + self.recent_connect_attempts.total_entries(),
        }
    }

    pub fn local_hot_state_entries(&self) -> usize {
        let breakdown = self.local_hot_state_breakdown();
        breakdown.live_connections
            + breakdown.transient_send_queue_entries
            + breakdown.short_lived_tracking_entries
    }

    pub fn approximate_local_hot_state_bytes(&self) -> usize {
        self.local_sessions
            .values_cloned()
            .iter()
            .map(|state| approx_session_record_bytes(&state.record) + std::mem::size_of::<u64>())
            .sum::<usize>()
            + self
                .local_deliveries
                .values_cloned()
                .iter()
                .map(approx_delivery_bytes)
                .sum::<usize>()
            + self.pending_will_generations.total_key_bytes()
            + self.recent_connect_attempts.total_key_bytes()
    }

    pub(crate) fn local_stats(&self) -> BrokerStats {
        let guard = self.local_stats.read().expect("broker poisoned");
        BrokerStats {
            peer_nodes: 0,
            local_online_sessions: guard.online_sessions,
            local_persistent_sessions: guard.persistent_sessions,
            local_transient_sessions: guard.transient_sessions,
            local_pending_deliveries: guard.pending_deliveries,
            global_session_records: 0,
            route_records: 0,
            subscription_records: 0,
            offline_messages: 0,
            inflight_messages: 0,
            retained_messages: 0,
        }
    }

    pub async fn stats(&self) -> anyhow::Result<BrokerStats> {
        let mut stats = self.local_stats();
        let (
            global_session_records,
            route_records,
            subscription_records,
            offline_messages,
            inflight_messages,
            retained_messages,
        ) = tokio::try_join!(
            self.sessiondict.session_count(),
            self.dist.route_count(),
            self.inbox.subscription_count(),
            self.inbox.offline_count(),
            self.inbox.inflight_count(),
            self.retain.retained_count(),
        )?;
        stats.global_session_records = global_session_records;
        stats.route_records = route_records;
        stats.subscription_records = subscription_records;
        stats.offline_messages = offline_messages;
        stats.inflight_messages = inflight_messages;
        stats.retained_messages = retained_messages;
        Ok(stats)
    }
}

fn approx_string_bytes(value: &str) -> usize {
    value.len()
}

fn approx_publish_properties_bytes(properties: &PublishProperties) -> usize {
    properties.content_type.as_deref().map_or(0, str::len)
        + properties.response_topic.as_deref().map_or(0, str::len)
        + properties.correlation_data.as_ref().map_or(0, Vec::len)
        + properties.subscription_identifiers.len() * std::mem::size_of::<u32>()
        + properties
            .user_properties
            .iter()
            .map(|property| property.key.len() + property.value.len())
            .sum::<usize>()
}

fn approx_session_record_bytes(record: &SessionRecord) -> usize {
    approx_string_bytes(&record.session_id)
        + approx_string_bytes(&record.identity.tenant_id)
        + approx_string_bytes(&record.identity.user_id)
        + approx_string_bytes(&record.identity.client_id)
}

fn approx_delivery_bytes(delivery: &Delivery) -> usize {
    approx_string_bytes(&delivery.tenant_id)
        + approx_string_bytes(&delivery.session_id)
        + approx_string_bytes(&delivery.topic)
        + approx_string_bytes(&delivery.from_session_id)
        + delivery.payload.len()
        + approx_publish_properties_bytes(&delivery.properties)
}
