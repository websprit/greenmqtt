use super::admission::AdmissionController;
use super::metrics::BrokerMetrics;
use super::pressure::PressureLevel;
use super::session_mgr::{load_admin_audit, now_millis, session_expired};
use crate::*;
use async_trait::async_trait;
use greenmqtt_core::{
    ClientIdentity, Delivery, Lifecycle, NodeId, OfflineMessage, PublishOutcome,
    PublishProperties, PublishRequest, RouteRecord, SessionKind, SessionRecord,
    TenantQuota, TenantUsageSnapshot,
};
use greenmqtt_dist::{DistDeliveryReport, DistDeliverySink, DistFanoutRequest, DistFanoutWorker};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Duration;

type BandwidthLimit = Option<(u64, u64)>;
type BandwidthLimits = (BandwidthLimit, BandwidthLimit);

impl<A, C, H> BrokerRuntime<A, C, H>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    #[allow(clippy::too_many_arguments)]
    pub fn with_plugins(
        config: BrokerConfig,
        auth: A,
        acl: C,
        hooks: H,
        sessiondict: Arc<dyn SessionDirectory>,
        dist: Arc<dyn DistRouter>,
        inbox: Arc<dyn InboxService>,
        retain: Arc<dyn RetainService>,
    ) -> Self {
        Self::with_cluster(
            config,
            auth,
            acl,
            hooks,
            Arc::new(LocalPeerForwarder),
            sessiondict,
            dist,
            inbox,
            retain,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_cluster(
        config: BrokerConfig,
        auth: A,
        acl: C,
        hooks: H,
        peer_forwarder: Arc<dyn PeerForwarder>,
        sessiondict: Arc<dyn SessionDirectory>,
        dist: Arc<dyn DistRouter>,
        inbox: Arc<dyn InboxService>,
        retain: Arc<dyn RetainService>,
    ) -> Self {
        let (admin_audit, next_audit_seq) = load_admin_audit(config.audit_log_path.as_deref());
        Self {
            config,
            auth,
            acl,
            hooks,
            sessiondict,
            dist,
            inbox,
            retain,
            peer_forwarder,
            client_balancer: Arc::new(NoopClientBalancer),
            local_sessions: Arc::new(ShardedLocalSessions::default()),
            local_deliveries: Arc::new(ShardedLocalDeliveries::default()),
            local_stats: Arc::new(RwLock::new(LocalStatsState::default())),
            pending_will_generations: Arc::new(ShardedWillGenerations::default()),
            admin_audit: Arc::new(RwLock::new(admin_audit)),
            next_session_seq: AtomicU64::new(1),
            next_local_session_epoch: AtomicU64::new(1),
            next_will_seq: AtomicU64::new(1),
            next_audit_seq: AtomicU64::new(next_audit_seq),
            connection_slots: None,
            admission: AdmissionController::default(),
            publish_rate_limiter: MessageRateLimiter::default(),
            tenant_resources: Arc::new(TenantResourceManager::default()),
            inbound_bandwidth_limit: None,
            outbound_bandwidth_limit: None,
            inbound_bandwidth_overrides: DashMap::new(),
            outbound_bandwidth_overrides: DashMap::new(),
            connect_debounce_window_ms: None,
            recent_connect_attempts: Arc::new(ShardedWillGenerations::default()),
        }
    }

    pub fn set_connection_limit(&mut self, limit: usize) {
        self.connection_slots = Some(Arc::new(Semaphore::new(limit)));
    }

    pub(crate) fn try_acquire_connection_slot(&self) -> Result<Option<OwnedSemaphorePermit>, ()> {
        match &self.connection_slots {
            Some(slots) => slots.clone().try_acquire_owned().map(Some).map_err(|_| ()),
            None => Ok(None),
        }
    }

    pub fn set_max_online_sessions(&mut self, limit: usize) {
        self.admission.set_max_online_sessions(limit);
    }

    pub fn set_connection_rate_limit(&mut self, limit_per_sec: usize) {
        self.admission.set_connection_rate_limit(limit_per_sec);
    }

    pub(crate) fn allow_connection_attempt(&self) -> bool {
        self.admission.allow_connection_attempt()
    }

    #[cfg(test)]
    pub(crate) fn reset_connection_rate_window(&self) {
        self.admission.reset_connection_rate_window();
    }

    pub(crate) fn connect_pressure_exceeded(&self) -> bool {
        self.admission
            .connect_pressure_exceeded(self.local_stats().local_online_sessions)
    }

    pub fn set_connection_slowdown(&mut self, threshold: usize, delay: Duration) {
        self.admission.set_connection_slowdown(threshold, delay);
    }

    #[cfg(test)]
    pub(crate) fn connect_slowdown_delay(&self) -> Option<Duration> {
        self.admission
            .connect_slowdown_delay(self.local_stats().local_online_sessions)
    }

    pub fn set_connection_shaping(&mut self, delay: Duration) {
        self.admission.set_connection_shaping(delay);
    }

    pub(crate) fn connect_effective_delay(&self) -> Option<Duration> {
        self.admission
            .connect_effective_delay(self.local_stats().local_online_sessions)
    }

    pub fn set_connect_debounce_window(&mut self, window: Duration) {
        self.connect_debounce_window_ms = Some(window.as_millis() as u64);
    }

    pub fn set_publish_rate_limit_per_connection(&mut self, rate_per_sec: u64, burst: u64) {
        self.publish_rate_limiter
            .set_per_connection(rate_per_sec, burst);
    }

    pub fn set_publish_rate_limit_per_tenant(&mut self, rate_per_sec: u64, burst: u64) {
        self.publish_rate_limiter
            .set_per_tenant(rate_per_sec, burst);
    }

    pub fn set_tenant_quota(&self, tenant_id: impl Into<String>, quota: TenantQuota) {
        self.tenant_resources.set_quota(tenant_id, quota);
    }

    pub fn set_client_balancer(&mut self, balancer: Arc<dyn ClientBalancer>) {
        self.client_balancer = balancer;
    }

    pub fn set_inbound_bandwidth_limit(&mut self, rate_bytes_per_sec: u64, burst_bytes: u64) {
        self.inbound_bandwidth_limit = Some((rate_bytes_per_sec, burst_bytes));
    }

    pub fn set_outbound_bandwidth_limit(&mut self, rate_bytes_per_sec: u64, burst_bytes: u64) {
        self.outbound_bandwidth_limit = Some((rate_bytes_per_sec, burst_bytes));
    }

    pub fn set_tenant_inbound_bandwidth_limit(
        &self,
        tenant_id: impl Into<String>,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
    ) {
        self.inbound_bandwidth_overrides
            .insert(tenant_id.into(), (rate_bytes_per_sec, burst_bytes));
    }

    pub fn set_tenant_outbound_bandwidth_limit(
        &self,
        tenant_id: impl Into<String>,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
    ) {
        self.outbound_bandwidth_overrides
            .insert(tenant_id.into(), (rate_bytes_per_sec, burst_bytes));
    }

    pub fn tenant_quota(&self, tenant_id: &str) -> Option<TenantQuota> {
        self.tenant_resources.quota(tenant_id)
    }

    pub fn tenant_usage(&self, tenant_id: &str) -> Option<TenantUsageSnapshot> {
        self.tenant_resources.usage(tenant_id)
    }

    pub(crate) fn bandwidth_limits(&self) -> BandwidthLimits {
        (self.inbound_bandwidth_limit, self.outbound_bandwidth_limit)
    }

    pub(crate) fn bandwidth_limits_for_tenant(&self, tenant_id: &str) -> BandwidthLimits {
        let inbound = self
            .inbound_bandwidth_overrides
            .get(tenant_id)
            .map(|entry| *entry.value())
            .or(self.inbound_bandwidth_limit);
        let outbound = self
            .outbound_bandwidth_overrides
            .get(tenant_id)
            .map(|entry| *entry.value())
            .or(self.outbound_bandwidth_limit);
        (inbound, outbound)
    }

    pub(crate) fn connect_debounce_exceeded(&self, identity: &ClientIdentity) -> bool {
        let Some(window_ms) = self.connect_debounce_window_ms else {
            return false;
        };
        let now = now_millis();
        let minimum_allowed = now.saturating_sub(window_ms);
        self.recent_connect_attempts
            .prune_older_than(minimum_allowed);
        let key = Self::will_identity_key(identity);
        let previous = self.recent_connect_attempts.get_copied(&key);
        self.recent_connect_attempts.insert(key, now);
        previous.is_some_and(|last| now.saturating_sub(last) < window_ms)
    }

    #[cfg(test)]
    pub(crate) fn force_memory_pressure_level(&self, level: super::pressure::PressureLevel) {
        self.admission.force_pressure_level(level);
    }

    pub(crate) fn allow_publish_rate_for_session(&self, session_id: &str) -> bool {
        self.local_sessions
            .get_cloned(session_id)
            .map(|state| {
                self.publish_rate_limiter
                    .allow(session_id, &state.record.identity.tenant_id)
            })
            .unwrap_or(true)
    }

    pub fn memory_pressure_level(&self) -> u8 {
        self.admission.current_pressure_level() as u8
    }

    pub(crate) fn current_pressure_level(&self) -> PressureLevel {
        self.admission.current_pressure_level()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &self,
        session_id: &str,
        topic_filter: &str,
        qos: u8,
        subscription_identifier: Option<u32>,
        no_local: bool,
        retain_as_published: bool,
        retain_handling: u8,
        shared_group: Option<String>,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let state = self
            .local_sessions
            .get_cloned(session_id)
            .ok_or_else(|| anyhow::anyhow!("session not found"))?;
        let subscription = Subscription {
            session_id: state.record.session_id.clone(),
            tenant_id: state.record.identity.tenant_id.clone(),
            topic_filter: topic_filter.to_string(),
            qos,
            subscription_identifier,
            no_local,
            retain_as_published,
            retain_handling,
            shared_group: shared_group.clone(),
            kind: state.record.kind.clone(),
        };

        if !self
            .acl
            .can_subscribe(&state.record.identity, &subscription)
            .await?
        {
            anyhow::bail!("subscribe denied");
        }

        let existing_subscription = self
            .inbox
            .lookup_subscription(
                &state.record.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?
            .is_some();

        self.inbox.subscribe(subscription.clone()).await?;
        BrokerMetrics::record_subscribe(&state.record.identity.tenant_id);
        self.hooks
            .on_subscribe(&state.record.identity, &subscription)
            .await?;
        self.dist
            .add_route(RouteRecord {
                tenant_id: subscription.tenant_id.clone(),
                topic_filter: subscription.topic_filter.clone(),
                session_id: subscription.session_id.clone(),
                node_id: state.record.node_id,
                subscription_identifier: subscription.subscription_identifier,
                no_local: subscription.no_local,
                retain_as_published: subscription.retain_as_published,
                shared_group,
                kind: state.record.kind.clone(),
            })
            .await?;

        let should_replay_retained = match retain_handling {
            0 => true,
            1 => !existing_subscription,
            2 => false,
            _ => anyhow::bail!("invalid retain handling"),
        };
        if !should_replay_retained {
            return Ok(Vec::new());
        }

        if topic_filter_is_exact(topic_filter) {
            return Ok(self
                .retain
                .lookup_topic(&state.record.identity.tenant_id, topic_filter)
                .await?
                .into_iter()
                .collect());
        }

        self.retain
            .match_topic(&state.record.identity.tenant_id, topic_filter)
            .await
    }

    pub async fn unsubscribe(&self, session_id: &str, topic_filter: &str) -> anyhow::Result<bool> {
        self.unsubscribe_with_group(session_id, topic_filter, None)
            .await
    }

    pub async fn unsubscribe_with_group(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<String>,
    ) -> anyhow::Result<bool> {
        let state = self
            .local_sessions
            .get_cloned(session_id)
            .ok_or_else(|| anyhow::anyhow!("session not found"))?;
        let existing_subscription = self
            .inbox
            .lookup_subscription(
                &state.record.session_id,
                topic_filter,
                shared_group.as_deref(),
            )
            .await?;

        let removed = self
            .inbox
            .unsubscribe_shared(
                &state.record.session_id,
                topic_filter,
                shared_group.as_deref(),
            )
            .await?;
        if removed {
            if let Some(existing_subscription) = existing_subscription.as_ref() {
                BrokerMetrics::record_unsubscribe(&state.record.identity.tenant_id);
                self.hooks
                    .on_unsubscribe(&state.record.identity, existing_subscription)
                    .await?;
            }
            self.dist
                .remove_route(&RouteRecord {
                    tenant_id: state.record.identity.tenant_id.clone(),
                    topic_filter: topic_filter.to_string(),
                    session_id: state.record.session_id.clone(),
                    node_id: state.record.node_id,
                    subscription_identifier: None,
                    no_local: false,
                    retain_as_published: false,
                    shared_group,
                    kind: state.record.kind,
                })
                .await?;
        }
        Ok(removed)
    }

    pub async fn publish(
        &self,
        session_id: &str,
        request: PublishRequest,
    ) -> anyhow::Result<PublishOutcome> {
        let publisher = self
            .local_sessions
            .get_cloned(session_id)
            .ok_or_else(|| anyhow::anyhow!("session not found"))?;
        self.publish_as_identity(&publisher.record.identity, session_id, request)
            .await
    }

    pub(crate) async fn publish_as_identity(
        &self,
        identity: &ClientIdentity,
        from_session_id: &str,
        request: PublishRequest,
    ) -> anyhow::Result<PublishOutcome> {
        let publisher_identity = identity.clone();

        let request = self
            .hooks
            .rewrite_publish(&publisher_identity, &request)
            .await?;
        self.tenant_resources
            .check_publish(&publisher_identity.tenant_id)?;
        let mut properties = request.properties.clone();
        normalize_publish_properties(&mut properties);
        BrokerMetrics::record_publish_ingress(
            &publisher_identity.tenant_id,
            request.qos,
            request.payload.len(),
        );

        if !self
            .acl
            .can_publish(&publisher_identity, &request.topic)
            .await?
        {
            anyhow::bail!("publish denied");
        }

        if request.retain {
            let retained = RetainedMessage {
                tenant_id: publisher_identity.tenant_id.clone(),
                topic: request.topic.clone(),
                payload: request.payload.clone(),
                qos: request.qos,
            };
            self.retain.retain(retained.clone()).await?;
            self.hooks.on_retain_write(&retained).await?;
        }

        let outcome = DistFanoutWorker::new(self.dist.clone())
            .fanout(
                self,
                &publisher_identity.tenant_id,
                &DistFanoutRequest {
                    from_session_id: from_session_id.to_string(),
                    request: request.clone(),
                },
            )
            .await?;
        self.hooks
            .on_publish(&publisher_identity, &request, &outcome)
            .await?;
        Ok(outcome)
    }

    async fn deliver_matched_routes(
        &self,
        routes: &[RouteRecord],
        fanout: &DistFanoutRequest,
    ) -> anyhow::Result<DistDeliveryReport> {
        #[derive(Clone)]
        struct RemoteForwardTarget {
            kind: SessionKind,
            delivery: Delivery,
        }

        let mut session_records = HashMap::new();
        for session_id in routes
            .iter()
            .map(|route| route.session_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
        {
            session_records.insert(
                session_id.clone(),
                self.sessiondict.lookup_session(&session_id).await?,
            );
        }
        let mut online_deliveries = 0usize;
        let mut offline_messages = Vec::new();
        let mut remote_batches: HashMap<NodeId, Vec<RemoteForwardTarget>> = HashMap::new();
        let mut purged_sessions = std::collections::BTreeSet::new();
        let mut removed_remote_sessions = std::collections::BTreeSet::new();

        for route in routes {
            if route.no_local && route.session_id == fanout.from_session_id {
                continue;
            }
            if let Some(record) = session_records
                .get(&route.session_id)
                .and_then(|record| record.as_ref())
            {
                if session_expired(record) {
                    if purged_sessions.insert(route.session_id.clone()) {
                        self.purge_session_state(&route.session_id).await?;
                    }
                    continue;
                }
            } else if route.node_id != self.config.node_id {
                if removed_remote_sessions.insert(route.session_id.clone()) {
                    self.dist.remove_session_routes(&route.session_id).await?;
                }
                continue;
            }
            let mut delivery_properties = fanout.request.properties.clone();
            if let Some(subscription_identifier) = route.subscription_identifier {
                delivery_properties
                    .subscription_identifiers
                    .push(subscription_identifier);
            }
            let delivery = Delivery {
                tenant_id: route.tenant_id.clone(),
                session_id: route.session_id.clone(),
                topic: fanout.request.topic.clone(),
                payload: fanout.request.payload.clone(),
                qos: fanout.request.qos,
                retain: fanout.request.retain && route.retain_as_published,
                from_session_id: fanout.from_session_id.clone(),
                properties: delivery_properties.clone(),
            };

            let delivered_locally = if self.local_sessions.contains_key(&route.session_id) {
                self.local_deliveries
                    .push(&route.session_id, delivery.clone());
                true
            } else {
                false
            };

            if delivered_locally {
                self.note_pending_deliveries_delta(1);
                online_deliveries += 1;
                continue;
            }

            if route.node_id != self.config.node_id {
                remote_batches
                    .entry(route.node_id)
                    .or_default()
                    .push(RemoteForwardTarget {
                        kind: route.kind.clone(),
                        delivery,
                    });
                continue;
            }

            if matches!(route.kind, SessionKind::Persistent) {
                offline_messages.push(OfflineMessage {
                    tenant_id: route.tenant_id.clone(),
                    session_id: route.session_id.clone(),
                    topic: fanout.request.topic.clone(),
                    payload: fanout.request.payload.clone(),
                    qos: fanout.request.qos,
                    retain: fanout.request.retain && route.retain_as_published,
                    from_session_id: fanout.from_session_id.clone(),
                    properties: delivery_properties,
                });
            }
        }

        for (node_id, targets) in remote_batches {
            let deliveries: Vec<_> = targets
                .iter()
                .map(|target| target.delivery.clone())
                .collect();
            let undelivered = match self
                .peer_forwarder
                .forward_deliveries(node_id, deliveries)
                .await
            {
                Ok(undelivered) => undelivered,
                Err(error) => {
                    eprintln!(
                        "greenmqtt peer batch forward to node {} failed, falling back: {error:#}",
                        node_id
                    );
                    targets
                        .iter()
                        .map(|target| target.delivery.clone())
                        .collect()
                }
            };

            online_deliveries += targets.len().saturating_sub(undelivered.len());
            let mut remaining_targets = targets;

            for delivery in undelivered {
                if let Some(position) = remaining_targets
                    .iter()
                    .position(|target| target.delivery == delivery)
                {
                    let target = remaining_targets.remove(position);
                    if matches!(target.kind, SessionKind::Persistent) {
                        offline_messages.push(OfflineMessage {
                            tenant_id: delivery.tenant_id.clone(),
                            session_id: delivery.session_id.clone(),
                            topic: delivery.topic.clone(),
                            payload: delivery.payload.clone(),
                            qos: delivery.qos,
                            retain: delivery.retain,
                            from_session_id: delivery.from_session_id.clone(),
                            properties: delivery.properties.clone(),
                        });
                    }
                }
            }
        }

        if !offline_messages.is_empty() {
            self.inbox.enqueue_batch(offline_messages.clone()).await?;
            for offline in &offline_messages {
                self.hooks.on_offline_enqueue(offline).await?;
            }
        }

        Ok(DistDeliveryReport {
            online_deliveries,
            offline_enqueues: offline_messages.len(),
        })
    }

    pub async fn accept_forwarded_delivery(&self, delivery: Delivery) -> anyhow::Result<bool> {
        let session_id = delivery.session_id.clone();
        if self.local_sessions.contains_key(&session_id) {
            self.local_deliveries.push(&session_id, delivery);
            self.note_pending_deliveries_delta(1);
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn drain_deliveries(&self, session_id: &str) -> anyhow::Result<Vec<Delivery>> {
        if !self.local_sessions.contains_key(session_id) {
            anyhow::bail!("session not found");
        }
        let drained = self.local_deliveries.drain(session_id);
        let drained_len = drained.len();
        self.note_pending_deliveries_delta(-(drained_len as isize));
        Ok(drained)
    }

    pub async fn requeue_local_deliveries(
        &self,
        session_id: &str,
        deliveries: Vec<Delivery>,
    ) -> anyhow::Result<()> {
        if !self.local_sessions.contains_key(session_id) {
            anyhow::bail!("session not found");
        }
        let (previous_len, new_len) = self.local_deliveries.prepend_batch(session_id, deliveries);
        self.note_pending_deliveries_delta(new_len as isize - previous_len as isize);
        Ok(())
    }

    pub async fn session_record(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .local_sessions
            .get_cloned(session_id)
            .map(|state| state.record))
    }

    pub async fn list_local_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        let mut sessions: Vec<_> = self
            .local_sessions
            .values_cloned()
            .into_iter()
            .map(|state| SessionSummary {
                session_id: state.record.session_id.clone(),
                node_id: state.record.node_id,
                kind: state.record.kind.clone(),
                tenant_id: state.record.identity.tenant_id.clone(),
                user_id: state.record.identity.user_id.clone(),
                client_id: state.record.identity.client_id.clone(),
                pending_deliveries: self.local_deliveries.pending_len(&state.record.session_id),
            })
            .collect();
        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(sessions)
    }
}

#[async_trait]
impl<A, C, H> Lifecycle for BrokerRuntime<A, C, H>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    async fn start(&self) -> anyhow::Result<()> {
        self.admission.start().await;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        self.admission.stop().await;
        Ok(())
    }
}

#[async_trait]
impl<A, C, H> DeliverySink for BrokerRuntime<A, C, H>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    async fn push_delivery(&self, delivery: Delivery) -> anyhow::Result<bool> {
        self.accept_forwarded_delivery(delivery).await
    }
}

#[async_trait]
impl<A, C, H> DistDeliverySink for BrokerRuntime<A, C, H>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    async fn deliver(
        &self,
        _tenant_id: &str,
        fanout: &DistFanoutRequest,
        routes: &[RouteRecord],
    ) -> anyhow::Result<DistDeliveryReport> {
        self.deliver_matched_routes(routes, fanout).await
    }
}

fn normalize_publish_properties(properties: &mut PublishProperties) {
    if properties.message_expiry_interval_secs.is_some() && properties.stored_at_ms.is_none() {
        properties.stored_at_ms = Some(now_millis());
    }
}

pub(crate) fn publish_properties_expired(properties: &PublishProperties) -> bool {
    let Some(expiry_secs) = properties.message_expiry_interval_secs else {
        return false;
    };
    let Some(stored_at_ms) = properties.stored_at_ms else {
        return false;
    };
    now_millis().saturating_sub(stored_at_ms) >= u64::from(expiry_secs) * 1000
}
