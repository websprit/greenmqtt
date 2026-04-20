use super::metrics::BrokerMetrics;
use super::runtime::publish_properties_expired;
use crate::*;
use greenmqtt_core::{
    ClientIdentity, ConnectReply, ConnectRequest, InflightMessage, NodeId, OfflineMessage,
    PublishRequest, SessionId, SessionKind, SessionRecord,
};
use greenmqtt_inbox::{inbox_send_lwt, DelayedLwtPublish, DelayedLwtSink, InboxLwtResult};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use greenmqtt_sessiondict::{
    SessionKillListener, SessionKillNotification, SessionOwnerResolver, SessionRegistrationHandle,
    SessionRegistrationManager,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

#[derive(Clone)]
struct BrokerSessionOwnerResolver {
    local_node_id: NodeId,
    local_server_reference: Option<String>,
    admin_audit: Arc<RwLock<VecDeque<AdminAuditEntry>>>,
}

impl SessionOwnerResolver for BrokerSessionOwnerResolver {
    fn server_reference(&self, node_id: u64) -> Option<String> {
        if node_id == self.local_node_id {
            return self.local_server_reference.clone();
        }
        desired_peer_registry(self.admin_audit.read().expect("broker poisoned").iter())
            .get(&node_id)
            .cloned()
    }
}

struct BrokerSessionKillListener {
    session_id: String,
    controls: Arc<ShardedPendingSessionControls>,
}

struct BrokerDelayedLwtSink<A, C, H> {
    broker: Arc<BrokerRuntime<A, C, H>>,
    identity: ClientIdentity,
    session_id: String,
}

#[async_trait]
impl<A, C, H> DelayedLwtSink for BrokerDelayedLwtSink<A, C, H>
where
    A: AuthProvider + Send + Sync + 'static,
    C: AclProvider + Send + Sync + 'static,
    H: EventHook + Send + Sync + 'static,
{
    async fn send_lwt(&self, publish: &DelayedLwtPublish) -> anyhow::Result<()> {
        let _ = self
            .broker
            .publish_as_identity(&self.identity, &self.session_id, publish.publish.clone())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SessionKillListener for BrokerSessionKillListener {
    async fn on_kill(&self, notification: SessionKillNotification) -> anyhow::Result<()> {
        let (reason_code, reason) = if notification.server_reference.is_some() {
            (0x9C, "use another server".to_string())
        } else if notification.current_owner.is_some() {
            (0x8E, "session taken over".to_string())
        } else {
            (0x8E, "session killed".to_string())
        };
        self.controls.insert(
            self.session_id.clone(),
            PendingSessionControl {
                reason_code,
                reason,
                server_reference: notification.server_reference,
            },
        );
        Ok(())
    }
}

struct SessionConnectPlan {
    session_present: bool,
    session_id: SessionId,
}

impl<A, C, H> BrokerRuntime<A, C, H>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    fn next_session_id(&self, identity: &ClientIdentity) -> SessionId {
        let seq = self.next_session_seq.fetch_add(1, Ordering::SeqCst);
        format!(
            "{}:{}:{}:{}",
            identity.tenant_id, identity.user_id, identity.client_id, seq
        )
    }

    pub(crate) fn next_assigned_client_id(&self) -> String {
        let seq = self.next_session_seq.fetch_add(1, Ordering::SeqCst);
        format!("greenmqtt-{:x}-{:x}", self.config.node_id, seq)
    }

    pub(crate) fn will_identity_key(identity: &ClientIdentity) -> String {
        format!(
            "{}:{}:{}",
            identity.tenant_id, identity.user_id, identity.client_id
        )
    }

    async fn plan_session_connect(
        &self,
        request: &ConnectRequest,
    ) -> anyhow::Result<SessionConnectPlan> {
        let mut existing = self.sessiondict.lookup_identity(&request.identity).await?;
        if let Some(record) = existing.as_ref() {
            if request.clean_start || session_expired(record) {
                self.purge_session_state(&record.session_id).await?;
                existing = None;
            }
        }
        let session_present = matches!(request.kind, SessionKind::Persistent)
            && !request.clean_start
            && existing.is_some();
        let session_id = if matches!(request.kind, SessionKind::Persistent) && !request.clean_start
        {
            existing
                .as_ref()
                .map(|record| record.session_id.clone())
                .unwrap_or_else(|| self.next_session_id(&request.identity))
        } else {
            self.next_session_id(&request.identity)
        };
        Ok(SessionConnectPlan {
            session_present,
            session_id,
        })
    }

    fn session_registration_manager(&self) -> SessionRegistrationManager {
        SessionRegistrationManager::new(self.sessiondict.clone())
            .with_poll_interval(self.session_registration_poll_interval)
            .with_owner_resolver(Arc::new(BrokerSessionOwnerResolver {
                local_node_id: self.config.node_id,
                local_server_reference: self.config.server_reference.clone(),
                admin_audit: self.admin_audit.clone(),
            }))
    }

    async fn register_live_session(
        &self,
        session: SessionRecord,
    ) -> anyhow::Result<(Option<SessionRecord>, Arc<SessionRegistrationHandle>)> {
        let listener = Arc::new(BrokerSessionKillListener {
            session_id: session.session_id.clone(),
            controls: self.pending_session_controls.clone(),
        });
        let (replaced, registration) = self
            .session_registration_manager()
            .register_with_previous(session, listener)
            .await?;
        Ok((replaced, Arc::new(registration)))
    }

    async fn persist_disconnected_session(
        &self,
        state: &LocalSessionState,
        expiry_override_secs: Option<u32>,
    ) -> anyhow::Result<SessionRecord> {
        let mut persisted = state.record.clone();
        persisted.session_expiry_interval_secs = updated_session_expiry_interval(
            persisted.session_expiry_interval_secs,
            expiry_override_secs,
        );
        persisted.expires_at_ms =
            session_expiration_deadline_ms(persisted.session_expiry_interval_secs);
        self.sessiondict.register(persisted.clone()).await?;
        Ok(persisted)
    }

    pub(crate) fn cancel_delayed_will_for_identity(&self, identity: &ClientIdentity) {
        self.pending_will_generations
            .remove(&Self::will_identity_key(identity));
    }

    pub(crate) fn take_pending_session_control(
        &self,
        session_id: &str,
    ) -> Option<PendingSessionControl> {
        self.pending_session_controls.remove(session_id)
    }

    async fn cancel_delayed_will_for_session(&self, session_id: &str) -> anyhow::Result<()> {
        self.inbox.clear_delayed_lwt(&session_id.to_string()).await?;
        let identity = if let Some(state) = self.local_sessions.get_cloned(session_id) {
            Some(state.record.identity)
        } else {
            self.sessiondict
                .lookup_session(session_id)
                .await?
                .map(|record| record.identity)
        };
        if let Some(identity) = identity {
            self.cancel_delayed_will_for_identity(&identity);
        }
        Ok(())
    }

    pub async fn schedule_delayed_will(
        self: &Arc<Self>,
        session_id: &str,
        will: PublishRequest,
        delay_secs: u32,
    ) -> anyhow::Result<()>
    where
        A: 'static,
        C: 'static,
        H: 'static,
    {
        let local_identity = self
            .local_sessions
            .get_cloned(session_id)
            .map(|state| state.record.identity.clone());
        let identity = if let Some(identity) = local_identity {
            identity
        } else if let Some(record) = self.sessiondict.lookup_session(session_id).await? {
            record.identity
        } else {
            anyhow::bail!("session not found");
        };
        let key = Self::will_identity_key(&identity);
        let generation = self.next_will_seq.fetch_add(1, Ordering::SeqCst);
        self.pending_will_generations
            .insert(key.clone(), generation);
        self.inbox
            .register_delayed_lwt(
                generation,
                DelayedLwtPublish {
                    tenant_id: identity.tenant_id.clone(),
                    session_id: session_id.to_string(),
                    publish: will.clone(),
                },
            )
            .await?;
        let broker = Arc::clone(self);
        let session_id = session_id.to_string();
        tokio::spawn(async move {
            sleep(Duration::from_secs(u64::from(delay_secs))).await;
            let should_publish =
                broker.pending_will_generations.get_copied(&key) == Some(generation);
            if !should_publish {
                return;
            }
            broker.pending_will_generations.remove(&key);
            let sink = BrokerDelayedLwtSink {
                broker: Arc::clone(&broker),
                identity: identity.clone(),
                session_id: session_id.clone(),
            };
            match inbox_send_lwt(broker.inbox.as_ref(), &session_id, generation, &sink).await {
                Ok(InboxLwtResult::Ok)
                | Ok(InboxLwtResult::NoInbox)
                | Ok(InboxLwtResult::NoLwt)
                | Ok(InboxLwtResult::NoDetach)
                | Ok(InboxLwtResult::Conflict) => {}
                Ok(result) => {
                    eprintln!("greenmqtt delayed will publish result: {result:?}");
                }
                Err(error) => {
                    eprintln!("greenmqtt delayed will publish error: {error:#}");
                }
            }
        });
        Ok(())
    }

    pub async fn connect(&self, request: ConnectRequest) -> anyhow::Result<ConnectReply> {
        self.connect_inner(request, false, true).await
    }

    pub async fn connect_without_replay(
        &self,
        request: ConnectRequest,
    ) -> anyhow::Result<ConnectReply> {
        self.connect_inner(request, false, false).await
    }

    pub(crate) async fn connect_authenticated_without_replay(
        &self,
        request: ConnectRequest,
    ) -> anyhow::Result<ConnectReply> {
        self.connect_inner(request, true, false).await
    }

    async fn connect_inner(
        &self,
        request: ConnectRequest,
        authenticated: bool,
        hydrate_replay: bool,
    ) -> anyhow::Result<ConnectReply> {
        if !authenticated && !self.auth.authenticate(&request.identity).await? {
            anyhow::bail!("authentication failed");
        }
        let connection_reservation = self
            .tenant_resources
            .reserve_connection(&request.identity.tenant_id)?;
        self.cancel_delayed_will_for_identity(&request.identity);
        let connect_plan = self.plan_session_connect(&request).await?;
        let session_present = connect_plan.session_present;
        let session = SessionRecord {
            session_id: connect_plan.session_id,
            node_id: request.node_id,
            kind: request.kind.clone(),
            identity: request.identity.clone(),
            session_expiry_interval_secs: if matches!(request.kind, SessionKind::Persistent) {
                request.session_expiry_interval_secs
            } else {
                Some(0)
            },
            expires_at_ms: None,
        };

        let (replaced, registration) = self.register_live_session(session.clone()).await?;
        self.inbox.clear_delayed_lwt(&session.session_id).await?;
        let mut carried_local_deliveries = Vec::new();
        if let Some(previous) = &replaced {
            let removed = self.local_sessions.remove(&previous.session_id);
            if let Some(state) = removed.as_ref() {
                let pending = self.local_deliveries.pending_len(&previous.session_id);
                self.note_local_session_removed(state, pending);
            }
            if previous.session_id != session.session_id {
                self.purge_session_state(&previous.session_id).await?;
            } else if matches!(previous.kind, SessionKind::Transient) {
                self.cleanup_replaced_transient_session(&previous.session_id)
                    .await?;
            } else if matches!(previous.kind, SessionKind::Persistent) {
                carried_local_deliveries = self.local_deliveries.drain(&previous.session_id);
            }
        }

        self.inbox.attach(&session.session_id).await?;
        if !carried_local_deliveries.is_empty() {
            self.inbox
                .enqueue_batch(
                    carried_local_deliveries
                        .into_iter()
                        .map(|delivery| OfflineMessage {
                            tenant_id: delivery.tenant_id,
                            session_id: delivery.session_id,
                            topic: delivery.topic,
                            payload: delivery.payload,
                            qos: delivery.qos,
                            retain: delivery.retain,
                            from_session_id: delivery.from_session_id,
                            properties: delivery.properties,
                        })
                        .collect(),
                )
                .await?;
        }
        let (offline_messages, inflight_messages) = if hydrate_replay {
            self.load_session_replay_with_clean_start(&session, request.clean_start)
                .await?
        } else {
            (Vec::new(), Vec::new())
        };

        let local_session_epoch = self.next_local_session_epoch.fetch_add(1, Ordering::SeqCst);
        let state = LocalSessionState {
            record: session.clone(),
            session_epoch: local_session_epoch,
            registration,
        };
        let carried_pending_deliveries = self.local_deliveries.pending_len(&session.session_id);
        self.local_sessions
            .insert(session.session_id.clone(), state.clone());
        connection_reservation.commit();
        self.note_local_session_added(&state, carried_pending_deliveries);
        BrokerMetrics::record_connect(&session.identity.tenant_id);
        BrokerMetrics::set_active_connections(
            &session.identity.tenant_id,
            self.local_stats().local_online_sessions,
        );
        self.hooks.on_connect(&session).await?;

        Ok(ConnectReply {
            session,
            session_present,
            local_session_epoch,
            replaced,
            offline_messages,
            inflight_messages,
        })
    }

    async fn load_session_replay_with_clean_start(
        &self,
        session: &SessionRecord,
        clean_start: bool,
    ) -> anyhow::Result<(Vec<OfflineMessage>, Vec<InflightMessage>)> {
        if !matches!(session.kind, SessionKind::Persistent) || clean_start {
            return Ok((Vec::new(), Vec::new()));
        }
        let offline = self
            .inbox
            .fetch(&session.session_id)
            .await?
            .into_iter()
            .filter(|message| !publish_properties_expired(&message.properties))
            .collect();
        let mut inflight = Vec::new();
        for message in self.inbox.fetch_inflight(&session.session_id).await? {
            if publish_properties_expired(&message.properties) {
                self.inbox
                    .ack_inflight(&session.session_id, message.packet_id)
                    .await?;
            } else {
                inflight.push(message);
            }
        }
        Ok((offline, inflight))
    }

    pub(crate) async fn load_session_replay(
        &self,
        session_id: &str,
    ) -> anyhow::Result<(Vec<OfflineMessage>, Vec<InflightMessage>)> {
        let Some(session) = self.sessiondict.lookup_session(session_id).await? else {
            anyhow::bail!("session not found");
        };
        self.load_session_replay_with_clean_start(&session, false)
            .await
    }

    pub async fn disconnect(&self, session_id: &str) -> anyhow::Result<()> {
        self.disconnect_internal(session_id, None).await
    }

    pub async fn disconnect_with_session_expiry(
        &self,
        session_id: &str,
        session_expiry_interval_secs: u32,
    ) -> anyhow::Result<()> {
        self.disconnect_internal(session_id, Some(session_expiry_interval_secs))
            .await
    }

    pub fn local_session_epoch_matches(&self, session_id: &str, session_epoch: u64) -> bool {
        self.local_sessions
            .get_session_epoch(session_id)
            .is_some_and(|epoch| epoch == session_epoch)
    }

    pub async fn disconnect_current_session(
        &self,
        session_id: &str,
        session_epoch: u64,
        expiry_override_secs: Option<u32>,
    ) -> anyhow::Result<bool> {
        let removed = match self.local_sessions.get_session_epoch(session_id) {
            Some(epoch) if epoch == session_epoch => self.local_sessions.remove(session_id),
            _ => None,
        };
        if let Some(state) = removed {
            self.finish_disconnect(state, expiry_override_secs).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn fence_current_session(
        &self,
        session_id: &str,
        session_epoch: u64,
        expiry_override_secs: Option<u32>,
    ) -> anyhow::Result<bool> {
        let fenced = self
            .disconnect_current_session(session_id, session_epoch, expiry_override_secs)
            .await?;
        if fenced {
            let mut details = BTreeMap::from([
                ("session_id".into(), session_id.to_string()),
                ("session_epoch".into(), session_epoch.to_string()),
            ]);
            if let Some(expiry_override_secs) = expiry_override_secs {
                details.insert(
                    "expiry_override_secs".into(),
                    expiry_override_secs.to_string(),
                );
            }
            self.record_admin_audit("fence_session", "sessions", details);
        }
        Ok(fenced)
    }

    pub async fn kill_session_admin(&self, session_id: &str) -> anyhow::Result<bool> {
        let known = self.local_sessions.contains_key(session_id)
            || self.sessiondict.lookup_session(session_id).await?.is_some();
        if !known {
            return Ok(false);
        }

        let removed = self.local_sessions.remove(session_id);
        if let Some(state) = removed.as_ref() {
            let pending = self.local_deliveries.pending_len(session_id);
            self.note_local_session_removed(state, pending);
            self.tenant_resources
                .release_connection(&state.record.identity.tenant_id);
            state.registration.stop().await?;
            self.inbox.detach(&state.record.session_id).await?;
            self.hooks.on_disconnect(&state.record).await?;
        }

        self.purge_session_state(session_id).await?;
        Ok(true)
    }

    pub async fn kill_sessions_admin(
        &self,
        tenant_id: Option<&str>,
        user_id: Option<&str>,
        client_id: Option<&str>,
    ) -> anyhow::Result<usize> {
        let sessions = self.sessiondict.list_sessions(tenant_id).await?;
        let session_ids: BTreeSet<_> = sessions
            .into_iter()
            .filter(|record| {
                user_id
                    .map(|value| record.identity.user_id == value)
                    .unwrap_or(true)
            })
            .filter(|record| {
                client_id
                    .map(|value| record.identity.client_id == value)
                    .unwrap_or(true)
            })
            .map(|record| record.session_id)
            .collect();

        let mut removed = 0usize;
        for session_id in session_ids {
            if self.kill_session_admin(&session_id).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn takeover_session_admin(
        &self,
        session_id: &str,
        expiry_override_secs: Option<u32>,
    ) -> anyhow::Result<bool> {
        let Some(session_epoch) = self.local_sessions.get_session_epoch(session_id) else {
            return Ok(false);
        };
        self.fence_current_session(session_id, session_epoch, expiry_override_secs)
            .await
    }

    async fn disconnect_internal(
        &self,
        session_id: &str,
        expiry_override_secs: Option<u32>,
    ) -> anyhow::Result<()> {
        let removed = self.local_sessions.remove(session_id);
        if let Some(state) = removed {
            self.finish_disconnect(state, expiry_override_secs).await?;
        }
        Ok(())
    }

    async fn finish_disconnect(
        &self,
        state: LocalSessionState,
        expiry_override_secs: Option<u32>,
    ) -> anyhow::Result<()> {
        let pending_deliveries = self.local_deliveries.pending_len(&state.record.session_id);
        self.note_local_session_removed(&state, pending_deliveries);
        self.tenant_resources
            .release_connection(&state.record.identity.tenant_id);
        state.registration.stop().await?;
        self.inbox.detach(&state.record.session_id).await?;
        let queued_deliveries = self.local_deliveries.drain(&state.record.session_id);
        if matches!(state.record.kind, SessionKind::Persistent) {
            let effective_expiry_secs =
                expiry_override_secs.or(state.record.session_expiry_interval_secs);
            if effective_expiry_secs == Some(0) {
                self.purge_session_state(&state.record.session_id).await?;
                self.hooks.on_disconnect(&state.record).await?;
                return Ok(());
            }
            for delivery in queued_deliveries {
                self.inbox
                    .enqueue(OfflineMessage {
                        tenant_id: delivery.tenant_id,
                        session_id: delivery.session_id,
                        topic: delivery.topic,
                        payload: delivery.payload,
                        qos: delivery.qos,
                        retain: delivery.retain,
                        from_session_id: delivery.from_session_id,
                        properties: delivery.properties,
                    })
                    .await?;
            }
            self.persist_disconnected_session(&state, expiry_override_secs)
                .await?;
        }
        if matches!(state.record.kind, SessionKind::Transient) {
            self.cleanup_transient_session(&state.record.session_id)
                .await?;
        }
        BrokerMetrics::record_disconnect(&state.record.identity.tenant_id);
        BrokerMetrics::set_active_connections(
            &state.record.identity.tenant_id,
            self.local_stats().local_online_sessions,
        );
        self.hooks.on_disconnect(&state.record).await?;
        Ok(())
    }

    pub fn record_admin_audit(
        &self,
        action: impl Into<String>,
        target: impl Into<String>,
        details: BTreeMap<String, String>,
    ) {
        let entry = AdminAuditEntry {
            seq: self.next_audit_seq.fetch_add(1, Ordering::SeqCst),
            timestamp_ms: now_millis(),
            action: action.into(),
            target: target.into(),
            details,
        };
        let mut guard = self.admin_audit.write().expect("broker poisoned");
        if guard.len() >= 256 {
            guard.pop_front();
        }
        guard.push_back(entry);
        if let Some(path) = self.config.audit_log_path.as_deref() {
            let _ = append_admin_audit(path, guard.back().expect("just pushed"));
        }
    }

    pub fn list_admin_audit(&self, limit: Option<usize>) -> Vec<AdminAuditEntry> {
        self.list_admin_audit_filtered(None, None, None, None, None, None, None, limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn list_admin_audit_filtered(
        &self,
        action: Option<&str>,
        target: Option<&str>,
        since_seq: Option<u64>,
        before_seq: Option<u64>,
        since_timestamp_ms: Option<u64>,
        details_key: Option<&str>,
        details_value: Option<&str>,
        limit: Option<usize>,
    ) -> Vec<AdminAuditEntry> {
        let guard = self.admin_audit.read().expect("broker poisoned");
        let mut entries: Vec<_> = guard
            .iter()
            .rev()
            .filter(|entry| action.is_none_or(|value| entry.action == value))
            .filter(|entry| target.is_none_or(|value| entry.target == value))
            .filter(|entry| since_seq.is_none_or(|value| entry.seq > value))
            .filter(|entry| before_seq.is_none_or(|value| entry.seq < value))
            .filter(|entry| since_timestamp_ms.is_none_or(|value| entry.timestamp_ms >= value))
            .filter(|entry| {
                details_key.is_none_or(|key| {
                    entry.details.contains_key(key)
                        && details_value.is_none_or(|value| {
                            entry
                                .details
                                .get(key)
                                .is_some_and(|entry_value| entry_value == value)
                        })
                })
            })
            .cloned()
            .collect();
        let take = limit.unwrap_or(100).min(entries.len());
        entries.truncate(take);
        entries
    }

    pub async fn restore_peer_registry(
        &self,
        registry: &dyn PeerRegistry,
    ) -> anyhow::Result<usize> {
        if !registry.list_peer_nodes().is_empty() {
            return Ok(0);
        }
        let desired =
            desired_peer_registry(self.admin_audit.read().expect("broker poisoned").iter());
        let mut restored = 0usize;
        for (node_id, endpoint) in desired {
            registry.add_peer_node(node_id, endpoint).await?;
            restored += 1;
        }
        Ok(restored)
    }

    pub async fn upsert_peer_registry_entry(
        &self,
        registry: &dyn PeerRegistry,
        node_id: NodeId,
        endpoint: String,
    ) -> anyhow::Result<bool> {
        let changed = registry.list_peer_endpoints().get(&node_id) != Some(&endpoint);
        registry.add_peer_node(node_id, endpoint.clone()).await?;
        self.record_admin_audit(
            "upsert_peer",
            "peers",
            BTreeMap::from([
                ("node_id".into(), node_id.to_string()),
                ("rpc_addr".into(), endpoint),
            ]),
        );
        Ok(changed)
    }

    pub fn suspect_peer_registry_entry(
        &self,
        registry: &dyn PeerRegistry,
        node_id: NodeId,
    ) -> bool {
        let endpoint = registry
            .list_peer_endpoints()
            .get(&node_id)
            .cloned()
            .or_else(|| {
                desired_peer_membership(self.admin_audit.read().expect("broker poisoned").iter())
                    .get(&node_id)
                    .and_then(|state| state.endpoint.clone())
            });
        let removed = registry.remove_peer_node(node_id);
        let mut details = BTreeMap::from([("node_id".into(), node_id.to_string())]);
        if let Some(endpoint) = endpoint {
            details.insert("rpc_addr".into(), endpoint);
        }
        self.record_admin_audit("suspect_peer", "peers", details);
        removed
    }

    pub async fn confirm_peer_registry_entry(
        &self,
        registry: &dyn PeerRegistry,
        node_id: NodeId,
    ) -> anyhow::Result<bool> {
        let Some(endpoint) =
            desired_peer_membership(self.admin_audit.read().expect("broker poisoned").iter())
                .get(&node_id)
                .and_then(|state| state.endpoint.clone())
        else {
            anyhow::bail!("peer endpoint not found");
        };
        let changed = registry.list_peer_endpoints().get(&node_id) != Some(&endpoint);
        registry.add_peer_node(node_id, endpoint.clone()).await?;
        self.record_admin_audit(
            "confirm_peer",
            "peers",
            BTreeMap::from([
                ("node_id".into(), node_id.to_string()),
                ("rpc_addr".into(), endpoint),
            ]),
        );
        Ok(changed)
    }

    pub fn delete_peer_registry_entry(&self, registry: &dyn PeerRegistry, node_id: NodeId) -> bool {
        let removed = registry.remove_peer_node(node_id);
        self.record_admin_audit(
            "delete_peer",
            "peers",
            BTreeMap::from([("node_id".into(), node_id.to_string())]),
        );
        removed
    }

    pub async fn sync_peer_registry(&self, registry: &dyn PeerRegistry) -> anyhow::Result<usize> {
        let desired =
            desired_peer_registry(self.admin_audit.read().expect("broker poisoned").iter());
        let current = registry.list_peer_endpoints();
        let mut changed = 0usize;

        for node_id in current.keys().copied().collect::<Vec<_>>() {
            if !desired.contains_key(&node_id) && registry.remove_peer_node(node_id) {
                changed += 1;
            }
        }

        for (node_id, endpoint) in desired {
            let needs_update = current.get(&node_id) != Some(&endpoint);
            if needs_update {
                registry.add_peer_node(node_id, endpoint).await?;
                changed += 1;
            }
        }

        Ok(changed)
    }

    pub async fn rebalance_sessions_to_registry(
        &self,
        registry: &dyn PeerRegistry,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<SessionReassignOutcome> {
        let candidates = placement_candidates(self.config.node_id, registry.list_peer_nodes());
        let active_nodes: BTreeSet<_> = candidates.iter().copied().collect();
        let mut outcome = SessionReassignOutcome {
            updated_sessions: 0,
            updated_routes: 0,
        };

        for session in self.sessiondict.list_sessions(tenant_id).await? {
            if active_nodes.contains(&session.node_id) {
                continue;
            }
            let target_node = placement_node_for_session(&session.session_id, &candidates);
            let session_id = session.session_id.clone();
            let source_node = session.node_id;
            let reassign = self
                .reassign_known_session_node(session, target_node)
                .await?;
            if reassign.updated_sessions > 0 {
                self.record_admin_audit(
                    "reassign_session",
                    "sessions",
                    BTreeMap::from([
                        ("session_id".into(), session_id),
                        ("from_node_id".into(), source_node.to_string()),
                        ("to_node_id".into(), target_node.to_string()),
                        ("reason".into(), "registry_rebalance".into()),
                    ]),
                );
            }
            outcome.updated_sessions += reassign.updated_sessions;
            outcome.updated_routes += reassign.updated_routes;
        }

        Ok(outcome)
    }

    pub async fn failover_sessions_from_node(
        &self,
        registry: &dyn PeerRegistry,
        failed_node_id: NodeId,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<SessionReassignOutcome> {
        let candidates = placement_candidates_excluding(
            self.config.node_id,
            registry.list_peer_nodes(),
            failed_node_id,
        );
        anyhow::ensure!(!candidates.is_empty(), "no available nodes for failover");

        let mut outcome = SessionReassignOutcome {
            updated_sessions: 0,
            updated_routes: 0,
        };

        for session in self.sessiondict.list_sessions(tenant_id).await? {
            if session.node_id != failed_node_id {
                continue;
            }
            let target_node = placement_node_for_session(&session.session_id, &candidates);
            let session_id = session.session_id.clone();
            let reassign = self
                .reassign_known_session_node(session, target_node)
                .await?;
            if reassign.updated_sessions > 0 {
                self.record_admin_audit(
                    "reassign_session",
                    "sessions",
                    BTreeMap::from([
                        ("session_id".into(), session_id),
                        ("from_node_id".into(), failed_node_id.to_string()),
                        ("to_node_id".into(), target_node.to_string()),
                        ("reason".into(), "node_failover".into()),
                    ]),
                );
            }
            outcome.updated_sessions += reassign.updated_sessions;
            outcome.updated_routes += reassign.updated_routes;
        }

        Ok(outcome)
    }

    pub async fn drain_sessions_from_node_to_target(
        &self,
        registry: &dyn PeerRegistry,
        source_node_id: NodeId,
        target_node_id: NodeId,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<SessionReassignOutcome> {
        anyhow::ensure!(
            source_node_id != target_node_id,
            "drain target must differ from source node"
        );
        let active_nodes: BTreeSet<_> =
            placement_candidates(self.config.node_id, registry.list_peer_nodes())
                .into_iter()
                .collect();
        anyhow::ensure!(
            active_nodes.contains(&target_node_id),
            "drain target is not active"
        );

        let mut outcome = SessionReassignOutcome {
            updated_sessions: 0,
            updated_routes: 0,
        };

        for session in self.sessiondict.list_sessions(tenant_id).await? {
            if session.node_id != source_node_id {
                continue;
            }
            let session_id = session.session_id.clone();
            let reassign = self
                .reassign_known_session_node(session, target_node_id)
                .await?;
            if reassign.updated_sessions > 0 {
                self.record_admin_audit(
                    "reassign_session",
                    "sessions",
                    BTreeMap::from([
                        ("session_id".into(), session_id),
                        ("from_node_id".into(), source_node_id.to_string()),
                        ("to_node_id".into(), target_node_id.to_string()),
                        ("reason".into(), "node_drain".into()),
                    ]),
                );
            }
            outcome.updated_sessions += reassign.updated_sessions;
            outcome.updated_routes += reassign.updated_routes;
        }

        Ok(outcome)
    }

    pub async fn reassign_session_node(
        &self,
        session_id: &str,
        node_id: NodeId,
    ) -> anyhow::Result<SessionReassignOutcome> {
        if self.local_sessions.contains_key(session_id) {
            anyhow::bail!("cannot reassign a locally online session");
        }

        let Some(session) = self.sessiondict.lookup_session(session_id).await? else {
            return Ok(SessionReassignOutcome {
                updated_sessions: 0,
                updated_routes: 0,
            });
        };
        self.reassign_known_session_node(session, node_id).await
    }

    pub async fn reassign_known_session_node(
        &self,
        mut session: SessionRecord,
        node_id: NodeId,
    ) -> anyhow::Result<SessionReassignOutcome> {
        let session_id = session.session_id.clone();
        if self.local_sessions.contains_key(&session_id) {
            anyhow::bail!("cannot reassign a locally online session");
        }

        if session.node_id == node_id {
            let routes = self.dist.count_session_routes(&session_id).await?;
            return Ok(SessionReassignOutcome {
                updated_sessions: 0,
                updated_routes: routes,
            });
        }

        let routes = self
            .dist
            .reassign_session_routes(&session_id, node_id)
            .await?;
        session.node_id = node_id;
        self.sessiondict.register(session).await?;
        Ok(SessionReassignOutcome {
            updated_sessions: 1,
            updated_routes: routes,
        })
    }

    pub async fn repair_orphan_session_state(&self, session_id: &str) -> anyhow::Result<bool> {
        if self.sessiondict.lookup_session(session_id).await?.is_some() {
            return Ok(false);
        }
        let session_id = session_id.to_string();
        self.cancel_delayed_will_for_session(&session_id).await?;
        let had_inbox = !self.inbox.peek(&session_id).await?.is_empty()
            || !self.inbox.fetch_inflight(&session_id).await?.is_empty()
            || !self.inbox.list_subscriptions(&session_id).await?.is_empty();
        self.inbox.purge_session(&session_id).await?;
        let removed_routes = self.dist.remove_session_routes(&session_id).await?;
        let removed_local = self.local_sessions.remove(&session_id);
        let removed_pending = self.local_deliveries.drain(&session_id).len();
        if let Some(state) = removed_local.as_ref() {
            self.note_local_session_removed(state, removed_pending);
        }
        Ok(had_inbox || removed_routes > 0 || removed_local.is_some() || removed_pending > 0)
    }

    async fn cleanup_replaced_transient_session(&self, session_id: &str) -> anyhow::Result<()> {
        let session_id = session_id.to_string();
        self.cancel_delayed_will_for_session(&session_id).await?;
        self.inbox.purge_session(&session_id).await?;
        self.dist.remove_session_routes(&session_id).await?;
        let removed = self.local_sessions.remove(&session_id);
        if let Some(state) = removed.as_ref() {
            let pending = self.local_deliveries.pending_len(&session_id);
            self.note_local_session_removed(state, pending);
            state.registration.stop().await?;
        }
        self.local_deliveries.drain(&session_id);
        Ok(())
    }

    async fn cleanup_transient_session(&self, session_id: &str) -> anyhow::Result<()> {
        self.cleanup_replaced_transient_session(session_id).await?;
        self.sessiondict.unregister(session_id).await?;
        Ok(())
    }

    pub(crate) async fn purge_session_state(&self, session_id: &str) -> anyhow::Result<()> {
        let session_id = session_id.to_string();
        self.cancel_delayed_will_for_session(&session_id).await?;
        self.inbox.purge_session(&session_id).await?;
        self.dist.remove_session_routes(&session_id).await?;
        self.sessiondict.unregister(&session_id).await?;
        let removed = self.local_sessions.remove(&session_id);
        if let Some(state) = removed.as_ref() {
            let pending = self.local_deliveries.pending_len(&session_id);
            self.note_local_session_removed(state, pending);
            state.registration.stop().await?;
        }
        self.pending_session_controls.remove(&session_id);
        self.local_deliveries.drain(&session_id);
        Ok(())
    }
}

pub(crate) fn session_expired(record: &SessionRecord) -> bool {
    match record.expires_at_ms {
        Some(deadline) => now_millis() >= deadline,
        None => false,
    }
}

fn session_expiration_deadline_ms(expiry_secs: Option<u32>) -> Option<u64> {
    expiry_secs.map(|secs| now_millis().saturating_add(u64::from(secs) * 1000))
}

fn updated_session_expiry_interval(
    current: Option<u32>,
    override_expiry: Option<u32>,
) -> Option<u32> {
    override_expiry.or(current)
}

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift before unix epoch")
        .as_millis() as u64
}

pub(crate) fn desired_peer_membership<'a>(
    entries: impl IntoIterator<Item = &'a AdminAuditEntry>,
) -> BTreeMap<NodeId, DesiredPeerState> {
    let mut peers = BTreeMap::new();
    for entry in entries {
        if entry.target != "peers" {
            continue;
        }
        let Some(node_id) = entry
            .details
            .get("node_id")
            .and_then(|value| value.parse::<NodeId>().ok())
        else {
            continue;
        };
        match entry.action.as_str() {
            "upsert_peer" => {
                if let Some(endpoint) = entry.details.get("rpc_addr").cloned() {
                    peers.insert(
                        node_id,
                        DesiredPeerState {
                            endpoint: Some(endpoint),
                            active: true,
                        },
                    );
                }
            }
            "suspect_peer" => {
                let state = peers.entry(node_id).or_default();
                if let Some(endpoint) = entry.details.get("rpc_addr").cloned() {
                    state.endpoint = Some(endpoint);
                }
                state.active = false;
            }
            "confirm_peer" => {
                let state = peers.entry(node_id).or_default();
                if let Some(endpoint) = entry.details.get("rpc_addr").cloned() {
                    state.endpoint = Some(endpoint);
                }
                if state.endpoint.is_some() {
                    state.active = true;
                }
            }
            "delete_peer" => {
                peers.remove(&node_id);
            }
            _ => {}
        }
    }
    peers
}

pub(crate) fn desired_peer_registry<'a>(
    entries: impl IntoIterator<Item = &'a AdminAuditEntry>,
) -> BTreeMap<NodeId, String> {
    let mut peers = BTreeMap::new();
    for (node_id, state) in desired_peer_membership(entries) {
        if state.active {
            if let Some(endpoint) = state.endpoint {
                peers.insert(node_id, endpoint);
            }
        }
    }
    peers
}

pub(crate) fn placement_candidates(
    local_node_id: NodeId,
    peer_nodes: impl IntoIterator<Item = NodeId>,
) -> Vec<NodeId> {
    let mut nodes = peer_nodes.into_iter().collect::<Vec<_>>();
    nodes.push(local_node_id);
    nodes.sort_unstable();
    nodes.dedup();
    nodes
}

pub(crate) fn placement_candidates_excluding(
    local_node_id: NodeId,
    peer_nodes: impl IntoIterator<Item = NodeId>,
    excluded_node_id: NodeId,
) -> Vec<NodeId> {
    placement_candidates(local_node_id, peer_nodes)
        .into_iter()
        .filter(|node_id| *node_id != excluded_node_id)
        .collect()
}

pub(crate) fn placement_node_for_session(session_id: &str, candidates: &[NodeId]) -> NodeId {
    candidates[shard_index_for_session(session_id, candidates.len())]
}

pub(crate) fn load_admin_audit(path: Option<&Path>) -> (VecDeque<AdminAuditEntry>, u64) {
    let Some(path) = path else {
        return (VecDeque::with_capacity(256), 1);
    };
    let Ok(contents) = fs::read_to_string(path) else {
        return (VecDeque::with_capacity(256), 1);
    };
    let mut entries = VecDeque::with_capacity(256);
    let mut max_seq = 0u64;
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        if let Ok(entry) = serde_json::from_str::<AdminAuditEntry>(line) {
            max_seq = max_seq.max(entry.seq);
            if entries.len() >= 256 {
                entries.pop_front();
            }
            entries.push_back(entry);
        }
    }
    (entries, max_seq + 1)
}

fn append_admin_audit(path: &Path, entry: &AdminAuditEntry) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, entry)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}
