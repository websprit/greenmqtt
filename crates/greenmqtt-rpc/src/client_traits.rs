use super::*;
use async_trait::async_trait;
use greenmqtt_broker::{DeliverySink, PeerForwarder};
use greenmqtt_dist::DistRouter;
use greenmqtt_inbox::InboxService;
use greenmqtt_kv_server::ReplicaTransport;
use greenmqtt_retain::RetainService as RetainStoreService;
use greenmqtt_sessiondict::SessionDirectory;

#[async_trait]
impl SessionDirectory for SessionDictGrpcClient {
    async fn register(
        &self,
        record: greenmqtt_core::SessionRecord,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .register_session(RegisterSessionRequest {
                record: Some(to_proto_session(&record)),
            })
            .await?
            .into_inner();
        Ok(reply.replaced.map(from_proto_session))
    }

    async fn unregister(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .unregister_session(UnregisterSessionRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.replaced.map(from_proto_session))
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_session(LookupSessionRequest {
                identity: Some(greenmqtt_proto::to_proto_client_identity(identity)),
            })
            .await?
            .into_inner();
        Ok(reply.record.map(from_proto_session))
    }

    async fn lookup_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_session_by_id(LookupSessionByIdRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.record.map(from_proto_session))
    }

    async fn list_sessions(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<greenmqtt_core::SessionRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_sessions(ListSessionsRequest {
                tenant_id: tenant_id.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.records.into_iter().map(from_proto_session).collect())
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.count_sessions(()).await?.into_inner();
        Ok(reply.count as usize)
    }
}

impl SessionDictGrpcClient {
    pub async fn session_exists(
        &self,
        session_ids: &[String],
    ) -> anyhow::Result<BTreeMap<String, bool>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .session_exists(SessionExistRequest {
                session_ids: session_ids.to_vec(),
            })
            .await?
            .into_inner();
        Ok(reply
            .entries
            .into_iter()
            .map(|entry| (entry.session_id, entry.exists))
            .collect())
    }

    pub async fn identity_exists(
        &self,
        identities: &[ClientIdentity],
    ) -> anyhow::Result<BTreeMap<(String, String, String), bool>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .identity_exists(IdentityExistRequest {
                identities: identities
                    .iter()
                    .map(greenmqtt_proto::to_proto_client_identity)
                    .collect(),
            })
            .await?
            .into_inner();
        Ok(reply
            .entries
            .into_iter()
            .filter_map(|entry| {
                entry.identity.map(|identity| {
                    let identity = from_proto_client_identity(identity);
                    (
                        (
                            identity.tenant_id,
                            identity.user_id,
                            identity.client_id,
                        ),
                        entry.exists,
                    )
                })
            })
            .collect())
    }
}

#[async_trait]
impl DistRouter for DistGrpcClient {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .add_route(AddRouteRequest {
                route: Some(to_proto_route(&route)),
            })
            .await?;
        Ok(())
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .remove_route(RemoveRouteRequest {
                route: Some(to_proto_route(route)),
            })
            .await?;
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_session_routes(RemoveSessionRoutesRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.removed as usize)
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_session_routes(ListSessionRoutesRequest {
                session_id: session_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.routes.into_iter().map(from_proto_route).collect())
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &greenmqtt_core::TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .match_topic(MatchTopicRequest {
                tenant_id: tenant_id.to_string(),
                topic: topic.clone(),
            })
            .await?
            .into_inner();
        Ok(reply.routes.into_iter().map(from_proto_route).collect())
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_routes(ListRoutesRequest {
                tenant_id: tenant_id.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.routes.into_iter().map(from_proto_route).collect())
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.count_routes(()).await?.into_inner();
        Ok(reply.count as usize)
    }
}

#[async_trait]
impl InboxService for InboxGrpcClient {
    async fn attach(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .attach(InboxAttachRequest {
                session_id: session_id.clone(),
            })
            .await?;
        Ok(())
    }

    async fn detach(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .detach(InboxDetachRequest {
                session_id: session_id.clone(),
            })
            .await?;
        Ok(())
    }

    async fn purge_session(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .purge_session(InboxPurgeSessionRequest {
                session_id: session_id.clone(),
            })
            .await?;
        Ok(())
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .subscribe(InboxSubscribeRequest {
                session_id: subscription.session_id,
                tenant_id: subscription.tenant_id,
                topic_filter: subscription.topic_filter,
                qos: subscription.qos as u32,
                subscription_identifier: subscription.subscription_identifier.unwrap_or_default(),
                no_local: subscription.no_local,
                retain_as_published: subscription.retain_as_published,
                retain_handling: subscription.retain_handling as u32,
                shared_group: subscription.shared_group.unwrap_or_default(),
                kind: greenmqtt_proto::to_proto_session_kind(&subscription.kind),
            })
            .await?;
        Ok(())
    }

    async fn unsubscribe(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<bool> {
        self.unsubscribe_shared(session_id, topic_filter, None)
            .await
    }

    async fn unsubscribe_shared(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let mut client = self.inner.lock().await;
        let reply = client
            .unsubscribe(InboxUnsubscribeRequest {
                session_id: session_id.clone(),
                topic_filter: topic_filter.to_string(),
                shared_group: shared_group.unwrap_or_default().to_string(),
            })
            .await?;
        Ok(reply.into_inner().removed)
    }

    async fn lookup_subscription(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_subscription(InboxLookupSubscriptionRequest {
                session_id: session_id.clone(),
                topic_filter: topic_filter.to_string(),
                shared_group: shared_group.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.subscription.map(from_proto_subscription))
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .enqueue(InboxEnqueueRequest {
                message: Some(to_proto_offline(&message)),
            })
            .await?;
        Ok(())
    }

    async fn list_subscriptions(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_subscriptions(InboxListSubscriptionsRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply
            .subscriptions
            .into_iter()
            .map(from_proto_subscription)
            .collect())
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_all_subscriptions(InboxListAllSubscriptionsRequest {
                tenant_id: String::new(),
                session_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(reply
            .subscriptions
            .into_iter()
            .map(from_proto_subscription)
            .collect())
    }

    async fn peek(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .peek(InboxFetchRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_offline).collect())
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_offline(InboxListMessagesRequest {
                tenant_id: String::new(),
                session_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_offline).collect())
    }

    async fn fetch(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .fetch(InboxFetchRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_offline).collect())
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .stage_inflight(InboxStageInflightRequest {
                message: Some(to_proto_inflight(&message)),
            })
            .await?;
        Ok(())
    }

    async fn ack_inflight(
        &self,
        session_id: &greenmqtt_core::SessionId,
        packet_id: u16,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .ack_inflight(InboxAckInflightRequest {
                session_id: session_id.clone(),
                packet_id: packet_id as u32,
            })
            .await?;
        Ok(())
    }

    async fn fetch_inflight(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<InflightMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .fetch_inflight(InboxFetchInflightRequest {
                session_id: session_id.clone(),
            })
            .await?
            .into_inner();
        Ok(reply
            .messages
            .into_iter()
            .map(from_proto_inflight)
            .collect())
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_inflight(InboxListMessagesRequest {
                tenant_id: String::new(),
                session_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(reply
            .messages
            .into_iter()
            .map(from_proto_inflight)
            .collect())
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.stats(()).await?.into_inner();
        Ok(reply.subscriptions as usize)
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.stats(()).await?.into_inner();
        Ok(reply.offline_messages as usize)
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.stats(()).await?.into_inner();
        Ok(reply.inflight_messages as usize)
    }
}

#[async_trait]
impl RetainStoreService for RetainGrpcClient {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .write(RetainWriteRequest {
                message: Some(to_proto_retain(&message)),
            })
            .await?;
        Ok(())
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        self.match_topic(tenant_id, "#").await
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self.match_topic(tenant_id, topic).await?.into_iter().next())
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .r#match(RetainMatchRequest {
                tenant_id: tenant_id.to_string(),
                topic_filter: topic_filter.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.messages.into_iter().map(from_proto_retain).collect())
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client.count_retained(()).await?.into_inner();
        Ok(reply.count as usize)
    }
}

#[async_trait]
impl PeerForwarder for StaticPeerForwarder {
    async fn forward_delivery(&self, node_id: NodeId, delivery: Delivery) -> anyhow::Result<bool> {
        self.push_delivery_calls.fetch_add(1, Ordering::SeqCst);
        let peer = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .get(&node_id)
            .map(|peer| peer.client.clone())
            .ok_or_else(|| anyhow::anyhow!("peer node {node_id} not configured"))?;
        let mut client = peer.lock().await;
        let reply = client
            .push_delivery(PushDeliveryRequest {
                delivery: Some(to_proto_delivery(&delivery)),
            })
            .await?
            .into_inner();
        Ok(reply.delivered)
    }

    async fn forward_deliveries(
        &self,
        node_id: NodeId,
        deliveries: Vec<Delivery>,
    ) -> anyhow::Result<Vec<Delivery>> {
        self.push_deliveries_calls.fetch_add(1, Ordering::SeqCst);
        let peer = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .get(&node_id)
            .map(|peer| peer.client.clone())
            .ok_or_else(|| anyhow::anyhow!("peer node {node_id} not configured"))?;
        let mut client = peer.lock().await;
        let reply = client
            .push_deliveries(PushDeliveriesRequest {
                deliveries: deliveries.iter().map(to_proto_delivery).collect(),
            })
            .await?
            .into_inner();
        let undelivered_indices: std::collections::BTreeSet<_> = reply
            .undelivered_indices
            .into_iter()
            .map(|index| index as usize)
            .collect();
        Ok(deliveries
            .into_iter()
            .enumerate()
            .filter_map(|(index, delivery)| {
                undelivered_indices.contains(&index).then_some(delivery)
            })
            .collect())
    }
}

#[async_trait]
impl DeliverySink for NoopDeliverySink {
    async fn push_delivery(&self, _delivery: Delivery) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[async_trait]
impl ReplicaTransport for NoopReplicaTransport {
    async fn send(
        &self,
        _from_node_id: NodeId,
        _target_node_id: NodeId,
        _range_id: &str,
        _message: &RaftMessage,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
