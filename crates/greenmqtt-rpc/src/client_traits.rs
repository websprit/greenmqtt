use super::*;
use async_trait::async_trait;
use greenmqtt_broker::{DeliverySink, PeerForwarder};
use greenmqtt_dist::{DistMaintenance, DistRouter};
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
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .register_session(RegisterSessionRequest {
                        record: Some(to_proto_session(&record)),
                    })
                    .await?
                    .into_inner();
                Ok(reply.replaced.map(from_proto_session))
            }
            crate::SessionDictClientTransport::Quic(inner) => inner.register(record).await,
        }
    }

    async fn unregister(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .unregister_session(UnregisterSessionRequest {
                        session_id: session_id.to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.replaced.map(from_proto_session))
            }
            crate::SessionDictClientTransport::Quic(inner) => inner.unregister(session_id).await,
        }
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .lookup_session(LookupSessionRequest {
                        identity: Some(greenmqtt_proto::to_proto_client_identity(identity)),
                    })
                    .await?
                    .into_inner();
                Ok(reply.record.map(from_proto_session))
            }
            crate::SessionDictClientTransport::Quic(inner) => inner.lookup_identity(identity).await,
        }
    }

    async fn lookup_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_core::SessionRecord>> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .lookup_session_by_id(LookupSessionByIdRequest {
                        session_id: session_id.to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.record.map(from_proto_session))
            }
            crate::SessionDictClientTransport::Quic(inner) => {
                inner.lookup_session(session_id).await
            }
        }
    }

    async fn list_sessions(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<greenmqtt_core::SessionRecord>> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .list_sessions(ListSessionsRequest {
                        tenant_id: tenant_id.unwrap_or_default().to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.records.into_iter().map(from_proto_session).collect())
            }
            crate::SessionDictClientTransport::Quic(inner) => inner.list_sessions(tenant_id).await,
        }
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client.count_sessions(()).await?.into_inner();
                Ok(reply.count as usize)
            }
            crate::SessionDictClientTransport::Quic(_) => Ok(self.list_sessions(None).await?.len()),
        }
    }

    async fn session_exists_many(
        &self,
        session_ids: &[String],
    ) -> anyhow::Result<BTreeMap<String, bool>> {
        SessionDictGrpcClient::session_exists(self, session_ids).await
    }

    async fn identity_exists_many(
        &self,
        identities: &[ClientIdentity],
    ) -> anyhow::Result<BTreeMap<(String, String, String), bool>> {
        SessionDictGrpcClient::identity_exists(self, identities).await
    }
}

impl SessionDictGrpcClient {
    pub async fn session_exists(
        &self,
        session_ids: &[String],
    ) -> anyhow::Result<BTreeMap<String, bool>> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
            crate::SessionDictClientTransport::Quic(_) => {
                let mut results = BTreeMap::new();
                for session_id in session_ids {
                    results.insert(
                        session_id.clone(),
                        self.lookup_session(session_id).await?.is_some(),
                    );
                }
                Ok(results)
            }
        }
    }

    pub async fn identity_exists(
        &self,
        identities: &[ClientIdentity],
    ) -> anyhow::Result<BTreeMap<(String, String, String), bool>> {
        match &self.inner {
            crate::SessionDictClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
                                (identity.tenant_id, identity.user_id, identity.client_id),
                                entry.exists,
                            )
                        })
                    })
                    .collect())
            }
            crate::SessionDictClientTransport::Quic(_) => {
                let mut results = BTreeMap::new();
                for identity in identities {
                    results.insert(
                        (
                            identity.tenant_id.clone(),
                            identity.user_id.clone(),
                            identity.client_id.clone(),
                        ),
                        self.lookup_identity(identity).await?.is_some(),
                    );
                }
                Ok(results)
            }
        }
    }
}

#[async_trait]
impl DistRouter for DistGrpcClient {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .add_route(AddRouteRequest {
                        route: Some(to_proto_route(&route)),
                    })
                    .await?;
                Ok(())
            }
            crate::DistClientTransport::Quic(inner) => inner.add_route(route).await,
        }
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .remove_route(RemoveRouteRequest {
                        route: Some(to_proto_route(route)),
                    })
                    .await?;
                Ok(())
            }
            crate::DistClientTransport::Quic(inner) => inner.remove_route(route).await,
        }
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .remove_session_routes(RemoveSessionRoutesRequest {
                        session_id: session_id.to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.removed as usize)
            }
            crate::DistClientTransport::Quic(_) => {
                let routes = self.list_session_routes(session_id).await?;
                let removed = routes.len();
                for route in routes {
                    self.remove_route(&route).await?;
                }
                Ok(removed)
            }
        }
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .list_session_routes(ListSessionRoutesRequest {
                        session_id: session_id.to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.routes.into_iter().map(from_proto_route).collect())
            }
            crate::DistClientTransport::Quic(inner) => inner.list_session_routes(session_id).await,
        }
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &greenmqtt_core::TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .match_topic(MatchTopicRequest {
                        tenant_id: tenant_id.to_string(),
                        topic: topic.clone(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.routes.into_iter().map(from_proto_route).collect())
            }
            crate::DistClientTransport::Quic(inner) => inner.match_topic(tenant_id, topic).await,
        }
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .list_routes(ListRoutesRequest {
                        tenant_id: tenant_id.unwrap_or_default().to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.routes.into_iter().map(from_proto_route).collect())
            }
            crate::DistClientTransport::Quic(inner) => inner.list_routes(tenant_id).await,
        }
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        match &self.inner {
            crate::DistClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client.count_routes(()).await?.into_inner();
                Ok(reply.count as usize)
            }
            crate::DistClientTransport::Quic(_) => Ok(self.list_routes(None).await?.len()),
        }
    }
}

#[async_trait]
impl DistMaintenance for DistGrpcClient {
    async fn refresh_tenant(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_routes(Some(tenant_id)).await?.len())
    }
}

#[async_trait]
impl InboxService for InboxGrpcClient {
    async fn attach(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .attach(InboxAttachRequest {
                        session_id: session_id.clone(),
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(inner) => inner.attach(session_id).await,
        }
    }

    async fn detach(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .detach(InboxDetachRequest {
                        session_id: session_id.clone(),
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(inner) => inner.detach(session_id).await,
        }
    }

    async fn register_delayed_lwt(
        &self,
        _generation: u64,
        _publish: greenmqtt_inbox::DelayedLwtPublish,
    ) -> anyhow::Result<()> {
        anyhow::bail!("register_delayed_lwt is not supported over InboxGrpcClient")
    }

    async fn clear_delayed_lwt(
        &self,
        _session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send_delayed_lwt(
        &self,
        _session_id: &greenmqtt_core::SessionId,
        _generation: u64,
        _sink: &dyn greenmqtt_inbox::DelayedLwtSink,
    ) -> anyhow::Result<greenmqtt_inbox::InboxLwtResult> {
        anyhow::bail!("send_delayed_lwt is not supported over InboxGrpcClient")
    }

    async fn purge_session(&self, session_id: &greenmqtt_core::SessionId) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .purge_session(InboxPurgeSessionRequest {
                        session_id: session_id.clone(),
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(inner) => inner.purge_session(session_id).await,
        }
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .subscribe(InboxSubscribeRequest {
                        session_id: subscription.session_id,
                        tenant_id: subscription.tenant_id,
                        topic_filter: subscription.topic_filter,
                        qos: subscription.qos as u32,
                        subscription_identifier: subscription
                            .subscription_identifier
                            .unwrap_or_default(),
                        no_local: subscription.no_local,
                        retain_as_published: subscription.retain_as_published,
                        retain_handling: subscription.retain_handling as u32,
                        shared_group: subscription.shared_group.unwrap_or_default(),
                        kind: greenmqtt_proto::to_proto_session_kind(&subscription.kind),
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support subscriptions yet")
            }
        }
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
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .unsubscribe(InboxUnsubscribeRequest {
                        session_id: session_id.clone(),
                        topic_filter: topic_filter.to_string(),
                        shared_group: shared_group.unwrap_or_default().to_string(),
                    })
                    .await?;
                Ok(reply.into_inner().removed)
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support subscriptions yet")
            }
        }
    }

    async fn lookup_subscription(
        &self,
        session_id: &greenmqtt_core::SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support subscriptions yet")
            }
        }
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .enqueue(InboxEnqueueRequest {
                        message: Some(to_proto_offline(&message)),
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support enqueue yet")
            }
        }
    }

    async fn list_subscriptions(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support subscriptions yet")
            }
        }
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support subscriptions yet")
            }
        }
    }

    async fn peek(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .peek(InboxFetchRequest {
                        session_id: session_id.clone(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.messages.into_iter().map(from_proto_offline).collect())
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support offline fetch yet")
            }
        }
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .list_offline(InboxListMessagesRequest {
                        tenant_id: String::new(),
                        session_id: String::new(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.messages.into_iter().map(from_proto_offline).collect())
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support offline fetch yet")
            }
        }
    }

    async fn fetch(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .fetch(InboxFetchRequest {
                        session_id: session_id.clone(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.messages.into_iter().map(from_proto_offline).collect())
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support offline fetch yet")
            }
        }
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .stage_inflight(InboxStageInflightRequest {
                        message: Some(to_proto_inflight(&message)),
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support inflight staging yet")
            }
        }
    }

    async fn ack_inflight(
        &self,
        session_id: &greenmqtt_core::SessionId,
        packet_id: u16,
    ) -> anyhow::Result<()> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .ack_inflight(InboxAckInflightRequest {
                        session_id: session_id.clone(),
                        packet_id: packet_id as u32,
                    })
                    .await?;
                Ok(())
            }
            crate::InboxClientTransport::Quic(inner) => {
                inner.ack_inflight(session_id, packet_id).await
            }
        }
    }

    async fn fetch_inflight(
        &self,
        session_id: &greenmqtt_core::SessionId,
    ) -> anyhow::Result<Vec<InflightMessage>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support inflight fetch yet")
            }
        }
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
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
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support inflight fetch yet")
            }
        }
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client.stats(()).await?.into_inner();
                Ok(reply.subscriptions as usize)
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support stats yet")
            }
        }
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client.stats(()).await?.into_inner();
                Ok(reply.offline_messages as usize)
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support stats yet")
            }
        }
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        match &self.inner {
            crate::InboxClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client.stats(()).await?.into_inner();
                Ok(reply.inflight_messages as usize)
            }
            crate::InboxClientTransport::Quic(_) => {
                anyhow::bail!("Inbox QUIC facade does not support stats yet")
            }
        }
    }
}

#[async_trait]
impl RetainStoreService for RetainGrpcClient {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        match &self.inner {
            crate::RetainClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                client
                    .write(RetainWriteRequest {
                        message: Some(to_proto_retain(&message)),
                    })
                    .await?;
                Ok(())
            }
            crate::RetainClientTransport::Quic(inner) => inner.retain(message).await,
        }
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
        match &self.inner {
            crate::RetainClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client
                    .r#match(RetainMatchRequest {
                        tenant_id: tenant_id.to_string(),
                        topic_filter: topic_filter.to_string(),
                    })
                    .await?
                    .into_inner();
                Ok(reply.messages.into_iter().map(from_proto_retain).collect())
            }
            crate::RetainClientTransport::Quic(inner) => {
                inner.match_topic(tenant_id, topic_filter).await
            }
        }
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        match &self.inner {
            crate::RetainClientTransport::Grpc(inner) => {
                let mut client = inner.lock().await;
                let reply = client.count_retained(()).await?.into_inner();
                Ok(reply.count as usize)
            }
            crate::RetainClientTransport::Quic(_) => {
                anyhow::bail!("Retain QUIC facade does not support retained_count yet")
            }
        }
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
