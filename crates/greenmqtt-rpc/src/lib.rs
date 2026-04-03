use async_trait::async_trait;
use greenmqtt_broker::{DeliverySink, PeerForwarder, PeerRegistry};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeMembership, Delivery, InflightMessage,
    NodeId, OfflineMessage, RetainedMessage, RouteRecord, ServiceEndpoint, ServiceEndpointRegistry,
    ServiceShardAssignment, ServiceShardKey, ServiceShardKind, ServiceShardLifecycle,
    ServiceShardRecoveryControl, ServiceShardTransition, ServiceShardTransitionKind, Subscription,
};
use greenmqtt_dist::{dist_tenant_shard, DistRouter};
use greenmqtt_inbox::{inbox_session_shard, InboxService};
use greenmqtt_proto::internal::{
    broker_peer_service_client::BrokerPeerServiceClient,
    broker_peer_service_server::{BrokerPeerService, BrokerPeerServiceServer},
    dist_service_client::DistServiceClient,
    dist_service_server::{DistService, DistServiceServer},
    inbox_service_client::InboxServiceClient,
    inbox_service_server::{InboxService as ProtoInboxService, InboxServiceServer},
    retain_service_client::RetainServiceClient,
    retain_service_server::{RetainService, RetainServiceServer},
    session_dict_service_client::SessionDictServiceClient,
    session_dict_service_server::{SessionDictService, SessionDictServiceServer},
    AddRouteRequest, CountReply, InboxAckInflightRequest, InboxAttachRequest, InboxDetachRequest,
    InboxEnqueueRequest, InboxFetchInflightReply, InboxFetchInflightRequest, InboxFetchReply,
    InboxFetchRequest, InboxListAllSubscriptionsRequest, InboxListMessagesRequest,
    InboxListSubscriptionsReply, InboxListSubscriptionsRequest, InboxLookupSubscriptionReply,
    InboxLookupSubscriptionRequest, InboxPurgeSessionRequest, InboxStageInflightRequest,
    InboxStatsReply, InboxSubscribeRequest, InboxUnsubscribeReply, InboxUnsubscribeRequest,
    ListRoutesReply, ListRoutesRequest, ListSessionRoutesReply, ListSessionRoutesRequest,
    ListSessionsReply, ListSessionsRequest, LookupSessionByIdRequest, LookupSessionReply,
    LookupSessionRequest, MatchTopicReply, MatchTopicRequest, PushDeliveriesReply,
    PushDeliveriesRequest, PushDeliveryReply, PushDeliveryRequest, RegisterSessionReply,
    RegisterSessionRequest, RemoveRouteRequest, RemoveSessionRoutesReply,
    RemoveSessionRoutesRequest, RetainMatchReply, RetainMatchRequest, RetainWriteRequest,
    UnregisterSessionRequest,
};
use greenmqtt_proto::{
    from_proto_client_identity, from_proto_delivery, from_proto_inflight, from_proto_offline,
    from_proto_retain, from_proto_route, from_proto_session, from_proto_subscription,
    to_proto_delivery, to_proto_inflight, to_proto_offline, to_proto_retain, to_proto_route,
    to_proto_session, to_proto_subscription,
};
use greenmqtt_retain::{retain_tenant_shard, RetainService as RetainStoreService};
use greenmqtt_sessiondict::{session_identity_shard, SessionDirectory};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RpcRuntime {
    pub sessiondict: Arc<dyn SessionDirectory>,
    pub dist: Arc<dyn DistRouter>,
    pub inbox: Arc<dyn InboxService>,
    pub retain: Arc<dyn RetainStoreService>,
    pub peer_sink: Arc<dyn DeliverySink>,
}

#[derive(Clone)]
pub struct SessionDictGrpcClient {
    inner: Arc<Mutex<SessionDictServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct DistGrpcClient {
    inner: Arc<Mutex<DistServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct InboxGrpcClient {
    inner: Arc<Mutex<InboxServiceClient<Channel>>>,
}

#[derive(Clone)]
pub struct RetainGrpcClient {
    inner: Arc<Mutex<RetainServiceClient<Channel>>>,
}

#[derive(Clone, Default)]
pub struct StaticPeerForwarder {
    peers: Arc<RwLock<HashMap<NodeId, StaticPeerClient>>>,
    push_delivery_calls: Arc<AtomicUsize>,
    push_deliveries_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
pub struct StaticServiceEndpointRegistry {
    assignments: Arc<RwLock<BTreeMap<ServiceShardKey, ServiceShardAssignment>>>,
    members: Arc<RwLock<BTreeMap<NodeId, ClusterNodeMembership>>>,
}

#[derive(Clone)]
struct StaticPeerClient {
    endpoint: String,
    client: Arc<Mutex<BrokerPeerServiceClient<Channel>>>,
}

#[derive(Clone, Default)]
pub struct NoopDeliverySink;

#[derive(Clone)]
struct SessionDictRpc {
    inner: Arc<dyn SessionDirectory>,
}

#[derive(Clone)]
struct DistRpc {
    inner: Arc<dyn DistRouter>,
}

#[derive(Clone)]
struct InboxRpc {
    inner: Arc<dyn InboxService>,
}

#[derive(Clone)]
struct RetainRpc {
    inner: Arc<dyn RetainStoreService>,
}

#[derive(Clone)]
struct BrokerPeerRpc {
    inner: Arc<dyn DeliverySink>,
}

impl SessionDictGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                SessionDictServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Self> {
        let shard = session_identity_shard(identity);
        let assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict endpoint registered for shard {shard:?}")
        })?;
        Self::connect(assignment.endpoint.endpoint).await
    }
}

impl StaticPeerForwarder {
    pub fn push_delivery_calls(&self) -> usize {
        self.push_delivery_calls.load(Ordering::SeqCst)
    }

    pub fn push_deliveries_calls(&self) -> usize {
        self.push_deliveries_calls.load(Ordering::SeqCst)
    }
}

impl StaticServiceEndpointRegistry {
    pub async fn transition_shard_to_member(
        &self,
        kind: ServiceShardTransitionKind,
        shard: ServiceShardKey,
        source_node_id: Option<NodeId>,
        target_node_id: NodeId,
        lifecycle: ServiceShardLifecycle,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let member = self
            .resolve_member(target_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target member {target_node_id} not found"))?;
        let service_kind = shard.service_kind();
        let endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == service_kind)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("target member {target_node_id} has no {service_kind:?} endpoint")
            })?;
        let current = self.resolve_assignment(&shard).await?;
        let next_epoch = current
            .as_ref()
            .map(|assignment| assignment.epoch + 1)
            .unwrap_or(1);
        let next_fencing_token = current
            .as_ref()
            .map(|assignment| assignment.fencing_token + 1)
            .unwrap_or(1);
        let assignment = ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(service_kind, target_node_id, endpoint.endpoint),
            next_epoch,
            next_fencing_token,
            lifecycle,
        );
        self.apply_transition(ServiceShardTransition::new(
            kind,
            shard,
            source_node_id,
            assignment.clone(),
        ))
        .await?;
        Ok(assignment)
    }

    pub async fn move_shard_to_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_node_id = self
            .resolve_assignment(&shard)
            .await?
            .map(|assignment| assignment.owner_node_id());
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Migration,
            shard,
            source_node_id,
            target_node_id,
            ServiceShardLifecycle::Draining,
        )
        .await
    }

    pub async fn failover_shard_to_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_node_id = self
            .resolve_assignment(&shard)
            .await?
            .map(|assignment| assignment.owner_node_id());
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Failover,
            shard,
            source_node_id,
            target_node_id,
            ServiceShardLifecycle::Recovering,
        )
        .await
    }

    pub async fn bootstrap_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Bootstrap,
            shard,
            None,
            target_node_id,
            ServiceShardLifecycle::Bootstrapping,
        )
        .await
    }

    pub async fn catch_up_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_node_id = self
            .resolve_assignment(&shard)
            .await?
            .map(|assignment| assignment.owner_node_id());
        self.transition_shard_to_member(
            ServiceShardTransitionKind::CatchUp,
            shard,
            source_node_id,
            target_node_id,
            ServiceShardLifecycle::Recovering,
        )
        .await
    }

    pub async fn repair_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_node_id = self
            .resolve_assignment(&shard)
            .await?
            .map(|assignment| assignment.owner_node_id());
        self.transition_shard_to_member(
            ServiceShardTransitionKind::AntiEntropy,
            shard,
            source_node_id,
            target_node_id,
            ServiceShardLifecycle::Serving,
        )
        .await
    }
}

#[async_trait]
impl ServiceEndpointRegistry for StaticServiceEndpointRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("service registry poisoned")
            .insert(assignment.shard.clone(), assignment))
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .read()
            .expect("service registry poisoned")
            .get(shard)
            .cloned())
    }

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("service registry poisoned")
            .remove(shard))
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments: Vec<_> = self
            .assignments
            .read()
            .expect("service registry poisoned")
            .values()
            .filter(|assignment| {
                kind.as_ref()
                    .map(|kind| assignment.shard.kind == *kind)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        assignments.sort_by(|left, right| left.shard.cmp(&right.shard));
        Ok(assignments)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for StaticServiceEndpointRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("service registry poisoned")
            .insert(member.node_id, member))
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .read()
            .expect("service registry poisoned")
            .get(&node_id)
            .cloned())
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("service registry poisoned")
            .remove(&node_id))
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut members: Vec<_> = self
            .members
            .read()
            .expect("service registry poisoned")
            .values()
            .cloned()
            .collect();
        members.sort_by_key(|member| member.node_id);
        Ok(members)
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for StaticServiceEndpointRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let mut assignments = self.assignments.write().expect("service registry poisoned");
        if let Some(source_node_id) = transition.source_node_id {
            if let Some(current) = assignments.get(&transition.shard) {
                anyhow::ensure!(
                    current.owner_node_id() == source_node_id,
                    "source owner mismatch for shard transition"
                );
            }
        }
        Ok(assignments.insert(transition.shard.clone(), transition.target_assignment))
    }
}

impl DistGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                DistServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = dist_tenant_shard(tenant_id);
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }
}

impl InboxGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                InboxServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
        session_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = inbox_session_shard(tenant_id, &session_id.to_string());
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }
}

impl RetainGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RetainServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = retain_tenant_shard(tenant_id);
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no retain endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }
}

impl StaticPeerForwarder {
    pub async fn connect_node(
        &self,
        node_id: NodeId,
        endpoint: impl Into<String>,
    ) -> anyhow::Result<()> {
        let endpoint = endpoint.into();
        let client = BrokerPeerServiceClient::connect(endpoint.clone()).await?;
        self.peers.write().expect("peer forwarder poisoned").insert(
            node_id,
            StaticPeerClient {
                endpoint,
                client: Arc::new(Mutex::new(client)),
            },
        );
        Ok(())
    }

    pub fn configured_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<_> = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .keys()
            .copied()
            .collect();
        nodes.sort_unstable();
        nodes
    }

    pub fn disconnect_node(&self, node_id: NodeId) -> bool {
        self.peers
            .write()
            .expect("peer forwarder poisoned")
            .remove(&node_id)
            .is_some()
    }
}

#[async_trait]
impl PeerRegistry for StaticPeerForwarder {
    fn list_peer_nodes(&self) -> Vec<NodeId> {
        self.configured_nodes()
    }

    fn list_peer_endpoints(&self) -> BTreeMap<NodeId, String> {
        self.peers
            .read()
            .expect("peer forwarder poisoned")
            .iter()
            .map(|(node_id, peer)| (*node_id, peer.endpoint.clone()))
            .collect()
    }

    fn remove_peer_node(&self, node_id: NodeId) -> bool {
        self.disconnect_node(node_id)
    }

    async fn add_peer_node(&self, node_id: NodeId, endpoint: String) -> anyhow::Result<()> {
        self.connect_node(node_id, endpoint).await
    }
}

impl RpcRuntime {
    pub async fn serve(self, bind: SocketAddr) -> anyhow::Result<()> {
        Server::builder()
            .add_service(SessionDictServiceServer::new(SessionDictRpc {
                inner: self.sessiondict,
            }))
            .add_service(DistServiceServer::new(DistRpc { inner: self.dist }))
            .add_service(InboxServiceServer::new(InboxRpc { inner: self.inbox }))
            .add_service(RetainServiceServer::new(RetainRpc { inner: self.retain }))
            .add_service(BrokerPeerServiceServer::new(BrokerPeerRpc {
                inner: self.peer_sink,
            }))
            .serve(bind)
            .await?;
        Ok(())
    }
}

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

#[tonic::async_trait]
impl SessionDictService for SessionDictRpc {
    async fn register_session(
        &self,
        request: Request<RegisterSessionRequest>,
    ) -> Result<Response<RegisterSessionReply>, Status> {
        let record = request
            .into_inner()
            .record
            .ok_or_else(|| Status::invalid_argument("missing session record"))?;
        let replaced = self
            .inner
            .register(from_proto_session(record))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RegisterSessionReply {
            replaced: replaced.as_ref().map(to_proto_session),
        }))
    }

    async fn unregister_session(
        &self,
        request: Request<UnregisterSessionRequest>,
    ) -> Result<Response<RegisterSessionReply>, Status> {
        let replaced = self
            .inner
            .unregister(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RegisterSessionReply {
            replaced: replaced.as_ref().map(to_proto_session),
        }))
    }

    async fn lookup_session(
        &self,
        request: Request<LookupSessionRequest>,
    ) -> Result<Response<LookupSessionReply>, Status> {
        let identity = request
            .into_inner()
            .identity
            .ok_or_else(|| Status::invalid_argument("missing client identity"))?;
        let record = self
            .inner
            .lookup_identity(&from_proto_client_identity(identity))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(LookupSessionReply {
            record: record.as_ref().map(to_proto_session),
        }))
    }

    async fn lookup_session_by_id(
        &self,
        request: Request<LookupSessionByIdRequest>,
    ) -> Result<Response<LookupSessionReply>, Status> {
        let record = self
            .inner
            .lookup_session(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(LookupSessionReply {
            record: record.as_ref().map(to_proto_session),
        }))
    }

    async fn count_sessions(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.session_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsReply>, Status> {
        let request = request.into_inner();
        let tenant_id = if request.tenant_id.is_empty() {
            None
        } else {
            Some(request.tenant_id.as_str())
        };
        let records = self
            .inner
            .list_sessions(tenant_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(ListSessionsReply {
            records: records.iter().map(to_proto_session).collect(),
        }))
    }
}

#[tonic::async_trait]
impl DistService for DistRpc {
    async fn add_route(&self, request: Request<AddRouteRequest>) -> Result<Response<()>, Status> {
        let route = request
            .into_inner()
            .route
            .ok_or_else(|| Status::invalid_argument("missing route"))?;
        self.inner
            .add_route(from_proto_route(route))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn remove_route(
        &self,
        request: Request<RemoveRouteRequest>,
    ) -> Result<Response<()>, Status> {
        let route = request
            .into_inner()
            .route
            .ok_or_else(|| Status::invalid_argument("missing route"))?;
        self.inner
            .remove_route(&from_proto_route(route))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn remove_session_routes(
        &self,
        request: Request<RemoveSessionRoutesRequest>,
    ) -> Result<Response<RemoveSessionRoutesReply>, Status> {
        let removed = self
            .inner
            .remove_session_routes(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RemoveSessionRoutesReply {
            removed: removed as u32,
        }))
    }

    async fn list_session_routes(
        &self,
        request: Request<ListSessionRoutesRequest>,
    ) -> Result<Response<ListSessionRoutesReply>, Status> {
        let routes = self
            .inner
            .list_session_routes(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(ListSessionRoutesReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn match_topic(
        &self,
        request: Request<MatchTopicRequest>,
    ) -> Result<Response<MatchTopicReply>, Status> {
        let request = request.into_inner();
        let routes = self
            .inner
            .match_topic(&request.tenant_id, &request.topic)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(MatchTopicReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn list_routes(
        &self,
        request: Request<ListRoutesRequest>,
    ) -> Result<Response<ListRoutesReply>, Status> {
        let request = request.into_inner();
        let tenant_id = if request.tenant_id.is_empty() {
            None
        } else {
            Some(request.tenant_id.as_str())
        };
        let routes = self
            .inner
            .list_routes(tenant_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(ListRoutesReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn count_routes(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.route_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }
}

#[tonic::async_trait]
impl ProtoInboxService for InboxRpc {
    async fn attach(&self, request: Request<InboxAttachRequest>) -> Result<Response<()>, Status> {
        self.inner
            .attach(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn detach(&self, request: Request<InboxDetachRequest>) -> Result<Response<()>, Status> {
        self.inner
            .detach(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn purge_session(
        &self,
        request: Request<InboxPurgeSessionRequest>,
    ) -> Result<Response<()>, Status> {
        self.inner
            .purge_session(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn subscribe(
        &self,
        request: Request<InboxSubscribeRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.inner
            .subscribe(Subscription {
                session_id: request.session_id,
                tenant_id: request.tenant_id,
                topic_filter: request.topic_filter,
                qos: request.qos as u8,
                subscription_identifier: if request.subscription_identifier == 0 {
                    None
                } else {
                    Some(request.subscription_identifier)
                },
                no_local: request.no_local,
                retain_as_published: request.retain_as_published,
                retain_handling: request.retain_handling as u8,
                shared_group: if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group)
                },
                kind: greenmqtt_proto::from_proto_session_kind(&request.kind),
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn unsubscribe(
        &self,
        request: Request<InboxUnsubscribeRequest>,
    ) -> Result<Response<InboxUnsubscribeReply>, Status> {
        let request = request.into_inner();
        let removed = self
            .inner
            .unsubscribe_shared(
                &request.session_id,
                &request.topic_filter,
                if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group.as_str())
                },
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxUnsubscribeReply { removed }))
    }

    async fn enqueue(&self, request: Request<InboxEnqueueRequest>) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing offline message"))?;
        self.inner
            .enqueue(from_proto_offline(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn list_subscriptions(
        &self,
        request: Request<InboxListSubscriptionsRequest>,
    ) -> Result<Response<InboxListSubscriptionsReply>, Status> {
        let subscriptions = self
            .inner
            .list_subscriptions(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxListSubscriptionsReply {
            subscriptions: subscriptions.iter().map(to_proto_subscription).collect(),
        }))
    }

    async fn lookup_subscription(
        &self,
        request: Request<InboxLookupSubscriptionRequest>,
    ) -> Result<Response<InboxLookupSubscriptionReply>, Status> {
        let request = request.into_inner();
        let subscription = self
            .inner
            .lookup_subscription(
                &request.session_id,
                &request.topic_filter,
                if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group.as_str())
                },
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxLookupSubscriptionReply {
            subscription: subscription.as_ref().map(to_proto_subscription),
        }))
    }

    async fn list_all_subscriptions(
        &self,
        request: Request<InboxListAllSubscriptionsRequest>,
    ) -> Result<Response<InboxListSubscriptionsReply>, Status> {
        let request = request.into_inner();
        let mut subscriptions = self
            .inner
            .list_all_subscriptions()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            subscriptions.retain(|subscription| subscription.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            subscriptions.retain(|subscription| subscription.session_id == request.session_id);
        }
        Ok(Response::new(InboxListSubscriptionsReply {
            subscriptions: subscriptions.iter().map(to_proto_subscription).collect(),
        }))
    }

    async fn fetch(
        &self,
        request: Request<InboxFetchRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let messages = self
            .inner
            .fetch(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn peek(
        &self,
        request: Request<InboxFetchRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let messages = self
            .inner
            .peek(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn list_offline(
        &self,
        request: Request<InboxListMessagesRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let request = request.into_inner();
        let mut messages = self
            .inner
            .list_all_offline()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            messages.retain(|message| message.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            messages.retain(|message| message.session_id == request.session_id);
        }
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn stage_inflight(
        &self,
        request: Request<InboxStageInflightRequest>,
    ) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing inflight message"))?;
        self.inner
            .stage_inflight(from_proto_inflight(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn ack_inflight(
        &self,
        request: Request<InboxAckInflightRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.inner
            .ack_inflight(&request.session_id, request.packet_id as u16)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn fetch_inflight(
        &self,
        request: Request<InboxFetchInflightRequest>,
    ) -> Result<Response<InboxFetchInflightReply>, Status> {
        let messages = self
            .inner
            .fetch_inflight(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchInflightReply {
            messages: messages.iter().map(to_proto_inflight).collect(),
        }))
    }

    async fn list_inflight(
        &self,
        request: Request<InboxListMessagesRequest>,
    ) -> Result<Response<InboxFetchInflightReply>, Status> {
        let request = request.into_inner();
        let mut messages = self
            .inner
            .list_all_inflight()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            messages.retain(|message| message.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            messages.retain(|message| message.session_id == request.session_id);
        }
        Ok(Response::new(InboxFetchInflightReply {
            messages: messages.iter().map(to_proto_inflight).collect(),
        }))
    }

    async fn stats(&self, _request: Request<()>) -> Result<Response<InboxStatsReply>, Status> {
        let subscriptions = self
            .inner
            .subscription_count()
            .await
            .map_err(internal_status)?;
        let offline_messages = self.inner.offline_count().await.map_err(internal_status)?;
        let inflight_messages = self.inner.inflight_count().await.map_err(internal_status)?;
        Ok(Response::new(InboxStatsReply {
            subscriptions: subscriptions as u64,
            offline_messages: offline_messages as u64,
            inflight_messages: inflight_messages as u64,
        }))
    }
}

#[tonic::async_trait]
impl RetainService for RetainRpc {
    async fn write(&self, request: Request<RetainWriteRequest>) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing retained message"))?;
        self.inner
            .retain(from_proto_retain(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn r#match(
        &self,
        request: Request<RetainMatchRequest>,
    ) -> Result<Response<RetainMatchReply>, Status> {
        let request = request.into_inner();
        let messages = self
            .inner
            .match_topic(&request.tenant_id, &request.topic_filter)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RetainMatchReply {
            messages: messages.iter().map(to_proto_retain).collect(),
        }))
    }

    async fn count_retained(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.retained_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }
}

#[tonic::async_trait]
impl BrokerPeerService for BrokerPeerRpc {
    async fn push_delivery(
        &self,
        request: Request<PushDeliveryRequest>,
    ) -> Result<Response<PushDeliveryReply>, Status> {
        let delivery = request
            .into_inner()
            .delivery
            .ok_or_else(|| Status::invalid_argument("missing delivery"))?;
        let delivered = self
            .inner
            .push_delivery(from_proto_delivery(delivery))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(PushDeliveryReply { delivered }))
    }

    async fn push_deliveries(
        &self,
        request: Request<PushDeliveriesRequest>,
    ) -> Result<Response<PushDeliveriesReply>, Status> {
        let deliveries: Vec<_> = request
            .into_inner()
            .deliveries
            .into_iter()
            .map(from_proto_delivery)
            .collect();
        let undelivered = self
            .inner
            .push_deliveries(deliveries.clone())
            .await
            .map_err(internal_status)?;
        let mut remaining = undelivered;
        let mut undelivered_indices = Vec::new();
        for (index, delivery) in deliveries.into_iter().enumerate() {
            if let Some(position) = remaining.iter().position(|current| *current == delivery) {
                remaining.remove(position);
                undelivered_indices.push(index as u32);
            }
        }
        Ok(Response::new(PushDeliveriesReply {
            undelivered_indices,
        }))
    }
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests;
