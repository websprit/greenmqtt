use crate::{
    DistGrpcClient, InboxGrpcClient, KvRangeGrpcClient, MetadataGrpcClient, MetadataPlaneLayout,
    NoopDeliverySink, PeriodicAntiEntropyReconciler, PersistentMetadataRegistry,
    RaftTransportGrpcClient, RangeAdminGrpcClient, RangeControlGrpcClient,
    ReplicatedMetadataRegistry, RetainGrpcClient, RpcRuntime, SessionDictGrpcClient,
    StaticPeerForwarder, StaticServiceEndpointRegistry,
};
use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_broker::{BrokerConfig, BrokerRuntime, DefaultBroker, PeerRegistry};
use greenmqtt_core::{
    BalancerState, BalancerStateRegistry, ClientIdentity, ClusterMembershipRegistry,
    ClusterNodeLifecycle, ClusterNodeMembership, ConnectRequest, InflightMessage, OfflineMessage,
    PublishProperties, PublishRequest, RangeBoundary, RangeReconfigurationRegistry,
    RangeReplica, ReconfigurationPhase, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
    ReplicatedRangeRegistry, RetainedMessage, RouteRecord, ServiceEndpoint,
    ServiceEndpointRegistry, ServiceKind, ServiceShardAssignment, ServiceShardKey,
    ServiceShardKind, ServiceShardLifecycle, ServiceShardRecoveryControl,
    ServiceShardTransition, SessionKind, SessionRecord, Subscription,
};
use greenmqtt_dist::{DistHandle, DistRouter};
use greenmqtt_inbox::PersistentInboxHandle;
use greenmqtt_inbox::{InboxHandle, InboxService};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, MemoryKvRangeRouter};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeSpace, MemoryKvEngine, RocksDbKvEngine,
};
use greenmqtt_kv_raft::{
    AppendEntriesRequest, AppendEntriesResponse, MemoryRaftNode, MemoryRaftStateStore, RaftMessage,
    RaftNode, RaftStateStore, RequestVoteResponse,
};
use greenmqtt_kv_server::{
    HostedRange, KvRangeHost, MemoryKvRangeHost, RangeLifecycleManager, ReplicaRuntime,
};
use greenmqtt_plugin_api::{AllowAllAcl, AllowAllAuth, NoopEventHook};
use greenmqtt_proto::internal::{
    dist_service_client::DistServiceClient, inbox_service_client::InboxServiceClient,
    retain_service_client::RetainServiceClient,
    session_dict_service_client::SessionDictServiceClient, AddRouteRequest, InboxAttachRequest,
    InboxEnqueueRequest, InboxFetchRequest, InboxLookupSubscriptionRequest, InboxSubscribeRequest,
    ListRoutesRequest, ListSessionRoutesRequest, ListSessionsRequest, LookupSessionRequest,
    MatchTopicRequest, RegisterSessionRequest, RetainMatchRequest, RetainWriteRequest,
    ShardSnapshotRequest,
};
use greenmqtt_proto::{
    to_proto_client_identity, to_proto_offline, to_proto_retain, to_proto_route, to_proto_session,
};
use greenmqtt_retain::{ReplicatedRetainHandle, RetainHandle, RetainService};
use greenmqtt_sessiondict::{PersistentSessionDictHandle, SessionDictHandle, SessionDirectory};
use greenmqtt_storage::{
    MemoryInboxStore, MemoryInflightStore, MemorySessionStore, MemorySubscriptionStore,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
struct TestRangeLifecycleManager {
    engine: MemoryKvEngine,
}

#[async_trait]
impl RangeLifecycleManager for TestRangeLifecycleManager {
    async fn create_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<HostedRange> {
        self.engine
            .bootstrap(KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
            self.engine.open_range(&descriptor.id).await?,
        );
        let raft = Arc::new(MemoryRaftNode::single_node(
            descriptor.leader_node_id.unwrap_or(1),
            &descriptor.id,
        ));
        raft.recover().await?;
        Ok(HostedRange {
            descriptor,
            raft,
            space,
        })
    }

    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.engine.destroy_range(range_id).await
    }
}

struct ReconnectingDistClient {
    registry: Arc<StaticServiceEndpointRegistry>,
    tenant_id: String,
    current: DistGrpcClient,
}

impl ReconnectingDistClient {
    async fn connect(
        registry: Arc<StaticServiceEndpointRegistry>,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let current = DistGrpcClient::connect_via_registry(&*registry, tenant_id).await?;
        Ok(Self {
            registry,
            tenant_id: tenant_id.to_string(),
            current,
        })
    }

    async fn list_routes(&mut self) -> anyhow::Result<Vec<RouteRecord>> {
        match self.current.list_routes(Some(&self.tenant_id)).await {
            Ok(routes) => Ok(routes),
            Err(_) => {
                self.current =
                    DistGrpcClient::connect_via_registry(&*self.registry, &self.tenant_id).await?;
                self.current.list_routes(Some(&self.tenant_id)).await
            }
        }
    }
}

fn state_runtime() -> RpcRuntime {
    RpcRuntime {
        sessiondict: Arc::new(SessionDictHandle::default()),
        dist: Arc::new(DistHandle::default()),
        inbox: Arc::new(InboxHandle::default()),
        retain: Arc::new(RetainHandle::default()),
        peer_sink: Arc::new(NoopDeliverySink),
        assignment_registry: None,
        range_host: None,
        range_runtime: None,
    }
}

#[tokio::test]
async fn grpc_round_trip_for_internal_services() {
    let bind = "127.0.0.1:50061".parse().unwrap();
    let server = tokio::spawn(state_runtime().serve(bind));
    sleep(Duration::from_millis(50)).await;

    let endpoint = "http://127.0.0.1:50061";
    let mut session_client = SessionDictServiceClient::connect(endpoint.to_string())
        .await
        .unwrap();
    let mut dist_client = DistServiceClient::connect(endpoint.to_string())
        .await
        .unwrap();
    let mut inbox_client = InboxServiceClient::connect(endpoint.to_string())
        .await
        .unwrap();
    let mut retain_client = RetainServiceClient::connect(endpoint.to_string())
        .await
        .unwrap();

    session_client
        .register_session(RegisterSessionRequest {
            record: Some(to_proto_session(&SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })),
        })
        .await
        .unwrap();

    let lookup = session_client
        .lookup_session(LookupSessionRequest {
            identity: Some(to_proto_client_identity(&ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            })),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(lookup.record.is_some());
    assert_eq!(
        session_client
            .count_sessions(())
            .await
            .unwrap()
            .into_inner()
            .count,
        1
    );
    assert_eq!(
        session_client
            .list_sessions(ListSessionsRequest {
                tenant_id: "t1".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .records
            .len(),
        1
    );

    dist_client
        .add_route(AddRouteRequest {
            route: Some(to_proto_route(&RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: "s1".into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: None,
                kind: SessionKind::Persistent,
            })),
        })
        .await
        .unwrap();
    let matched = dist_client
        .match_topic(MatchTopicRequest {
            tenant_id: "t1".into(),
            topic: "devices/d1/state".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(matched.routes.len(), 1);
    assert_eq!(
        dist_client
            .list_routes(ListRoutesRequest {
                tenant_id: "t1".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .routes
            .len(),
        1
    );
    assert_eq!(
        dist_client
            .list_session_routes(ListSessionRoutesRequest {
                session_id: "s1".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .routes
            .len(),
        1
    );
    assert_eq!(
        dist_client
            .count_routes(())
            .await
            .unwrap()
            .into_inner()
            .count,
        1
    );

    inbox_client
        .attach(InboxAttachRequest {
            session_id: "s1".into(),
        })
        .await
        .unwrap();
    inbox_client
        .subscribe(InboxSubscribeRequest {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: 0,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: String::new(),
            kind: "persistent".into(),
        })
        .await
        .unwrap();
    let looked_up = inbox_client
        .lookup_subscription(InboxLookupSubscriptionRequest {
            session_id: "s1".into(),
            topic_filter: "devices/+/state".into(),
            shared_group: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(looked_up.subscription.is_some());
    inbox_client
        .enqueue(InboxEnqueueRequest {
            message: Some(to_proto_offline(&OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })),
        })
        .await
        .unwrap();
    let fetched = inbox_client
        .fetch(InboxFetchRequest {
            session_id: "s1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.messages.len(), 1);
    let inbox_stats = inbox_client.stats(()).await.unwrap().into_inner();
    assert_eq!(inbox_stats.subscriptions, 1);
    assert_eq!(inbox_stats.offline_messages, 0);
    assert_eq!(inbox_stats.inflight_messages, 0);

    retain_client
        .write(RetainWriteRequest {
            message: Some(to_proto_retain(&greenmqtt_core::RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/d1/state".into(),
                payload: b"retained".to_vec().into(),
                qos: 1,
            })),
        })
        .await
        .unwrap();
    let retained = retain_client
        .r#match(RetainMatchRequest {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retained.messages.len(), 1);
    assert_eq!(
        retain_client
            .count_retained(())
            .await
            .unwrap()
            .into_inner()
            .count,
        1
    );

    server.abort();
}

#[tokio::test]
async fn grpc_round_trip_for_session_online_check_surface() {
    let bind = "127.0.0.1:50111".parse().unwrap();
    let server = tokio::spawn(state_runtime().serve(bind));
    sleep(Duration::from_millis(50)).await;

    let client = SessionDictGrpcClient::connect("http://127.0.0.1:50111")
        .await
        .unwrap();
    client
        .register(SessionRecord {
            session_id: "s-check".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();

    let sessions = client
        .session_exists(&["s-check".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(sessions["s-check"], true);
    assert_eq!(sessions["missing"], false);

    let identities = client
        .identity_exists(&[
            ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u9".into(),
                client_id: "c9".into(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(identities[&("t1".into(), "u1".into(), "c1".into())], true);
    assert_eq!(identities[&("t1".into(), "u9".into(), "c9".into())], false);

    server.abort();
}

#[tokio::test]
async fn grpc_round_trip_for_metadata_service() {
    let bind = "127.0.0.1:50062".parse().unwrap();
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = MetadataGrpcClient::connect("http://127.0.0.1:50062")
        .await
        .unwrap();
    client
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                7,
                "http://127.0.0.1:50062",
            )],
        ))
        .await
        .unwrap();
    let shard = ServiceShardKey::retain("t1");
    client
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-1",
            shard.clone(),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            2,
            Some(7),
            vec![
                RangeReplica::new(7, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(9, ReplicaRole::Learner, ReplicaSyncState::Snapshotting),
            ],
            11,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    client
        .upsert_balancer_state(
            "replica-count",
            BalancerState {
                disabled: false,
                load_rules: BTreeMap::from([("target_voters".into(), "3".into())]),
            },
        )
        .await
        .unwrap();

    let routed = client.route_range(&shard, b"pear").await.unwrap().unwrap();
    assert_eq!(routed.id, "range-1");
    assert_eq!(routed.leader_node_id, Some(7));

    let listed = client
        .list_ranges(Some(ServiceShardKind::Retain), Some("t1"), None)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].config_version, 2);

    let members = client.list_members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].node_id, 7);
    assert_eq!(
        client.lookup_member(7).await.unwrap().unwrap().endpoints[0].kind,
        ServiceKind::Retain
    );

    let states = client.list_balancer_states().await.unwrap();
    assert_eq!(states["replica-count"].load_rules["target_voters"], "3");

    server.abort();
}

#[tokio::test]
async fn replicated_metadata_registry_round_trips_over_kv_range_service() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "__metadata".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let range = engine.open_range("__metadata").await.unwrap();
    let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "__metadata"));
    raft_impl.recover().await.unwrap();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "__metadata",
            ServiceShardKey::retain("meta"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft: raft_impl,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(100)).await;

    let executor = Arc::new(KvRangeGrpcClient::connect(format!("http://{bind}")).await.unwrap());
    let registry = ReplicatedMetadataRegistry::new(executor, "__metadata");
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                9,
                format!("http://{bind}"),
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "meta-range-1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(9),
            vec![RangeReplica::new(
                9,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_balancer_state(
            "retain-balancer",
            BalancerState {
                disabled: false,
                load_rules: BTreeMap::from([("mode".into(), "replicated".into())]),
            },
        )
        .await
        .unwrap();

    assert!(registry.resolve_member(9).await.unwrap().is_some());
    assert!(registry.resolve_range("meta-range-1").await.unwrap().is_some());
    assert!(registry
        .resolve_balancer_state("retain-balancer")
        .await
        .unwrap()
        .is_some());

    server.abort();
}

#[tokio::test]
async fn replicated_metadata_registry_shards_records_across_metadata_ranges() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let engine = MemoryKvEngine::default();
    let layout = MetadataPlaneLayout::sharded("__metadata");
    let host = Arc::new(MemoryKvRangeHost::default());

    for range_id in [
        layout.assignments_range_id.clone(),
        layout.members_range_id.clone(),
        layout.ranges_range_id.clone(),
        layout.balancers_range_id.clone(),
    ] {
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: range_id.clone(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let range = engine.open_range(&range_id).await.unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, &range_id));
        raft_impl.recover().await.unwrap();
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                &range_id,
                ServiceShardKey::retain("meta"),
                RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            raft: raft_impl,
            space: Arc::from(range),
        })
        .await
        .unwrap();
    }

    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(100)).await;

    let executor = Arc::new(KvRangeGrpcClient::connect(format!("http://{bind}")).await.unwrap());
    let registry = ReplicatedMetadataRegistry::with_layout(executor, layout.clone());
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(ServiceKind::Retain, 9, format!("http://{bind}"))],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::retain("t1"),
            ServiceEndpoint::new(ServiceKind::Retain, 9, format!("http://{bind}")),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "meta-range-1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(9),
            vec![RangeReplica::new(
                9,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_balancer_state(
            "dist",
            BalancerState {
                disabled: false,
                load_rules: BTreeMap::from([("mode".into(), "hot".into())]),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        engine
            .open_range(&layout.assignments_range_id)
            .await
            .unwrap()
            .reader()
            .scan(&RangeBoundary::full(), usize::MAX)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        engine
            .open_range(&layout.members_range_id)
            .await
            .unwrap()
            .reader()
            .scan(&RangeBoundary::full(), usize::MAX)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        engine
            .open_range(&layout.ranges_range_id)
            .await
            .unwrap()
            .reader()
            .scan(&RangeBoundary::full(), usize::MAX)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        engine
            .open_range(&layout.balancers_range_id)
            .await
            .unwrap()
            .reader()
            .scan(&RangeBoundary::full(), usize::MAX)
            .await
            .unwrap()
            .len(),
        1
    );

    server.abort();
}

#[tokio::test]
async fn metadata_service_restores_members_and_ranges_after_restart() {
    let tempdir = tempfile::tempdir().unwrap();
    let bind = "127.0.0.1:50100".parse().unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(Arc::new(
                PersistentMetadataRegistry::open(tempdir.path())
                    .await
                    .unwrap(),
            )),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = MetadataGrpcClient::connect("http://127.0.0.1:50100")
        .await
        .unwrap();
    client
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                7,
                "http://127.0.0.1:50100",
            )],
        ))
        .await
        .unwrap();
    client
        .upsert_range(ReplicatedRangeDescriptor::new(
            "retain-range-1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(7),
            vec![RangeReplica::new(
                7,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    server.abort();
    sleep(Duration::from_millis(50)).await;

    let restarted = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(Arc::new(
                PersistentMetadataRegistry::open(tempdir.path())
                    .await
                    .unwrap(),
            )),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let restarted_client = MetadataGrpcClient::connect("http://127.0.0.1:50100")
        .await
        .unwrap();
    let member = restarted_client.lookup_member(7).await.unwrap().unwrap();
    assert_eq!(member.endpoints[0].kind, ServiceKind::Retain);
    let routed = restarted_client
        .route_range(&ServiceShardKey::retain("t1"), b"metric")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(routed.id, "retain-range-1");
    assert_eq!(routed.leader_node_id, Some(7));

    restarted.abort();
}

#[tokio::test]
async fn grpc_round_trip_for_kv_range_service() {
    let bind = "127.0.0.1:50063".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-1".into(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
        })
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let range = engine.open_range("range-1").await.unwrap();
    let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-1"));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl.clone();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = KvRangeGrpcClient::connect("http://127.0.0.1:50063")
        .await
        .unwrap();
    client
        .apply(
            "range-1",
            vec![
                KvMutation {
                    key: b"mango".as_slice().into(),
                    value: Some(b"yellow".as_slice().into()),
                },
                KvMutation {
                    key: b"pear".as_slice().into(),
                    value: Some(b"green".as_slice().into()),
                },
            ],
        )
        .await
        .unwrap();
    let status = raft_impl.status().await.unwrap();
    assert_eq!(status.commit_index, 1);
    assert_eq!(status.applied_index, 1);

    let mango = client.get("range-1", b"mango").await.unwrap().unwrap();
    assert_eq!(mango, b"yellow".as_slice());

    let scanned = client
        .scan(
            "range-1",
            Some(RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec()))),
            10,
        )
        .await
        .unwrap();
    assert_eq!(scanned.len(), 2);

    let checkpoint = client.checkpoint("range-1", "cp-1").await.unwrap();
    assert_eq!(checkpoint.checkpoint_id, "cp-1");
    let snapshot = client.snapshot("range-1").await.unwrap();
    assert_eq!(snapshot.range_id, "range-1");
    assert!(snapshot.boundary.contains(b"pear"));

    server.abort();
}

#[tokio::test]
async fn grpc_round_trip_for_kv_range_service_with_rocksdb_backend() {
    let tempdir = tempfile::tempdir().unwrap();
    let bind = "127.0.0.1:50064".parse().unwrap();
    let engine = RocksDbKvEngine::open(tempdir.path()).unwrap();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-r".into(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
        })
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let range = engine.open_range("range-r").await.unwrap();
    let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-r"));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl;
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-r",
            ServiceShardKey::retain("t1"),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = KvRangeGrpcClient::connect("http://127.0.0.1:50064")
        .await
        .unwrap();
    client
        .apply(
            "range-r",
            vec![KvMutation {
                key: b"mango".as_slice().into(),
                value: Some(b"yellow".as_slice().into()),
            }],
        )
        .await
        .unwrap();

    let value = client.get("range-r", b"mango").await.unwrap().unwrap();
    assert_eq!(value, b"yellow".as_slice());
    let snapshot = client.snapshot("range-r").await.unwrap();
    assert_eq!(snapshot.range_id, "range-r");
    assert!(std::path::Path::new(&snapshot.data_path).exists());

    server.abort();
}

#[tokio::test]
async fn kv_range_service_rejects_reads_from_follower_replica() {
    let bind = "127.0.0.1:50101".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-follower".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let range = engine.open_range("range-follower").await.unwrap();
    range
        .writer()
        .apply(vec![KvMutation {
            key: b"alpha".as_slice().into(),
            value: Some(b"one".as_slice().into()),
        }])
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let raft_impl = Arc::new(MemoryRaftNode::new(
        2,
        "range-follower",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
    ));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl;
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-follower",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                2,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = KvRangeGrpcClient::connect("http://127.0.0.1:50101")
        .await
        .unwrap();
    assert!(client.get("range-follower", b"alpha").await.is_err());
    assert!(client.scan("range-follower", None, 10).await.is_err());

    server.abort();
}

#[tokio::test]
async fn kv_range_service_requires_leader_lease_for_reads() {
    let bind = "127.0.0.1:50102".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-lease".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let range = engine.open_range("range-lease").await.unwrap();
    range
        .writer()
        .apply(vec![KvMutation {
            key: b"alpha".as_slice().into(),
            value: Some(b"one".as_slice().into()),
        }])
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let raft_impl = Arc::new(MemoryRaftNode::new(
        1,
        "range-lease",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
    ));
    raft_impl.recover().await.unwrap();
    for _ in 0..8 {
        raft_impl.tick().await.unwrap();
    }
    raft_impl
        .receive(
            2,
            RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: raft_impl.status().await.unwrap().current_term,
                vote_granted: true,
            }),
        )
        .await
        .unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl.clone();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-lease",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = KvRangeGrpcClient::connect("http://127.0.0.1:50102")
        .await
        .unwrap();
    assert!(client.get("range-lease", b"alpha").await.is_err());

    raft_impl
        .receive(
            2,
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: raft_impl.status().await.unwrap().current_term,
                success: true,
                match_index: 0,
            }),
        )
        .await
        .unwrap();
    let value = client.get("range-lease", b"alpha").await.unwrap().unwrap();
    assert_eq!(value, b"one".as_slice());

    server.abort();
}

#[tokio::test]
async fn kv_range_service_rejects_stale_epoch_requests() {
    let bind = "127.0.0.1:50103".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-epoch".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let range = engine.open_range("range-epoch").await.unwrap();
    range
        .writer()
        .apply(vec![KvMutation {
            key: b"alpha".as_slice().into(),
            value: Some(b"one".as_slice().into()),
        }])
        .await
        .unwrap();
    let raft: Arc<dyn RaftNode> = Arc::new(MemoryRaftNode::single_node(1, "range-epoch"));
    raft.recover().await.unwrap();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-epoch",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            3,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = KvRangeGrpcClient::connect("http://127.0.0.1:50103")
        .await
        .unwrap();
    let error = client
        .get_fenced("range-epoch", b"alpha", Some(2))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("kv/epoch-mismatch"));

    server.abort();
}

#[tokio::test]
async fn kv_range_service_rejects_non_serving_range_requests() {
    let bind = "127.0.0.1:50104".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-draining".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let range = engine.open_range("range-draining").await.unwrap();
    range
        .writer()
        .apply(vec![KvMutation {
            key: b"alpha".as_slice().into(),
            value: Some(b"one".as_slice().into()),
        }])
        .await
        .unwrap();
    let raft: Arc<dyn RaftNode> = Arc::new(MemoryRaftNode::single_node(1, "range-draining"));
    raft.recover().await.unwrap();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-draining",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            4,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Draining,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = KvRangeGrpcClient::connect("http://127.0.0.1:50104")
        .await
        .unwrap();
    let error = client
        .get_fenced("range-draining", b"alpha", Some(4))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("kv/config-changing"));

    server.abort();
}

#[tokio::test]
async fn grpc_round_trip_for_raft_transport_service() {
    let bind = "127.0.0.1:50103".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-transport".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let range = engine.open_range("range-transport").await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let state_store = Arc::new(MemoryRaftStateStore::default());
    let raft_impl = Arc::new(MemoryRaftNode::with_state_store(
        2,
        "range-transport",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
        state_store.clone(),
    ));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl.clone();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-transport",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                2,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = RaftTransportGrpcClient::connect("http://127.0.0.1:50103")
        .await
        .unwrap();
    client
        .send(
            "range-transport",
            1,
            &RaftMessage::AppendEntries(AppendEntriesRequest {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![greenmqtt_kv_raft::RaftLogEntry {
                    term: 1,
                    index: 1,
                    command: b"put:a".as_slice().into(),
                }],
                leader_commit: 1,
            }),
        )
        .await
        .unwrap();

    let persisted = state_store.load("range-transport").await.unwrap().unwrap();
    assert_eq!(persisted.current_term, 1);
    assert_eq!(persisted.commit_index, 1);
    assert_eq!(persisted.log.len(), 1);
    let outbound = raft_impl.drain_outbox().await.unwrap();
    assert!(outbound.iter().any(|message| matches!(
        &message.message,
        RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
            success: true,
            match_index: 1,
            ..
        })
    )));

    server.abort();
}

#[tokio::test]
async fn raft_transport_service_delivers_install_snapshot_messages() {
    let bind = "127.0.0.1:50104".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-transport-snapshot".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let range = engine.open_range("range-transport-snapshot").await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let state_store = Arc::new(MemoryRaftStateStore::default());
    let raft_impl = Arc::new(MemoryRaftNode::with_state_store(
        2,
        "range-transport-snapshot",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2, 3],
            learners: Vec::new(),
        },
        state_store.clone(),
    ));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl.clone();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-transport-snapshot",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                2,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = RaftTransportGrpcClient::connect("http://127.0.0.1:50104")
        .await
        .unwrap();
    client
        .send(
            "range-transport-snapshot",
            1,
            &RaftMessage::InstallSnapshot(greenmqtt_kv_raft::InstallSnapshotRequest {
                term: 5,
                leader_id: 1,
                snapshot: greenmqtt_kv_raft::RaftSnapshot {
                    range_id: "range-transport-snapshot".into(),
                    term: 5,
                    index: 7,
                    payload: b"snapshot".as_slice().into(),
                },
            }),
        )
        .await
        .unwrap();

    let persisted = state_store
        .load("range-transport-snapshot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.current_term, 5);
    assert_eq!(persisted.commit_index, 7);
    assert_eq!(persisted.applied_index, 7);
    assert!(persisted.latest_snapshot.is_some());

    server.abort();
}

#[tokio::test]
async fn range_admin_service_reports_health_and_debug_dump() {
    let bind = "127.0.0.1:50105".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-admin".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let range = engine.open_range("range-admin").await.unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-admin"));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl;
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-admin",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = RangeAdminGrpcClient::connect("http://127.0.0.1:50105")
        .await
        .unwrap();
    let listed = client.list_range_health().await.unwrap();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].range_id, "range-admin");
    let listed_reconfig = listed.entries[0].reconfiguration.as_ref().unwrap();
    assert_eq!(listed_reconfig.current_voters, vec![1]);
    assert!(listed_reconfig.pending_voters.is_empty());
    assert!(!listed_reconfig.blocked_on_catch_up);
    let fetched = client.get_range_health("range-admin").await.unwrap();
    let fetched = fetched.health.unwrap();
    assert_eq!(fetched.range_id, "range-admin");
    assert_eq!(
        fetched.reconfiguration.unwrap().current_voters,
        vec![1]
    );
    let dump = client.debug_dump().await.unwrap();
    assert!(dump.contains("range=range-admin"));
    assert!(dump.contains("current_voters=[1]"));

    server.abort();
}

#[tokio::test]
async fn grpc_round_trip_for_range_control_service() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(crate::NoopReplicaTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine.clone(),
        })),
        Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host.clone()),
            range_runtime: Some(runtime.clone()),
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(100)).await;

    let client = crate::RangeControlGrpcClient::connect(format!("http://{bind}"))
        .await
        .unwrap();
    let range_id = client
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-control",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Bootstrapping,
        ))
        .await
        .unwrap();
    assert_eq!(range_id, "range-control");

    client
        .change_replicas("range-control", vec![1, 2], Vec::new())
        .await
        .unwrap();
    client
        .change_replicas("range-control", vec![1, 2], Vec::new())
        .await
        .unwrap();
    client
        .transfer_leadership("range-control", 2)
        .await
        .unwrap();
    client.recover_range("range-control", 1).await.unwrap();

    let (left, right) = client
        .split_range("range-control", b"m".to_vec())
        .await
        .unwrap();
    let merged = client.merge_ranges(&left, &right).await.unwrap();
    assert!(merged.contains(&left));
    client.drain_range(&merged).await.unwrap();
    let zombies = client.list_zombie_ranges().await.unwrap();
    assert!(zombies.iter().any(|entry| entry.range_id == merged));
    client.retire_range(&merged).await.unwrap();

    server.abort();
}

#[tokio::test]
async fn range_control_service_is_idempotent_for_repeated_operator_commands() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(crate::NoopReplicaTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine.clone(),
        })),
        Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host.clone()),
            range_runtime: Some(runtime.clone()),
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(100)).await;

    let client = crate::RangeControlGrpcClient::connect(format!("http://{bind}"))
        .await
        .unwrap();
    let descriptor = ReplicatedRangeDescriptor::new(
        "range-idem",
        ServiceShardKey::retain("t1"),
        RangeBoundary::full(),
        1,
        1,
        Some(1),
        vec![RangeReplica::new(
            1,
            ReplicaRole::Voter,
            ReplicaSyncState::Replicating,
        )],
        0,
        0,
        ServiceShardLifecycle::Bootstrapping,
    );
    let range_id = client.bootstrap_range(descriptor.clone()).await.unwrap();
    assert_eq!(range_id, "range-idem");
    let second = client.bootstrap_range(descriptor).await.unwrap();
    assert_eq!(second, "range-idem");

    client.transfer_leadership("range-idem", 1).await.unwrap();
    client.transfer_leadership("range-idem", 1).await.unwrap();
    client.recover_range("range-idem", 1).await.unwrap();
    client.recover_range("range-idem", 1).await.unwrap();

    let (left, right) = client.split_range("range-idem", b"m".to_vec()).await.unwrap();
    let (left_again, right_again) = client.split_range("range-idem", b"m".to_vec()).await.unwrap();
    assert_eq!((left_again, right_again), (left.clone(), right.clone()));

    let merged = client.merge_ranges(&left, &right).await.unwrap();
    let merged_again = client.merge_ranges(&left, &right).await.unwrap();
    assert_eq!(merged_again, merged);

    client.drain_range(&merged).await.unwrap();
    client.drain_range(&merged).await.unwrap();
    client.retire_range(&merged).await.unwrap();
    client.retire_range(&merged).await.unwrap();

    server.abort();
}

#[tokio::test]
async fn range_control_service_persists_reconfiguration_state_into_metadata_registry() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(crate::NoopReplicaTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine.clone(),
        })),
        Duration::from_secs(1),
        1024,
        64,
        64,
    ));
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-reconfig".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let range = engine.open_range("range-reconfig").await.unwrap();
    let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "range-reconfig"));
    raft_impl.recover().await.unwrap();
    let _ = raft_impl.propose(Bytes::from_static(b"cmd")).await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl.clone();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-reconfig",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: Some(host),
            range_runtime: Some(runtime.clone()),
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let client = RangeControlGrpcClient::connect(format!("http://{bind}"))
        .await
        .unwrap();
    client
        .change_replicas("range-reconfig", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let error = client
        .change_replicas("range-reconfig", vec![1, 2], Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("config-changing"));
    let state = registry
        .resolve_reconfiguration_state("range-reconfig")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.old_voters, vec![1]);
    assert_eq!(state.current_voters, vec![1]);
    assert_eq!(state.joint_voters, vec![1, 2]);
    assert_eq!(state.pending_voters, vec![1, 2]);
    assert_eq!(state.phase, Some(ReconfigurationPhase::JointConsensus));
    assert!(state.blocked_on_catch_up);

    server.abort();
}

#[tokio::test]
async fn multi_node_replicated_metadata_range_control_and_zombie_detection_work_together() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metadata_bind = listener.local_addr().unwrap();
    drop(listener);
    let layout = MetadataPlaneLayout::sharded("__metadata");
    let metadata_engine = MemoryKvEngine::default();
    let metadata_host = Arc::new(MemoryKvRangeHost::default());
    for range_id in [
        layout.assignments_range_id.clone(),
        layout.members_range_id.clone(),
        layout.ranges_range_id.clone(),
        layout.balancers_range_id.clone(),
    ] {
        metadata_engine
            .bootstrap(KvRangeBootstrap {
                range_id: range_id.clone(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let range = metadata_engine.open_range(&range_id).await.unwrap();
        let raft_impl = Arc::new(MemoryRaftNode::single_node(1, &range_id));
        raft_impl.recover().await.unwrap();
        metadata_host
            .add_range(HostedRange {
                descriptor: ReplicatedRangeDescriptor::new(
                    &range_id,
                    ServiceShardKey::retain("meta"),
                    RangeBoundary::full(),
                    1,
                    1,
                    Some(1),
                    vec![RangeReplica::new(
                        1,
                        ReplicaRole::Voter,
                        ReplicaSyncState::Replicating,
                    )],
                    0,
                    0,
                    ServiceShardLifecycle::Serving,
                ),
                raft: raft_impl,
                space: Arc::from(range),
            })
            .await
            .unwrap();
    }
    let metadata_server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(metadata_host),
            range_runtime: None,
        }
        .serve(metadata_bind),
    );
    sleep(Duration::from_millis(50)).await;

    let metadata_registry = Arc::new(ReplicatedMetadataRegistry::with_layout(
        Arc::new(KvRangeGrpcClient::connect(format!("http://{metadata_bind}")).await.unwrap()),
        layout.clone(),
    ));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let node1_bind = listener.local_addr().unwrap();
    drop(listener);
    let node1_engine = MemoryKvEngine::default();
    let node1_host = Arc::new(MemoryKvRangeHost::default());
    let node1_runtime = Arc::new(ReplicaRuntime::with_config(
        node1_host.clone(),
        Arc::new(crate::NoopReplicaTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: node1_engine.clone(),
        })),
        Duration::from_secs(1),
        1024,
        64,
        64,
    ));
    node1_runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-node1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Bootstrapping,
        ))
        .await
        .unwrap();
    metadata_registry
        .upsert_member(ClusterNodeMembership::new(
            1,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                1,
                format!("http://{node1_bind}"),
            )],
        ))
        .await
        .unwrap();
    metadata_registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-node1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let node1_server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(metadata_registry.clone()),
            range_host: Some(node1_host),
            range_runtime: Some(node1_runtime.clone()),
        }
        .serve(node1_bind),
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let node2_bind = listener.local_addr().unwrap();
    drop(listener);
    let node2_engine = MemoryKvEngine::default();
    node2_engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-zombie-node2".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let zombie_space = Arc::<dyn KvRangeSpace>::from(
        node2_engine.open_range("range-zombie-node2").await.unwrap(),
    );
    let zombie_raft = Arc::new(MemoryRaftNode::new(
        2,
        "range-zombie-node2",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![2, 3],
            learners: Vec::new(),
        },
    ));
    zombie_raft.recover().await.unwrap();
    let node2_host = Arc::new(MemoryKvRangeHost::default());
    node2_host
        .add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-zombie-node2",
                ServiceShardKey::retain("t2"),
                RangeBoundary::full(),
                1,
                1,
                None,
                vec![RangeReplica::new(
                    2,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Recovering,
            ),
            raft: zombie_raft,
            space: zombie_space,
        })
        .await
        .unwrap();
    metadata_registry
        .upsert_member(ClusterNodeMembership::new(
            2,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                2,
                format!("http://{node2_bind}"),
            )],
        ))
        .await
        .unwrap();
    let node2_server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(metadata_registry.clone()),
            range_host: Some(node2_host),
            range_runtime: None,
        }
        .serve(node2_bind),
    );
    sleep(Duration::from_millis(100)).await;

    let range_control = RangeControlGrpcClient::connect(format!("http://{node1_bind}"))
        .await
        .unwrap();
    range_control
        .change_replicas("range-node1", vec![1, 2], Vec::new())
        .await
        .unwrap();
    let reconfig = metadata_registry
        .resolve_reconfiguration_state("range-node1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconfig.current_voters, vec![1]);
    assert_eq!(reconfig.pending_voters, vec![1, 2]);
    assert!(reconfig.blocked_on_catch_up || reconfig.phase.is_some());

    let range_admin = RangeAdminGrpcClient::connect(format!("http://{node2_bind}"))
        .await
        .unwrap();
    let zombies = range_admin.list_range_health().await.unwrap();
    assert!(zombies
        .entries
        .iter()
        .any(|entry| entry.range_id == "range-zombie-node2" && !entry.has_leader_node_id));

    metadata_server.abort();
    node1_server.abort();
    node2_server.abort();
}

#[tokio::test]
async fn replicated_retain_can_use_kv_range_grpc_executor() {
    let bind = "127.0.0.1:50065".parse().unwrap();
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "retain-range-1".into(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
        })
        .await
        .unwrap();
    let host = Arc::new(MemoryKvRangeHost::default());
    let range = engine.open_range("retain-range-1").await.unwrap();
    let raft_impl = Arc::new(MemoryRaftNode::single_node(1, "retain-range-1"));
    raft_impl.recover().await.unwrap();
    let raft: Arc<dyn RaftNode> = raft_impl;
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "retain-range-1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            1,
            Some(1),
            vec![
                RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(2, ReplicaRole::Voter, ReplicaSyncState::Replicating),
            ],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ),
        raft,
        space: Arc::from(range),
    })
    .await
    .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: Some(host),
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "retain-range-1",
            ServiceShardKey::retain("t1"),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            1,
            Some(1),
            vec![RangeReplica::new(
                1,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let executor: Arc<dyn KvRangeExecutor> = Arc::new(
        KvRangeGrpcClient::connect("http://127.0.0.1:50065")
            .await
            .unwrap(),
    );
    let retain = ReplicatedRetainHandle::new(router, executor);

    retain
        .retain(RetainedMessage {
            tenant_id: "t1".into(),
            topic: "metrics/cpu".into(),
            payload: b"high".to_vec().into(),
            qos: 0,
        })
        .await
        .unwrap();
    let retained = retain
        .lookup_topic("t1", "metrics/cpu")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.payload, b"high".as_slice());
    assert_eq!(retain.match_topic("t1", "#").await.unwrap().len(), 1);

    server.abort();
}

#[tokio::test]
async fn shard_endpoint_registry_resolves_grpc_clients_by_service_shard() {
    let bind = "127.0.0.1:50071".parse().unwrap();
    let server = tokio::spawn(state_runtime().serve(bind));
    sleep(Duration::from_millis(50)).await;

    let registry = StaticServiceEndpointRegistry::default();
    let endpoint = "http://127.0.0.1:50071";
    let session_identity = ClientIdentity {
        tenant_id: "t1".into(),
        user_id: "u1".into(),
        client_id: "c1".into(),
    };
    for assignment in [
        ServiceShardAssignment::new(
            ServiceShardKey {
                kind: greenmqtt_core::ServiceShardKind::SessionDict,
                tenant_id: "t1".into(),
                scope: "identity:u1:c1".into(),
            },
            ServiceEndpoint::new(ServiceKind::SessionDict, 1, endpoint),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ),
        ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 1, endpoint),
            1,
            11,
            ServiceShardLifecycle::Serving,
        ),
        ServiceShardAssignment::new(
            ServiceShardKey::inbox("t1", "s1"),
            ServiceEndpoint::new(ServiceKind::Inbox, 1, endpoint),
            1,
            12,
            ServiceShardLifecycle::Serving,
        ),
        ServiceShardAssignment::new(
            ServiceShardKey::retain("t1"),
            ServiceEndpoint::new(ServiceKind::Retain, 1, endpoint),
            1,
            13,
            ServiceShardLifecycle::Serving,
        ),
    ] {
        registry.upsert_assignment(assignment).await.unwrap();
    }

    let session_client = SessionDictGrpcClient::connect_via_registry(&registry, &session_identity)
        .await
        .unwrap();
    let dist_client = DistGrpcClient::connect_via_registry(&registry, "t1")
        .await
        .unwrap();
    let inbox_client = InboxGrpcClient::connect_via_registry(&registry, "t1", "s1")
        .await
        .unwrap();
    let retain_client = RetainGrpcClient::connect_via_registry(&registry, "t1")
        .await
        .unwrap();

    assert_eq!(
        registry
            .list_assignments(Some(greenmqtt_core::ServiceShardKind::Dist))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(session_client.session_count().await.unwrap(), 0);
    assert_eq!(dist_client.route_count().await.unwrap(), 0);
    assert_eq!(inbox_client.subscription_count().await.unwrap(), 0);
    assert_eq!(retain_client.retained_count().await.unwrap(), 0);

    server.abort();
}

#[tokio::test]
async fn brokers_forward_cross_node_deliveries_over_grpc() {
    let state_bind = "127.0.0.1:50062".parse().unwrap();
    let state_server = tokio::spawn(state_runtime().serve(state_bind));
    sleep(Duration::from_millis(50)).await;

    let shared_endpoint = "http://127.0.0.1:50062";
    let peer1 = StaticPeerForwarder::default();
    let peer2 = StaticPeerForwarder::default();

    let broker1: Arc<DefaultBroker> = Arc::new(BrokerRuntime::with_cluster(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(peer1.clone()),
        Arc::new(
            SessionDictGrpcClient::connect(shared_endpoint)
                .await
                .unwrap(),
        ),
        Arc::new(DistGrpcClient::connect(shared_endpoint).await.unwrap()),
        Arc::new(InboxGrpcClient::connect(shared_endpoint).await.unwrap()),
        Arc::new(RetainGrpcClient::connect(shared_endpoint).await.unwrap()),
    ));
    let broker2: Arc<DefaultBroker> = Arc::new(BrokerRuntime::with_cluster(
        BrokerConfig {
            node_id: 2,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(peer2.clone()),
        Arc::new(
            SessionDictGrpcClient::connect(shared_endpoint)
                .await
                .unwrap(),
        ),
        Arc::new(DistGrpcClient::connect(shared_endpoint).await.unwrap()),
        Arc::new(InboxGrpcClient::connect(shared_endpoint).await.unwrap()),
        Arc::new(RetainGrpcClient::connect(shared_endpoint).await.unwrap()),
    ));

    let peer_bind_1 = "127.0.0.1:50063".parse().unwrap();
    let peer_bind_2 = "127.0.0.1:50064".parse().unwrap();
    let peer_server_1 = tokio::spawn(
        RpcRuntime {
            sessiondict: broker1.sessiondict.clone(),
            dist: broker1.dist.clone(),
            inbox: broker1.inbox.clone(),
            retain: broker1.retain.clone(),
            peer_sink: broker1.clone(),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(peer_bind_1),
    );
    let peer_server_2 = tokio::spawn(
        RpcRuntime {
            sessiondict: broker2.sessiondict.clone(),
            dist: broker2.dist.clone(),
            inbox: broker2.inbox.clone(),
            retain: broker2.retain.clone(),
            peer_sink: broker2.clone(),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(peer_bind_2),
    );
    sleep(Duration::from_millis(50)).await;

    peer1
        .connect_node(2, "http://127.0.0.1:50064")
        .await
        .unwrap();
    peer2
        .connect_node(1, "http://127.0.0.1:50063")
        .await
        .unwrap();
    assert_eq!(peer1.configured_nodes(), vec![2]);
    assert_eq!(peer2.configured_nodes(), vec![1]);
    assert_eq!(
        peer1.list_peer_endpoints(),
        BTreeMap::from([(2, "http://127.0.0.1:50064".to_string())])
    );
    assert_eq!(
        peer2.list_peer_endpoints(),
        BTreeMap::from([(1, "http://127.0.0.1:50063".to_string())])
    );

    let subscriber = broker2
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker2
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let publisher = broker1
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let outcome = broker1
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"remote".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome.online_deliveries, 1);
    assert_eq!(peer1.push_deliveries_calls(), 1);
    assert_eq!(peer1.push_delivery_calls(), 0);

    let deliveries = broker2
        .drain_deliveries(&subscriber.session.session_id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].payload, b"remote".to_vec());
    assert!(peer1.disconnect_node(2));
    assert!(peer1.configured_nodes().is_empty());
    assert!(peer1.list_peer_endpoints().is_empty());
    assert!(!peer1.disconnect_node(2));

    peer_server_1.abort();
    peer_server_2.abort();
    state_server.abort();
}

#[tokio::test]
async fn static_peer_forwarder_replacing_existing_node_updates_endpoint_registry() {
    let bind1 = "127.0.0.1:50071".parse().unwrap();
    let bind2 = "127.0.0.1:50072".parse().unwrap();
    let server1 = tokio::spawn(state_runtime().serve(bind1));
    let server2 = tokio::spawn(state_runtime().serve(bind2));
    sleep(Duration::from_millis(50)).await;

    let peers = StaticPeerForwarder::default();

    peers
        .add_peer_node(7, "http://127.0.0.1:50071".into())
        .await
        .unwrap();
    peers
        .add_peer_node(7, "http://127.0.0.1:50072".into())
        .await
        .unwrap();

    assert_eq!(peers.configured_nodes(), vec![7]);
    assert_eq!(
        peers.list_peer_endpoints(),
        BTreeMap::from([(7, "http://127.0.0.1:50072".to_string())])
    );
    assert!(peers.remove_peer_node(7));
    assert!(peers.list_peer_endpoints().is_empty());

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn membership_registry_tracks_join_suspect_and_leave() {
    let registry = StaticServiceEndpointRegistry::default();
    let endpoint = ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070");

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Joining,
            vec![endpoint.clone()],
        ))
        .await
        .unwrap();

    let member = registry.resolve_member(7).await.unwrap().unwrap();
    assert_eq!(member.lifecycle, ClusterNodeLifecycle::Joining);
    assert_eq!(member.endpoints, vec![endpoint.clone()]);

    registry
        .set_member_lifecycle(7, ClusterNodeLifecycle::Suspect)
        .await
        .unwrap();
    let suspect = registry.resolve_member(7).await.unwrap().unwrap();
    assert_eq!(suspect.lifecycle, ClusterNodeLifecycle::Suspect);

    let members = registry.list_members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].node_id, 7);

    let removed = registry.remove_member(7).await.unwrap().unwrap();
    assert_eq!(removed.lifecycle, ClusterNodeLifecycle::Suspect);
    assert!(registry.resolve_member(7).await.unwrap().is_none());
}

#[tokio::test]
async fn dynamic_registry_sync_members_reconciles_membership_view() {
    let registry = StaticServiceEndpointRegistry::default();
    let first = ClusterNodeMembership::new(
        7,
        1,
        ClusterNodeLifecycle::Serving,
        vec![ServiceEndpoint::new(
            ServiceKind::Dist,
            7,
            "http://127.0.0.1:50070",
        )],
    );
    let second = ClusterNodeMembership::new(
        9,
        1,
        ClusterNodeLifecycle::Joining,
        vec![ServiceEndpoint::new(
            ServiceKind::Broker,
            9,
            "http://127.0.0.1:50090",
        )],
    );

    assert_eq!(
        registry
            .sync_members(vec![first.clone(), second.clone()])
            .await
            .unwrap(),
        2
    );
    let members = registry.list_members().await.unwrap();
    assert_eq!(members, vec![first.clone(), second.clone()]);

    assert_eq!(registry.sync_members(vec![first.clone()]).await.unwrap(), 1);
    let members = registry.list_members().await.unwrap();
    assert_eq!(members, vec![first]);
}

#[tokio::test]
async fn dynamic_registry_sync_members_updates_endpoints_after_partition_recovery() {
    let registry = StaticServiceEndpointRegistry::default();
    let first = ClusterNodeMembership::new(
        7,
        1,
        ClusterNodeLifecycle::Serving,
        vec![ServiceEndpoint::new(
            ServiceKind::Dist,
            7,
            "http://127.0.0.1:50070",
        )],
    );
    registry.sync_members(vec![first.clone()]).await.unwrap();
    assert_eq!(
        registry.resolve_member(7).await.unwrap().unwrap().endpoints[0].endpoint,
        "http://127.0.0.1:50070"
    );

    let recovered = ClusterNodeMembership::new(
        7,
        2,
        ClusterNodeLifecycle::Serving,
        vec![ServiceEndpoint::new(
            ServiceKind::Dist,
            7,
            "http://127.0.0.1:50071",
        )],
    );
    registry
        .sync_members(vec![recovered.clone()])
        .await
        .unwrap();
    let member = registry.resolve_member(7).await.unwrap().unwrap();
    assert_eq!(member.epoch, 2);
    assert_eq!(member.endpoints[0].endpoint, "http://127.0.0.1:50071");
}

#[tokio::test]
async fn dynamic_registry_tracks_replicated_ranges_and_routes_keys() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::retain("tenant-a");
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-a",
            shard.clone(),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"n".to_vec())),
            1,
            2,
            Some(7),
            vec![
                RangeReplica::new(7, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(9, ReplicaRole::Voter, ReplicaSyncState::Replicating),
            ],
            10,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-b",
            shard.clone(),
            RangeBoundary::new(Some(b"n".to_vec()), Some(b"z".to_vec())),
            1,
            3,
            Some(9),
            vec![
                RangeReplica::new(9, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(11, ReplicaRole::Learner, ReplicaSyncState::Snapshotting),
            ],
            12,
            11,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let ranges = registry
        .list_shard_ranges(&shard)
        .await
        .unwrap()
        .into_iter()
        .map(|descriptor| descriptor.id)
        .collect::<Vec<_>>();
    assert_eq!(ranges, vec!["range-a".to_string(), "range-b".to_string()]);

    let routed = registry
        .route_range_for_key(&shard, b"pear")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(routed.id, "range-b");
    assert_eq!(routed.leader_node_id, Some(9));
    assert_eq!(routed.config_version, 3);

    let removed = registry.remove_range("range-a").await.unwrap().unwrap();
    assert_eq!(removed.id, "range-a");
    assert!(registry.resolve_range("range-a").await.unwrap().is_none());
}

#[tokio::test]
async fn dynamic_registry_tracks_balancer_states() {
    let registry = StaticServiceEndpointRegistry::default();
    let previous = registry
        .upsert_balancer_state(
            "replica-count",
            BalancerState {
                disabled: false,
                load_rules: BTreeMap::from([("target_voters".to_string(), "3".to_string())]),
            },
        )
        .await
        .unwrap();
    assert!(previous.is_none());

    let snapshot = registry.list_balancer_states().await.unwrap();
    assert_eq!(
        snapshot.get("replica-count").unwrap().load_rules["target_voters"],
        "3"
    );

    let removed = registry
        .remove_balancer_state("replica-count")
        .await
        .unwrap()
        .unwrap();
    assert!(!removed.disabled);
    assert!(registry
        .resolve_balancer_state("replica-count")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn persistent_metadata_registry_restores_assignments_ranges_and_balancer_state() {
    let tempdir = tempfile::tempdir().unwrap();
    let registry = PersistentMetadataRegistry::open(tempdir.path())
        .await
        .unwrap();
    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-a",
            shard,
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            1,
            2,
            Some(7),
            vec![RangeReplica::new(
                7,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_balancer_state(
            "leader-balance",
            BalancerState {
                disabled: false,
                load_rules: BTreeMap::from([("max_skew".into(), "1".into())]),
            },
        )
        .await
        .unwrap();

    let reopened = PersistentMetadataRegistry::open(tempdir.path())
        .await
        .unwrap();
    assert!(reopened
        .resolve_assignment(&ServiceShardKey::dist("t1"))
        .await
        .unwrap()
        .is_some());
    assert!(reopened.resolve_range("range-a").await.unwrap().is_some());
    assert_eq!(
        reopened
            .resolve_balancer_state("leader-balance")
            .await
            .unwrap()
            .unwrap()
            .load_rules["max_skew"],
        "1"
    );
}

#[tokio::test]
async fn shard_recovery_control_applies_rebalance_and_failover() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::dist("t1");
    let source = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
        1,
        10,
        ServiceShardLifecycle::Serving,
    );
    registry.upsert_assignment(source.clone()).await.unwrap();

    let rebalanced = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Dist, 9, "http://127.0.0.1:50090"),
        2,
        11,
        ServiceShardLifecycle::Draining,
    );
    registry
        .apply_transition(ServiceShardTransition::new(
            greenmqtt_core::ServiceShardTransitionKind::Rebalance,
            shard.clone(),
            Some(7),
            rebalanced.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(
        registry
            .resolve_assignment(&shard)
            .await
            .unwrap()
            .unwrap()
            .owner_node_id(),
        9
    );

    let failed_over = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Dist, 11, "http://127.0.0.1:50110"),
        3,
        12,
        ServiceShardLifecycle::Recovering,
    );
    registry
        .apply_transition(ServiceShardTransition::new(
            greenmqtt_core::ServiceShardTransitionKind::Failover,
            shard.clone(),
            Some(9),
            failed_over.clone(),
        ))
        .await
        .unwrap();
    let assignment = registry.resolve_assignment(&shard).await.unwrap().unwrap();
    assert_eq!(assignment.owner_node_id(), 11);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Recovering);
    assert_eq!(assignment.epoch, 3);
    assert_eq!(assignment.fencing_token, 12);
}

#[tokio::test]
async fn shard_recovery_control_supports_migration_bootstrap_catchup_and_anti_entropy() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::inbox("t1", "s1");

    let bootstrapping = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Inbox, 5, "http://127.0.0.1:5050"),
        1,
        20,
        ServiceShardLifecycle::Bootstrapping,
    );
    registry
        .apply_transition(ServiceShardTransition::new(
            greenmqtt_core::ServiceShardTransitionKind::Bootstrap,
            shard.clone(),
            None,
            bootstrapping,
        ))
        .await
        .unwrap();

    let catch_up = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Inbox, 5, "http://127.0.0.1:5050"),
        2,
        21,
        ServiceShardLifecycle::Recovering,
    );
    registry
        .apply_transition(ServiceShardTransition::new(
            greenmqtt_core::ServiceShardTransitionKind::CatchUp,
            shard.clone(),
            Some(5),
            catch_up,
        ))
        .await
        .unwrap();

    let migrated = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Inbox, 6, "http://127.0.0.1:5060"),
        3,
        22,
        ServiceShardLifecycle::Draining,
    );
    registry
        .apply_transition(ServiceShardTransition::new(
            greenmqtt_core::ServiceShardTransitionKind::Migration,
            shard.clone(),
            Some(5),
            migrated,
        ))
        .await
        .unwrap();

    let repaired = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Inbox, 6, "http://127.0.0.1:5060"),
        4,
        23,
        ServiceShardLifecycle::Serving,
    );
    registry
        .apply_transition(ServiceShardTransition::new(
            greenmqtt_core::ServiceShardTransitionKind::AntiEntropy,
            shard.clone(),
            Some(6),
            repaired,
        ))
        .await
        .unwrap();

    let assignment = registry.resolve_assignment(&shard).await.unwrap().unwrap();
    assert_eq!(assignment.owner_node_id(), 6);
    assert_eq!(assignment.epoch, 4);
    assert_eq!(assignment.fencing_token, 23);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Serving);
}

#[tokio::test]
async fn transition_shard_to_member_uses_registered_member_endpoint_and_increments_tokens() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50070",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50090",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            3,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let assignment = registry
        .transition_shard_to_member(
            greenmqtt_core::ServiceShardTransitionKind::Failover,
            shard.clone(),
            Some(7),
            9,
            ServiceShardLifecycle::Recovering,
        )
        .await
        .unwrap();

    assert_eq!(assignment.owner_node_id(), 9);
    assert_eq!(assignment.endpoint.endpoint, "http://127.0.0.1:50090");
    assert_eq!(assignment.epoch, 4);
    assert_eq!(assignment.fencing_token, 11);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Recovering);
    assert_eq!(
        registry.resolve_assignment(&shard).await.unwrap().unwrap(),
        assignment
    );
}

#[tokio::test]
async fn move_and_failover_helpers_use_current_owner_as_source() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::retain("t1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            5,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                5,
                "http://127.0.0.1:5050",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            6,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                6,
                "http://127.0.0.1:5060",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                7,
                "http://127.0.0.1:5070",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Retain, 5, "http://127.0.0.1:5050"),
            2,
            8,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let moved = registry
        .move_shard_to_member(shard.clone(), 6)
        .await
        .unwrap();
    assert_eq!(moved.owner_node_id(), 6);
    assert_eq!(moved.lifecycle, ServiceShardLifecycle::Draining);
    assert_eq!(moved.epoch, 3);
    assert_eq!(moved.fencing_token, 9);

    let failed_over = registry
        .failover_shard_to_member(shard.clone(), 7)
        .await
        .unwrap();
    assert_eq!(failed_over.owner_node_id(), 7);
    assert_eq!(failed_over.lifecycle, ServiceShardLifecycle::Recovering);
    assert_eq!(failed_over.epoch, 4);
    assert_eq!(failed_over.fencing_token, 10);
    assert_eq!(
        registry.resolve_assignment(&shard).await.unwrap().unwrap(),
        failed_over
    );
}

#[tokio::test]
async fn bootstrap_catchup_and_repair_helpers_follow_member_endpoints() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            5,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                5,
                "http://127.0.0.1:5050",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            6,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                6,
                "http://127.0.0.1:5060",
            )],
        ))
        .await
        .unwrap();

    let bootstrapped = registry
        .bootstrap_shard_on_member(shard.clone(), 5)
        .await
        .unwrap();
    assert_eq!(bootstrapped.owner_node_id(), 5);
    assert_eq!(bootstrapped.lifecycle, ServiceShardLifecycle::Bootstrapping);
    assert_eq!(bootstrapped.epoch, 1);
    assert_eq!(bootstrapped.fencing_token, 1);

    let caught_up = registry
        .catch_up_shard_on_member(shard.clone(), 5)
        .await
        .unwrap();
    assert_eq!(caught_up.owner_node_id(), 5);
    assert_eq!(caught_up.lifecycle, ServiceShardLifecycle::Recovering);
    assert_eq!(caught_up.epoch, 2);
    assert_eq!(caught_up.fencing_token, 2);

    let repaired = registry
        .repair_shard_on_member(shard.clone(), 6)
        .await
        .unwrap();
    assert_eq!(repaired.owner_node_id(), 6);
    assert_eq!(repaired.lifecycle, ServiceShardLifecycle::Serving);
    assert_eq!(repaired.epoch, 3);
    assert_eq!(repaired.fencing_token, 3);
    assert_eq!(
        registry.resolve_assignment(&shard).await.unwrap().unwrap(),
        repaired
    );
}

#[tokio::test]
async fn drain_shard_marks_assignment_draining_and_increments_tokens() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let drained = registry.drain_shard(shard.clone()).await.unwrap().unwrap();
    assert_eq!(drained.lifecycle, ServiceShardLifecycle::Draining);
    assert_eq!(drained.epoch, 2);
    assert_eq!(drained.fencing_token, 11);
    assert_eq!(
        registry.resolve_assignment(&shard).await.unwrap().unwrap(),
        drained
    );
}

#[tokio::test]
async fn move_tenant_shard_via_registry_transfers_routes_and_updates_owner() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50063".parse().unwrap();
    let bind2 = "127.0.0.1:50064".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50063",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50064",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50063"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = DistGrpcClient::connect("http://127.0.0.1:50063")
        .await
        .unwrap();
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let assignment = DistGrpcClient::move_tenant_shard_via_registry(&registry, "t1", 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Draining);

    let source_routes = source.list_routes(Some("t1")).await.unwrap();
    assert!(source_routes.is_empty());
    let target = DistGrpcClient::connect("http://127.0.0.1:50064")
        .await
        .unwrap();
    let target_routes = target.list_routes(Some("t1")).await.unwrap();
    assert_eq!(target_routes.len(), 1);
    assert_eq!(target_routes[0].node_id, 9);
    assert_eq!(
        registry
            .resolve_assignment(&ServiceShardKey::dist("t1"))
            .await
            .unwrap()
            .unwrap()
            .owner_node_id(),
        9
    );

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn move_sessiondict_shard_via_registry_transfers_sessions_and_updates_owner() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50065".parse().unwrap();
    let bind2 = "127.0.0.1:50066".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let identity = ClientIdentity {
        tenant_id: "t1".into(),
        user_id: "u1".into(),
        client_id: "c1".into(),
    };
    let shard = greenmqtt_sessiondict::session_identity_shard(&identity);

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::SessionDict,
                7,
                "http://127.0.0.1:50065",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::SessionDict,
                9,
                "http://127.0.0.1:50066",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::SessionDict, 7, "http://127.0.0.1:50065"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = SessionDictGrpcClient::connect("http://127.0.0.1:50065")
        .await
        .unwrap();
    source
        .register(SessionRecord {
            session_id: "s1".into(),
            node_id: 7,
            kind: SessionKind::Persistent,
            identity: identity.clone(),
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1234),
        })
        .await
        .unwrap();

    let assignment = SessionDictGrpcClient::move_shard_via_registry(&registry, shard.clone(), 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Draining);

    assert!(source.lookup_identity(&identity).await.unwrap().is_none());
    let target = SessionDictGrpcClient::connect("http://127.0.0.1:50066")
        .await
        .unwrap();
    let moved = target.lookup_identity(&identity).await.unwrap().unwrap();
    assert_eq!(moved.session_id, "s1");
    assert_eq!(moved.node_id, 7);
    assert_eq!(
        registry
            .resolve_assignment(&shard)
            .await
            .unwrap()
            .unwrap()
            .owner_node_id(),
        9
    );

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn move_inbox_shard_via_registry_transfers_subscription_offline_and_inflight() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50067".parse().unwrap();
    let bind2 = "127.0.0.1:50068".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50067",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50068",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50067"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50067")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(9),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    source
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    source
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 42,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let assignment = InboxGrpcClient::move_shard_via_registry(&registry, shard.clone(), 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Draining);

    assert!(source
        .list_subscriptions(&"s1".to_string())
        .await
        .unwrap()
        .is_empty());
    assert!(source.peek(&"s1".to_string()).await.unwrap().is_empty());
    assert!(source
        .fetch_inflight(&"s1".to_string())
        .await
        .unwrap()
        .is_empty());

    let target = InboxGrpcClient::connect("http://127.0.0.1:50068")
        .await
        .unwrap();
    assert!(!target
        .list_subscriptions(&"s1".to_string())
        .await
        .unwrap()
        .is_empty());
    assert!(!target.peek(&"s1".to_string()).await.unwrap().is_empty());
    assert!(!target
        .fetch_inflight(&"s1".to_string())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        registry
            .resolve_assignment(&shard)
            .await
            .unwrap()
            .unwrap()
            .owner_node_id(),
        9
    );

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn move_inbox_shard_preserves_messages_at_least_once_during_concurrent_enqueue() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50082".parse().unwrap();
    let bind2 = "127.0.0.1:50083".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50082",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50083",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50082"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50082")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    for index in 0..256u16 {
        source
            .enqueue(OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                topic: format!("devices/{index}/state"),
                payload: format!("seed-{index}").into_bytes().into(),
                qos: 1,
                retain: false,
                from_session_id: "publisher".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let move_task = tokio::spawn({
        let registry = registry.clone();
        let shard = shard.clone();
        async move { InboxGrpcClient::move_shard_via_registry(&registry, shard, 9).await }
    });
    sleep(Duration::from_millis(2)).await;
    source
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/late/state".into(),
            payload: b"late".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let assignment = move_task.await.unwrap().unwrap();
    assert_eq!(assignment.owner_node_id(), 9);

    let target = InboxGrpcClient::connect("http://127.0.0.1:50083")
        .await
        .unwrap();
    let target_messages = target.peek(&"s1".to_string()).await.unwrap();
    assert!(target_messages.len() >= 257);
    assert!(target_messages.iter().any(
        |message| message.topic == "devices/late/state" && message.payload.as_ref() == b"late"
    ));

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn move_inbox_shard_survives_source_restart_mid_transfer() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50096".parse().unwrap();
    let bind2 = "127.0.0.1:50097".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50096",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50097",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50096"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50096")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    for index in 0..64u16 {
        source
            .enqueue(OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                topic: format!("devices/restart/{index}"),
                payload: format!("restart-{index}").into_bytes().into(),
                qos: 1,
                retain: false,
                from_session_id: "publisher".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let move_task = tokio::spawn({
        let registry = registry.clone();
        let shard = shard.clone();
        async move { InboxGrpcClient::move_shard_via_registry(&registry, shard, 9).await }
    });
    sleep(Duration::from_millis(1)).await;
    server1.abort();

    let assignment = move_task.await.unwrap().unwrap();
    assert_eq!(assignment.owner_node_id(), 9);

    let target = InboxGrpcClient::connect("http://127.0.0.1:50097")
        .await
        .unwrap();
    let target_messages = target.peek(&"s1".to_string()).await.unwrap();
    assert!(target_messages.len() >= 64);
    assert!(target_messages
        .iter()
        .any(|message| message.topic == "devices/restart/0"));

    server2.abort();
}

#[tokio::test]
async fn catch_up_inbox_shard_via_registry_restores_state_and_verifies_checksum() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50069".parse().unwrap();
    let bind2 = "127.0.0.1:50070".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50069",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50070",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50069"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50069")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(9),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    source
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let assignment = InboxGrpcClient::catch_up_shard_via_registry(&registry, shard.clone(), 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Recovering);

    let target = InboxGrpcClient::connect("http://127.0.0.1:50070")
        .await
        .unwrap();
    let source_snapshot = source.export_shard_snapshot(&shard).await.unwrap();
    let target_snapshot = target.export_shard_snapshot(&shard).await.unwrap();
    assert_eq!(
        source_snapshot.checksum().unwrap(),
        target_snapshot.checksum().unwrap()
    );
    assert_eq!(
        target
            .list_subscriptions(&"s1".to_string())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        target
            .fetch_inflight(&"s1".to_string())
            .await
            .unwrap()
            .len(),
        1
    );

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn catch_up_sessiondict_shard_state_survives_restart_via_storage() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50071".parse().unwrap();
    let bind2 = "127.0.0.1:50072".parse().unwrap();
    let target_store = Arc::new(MemorySessionStore::default());
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(
                PersistentSessionDictHandle::open(target_store.clone())
                    .await
                    .unwrap(),
            ),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let identity = ClientIdentity {
        tenant_id: "t1".into(),
        user_id: "u1".into(),
        client_id: "c1".into(),
    };
    let shard = greenmqtt_sessiondict::session_identity_shard(&identity);

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::SessionDict,
                7,
                "http://127.0.0.1:50071",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::SessionDict,
                9,
                "http://127.0.0.1:50072",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::SessionDict, 7, "http://127.0.0.1:50071"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = SessionDictGrpcClient::connect("http://127.0.0.1:50071")
        .await
        .unwrap();
    source
        .register(SessionRecord {
            session_id: "s1".into(),
            node_id: 7,
            kind: SessionKind::Persistent,
            identity: identity.clone(),
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1234),
        })
        .await
        .unwrap();

    let assignment =
        SessionDictGrpcClient::catch_up_shard_via_registry(&registry, shard.clone(), 9)
            .await
            .unwrap();
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Recovering);

    let direct_reopen = PersistentSessionDictHandle::open(target_store.clone())
        .await
        .unwrap();
    let recovered = direct_reopen
        .lookup_identity(&identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.session_id, "s1");
    assert_eq!(recovered.identity, identity);

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn catch_up_inbox_shard_state_survives_restart_via_storage() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50073".parse().unwrap();
    let bind2 = "127.0.0.1:50074".parse().unwrap();
    let subscription_store = Arc::new(MemorySubscriptionStore::default());
    let inbox_store = Arc::new(MemoryInboxStore::default());
    let inflight_store = Arc::new(MemoryInflightStore::default());
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(PersistentInboxHandle::open(
                subscription_store.clone(),
                inbox_store.clone(),
                inflight_store.clone(),
            )),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50073",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50074",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50073"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50073")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(9),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    source
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    source
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 8,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let assignment = InboxGrpcClient::catch_up_shard_via_registry(&registry, shard.clone(), 9)
        .await
        .unwrap();
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Recovering);

    let reopened = PersistentInboxHandle::open(
        subscription_store.clone(),
        inbox_store.clone(),
        inflight_store.clone(),
    );
    assert_eq!(
        reopened
            .list_subscriptions(&"s1".to_string())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(reopened.peek(&"s1".to_string()).await.unwrap().len(), 1);
    assert_eq!(
        reopened
            .fetch_inflight(&"s1".to_string())
            .await
            .unwrap()
            .len(),
        1
    );

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn catch_up_inbox_shard_survives_target_restart_mid_recovery() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50098".parse().unwrap();
    let bind2 = "127.0.0.1:50099".parse().unwrap();
    let subscription_store = Arc::new(MemorySubscriptionStore::default());
    let inbox_store = Arc::new(MemoryInboxStore::default());
    let inflight_store = Arc::new(MemoryInflightStore::default());
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(PersistentInboxHandle::open(
                subscription_store.clone(),
                inbox_store.clone(),
                inflight_store.clone(),
            )),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::inbox("t1", "s1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50098",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50099",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50098"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50098")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    source
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 21,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let assignment = InboxGrpcClient::catch_up_shard_via_registry(&registry, shard.clone(), 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);

    server2.abort();
    sleep(Duration::from_millis(50)).await;
    let restarted = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(PersistentInboxHandle::open(
                subscription_store.clone(),
                inbox_store.clone(),
                inflight_store.clone(),
            )),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let target = InboxGrpcClient::connect("http://127.0.0.1:50099")
        .await
        .unwrap();
    assert_eq!(
        target
            .list_subscriptions(&"s1".to_string())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        target
            .fetch_inflight(&"s1".to_string())
            .await
            .unwrap()
            .len(),
        1
    );

    server1.abort();
    restarted.abort();
}

#[tokio::test]
async fn anti_entropy_repair_tenant_shard_via_registry_reconciles_target_routes() {
    let registry = StaticServiceEndpointRegistry::default();
    let bind1 = "127.0.0.1:50075".parse().unwrap();
    let bind2 = "127.0.0.1:50076".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: None,
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50075",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50076",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50075"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = DistGrpcClient::connect("http://127.0.0.1:50075")
        .await
        .unwrap();
    let target = DistGrpcClient::connect("http://127.0.0.1:50076")
        .await
        .unwrap();
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    target
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "stale/topic".into(),
            session_id: "stale".into(),
            node_id: 9,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let assignment =
        DistGrpcClient::anti_entropy_repair_tenant_shard_via_registry(&registry, "t1", 9)
            .await
            .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);
    assert_eq!(assignment.lifecycle, ServiceShardLifecycle::Serving);

    let routes = target.list_routes(Some("t1")).await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "devices/+/state");
    assert_eq!(routes[0].session_id, "s1");

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn move_shard_with_stale_fencing_is_rejected() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50077",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50078",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50077"),
            2,
            12,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let stale = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50077"),
        1,
        11,
        ServiceShardLifecycle::Serving,
    );
    let error = registry
        .move_shard_to_member_with_fencing(&stale, 9)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("stale shard assignment"));
}

#[tokio::test]
async fn drain_shard_with_stale_fencing_is_rejected() {
    let registry = StaticServiceEndpointRegistry::default();
    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50077"),
            3,
            21,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let stale = ServiceShardAssignment::new(
        shard,
        ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50077"),
        2,
        20,
        ServiceShardLifecycle::Serving,
    );
    let error = registry.drain_shard_with_fencing(&stale).await.unwrap_err();
    assert!(error.to_string().contains("stale shard assignment"));
}

#[tokio::test]
async fn stream_shard_snapshot_with_stale_fencing_is_rejected_by_service() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind = "127.0.0.1:50079".parse().unwrap();
    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard,
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50079"),
            2,
            12,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let server = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind),
    );
    sleep(Duration::from_millis(50)).await;

    let mut client = DistServiceClient::connect("http://127.0.0.1:50079".to_string())
        .await
        .unwrap();
    let error = client
        .stream_shard_snapshot(ShardSnapshotRequest {
            tenant_id: "t1".into(),
            scope: "*".into(),
            owner_node_id: 7,
            epoch: 1,
            fencing_token: 11,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    server.abort();
}

#[tokio::test]
async fn shard_fencing_reject_can_be_retried_with_current_assignment() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50102".parse().unwrap();
    let bind2 = "127.0.0.1:50103".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50102",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50103",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50102"),
            2,
            12,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let stale = ServiceShardAssignment::new(
        shard.clone(),
        ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50102"),
        1,
        11,
        ServiceShardLifecycle::Serving,
    );
    let error = registry
        .move_shard_to_member_with_fencing(&stale, 9)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("stale shard assignment"));

    let current = registry.resolve_assignment(&shard).await.unwrap().unwrap();
    let moved = registry
        .move_shard_to_member_with_fencing(&current, 9)
        .await
        .unwrap();
    assert_eq!(moved.owner_node_id(), 9);
    assert_eq!(moved.lifecycle, ServiceShardLifecycle::Draining);

    let client = DistGrpcClient::connect("http://127.0.0.1:50103")
        .await
        .unwrap();
    let snapshot = client.stream_shard_snapshot(&moved).await.unwrap();
    assert!(snapshot.routes.is_empty());

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn member_leaving_triggers_shard_drain_and_move() {
    let registry = StaticServiceEndpointRegistry::default();
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50080",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50081",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50080"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let transitioned = registry
        .handle_member_lifecycle_transition(7, ClusterNodeLifecycle::Leaving)
        .await
        .unwrap();
    assert_eq!(transitioned.len(), 1);
    assert_eq!(transitioned[0].owner_node_id(), 9);
    assert_eq!(transitioned[0].lifecycle, ServiceShardLifecycle::Draining);
}

#[tokio::test]
async fn member_offline_triggers_shard_failover() {
    let registry = StaticServiceEndpointRegistry::default();
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50080",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50081",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50080"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let transitioned = registry
        .handle_member_lifecycle_transition(7, ClusterNodeLifecycle::Offline)
        .await
        .unwrap();
    assert_eq!(transitioned.len(), 1);
    assert_eq!(transitioned[0].owner_node_id(), 9);
    assert_eq!(transitioned[0].lifecycle, ServiceShardLifecycle::Recovering);
}

#[tokio::test]
async fn shard_aware_client_reconnects_to_new_owner_after_failover() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50084".parse().unwrap();
    let bind2 = "127.0.0.1:50085".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50084",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50085",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50084"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = DistGrpcClient::connect("http://127.0.0.1:50084")
        .await
        .unwrap();
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let mut client = ReconnectingDistClient::connect(registry.clone(), "t1")
        .await
        .unwrap();
    assert_eq!(client.list_routes().await.unwrap().len(), 1);

    let assignment = DistGrpcClient::catch_up_tenant_shard_via_registry(&registry, "t1", 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);
    server1.abort();
    sleep(Duration::from_millis(50)).await;

    let routes = client.list_routes().await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "devices/+/state");

    server2.abort();
}

#[tokio::test]
async fn network_partition_does_not_allow_dual_shard_owner() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50086".parse().unwrap();
    let bind2 = "127.0.0.1:50087".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    let shard = ServiceShardKey::dist("t1");
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50086",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50087",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50086"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = DistGrpcClient::connect("http://127.0.0.1:50086")
        .await
        .unwrap();
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let assignment = DistGrpcClient::catch_up_tenant_shard_via_registry(&registry, "t1", 9)
        .await
        .unwrap();
    assert_eq!(assignment.owner_node_id(), 9);

    let mut stale_client = DistServiceClient::connect("http://127.0.0.1:50086".to_string())
        .await
        .unwrap();
    let stale_error = stale_client
        .stream_shard_snapshot(ShardSnapshotRequest {
            tenant_id: "t1".into(),
            scope: "*".into(),
            owner_node_id: 7,
            epoch: 1,
            fencing_token: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(stale_error.code(), tonic::Code::FailedPrecondition);

    let current = registry.resolve_assignment(&shard).await.unwrap().unwrap();
    assert_eq!(current.owner_node_id(), 9);
    let target = DistGrpcClient::connect("http://127.0.0.1:50087")
        .await
        .unwrap();
    let routes = target.stream_shard_snapshot(&current).await.unwrap();
    assert_eq!(routes.routes.len(), 1);
    assert_eq!(routes.routes[0].topic_filter, "devices/+/state");

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn periodic_anti_entropy_reconciles_dist_replica_checksum() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50088".parse().unwrap();
    let bind2 = "127.0.0.1:50089".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50088",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50089",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50088"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = DistGrpcClient::connect("http://127.0.0.1:50088")
        .await
        .unwrap();
    let replica = DistGrpcClient::connect("http://127.0.0.1:50089")
        .await
        .unwrap();
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    replica
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "stale/topic".into(),
            session_id: "stale".into(),
            node_id: 9,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let reconciler =
        PeriodicAntiEntropyReconciler::new(registry.clone(), Duration::from_millis(20));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        reconciler
            .run_dist_tenant_replica_until_cancelled("t1".to_string(), 9, rx)
            .await
    });
    sleep(Duration::from_millis(120)).await;
    tx.send(true).unwrap();
    task.await.unwrap().unwrap();

    let routes = replica.list_routes(Some("t1")).await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "devices/+/state");
    let current = registry
        .resolve_assignment(&ServiceShardKey::dist("t1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.owner_node_id(), 7);

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn anti_entropy_dist_replica_converges_under_concurrent_write() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50100".parse().unwrap();
    let bind2 = "127.0.0.1:50101".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                7,
                "http://127.0.0.1:50100",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Dist,
                9,
                "http://127.0.0.1:50101",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50100"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = DistGrpcClient::connect("http://127.0.0.1:50100")
        .await
        .unwrap();
    let replica = DistGrpcClient::connect("http://127.0.0.1:50101")
        .await
        .unwrap();
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 7,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let reconciler =
        PeriodicAntiEntropyReconciler::new(registry.clone(), Duration::from_millis(20));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        reconciler
            .run_dist_tenant_replica_until_cancelled("t1".to_string(), 9, rx)
            .await
    });
    sleep(Duration::from_millis(30)).await;
    source
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/late/state".into(),
            session_id: "s2".into(),
            node_id: 7,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    sleep(Duration::from_millis(120)).await;
    tx.send(true).unwrap();
    task.await.unwrap().unwrap();

    let routes = replica.list_routes(Some("t1")).await.unwrap();
    assert_eq!(routes.len(), 2);
    assert!(routes
        .iter()
        .any(|route| route.topic_filter == "devices/late/state"));

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn periodic_anti_entropy_reconciles_sessiondict_replica_checksum() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50090".parse().unwrap();
    let bind2 = "127.0.0.1:50091".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::SessionDict,
                7,
                "http://127.0.0.1:50090",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::SessionDict,
                9,
                "http://127.0.0.1:50091",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            greenmqtt_sessiondict::session_scan_shard("t1"),
            ServiceEndpoint::new(ServiceKind::SessionDict, 7, "http://127.0.0.1:50090"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = SessionDictGrpcClient::connect("http://127.0.0.1:50090")
        .await
        .unwrap();
    let replica = SessionDictGrpcClient::connect("http://127.0.0.1:50091")
        .await
        .unwrap();
    source
        .register(SessionRecord {
            session_id: "s1".into(),
            node_id: 7,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1234),
        })
        .await
        .unwrap();
    replica
        .register(SessionRecord {
            session_id: "stale".into(),
            node_id: 9,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "stale".into(),
                client_id: "stale".into(),
            },
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(5678),
        })
        .await
        .unwrap();

    let reconciler =
        PeriodicAntiEntropyReconciler::new(registry.clone(), Duration::from_millis(20));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        reconciler
            .run_sessiondict_tenant_replica_until_cancelled("t1".to_string(), 9, rx)
            .await
    });
    sleep(Duration::from_millis(120)).await;
    tx.send(true).unwrap();
    task.await.unwrap().unwrap();

    let sessions = replica.list_sessions(Some("t1")).await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, "s1");
    let current = registry
        .resolve_assignment(&greenmqtt_sessiondict::session_scan_shard("t1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.owner_node_id(), 7);

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn periodic_anti_entropy_reconciles_inbox_replica_checksum() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50092".parse().unwrap();
    let bind2 = "127.0.0.1:50093".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                7,
                "http://127.0.0.1:50092",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Inbox,
                9,
                "http://127.0.0.1:50093",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            greenmqtt_inbox::inbox_tenant_scan_shard("t1"),
            ServiceEndpoint::new(ServiceKind::Inbox, 7, "http://127.0.0.1:50092"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = InboxGrpcClient::connect("http://127.0.0.1:50092")
        .await
        .unwrap();
    let replica = InboxGrpcClient::connect("http://127.0.0.1:50093")
        .await
        .unwrap();
    source
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            qos: 1,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    source
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    source
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 11,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    replica
        .subscribe(Subscription {
            session_id: "stale".into(),
            tenant_id: "t1".into(),
            topic_filter: "stale/topic".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    replica
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "stale".into(),
            topic: "stale/topic".into(),
            payload: b"stale-offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    replica
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "stale".into(),
            packet_id: 99,
            topic: "stale/topic".into(),
            payload: b"stale-inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "publisher".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let reconciler =
        PeriodicAntiEntropyReconciler::new(registry.clone(), Duration::from_millis(20));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        reconciler
            .run_inbox_tenant_replica_until_cancelled("t1".to_string(), 9, rx)
            .await
    });
    sleep(Duration::from_millis(120)).await;
    tx.send(true).unwrap();
    task.await.unwrap().unwrap();

    let subscriptions = replica.list_tenant_subscriptions("t1").await.unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].session_id, "s1");
    let offline = replica.list_tenant_offline("t1").await.unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].session_id, "s1");
    let inflight = replica.list_tenant_inflight("t1").await.unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].session_id, "s1");

    server1.abort();
    server2.abort();
}

#[tokio::test]
async fn periodic_anti_entropy_reconciles_retain_replica_checksum() {
    let registry = Arc::new(StaticServiceEndpointRegistry::default());
    let bind1 = "127.0.0.1:50094".parse().unwrap();
    let bind2 = "127.0.0.1:50095".parse().unwrap();
    let server1 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind1),
    );
    let server2 = tokio::spawn(
        RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: None,
            range_runtime: None,
        }
        .serve(bind2),
    );
    sleep(Duration::from_millis(50)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                7,
                "http://127.0.0.1:50094",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            9,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                9,
                "http://127.0.0.1:50095",
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::retain("t1"),
            ServiceEndpoint::new(ServiceKind::Retain, 7, "http://127.0.0.1:50094"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let source = RetainGrpcClient::connect("http://127.0.0.1:50094")
        .await
        .unwrap();
    let replica = RetainGrpcClient::connect("http://127.0.0.1:50095")
        .await
        .unwrap();
    source
        .retain(greenmqtt_core::RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"online".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();
    replica
        .retain(greenmqtt_core::RetainedMessage {
            tenant_id: "t1".into(),
            topic: "stale/topic".into(),
            payload: b"stale".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    let reconciler =
        PeriodicAntiEntropyReconciler::new(registry.clone(), Duration::from_millis(20));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        reconciler
            .run_retain_tenant_replica_until_cancelled("t1".to_string(), 9, rx)
            .await
    });
    sleep(Duration::from_millis(120)).await;
    tx.send(true).unwrap();
    task.await.unwrap().unwrap();

    let retained = replica.list_tenant_retained("t1").await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].topic, "devices/a/state");
    let current = registry
        .resolve_assignment(&ServiceShardKey::retain("t1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.owner_node_id(), 7);

    server1.abort();
    server2.abort();
}
