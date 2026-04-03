use crate::{
    DistGrpcClient, InboxGrpcClient, NoopDeliverySink, RetainGrpcClient, RpcRuntime,
    SessionDictGrpcClient, StaticPeerForwarder, StaticServiceEndpointRegistry,
};
use greenmqtt_broker::{BrokerConfig, BrokerRuntime, DefaultBroker, PeerRegistry};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership,
    ConnectRequest, OfflineMessage, PublishProperties, PublishRequest, RouteRecord,
    ServiceEndpoint, ServiceEndpointRegistry, ServiceKind, ServiceShardAssignment, ServiceShardKey,
    ServiceShardLifecycle, ServiceShardRecoveryControl, ServiceShardTransition, SessionKind,
    SessionRecord,
};
use greenmqtt_dist::{DistHandle, DistRouter};
use greenmqtt_inbox::{InboxHandle, InboxService};
use greenmqtt_plugin_api::{AllowAllAcl, AllowAllAuth, NoopEventHook};
use greenmqtt_proto::internal::{
    dist_service_client::DistServiceClient, inbox_service_client::InboxServiceClient,
    retain_service_client::RetainServiceClient,
    session_dict_service_client::SessionDictServiceClient, AddRouteRequest, InboxAttachRequest,
    InboxEnqueueRequest, InboxFetchRequest, InboxLookupSubscriptionRequest, InboxSubscribeRequest,
    ListRoutesRequest, ListSessionRoutesRequest, ListSessionsRequest, LookupSessionRequest,
    MatchTopicRequest, RegisterSessionRequest, RetainMatchRequest, RetainWriteRequest,
};
use greenmqtt_proto::{
    to_proto_client_identity, to_proto_offline, to_proto_retain, to_proto_route, to_proto_session,
};
use greenmqtt_retain::{RetainHandle, RetainService};
use greenmqtt_sessiondict::{SessionDictHandle, SessionDirectory};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

fn state_runtime() -> RpcRuntime {
    RpcRuntime {
        sessiondict: Arc::new(SessionDictHandle::default()),
        dist: Arc::new(DistHandle::default()),
        inbox: Arc::new(InboxHandle::default()),
        retain: Arc::new(RetainHandle::default()),
        peer_sink: Arc::new(NoopDeliverySink),
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
