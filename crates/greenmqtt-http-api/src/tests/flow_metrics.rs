use super::*;
#[tokio::test]
async fn http_flow_replays_offline_messages() {
    let broker = broker();
    let app = HttpApi::router(broker.clone());

    let connect_req = ConnectRequest {
        identity: ClientIdentity {
            tenant_id: "demo".into(),
            user_id: "alice".into(),
            client_id: "sub".into(),
        },
        node_id: 1,
        kind: SessionKind::Persistent,
        clean_start: true,
        session_expiry_interval_secs: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/connect")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&connect_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_publish_and_drain_deliveries() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
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

    let app = HttpApi::router(broker.clone());

    let subscribe = SubscribeRequest {
        session_id: subscriber.session.session_id.clone(),
        topic_filter: "devices/+/state".into(),
        qos: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        retain_handling: 0,
        shared_group: None,
    };
    let subscribe_response = app
        .clone()
        .oneshot(
            Request::post("/v1/subscribe")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&subscribe).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(subscribe_response.status(), StatusCode::CREATED);

    let publish = SessionPublishRequest {
        session_id: publisher.session.session_id,
        publish: PublishRequest {
            topic: "devices/d1/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
            retain: false,
            properties: PublishProperties::default(),
        },
    };
    let publish_response = app
        .clone()
        .oneshot(
            Request::post("/v1/publish")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&publish).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(publish_response.status(), StatusCode::OK);

    let deliveries_response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/deliveries",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deliveries_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_inbox_send_lwt_returns_typed_result_and_records_metrics_and_audit() {
    let metrics = test_prometheus_handle();
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "lwt".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: Some(60),
        })
        .await
        .unwrap();
    let app = HttpApi::router(broker.clone());

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/inbox/lwt")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&crate::admin::InboxSendLwtBody {
                        tenant_id: "demo".into(),
                        session_id: session.session.session_id.clone(),
                        generation: 1,
                        publish: PublishRequest {
                            topic: "clients/lwt/status".into(),
                            payload: b"bye".to_vec().into(),
                            qos: 1,
                            retain: false,
                            properties: PublishProperties::default(),
                        },
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::admin::InboxLwtResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.code, "NoDetach");

    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| {
        entry.action == "inbox_send_lwt"
            && entry.details.get("code").map(String::as_str) == Some("NoDetach")
    }));
    let rendered = metrics.render();
    assert!(rendered.contains("greenmqtt_inbox_delayed_lwt_dispatch_total"));
    assert!(rendered.contains("code=\"NoDetach\""));
}

#[tokio::test]
async fn http_publish_respects_topic_rewrite_hook() {
    let broker = Arc::new(BrokerRuntime::with_plugins(
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
        TopicRewriteEventHook::new(vec![TopicRewriteRule {
            tenant_id: Some("demo".into()),
            topic_filter: "devices/+/raw".into(),
            rewrite_to: "devices/{1}/normalized".into(),
        }]),
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(InboxHandle::default()),
        Arc::new(RetainHandle::default()),
    ));
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
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

    let app = HttpApi::router(broker.clone());
    let subscribe = SubscribeRequest {
        session_id: subscriber.session.session_id.clone(),
        topic_filter: "devices/+/normalized".into(),
        qos: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        retain_handling: 0,
        shared_group: None,
    };
    let subscribe_response = app
        .clone()
        .oneshot(
            Request::post("/v1/subscribe")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&subscribe).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(subscribe_response.status(), StatusCode::CREATED);

    let publish = SessionPublishRequest {
        session_id: publisher.session.session_id,
        publish: PublishRequest {
            topic: "devices/d1/raw".into(),
            payload: b"rewritten".to_vec().into(),
            qos: 1,
            retain: false,
            properties: PublishProperties::default(),
        },
    };
    let publish_response = app
        .clone()
        .oneshot(
            Request::post("/v1/publish")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&publish).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(publish_response.status(), StatusCode::OK);

    let deliveries_response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/deliveries",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deliveries_response.status(), StatusCode::OK);
    let body = to_bytes(deliveries_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let deliveries: Vec<Delivery> = serde_json::from_slice(&body).unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].topic, "devices/d1/normalized");
    assert_eq!(deliveries[0].payload, b"rewritten".to_vec());
}

#[tokio::test]
async fn http_stats_reports_local_session_and_delivery_counts() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
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
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/d1/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));
    let response = app
        .oneshot(Request::get("/v1/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let stats: BrokerStats = serde_json::from_slice(&body).unwrap();
    assert_eq!(stats.peer_nodes, 2);
    assert_eq!(stats.local_online_sessions, 2);
    assert_eq!(stats.local_persistent_sessions, 1);
    assert_eq!(stats.local_transient_sessions, 1);
    assert_eq!(stats.local_pending_deliveries, 1);
    assert_eq!(stats.global_session_records, 2);
    assert_eq!(stats.route_records, 1);
    assert_eq!(stats.subscription_records, 1);
    assert_eq!(stats.offline_messages, 0);
    assert_eq!(stats.inflight_messages, 0);
    assert_eq!(stats.retained_messages, 0);
}

#[tokio::test]
async fn http_metrics_returns_prometheus_counters() {
    let metrics = test_prometheus_handle();
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    shards
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
    shards
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
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("demo-metrics-http"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo-metrics-http".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
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
    for index in 0..100 {
        broker
            .publish(
                &subscriber.session.session_id,
                PublishRequest {
                    topic: format!("devices/{index}/state"),
                    payload: format!("value-{index}").into_bytes().into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
    }
    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        Some(Arc::new(TestPeerRegistry::new(vec![2, 5]))),
        Some(shards.clone()),
        None,
        Some(metrics),
    );
    let _ = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/demo-metrics-http/*/move")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/demo-metrics-http/*/failover")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":7}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/demo-metrics-http/*/repair")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("greenmqtt_peer_nodes 2"));
    assert!(text.contains("greenmqtt_local_online_sessions 1"));
    assert!(text.contains("greenmqtt_global_session_records 1"));
    assert!(text.contains("greenmqtt_sessions_count 1"));
    assert!(text.contains("greenmqtt_route_records 1"));
    assert!(text.contains("greenmqtt_subscription_records 1"));
    assert!(text.contains("greenmqtt_subscriptions_count 1"));
    assert!(text.contains("greenmqtt_inflight_count 0"));
    assert!(text.contains("greenmqtt_retained_messages_count 0"));
    assert!(text.contains("greenmqtt_replay_window_entries "));
    assert!(text.contains("greenmqtt_service_session_bytes "));
    assert!(text.contains("greenmqtt_service_route_bytes "));
    assert!(text.contains("greenmqtt_service_subscription_bytes "));
    assert!(text.contains("greenmqtt_service_offline_bytes "));
    assert!(text.contains("greenmqtt_service_inflight_bytes "));
    assert!(text.contains("greenmqtt_service_retained_bytes "));
    assert!(text.contains("greenmqtt_local_hot_state_entries "));
    assert!(text.contains("greenmqtt_local_hot_state_bytes "));
    assert!(text.contains("greenmqtt_pending_delayed_wills "));
    assert!(text.contains("greenmqtt_process_rss_bytes "));
    assert!(text.contains("greenmqtt_broker_rss_bytes "));
    assert!(text.contains("greenmqtt_broker_cpu_usage "));
    assert!(text.contains("greenmqtt_memory_pressure_level "));
    let connect_line = text
        .lines()
        .find(|line| {
            line.starts_with("mqtt_connect_total{")
                && line.contains("tenant_id=\"demo-metrics-http\"")
        })
        .expect("expected connect counter line");
    assert!(connect_line.ends_with(" 1"));
    let publish_count_line = text
        .lines()
        .find(|line| {
            line.starts_with("mqtt_publish_count{")
                && line.contains("tenant_id=\"demo-metrics-http\"")
                && line.contains("qos=\"1\"")
        })
        .expect("expected publish counter line");
    assert!(publish_count_line.ends_with(" 100"));
    assert!(text.lines().any(|line| {
        line.starts_with("mqtt_publish_ingress_bytes{")
            && line.contains("tenant_id=\"demo-metrics-http\"")
            && line.contains("qos=\"1\"")
    }));
    assert!(text.contains("mqtt_shard_move_total"));
    assert!(text.contains("mqtt_shard_failover_total"));
    assert!(text.contains("mqtt_shard_anti_entropy_total"));
}

#[tokio::test]
async fn http_retain_gc_run_records_metrics_and_audit() {
    let metrics = test_prometheus_handle();
    let broker = broker();
    broker
        .retain
        .retain(RetainedMessage {
            tenant_id: "demo".into(),
            topic: "devices/d1/state".into(),
            payload: b"retained".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();
    let app = HttpApi::router(broker.clone());
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/retain/gc/run?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::admin::RetainMaintenanceResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.refreshed, 1);

    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| entry.action == "retain_tenant_gc"));
    let rendered = metrics.render();
    assert!(rendered.contains("greenmqtt_retain_expire_sweep_total"));
    assert!(rendered.contains("action=\"tenant_gc\""));
}

#[tokio::test]
async fn http_metrics_supports_tenant_scoped_gauges() {
    let broker = broker();
    let demo = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-demo".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &demo.session.session_id,
            "devices/demo/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-other".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &other.session.session_id,
            "devices/other/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .publish(
            &demo.session.session_id,
            PublishRequest {
                topic: "retain/demo/state".into(),
                payload: b"retained".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/metrics?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("greenmqtt_tenant_session_records{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_route_records{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_subscription_records{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_retained_messages{tenant_id=\"demo\"} 1"));
    assert!(text.contains("greenmqtt_tenant_session_bytes{tenant_id=\"demo\"} "));
    assert!(text.contains("greenmqtt_tenant_route_bytes{tenant_id=\"demo\"} "));
    assert!(text.contains("greenmqtt_tenant_subscription_bytes{tenant_id=\"demo\"} "));
    assert!(text.contains("greenmqtt_tenant_retained_bytes{tenant_id=\"demo\"} "));
}

#[tokio::test]
async fn http_rpc_governance_rules_and_landscape_are_runtime_configurable() {
    let broker = broker();
    let governor = Arc::new(greenmqtt_rpc::RpcTrafficGovernor::new(
        greenmqtt_rpc::RpcTrafficGovernorConfig::default(),
    ));
    let app = HttpApi::router_with_rpc_governor(broker, Some(governor.clone()));

    let response = app
        .clone()
        .oneshot(Request::get("/v1/rpc/services").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let services: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(services
        .as_array()
        .unwrap()
        .iter()
        .any(|service| service.get("service").and_then(|v| v.as_str()) == Some("dist")));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/rpc/services/dist/rules")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&greenmqtt_rpc::RpcTrafficRules {
                        max_in_flight: 0,
                        retry_after_ms: 321,
                        tenant_prefix_limits: BTreeMap::from([("demo".into(), 1usize)]),
                        preferred_endpoints: vec!["http://127.0.0.1:50051".into()],
                        server_group_tags: vec!["edge".into()],
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    governor
        .service(greenmqtt_rpc::RpcServiceKind::Dist)
        .note_endpoint_overload("http://127.0.0.1:50051", 321);

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/rpc/services/dist/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let rules: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rules = rules.as_object().unwrap();
    assert_eq!(rules.get("source").and_then(|v| v.as_str()), Some("runtime"));
    assert_eq!(
        rules
            .get("effective")
            .and_then(|v| v.get("max_in_flight"))
            .and_then(|v| v.as_u64()),
        Some(0)
    );

    let response = app
        .oneshot(
            Request::get("/v1/rpc/services/dist/landscape")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let landscape: Option<crate::admin::RpcServiceLandscapeResponse> =
        serde_json::from_slice(&body).unwrap();
    let landscape = landscape.unwrap();
    assert_eq!(landscape.service, "dist");
    assert_eq!(landscape.preferred_endpoints, vec!["http://127.0.0.1:50051"]);
    assert_eq!(landscape.server_group_tags, vec!["edge"]);
    assert_eq!(landscape.endpoint_snapshots.len(), 1);
}

#[tokio::test]
async fn http_connect_supports_lazy_replay_query() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "lazy-sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "lazy-pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/demo/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/demo/state".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/connect?hydrate_replay=false")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&ConnectRequest {
                        identity: ClientIdentity {
                            tenant_id: "demo".into(),
                            user_id: "alice".into(),
                            client_id: "lazy-sub".into(),
                        },
                        node_id: 1,
                        kind: SessionKind::Persistent,
                        clean_start: false,
                        session_expiry_interval_secs: None,
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: ConnectReply = serde_json::from_slice(&body).unwrap();
    assert!(reply.offline_messages.is_empty());
    assert!(reply.inflight_messages.is_empty());

    let offline_response = app
        .oneshot(
            Request::get(format!(
                "/v1/offline?tenant_id=demo&session_id={}",
                reply.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(offline_response.status(), StatusCode::OK);
    let body = to_bytes(offline_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let offline: Vec<OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].payload, b"offline".to_vec());
}

#[tokio::test]
async fn http_lists_local_sessions_with_pending_delivery_counts() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let publisher = broker
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
    broker
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
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(Request::get("/v1/sessions").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sessions: Vec<SessionSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(sessions.len(), 2);
    let subscriber_entry = sessions
        .into_iter()
        .find(|session| session.client_id == "sub")
        .unwrap();
    assert_eq!(subscriber_entry.tenant_id, "demo");
    assert_eq!(subscriber_entry.pending_deliveries, 1);
}

#[tokio::test]
async fn http_lists_retained_messages_for_tenant_query() {
    let broker = broker();
    let demo = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .publish(
            &demo.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"retained".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "pub-other".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .publish(
            &other.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"other".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/retain?tenant_id=demo&topic_filter=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let retained: Vec<greenmqtt_core::RetainedMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].tenant_id, "demo");
    assert_eq!(retained[0].topic, "devices/d1/state");
    assert_eq!(retained[0].payload, b"retained".to_vec());
}

#[tokio::test]
async fn http_lists_all_retained_messages_for_tenant_query() {
    let broker = broker();
    let demo = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "pub".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (topic, payload) in [
        ("devices/d1/state", b"retained-1".to_vec()),
        ("devices/d2/state", b"retained-2".to_vec()),
    ] {
        broker
            .publish(
                &demo.session.session_id,
                PublishRequest {
                    topic: topic.into(),
                    payload: payload.into(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties::default(),
                },
            )
            .await
            .unwrap();
    }

    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "pub-other".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    broker
        .publish(
            &other.session.session_id,
            PublishRequest {
                topic: "devices/other/state".into(),
                payload: b"other".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/retain?tenant_id=demo&topic_filter=%23")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let retained: Vec<greenmqtt_core::RetainedMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(retained.len(), 2);
    assert!(retained.iter().all(|message| message.tenant_id == "demo"));
}

#[tokio::test]
async fn http_can_delete_retained_message() {
    let broker = broker();
    broker
        .retain
        .retain(RetainedMessage {
            tenant_id: "demo".into(),
            topic: "devices/d1/state".into(),
            payload: b"retained".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/retain?tenant_id=demo&topic=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert!(broker
        .retain
        .match_topic("demo", "devices/d1/state")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_dry_run_retained_delete_reports_without_removing() {
    let broker = broker();
    broker
        .retain
        .retain(RetainedMessage {
            tenant_id: "demo".into(),
            topic: "devices/d1/state".into(),
            payload: b"retained".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/retain?tenant_id=demo&topic=devices/d1/state&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    let retained = broker
        .retain
        .match_topic("demo", "devices/d1/state")
        .await
        .unwrap();
    assert_eq!(retained.len(), 1);
    let audit = broker.list_admin_audit(None);
    assert!(audit.is_empty());
}
