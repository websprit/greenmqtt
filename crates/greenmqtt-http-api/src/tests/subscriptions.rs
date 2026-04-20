use super::*;
#[tokio::test]
async fn http_lists_session_subscriptions() {
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
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(7),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/subscriptions",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
    assert_eq!(subscriptions[0].subscription_identifier, Some(7));
}

#[tokio::test]
async fn http_lists_all_subscriptions_with_filters() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/subscriptions?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].tenant_id, "demo");
    assert_eq!(subscriptions[0].session_id, first.session.session_id);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
}

#[tokio::test]
async fn http_lists_all_subscriptions_by_tenant() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/subscriptions?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].tenant_id, "demo");
    assert_eq!(subscriptions[0].session_id, first.session.session_id);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
}

#[tokio::test]
async fn http_lists_all_subscriptions_by_tenant_and_topic_filter() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let third = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-c".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &third.session.session_id,
            "devices/+/state",
            1,
            Some(3),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let subscriptions: Vec<greenmqtt_core::Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].tenant_id, "demo");
    assert_eq!(subscriptions[0].session_id, first.session.session_id);
    assert_eq!(subscriptions[0].topic_filter, "devices/+/state");
}

#[tokio::test]
async fn http_can_purge_subscriptions_by_tenant_and_topic_filter() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let third = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-c".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &third.session.session_id,
            "devices/+/state",
            1,
            Some(3),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let remaining = broker.inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|subscription| {
        !(subscription.tenant_id == "demo" && subscription.topic_filter == "devices/+/state")
    }));

    let demo_routes: Vec<_> = broker
        .dist
        .list_routes(Some("demo"))
        .await
        .unwrap()
        .into_iter()
        .filter(|route| route.topic_filter == "devices/+/state")
        .collect();
    assert!(demo_routes.is_empty());
    let other_routes: Vec<_> = broker
        .dist
        .list_routes(Some("other"))
        .await
        .unwrap()
        .into_iter()
        .filter(|route| route.topic_filter == "devices/+/state")
        .collect();
    assert_eq!(other_routes.len(), 1);
}

#[tokio::test]
async fn http_can_purge_subscriptions_and_routes_by_filter() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let remaining = broker.inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].topic_filter, "alerts/#");

    let routes = broker.dist.list_routes(Some("demo")).await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "alerts/#");
}

#[tokio::test]
async fn http_can_purge_subscriptions_by_session_only() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "alerts/#".into(),
            session_id: subscriber.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}",
                subscriber.session.session_id
            ))
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
        .inbox
        .list_subscriptions(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(dist.remove_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_can_purge_subscriptions_by_tenant() {
    let broker = broker();
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo")
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
        .inbox
        .list_subscriptions(&first.session.session_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        broker
            .inbox
            .list_subscriptions(&second.session.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn http_tenant_subscription_purge_uses_remove_tenant_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_tenant_routes_calls(), 1);
    assert_eq!(dist.remove_session_routes_calls(), 0);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_subscription_purge_by_topic_filter_uses_remove_topic_filter_routes_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
    assert_eq!(dist.remove_tenant_routes_calls(), 0);
    assert_eq!(dist.remove_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_reports_without_removing() {
    let broker = broker();
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        broker.inbox.list_all_subscriptions().await.unwrap().len(),
        1
    );
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_list_shared_subscription_uses_lookup_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let load_calls_before = subscriptions.load_subscription_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(subscriptions.load_subscription_calls(), load_calls_before);
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
}

#[tokio::test]
async fn http_dry_run_shared_subscription_purge_uses_lookup_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let load_calls_before = subscriptions.load_subscription_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.load_subscription_calls(), load_calls_before);
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
    assert_eq!(
        broker.inbox.list_all_subscriptions().await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn http_shared_subscription_purge_uses_lookup_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let load_calls_before = subscriptions.load_subscription_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.load_subscription_calls(), load_calls_before);
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
    assert!(broker
        .inbox
        .list_all_subscriptions()
        .await
        .unwrap()
        .is_empty());
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "purge_subscriptions");
    assert_eq!(audit[0].details.get("shared_group").unwrap(), "workers");
}

#[tokio::test]
async fn http_list_session_topic_subscriptions_uses_cached_or_direct_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    let list_calls_before = subscriptions.list_subscriptions_calls();
    let session_topic_calls_before = subscriptions.list_session_topic_subscriptions_calls();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        subscriptions.list_session_topic_subscriptions_calls(),
        session_topic_calls_before
    );
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_session_topic_cached_or_direct_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    let count_calls_before = subscriptions.count_session_topic_subscriptions_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        subscriptions.count_session_topic_subscriptions_calls(),
        count_calls_before
    );
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
}

#[tokio::test]
async fn http_subscription_purge_uses_session_topic_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_inbox_and_dist(inbox, dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    let purge_calls_before = subscriptions.purge_session_topic_subscriptions_calls();
    let remove_calls_before = dist.remove_session_topic_filter_routes_calls();
    let list_calls_before = subscriptions.list_subscriptions_calls();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(
        subscriptions.purge_session_topic_subscriptions_calls(),
        purge_calls_before + 1
    );
    assert_eq!(
        dist.remove_session_topic_filter_routes_calls(),
        remove_calls_before + 1
    );
    assert_eq!(subscriptions.list_subscriptions_calls(), list_calls_before);
    assert!(broker
        .inbox
        .list_session_topic_subscriptions(&subscriber.session.session_id, "devices/+/state")
        .await
        .unwrap()
        .is_empty());
    let audit = broker.list_admin_audit(None);
    assert_eq!(
        audit[0].details.get("topic_filter").unwrap(),
        "devices/+/state"
    );
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_session_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/subscriptions?session_id={}&dry_run=true",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_session_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_tenant_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_subscription_query_uses_tenant_shared_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/subscriptions?tenant_id=demo&shared_group=workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(subscriptions.list_tenant_shared_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_shared_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions?tenant_id=demo&shared_group=workers&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_tenant_shared_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_shared_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_topic_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let first = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    let second = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
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
            &first.session.session_id,
            "devices/+/state",
            1,
            Some(1),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &second.session.session_id,
            "alerts/#",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(subscriptions.count_tenant_topic_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_subscription_query_uses_tenant_topic_shared_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<Subscription> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(
        subscriptions.list_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_subscription_purge_uses_tenant_topic_shared_count_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        subscriptions.count_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(
        subscriptions.list_tenant_topic_shared_subscriptions_calls(),
        0
    );
}

#[tokio::test]
async fn http_subscription_purge_uses_tenant_topic_shared_fast_path() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let dist = Arc::new(CountingDistRouter::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        subscriptions.clone(),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox_and_dist(inbox, dist.clone());
    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
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
            Some(1),
            false,
            false,
            0,
            Some("workers".into()),
        )
        .await
        .unwrap();
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            Some(2),
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/subscriptions?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(
        subscriptions.purge_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(dist.remove_topic_filter_shared_routes_calls(), 1);
    let remaining = broker.inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shared_group, None);
}

#[tokio::test]
async fn http_rejects_subscription_purge_without_filter() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/subscriptions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
