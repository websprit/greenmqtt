use super::*;
#[tokio::test]
async fn http_can_purge_routes_by_filter() {
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
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state",
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

    let routes = broker.dist.list_routes(Some("demo")).await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "alerts/#");
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "purge_routes");
    assert_eq!(audit[0].details.get("removed").unwrap(), "1");
}

#[tokio::test]
async fn http_route_query_uses_dist_topic_filter_fast_path() {
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
            "devices/d1/state",
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
            topic_filter: "devices/+/state".into(),
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

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/routes/all?tenant_id=demo&topic_filter=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].topic_filter, "devices/d1/state");
    assert_eq!(dist.list_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
    assert_eq!(dist.list_exact_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_topic_filter_shared_fast_path() {
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
    for session_id in [&first.session.session_id, &second.session.session_id] {
        broker
            .subscribe(
                session_id,
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
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(dist.list_topic_filter_shared_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_tenant_shared_fast_path() {
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
    for (session_id, topic_filter, shared_group) in [
        (
            &first.session.session_id,
            "devices/+/state",
            Some("workers"),
        ),
        (&second.session.session_id, "alerts/#", Some("workers")),
        (&second.session.session_id, "metrics/#", Some("other")),
    ] {
        broker
            .dist
            .add_route(RouteRecord {
                tenant_id: "demo".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.clone(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/routes/all?tenant_id=demo&shared_group=workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(dist.list_tenant_shared_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_session_topic_fast_path() {
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

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(dist.list_session_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_route_query_uses_session_topic_shared_fast_path() {
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
            Some(2),
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
            Request::get(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].shared_group.as_deref(), Some("workers"));
    assert_eq!(dist.lookup_session_topic_filter_route_calls(), 1);
    assert_eq!(dist.list_session_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_session_route_purge_uses_remove_session_routes_fast_path() {
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
            "devices/d1/state",
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
                "/v1/routes/all?session_id={}",
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
    assert_eq!(dist.remove_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        0
    );
}

#[tokio::test]
async fn http_session_topic_route_purge_uses_fast_path() {
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

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state",
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
    assert_eq!(dist.remove_session_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_session_topic_filter_routes_calls(), 0);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert!(broker
        .dist
        .list_session_topic_filter_routes(&subscriber.session.session_id, "devices/+/state")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_session_topic_shared_route_dry_run_uses_fast_path() {
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
            Some(2),
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
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
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
    assert_eq!(dist.count_session_topic_filter_route_calls(), 1);
    assert_eq!(dist.lookup_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn http_session_topic_shared_route_purge_uses_fast_path() {
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

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&shared_group=workers",
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
    assert_eq!(dist.remove_session_topic_filter_route_calls(), 1);
    assert_eq!(dist.lookup_session_topic_filter_route_calls(), 0);
    let remaining = broker
        .dist
        .list_session_topic_filter_routes(&subscriber.session.session_id, "devices/+/state")
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shared_group, None);
}

#[tokio::test]
async fn http_route_purge_uses_remove_topic_filter_routes_fast_path() {
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
            Request::delete("/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state")
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
    assert_eq!(dist.list_routes_calls(), 0);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn http_route_purge_uses_remove_topic_filter_shared_fast_path() {
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
    for session_id in [&first.session.session_id, &second.session.session_id] {
        broker
            .subscribe(
                session_id,
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
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.remove_topic_filter_shared_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
    let remaining = broker.dist.list_routes(Some("demo")).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].shared_group, None);
}

#[tokio::test]
async fn http_tenant_route_purge_uses_remove_tenant_routes_fast_path() {
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
        .dist
        .add_route(RouteRecord {
            tenant_id: "demo".into(),
            topic_filter: "devices/+/state".into(),
            session_id: first.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: Some(1),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    broker
        .dist
        .add_route(RouteRecord {
            tenant_id: "other".into(),
            topic_filter: "alerts/#".into(),
            session_id: second.session.session_id.clone(),
            node_id: 1,
            subscription_identifier: Some(2),
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo")
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
    assert_eq!(dist.remove_topic_filter_routes_calls(), 0);
    assert_eq!(dist.remove_session_routes_calls(), 0);
    let remaining = broker.dist.list_routes(None).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].tenant_id, "other");
}

#[tokio::test]
async fn http_route_purge_uses_remove_tenant_shared_fast_path() {
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
    for (session_id, topic_filter, shared_group) in [
        (
            &first.session.session_id,
            "devices/+/state",
            Some("workers"),
        ),
        (&second.session.session_id, "alerts/#", Some("workers")),
        (&second.session.session_id, "metrics/#", Some("other")),
    ] {
        broker
            .dist
            .add_route(RouteRecord {
                tenant_id: "demo".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.clone(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo&shared_group=workers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.remove_tenant_shared_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
    assert_eq!(
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn http_dry_run_route_purge_reports_without_removing() {
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
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
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
        broker.dist.list_routes(Some("demo")).await.unwrap().len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_tenant_count_fast_path() {
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
            Request::delete("/v1/routes/all?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(dist.count_tenant_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_session_topic_count_fast_path() {
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

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/routes/all?session_id={}&topic_filter=devices/%2B/state&dry_run=true",
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
    assert_eq!(dist.count_session_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_topic_filter_count_fast_path() {
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
            Request::delete(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&dry_run=true",
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
    assert_eq!(dist.count_topic_filter_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_topic_filter_shared_count_fast_path() {
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
    for session_id in [&first.session.session_id, &second.session.session_id] {
        broker
            .subscribe(
                session_id,
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
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete(
                "/v1/routes/all?tenant_id=demo&topic_filter=devices/%2B/state&shared_group=workers&dry_run=true",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.count_topic_filter_shared_routes_calls(), 1);
    assert_eq!(dist.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_route_purge_uses_tenant_shared_count_fast_path() {
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
    for (session_id, topic_filter, shared_group) in [
        (
            &first.session.session_id,
            "devices/+/state",
            Some("workers"),
        ),
        (&second.session.session_id, "alerts/#", Some("workers")),
        (&second.session.session_id, "metrics/#", Some("other")),
    ] {
        broker
            .dist
            .add_route(RouteRecord {
                tenant_id: "demo".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.clone(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all?tenant_id=demo&shared_group=workers&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert_eq!(dist.count_tenant_shared_routes_calls(), 1);
    assert_eq!(dist.list_routes_calls(), 0);
}

#[tokio::test]
async fn http_rejects_route_purge_without_filter() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::delete("/v1/routes/all")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

