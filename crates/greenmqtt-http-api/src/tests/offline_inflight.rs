use super::*;
#[tokio::test]
async fn http_lists_session_offline_messages() {
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
        .disconnect(&subscriber.session.session_id)
        .await
        .unwrap();
    broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
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
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/offline",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let offline: Vec<greenmqtt_core::OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].session_id, subscriber.session.session_id);
    assert_eq!(offline[0].payload, b"offline".to_vec());
}

#[tokio::test]
async fn http_lists_all_offline_messages_with_filters() {
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
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: first.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    broker
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "other".into(),
            session_id: second.session.session_id.clone(),
            topic: "alerts/1".into(),
            payload: b"offline-b".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/offline?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let offline: Vec<greenmqtt_core::OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].tenant_id, "demo");
    assert_eq!(offline[0].session_id, first.session.session_id);
    assert_eq!(offline[0].payload, b"offline-a".to_vec());
}

#[tokio::test]
async fn http_lists_all_offline_messages_by_tenant() {
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
    for (tenant_id, session_id, payload) in [
        (
            "demo",
            first.session.session_id.clone(),
            b"offline-a".to_vec(),
        ),
        (
            "other",
            second.session.session_id.clone(),
            b"offline-b".to_vec(),
        ),
    ] {
        broker
            .inbox
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id,
                topic: "devices/d1/state".into(),
                payload: payload.into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/offline?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let offline: Vec<greenmqtt_core::OfflineMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].tenant_id, "demo");
    assert_eq!(offline[0].session_id, first.session.session_id);
}

#[tokio::test]
async fn http_can_purge_offline_messages_by_filter() {
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
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?tenant_id=demo&session_id={}",
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
        .peek(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_offline_messages_by_session_only() {
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
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?session_id={}",
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
        .peek(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_offline_messages_by_tenant_only() {
    let broker = broker();
    let demo_a = broker
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
    let demo_b = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
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
    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "carol".into(),
                client_id: "sub-c".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for session_id in [
        demo_a.session.session_id.clone(),
        demo_b.session.session_id.clone(),
        other.session.session_id.clone(),
    ] {
        let tenant_id = if session_id == other.session.session_id {
            "other"
        } else {
            "demo"
        };
        broker
            .inbox
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id,
                topic: "devices/d1/state".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/offline?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert!(broker
        .inbox
        .list_tenant_offline("demo")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        broker
            .inbox
            .list_tenant_offline("other")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn http_dry_run_offline_purge_reports_without_removing() {
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
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?tenant_id=demo&session_id={}&dry_run=true",
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
        broker
            .inbox
            .peek(&subscriber.session.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_offline_purge_uses_session_count_fast_path() {
    let messages = Arc::new(CountingInboxStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        messages.clone(),
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
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            topic: "devices/d1/state".into(),
            payload: b"offline-a".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/offline?tenant_id=demo&session_id={}&dry_run=true",
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
    assert_eq!(messages.count_session_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
    assert_eq!(messages.list_tenant_messages_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_offline_purge_uses_tenant_count_fast_path() {
    let messages = Arc::new(CountingInboxStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        messages.clone(),
        Arc::new(MemoryInflightStore::default()),
    ));
    let broker = broker_with_inbox(inbox);
    for (tenant_id, user_id, client_id) in [("demo", "alice", "sub-a"), ("other", "bob", "sub-b")] {
        let subscriber = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: tenant_id.into(),
                    user_id: user_id.into(),
                    client_id: client_id.into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        broker
            .inbox
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id: subscriber.session.session_id.clone(),
                topic: "devices/d1/state".into(),
                payload: client_id.as_bytes().to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/offline?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(messages.count_tenant_messages_calls(), 1);
    assert_eq!(messages.list_tenant_messages_calls(), 0);
    assert_eq!(messages.load_messages_calls(), 0);
}

#[tokio::test]
async fn http_rejects_offline_purge_without_filter() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let response = app
        .oneshot(Request::delete("/v1/offline").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_lists_session_inflight_messages() {
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
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "pub-session".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/sessions/{}/inflight",
                subscriber.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let inflight: Vec<greenmqtt_core::InflightMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].session_id, subscriber.session.session_id);
    assert_eq!(inflight[0].packet_id, 7);
    assert_eq!(inflight[0].payload, b"inflight".to_vec());
}

#[tokio::test]
async fn http_lists_all_inflight_messages_with_filters() {
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
    for (tenant_id, session_id, packet_id) in [
        ("demo", first.session.session_id.clone(), 7u16),
        ("other", second.session.session_id.clone(), 9u16),
    ] {
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id,
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/inflight?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let inflight: Vec<greenmqtt_core::InflightMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].tenant_id, "demo");
    assert_eq!(inflight[0].session_id, first.session.session_id);
    assert_eq!(inflight[0].packet_id, 7);
}

#[tokio::test]
async fn http_lists_all_inflight_messages_by_tenant() {
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
    for (tenant_id, session_id, packet_id) in [
        ("demo", first.session.session_id.clone(), 7u16),
        ("other", second.session.session_id.clone(), 9u16),
    ] {
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id,
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/inflight?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let inflight: Vec<greenmqtt_core::InflightMessage> = serde_json::from_slice(&body).unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].tenant_id, "demo");
    assert_eq!(inflight[0].session_id, first.session.session_id);
    assert_eq!(inflight[0].packet_id, 7);
}

#[tokio::test]
async fn http_can_purge_inflight_messages_by_filter() {
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
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?tenant_id=demo&session_id={}",
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
        .fetch_inflight(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_inflight_messages_by_session_only() {
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
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?session_id={}",
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
        .fetch_inflight(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn http_can_purge_inflight_messages_by_tenant_only() {
    let broker = broker();
    let demo_a = broker
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
    let demo_b = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
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
    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "carol".into(),
                client_id: "sub-c".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();
    for (tenant_id, session_id, packet_id) in [
        ("demo", demo_a.session.session_id.clone(), 7u16),
        ("demo", demo_b.session.session_id.clone(), 8u16),
        ("other", other.session.session_id.clone(), 9u16),
    ] {
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id,
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/inflight?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 2);
    assert!(broker
        .inbox
        .list_tenant_inflight("demo")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        broker
            .inbox
            .list_tenant_inflight("other")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn http_dry_run_inflight_purge_reports_without_removing() {
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
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?tenant_id=demo&session_id={}&dry_run=true",
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
        broker
            .inbox
            .fetch_inflight(&subscriber.session.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_inflight_purge_uses_session_count_fast_path() {
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        inflight.clone(),
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
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: subscriber.session.session_id.clone(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight-a".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: greenmqtt_core::InflightPhase::Publish,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/inflight?tenant_id=demo&session_id={}&dry_run=true",
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
    assert_eq!(inflight.count_session_inflight_calls(), 1);
    assert_eq!(inflight.load_inflight_calls(), 0);
    assert_eq!(inflight.list_tenant_inflight_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_inflight_purge_uses_tenant_count_fast_path() {
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox = Arc::new(PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        inflight.clone(),
    ));
    let broker = broker_with_inbox(inbox);
    for (tenant_id, user_id, client_id, packet_id) in [
        ("demo", "alice", "sub-a", 7u16),
        ("other", "bob", "sub-b", 9u16),
    ] {
        let subscriber = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: tenant_id.into(),
                    user_id: user_id.into(),
                    client_id: client_id.into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
        broker
            .inbox
            .stage_inflight(greenmqtt_core::InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: subscriber.session.session_id.clone(),
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: greenmqtt_core::InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/inflight?tenant_id=demo&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(inflight.count_tenant_inflight_calls(), 1);
    assert_eq!(inflight.list_tenant_inflight_calls(), 0);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

