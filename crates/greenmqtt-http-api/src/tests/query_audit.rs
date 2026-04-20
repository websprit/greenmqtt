use super::*;
#[tokio::test]
async fn http_lists_matched_routes_for_topic_query() {
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
            None,
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
            Request::get("/v1/routes?tenant_id=demo&topic=devices/d1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<greenmqtt_core::RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].tenant_id, "demo");
    assert_eq!(routes[0].topic_filter, "devices/+/state");
    assert_eq!(routes[0].session_id, subscriber.session.session_id);
}

#[tokio::test]
async fn http_lists_all_routes_with_filters() {
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
            None,
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
            None,
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
                "/v1/routes/all?tenant_id=demo&session_id={}",
                first.session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let routes: Vec<greenmqtt_core::RouteRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].tenant_id, "demo");
    assert_eq!(routes[0].topic_filter, "devices/+/state");
    assert_eq!(routes[0].session_id, first.session.session_id);
}

#[tokio::test]
async fn http_can_disconnect_local_session_via_delete() {
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

    let app = HttpApi::router(broker.clone());
    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/v1/sessions/{}", subscriber.session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(broker
        .session_record(&subscriber.session.session_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn http_disconnect_records_admin_audit_entry() {
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

    let app = HttpApi::router(broker);
    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/v1/sessions/{}", subscriber.session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/audit?limit=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "disconnect_session");
    assert_eq!(
        audit[0].details.get("session_id").unwrap(),
        &subscriber.session.session_id
    );
}

#[tokio::test]
async fn http_subscription_purge_records_admin_audit_entry() {
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
        .clone()
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

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/audit?limit=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    let purge_entry = audit
        .iter()
        .filter(|entry| entry.action == "purge_subscriptions")
        .max_by_key(|entry| entry.seq)
        .expect("missing purge_subscriptions audit entry");
    assert_eq!(purge_entry.details.get("removed").unwrap(), "1");
    assert_eq!(
        purge_entry.details.get("session_id").unwrap(),
        &subscriber.session.session_id
    );
}

#[tokio::test]
async fn http_audit_supports_action_and_since_seq_filters() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), "s1".into())]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("session_id".into(), "s2".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?action=purge_offline&since_seq=1&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "purge_offline");
    assert_eq!(audit[0].target, "offline");
    assert_eq!(audit[0].seq, 2);
    assert!(audit[0].timestamp_ms > 0);
}

#[tokio::test]
async fn http_audit_supports_details_filters() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([
            ("session_id".into(), "s1".into()),
            ("tenant_id".into(), "demo".into()),
        ]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([
            ("session_id".into(), "s2".into()),
            ("tenant_id".into(), "other".into()),
        ]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?details_key=tenant_id&details_value=demo&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "disconnect_session");
    assert_eq!(audit[0].details.get("tenant_id").unwrap(), "demo");
}

#[tokio::test]
async fn http_audit_supports_before_seq_pagination() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), "s1".into())]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("session_id".into(), "s2".into())]),
    );
    broker.record_admin_audit(
        "purge_subscriptions",
        "subscription",
        BTreeMap::from([("session_id".into(), "s3".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?before_seq=3&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[0].seq, 2);
    assert_eq!(audit[1].seq, 1);
}

#[tokio::test]
async fn http_audit_supports_since_timestamp_filters() {
    let broker = broker();
    broker.record_admin_audit(
        "disconnect_session",
        "session",
        BTreeMap::from([("session_id".into(), "s1".into())]),
    );
    let first_timestamp = broker.list_admin_audit(None)[0].timestamp_ms;
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("session_id".into(), "s2".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!(
                "/v1/audit?since_timestamp_ms={first_timestamp}&action=purge_offline&limit=10"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "purge_offline");
    assert!(audit[0].timestamp_ms >= first_timestamp);
}

#[tokio::test]
async fn http_delete_peer_records_admin_audit_entry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));

    let response = app
        .oneshot(Request::delete("/v1/peers/5").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "delete_peer");
    assert_eq!(audit[0].target, "peers");
    assert_eq!(audit[0].details.get("node_id").unwrap(), "5");
    assert_eq!(audit[0].details.get("removed").unwrap(), "1");
}

#[tokio::test]
async fn http_upsert_peer_records_admin_audit_entry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2]));
    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));

    let response = app
        .oneshot(
            Request::put("/v1/peers/5?rpc_addr=http://127.0.0.1:50065")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "upsert_peer");
    assert_eq!(audit[0].target, "peers");
    assert_eq!(audit[0].details.get("node_id").unwrap(), "5");
    assert_eq!(
        audit[0].details.get("rpc_addr").unwrap(),
        "http://127.0.0.1:50065"
    );
}

#[tokio::test]
async fn http_dry_run_peer_management_does_not_write_audit() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/peers/6?rpc_addr=http://127.0.0.1:50066&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::delete("/v1/peers/5?dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_audit_restores_persisted_entries_after_restart() {
    let audit_log_path = temp_audit_log_path("restore");
    let broker = broker_with_audit_log(audit_log_path.clone());
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

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!("/v1/sessions/{}", subscriber.session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    drop(broker);

    let restored = broker_with_audit_log(audit_log_path.clone());
    let restored_app = HttpApi::router(restored);
    let response = restored_app
        .oneshot(
            Request::get("/v1/audit?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    let disconnect_entry = audit
        .iter()
        .find(|entry| entry.action == "disconnect_session")
        .expect("missing disconnect_session audit entry");
    assert_eq!(
        disconnect_entry.details.get("session_id").unwrap(),
        &subscriber.session.session_id
    );

    let _ = std::fs::remove_file(&audit_log_path);
    if let Some(parent) = audit_log_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn http_audit_supports_shard_only_filter() {
    let broker = broker();
    broker.record_admin_audit(
        "shard_move",
        "shards",
        BTreeMap::from([("tenant_id".into(), "t1".into())]),
    );
    broker.record_admin_audit(
        "purge_offline",
        "offline",
        BTreeMap::from([("tenant_id".into(), "t1".into())]),
    );

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/audit?shard_only=true&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let audit: Vec<AdminAuditEntry> = serde_json::from_slice(&body).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "shard_move");
}
