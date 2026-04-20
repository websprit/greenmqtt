use super::*;
#[tokio::test]
async fn http_lookup_sessiondict_by_identity() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/sessiondict/by-identity?tenant_id=demo&user_id=alice&client_id=sub")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let record: Option<greenmqtt_core::SessionRecord> = serde_json::from_slice(&body).unwrap();
    let record = record.expect("session record must exist");
    assert_eq!(record.session_id, session.session_id);
    assert_eq!(record.identity.tenant_id, "demo");
    assert_eq!(record.identity.user_id, "alice");
    assert_eq!(record.identity.client_id, "sub");
}

#[tokio::test]
async fn http_lists_sessiondict_records_with_tenant_filter() {
    let broker = broker();
    for (tenant_id, client_id) in [("demo", "sub-a"), ("demo", "sub-b"), ("other", "sub-c")] {
        broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: tenant_id.into(),
                    user_id: "alice".into(),
                    client_id: client_id.into(),
                },
                node_id: 1,
                kind: SessionKind::Persistent,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await
            .unwrap();
    }

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get("/v1/sessiondict?tenant_id=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let records: Vec<greenmqtt_core::SessionRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|record| record.identity.tenant_id == "demo"));
}

#[tokio::test]
async fn http_lookup_sessiondict_by_session_id() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::get(format!("/v1/sessiondict/{}", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let record: Option<greenmqtt_core::SessionRecord> = serde_json::from_slice(&body).unwrap();
    let record = record.expect("session record must exist");
    assert_eq!(record.session_id, session.session_id);
    assert_eq!(record.identity.client_id, "sub");
}

#[tokio::test]
async fn http_can_delete_sessiondict_record() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!("/v1/sessiondict/{}", session.session_id))
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
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .is_none());
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "delete_sessiondict");
    assert_eq!(
        audit[0].details.get("session_id").unwrap(),
        &session.session_id
    );
}

#[tokio::test]
async fn http_delete_sessiondict_record_uses_unregister_fast_path() {
    let sessiondict = Arc::new(CountingSessionDirectory::default());
    let broker = broker_with_sessiondict(sessiondict.clone());
    let session = broker
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
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!("/v1/sessiondict/{}", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);
    assert_eq!(sessiondict.unregister_calls(), 1);
    assert_eq!(sessiondict.lookup_session_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_sessiondict_delete_reports_without_removing() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete(format!(
                "/v1/sessiondict/{}?dry_run=true",
                session.session_id
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
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .is_some());
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_delete_missing_sessiondict_record_returns_zero_removed() {
    let broker = broker();
    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::delete("/v1/sessiondict/missing-session")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 0);
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "delete_sessiondict");
    assert_eq!(audit[0].details.get("removed").unwrap(), "0");
}

#[tokio::test]
async fn http_can_kill_sessiondict_record_and_remove_live_state() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
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
            Request::post(format!("/v1/sessiondict/{}/kill", session.session_id))
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
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .is_none());
    assert!(broker.list_local_sessions().await.unwrap().is_empty());
    assert!(broker
        .dist
        .list_session_routes(&session.session_id)
        .await
        .unwrap()
        .is_empty());
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "kill_session");
}

#[tokio::test]
async fn http_can_kill_all_sessiondict_records_for_tenant() {
    let broker = broker();
    let demo_a = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-a".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    let demo_b = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub-b".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;
    let other = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "other".into(),
                user_id: "bob".into(),
                client_id: "sub-c".into(),
            },
            node_id: 2,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap()
        .session;

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::post("/v1/sessiondict/kill-all?tenant_id=demo")
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
        .sessiondict
        .lookup_session(&demo_a.session_id)
        .await
        .unwrap()
        .is_none());
    assert!(broker
        .sessiondict
        .lookup_session(&demo_b.session_id)
        .await
        .unwrap()
        .is_none());
    assert!(broker
        .sessiondict
        .lookup_session(&other.session_id)
        .await
        .unwrap()
        .is_some());
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "kill_sessions");
    assert_eq!(audit[0].details.get("tenant_id").unwrap(), "demo");
}

#[tokio::test]
async fn http_can_report_session_inbox_state() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
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
        .inbox
        .enqueue(OfflineMessage {
            tenant_id: "demo".into(),
            session_id: session.session_id.clone(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    broker
        .inbox
        .stage_inflight(greenmqtt_core::InflightMessage {
            tenant_id: "demo".into(),
            session_id: session.session_id.clone(),
            packet_id: 7,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
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
            Request::get(format!(
                "/v1/sessiondict/{}/inbox-state",
                session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionInboxStateReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.session_id, session.session_id);
    assert!(reply.session_present);
    assert_eq!(reply.subscriptions, 1);
    assert_eq!(reply.offline_messages, 1);
    assert_eq!(reply.inflight_messages, 1);
}

#[tokio::test]
async fn http_kill_sessiondict_record_redirects_to_remote_owner() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![7]));
    peers
        .add_peer_node(7, "http://127.0.0.1:50077".into())
        .await
        .unwrap();
    broker
        .sessiondict
        .register(SessionRecord {
            session_id: "remote-session".into(),
            node_id: 7,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router_with_peers(broker.clone(), Some(peers));
    let response = app
        .oneshot(
            Request::post("/v1/sessiondict/remote-session/kill")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: ErrorBody = serde_json::from_slice(&body).unwrap();
    assert!(error.error.contains("kill"));
    assert_eq!(
        error.server_reference.as_deref(),
        Some("http://127.0.0.1:50077")
    );
    assert_eq!(error.node_id, Some(7));
}

#[tokio::test]
async fn http_takeover_sessiondict_record_fences_local_session() {
    let broker = broker();
    let session = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "sub".into(),
            },
            node_id: broker.config.node_id,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await
        .unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::post(format!(
                "/v1/sessiondict/{}/takeover?expiry_override_secs=30",
                session.session.session_id
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
    assert!(broker.list_local_sessions().await.unwrap().is_empty());
    let persisted = broker
        .sessiondict
        .lookup_session(&session.session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.session_expiry_interval_secs, Some(30));
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "takeover_session");
}

#[tokio::test]
async fn http_can_reassign_sessiondict_record_and_routes() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply,
        SessionDictReassignReply {
            updated_sessions: 1,
            updated_routes: 1,
        }
    );
    let updated = broker
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.node_id, 7);
    let route = broker
        .dist
        .list_routes(Some("demo"))
        .await
        .unwrap()
        .into_iter()
        .find(|route| route.session_id == session.session_id)
        .unwrap();
    assert_eq!(route.node_id, 7);
    let audit = broker.list_admin_audit(None);
    assert_eq!(audit[0].action, "reassign_sessiondict");
    assert_eq!(audit[0].details.get("node_id").unwrap(), "7");
}

#[tokio::test]
async fn http_reassign_sessiondict_record_uses_single_session_lookup() {
    let sessiondict = Arc::new(CountingSessionDirectory::default());
    let broker = broker_with_sessiondict(sessiondict.clone());
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.updated_sessions, 1);
    assert_eq!(reply.updated_routes, 1);
    assert_eq!(sessiondict.lookup_session_calls(), 1);
}

#[tokio::test]
async fn http_reassign_sessiondict_record_skips_dry_run_route_count_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.updated_sessions, 1);
    assert_eq!(reply.updated_routes, 1);
    assert_eq!(dist.count_session_routes_calls(), 0);
}

#[tokio::test]
async fn http_dry_run_reassign_sessiondict_record_reports_without_removing() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!(
                "/v1/sessiondict/{}?node_id=7&dry_run=true",
                session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply,
        SessionDictReassignReply {
            updated_sessions: 1,
            updated_routes: 1,
        }
    );
    let unchanged = broker
        .sessiondict
        .lookup_session(&session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.node_id, 2);
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_dry_run_reassign_sessiondict_uses_session_route_count_fast_path() {
    let dist = Arc::new(CountingDistRouter::default());
    let broker = broker_with_dist(dist.clone());
    let session = broker
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
        .unwrap()
        .session;
    broker
        .subscribe(
            &session.session_id,
            "devices/#",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await
        .unwrap();
    broker.disconnect(&session.session_id).await.unwrap();

    let app = HttpApi::router(broker.clone());
    let response = app
        .oneshot(
            Request::put(format!(
                "/v1/sessiondict/{}?node_id=7&dry_run=true",
                session.session_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: SessionDictReassignReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply,
        SessionDictReassignReply {
            updated_sessions: 1,
            updated_routes: 1,
        }
    );
    assert_eq!(dist.count_session_routes_calls(), 1);
    assert_eq!(dist.list_session_routes_calls(), 0);
    assert_eq!(dist.remove_session_routes_calls(), 0);
    assert_eq!(dist.remove_tenant_routes_calls(), 0);
    assert_eq!(dist.remove_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn http_reassign_sessiondict_record_rejects_local_online_session() {
    let broker = broker();
    let session = broker
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
        .unwrap()
        .session;

    let app = HttpApi::router(broker);
    let response = app
        .oneshot(
            Request::put(format!("/v1/sessiondict/{}?node_id=7", session.session_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: ErrorBody = serde_json::from_slice(&body).unwrap();
    assert!(error
        .error
        .contains("cannot reassign a locally online session"));
}
