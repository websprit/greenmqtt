use super::*;
#[tokio::test]
async fn http_lists_peers_from_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_can_delete_peer_from_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(Request::delete("/v1/peers/5").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![PeerSummary {
            node_id: 2,
            rpc_addr: Some("http://127.0.0.1:50062".into()),
            connected: true,
        }]
    );
}

#[tokio::test]
async fn http_can_upsert_peer_into_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/peers/5?rpc_addr=http://127.0.0.1:50065")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_dry_run_delete_peer_reports_without_removing() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2, 5]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::delete("/v1/peers/5?dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}

#[tokio::test]
async fn http_dry_run_upsert_peer_reports_without_mutating_registry() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![2]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/peers/5?rpc_addr=http://127.0.0.1:50065&dry_run=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.removed, 1);

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![PeerSummary {
            node_id: 2,
            rpc_addr: Some("http://127.0.0.1:50062".into()),
            connected: true,
        }]
    );
}

#[tokio::test]
async fn http_can_set_and_get_tenant_quota() {
    let broker = broker();
    let app = HttpApi::router(broker);
    let quota = TenantQuota {
        max_connections: 3,
        max_subscriptions: 5,
        max_msg_per_sec: 7,
        max_memory_bytes: 1024,
    };

    let response = app
        .clone()
        .oneshot(
            Request::put("/v1/tenants/demo/quota")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&quota).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let updated: TenantQuotaResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(updated.tenant_id, "demo");
    assert_eq!(updated.quota, Some(quota.clone()));

    let response = app
        .oneshot(
            Request::get("/v1/tenants/demo/quota")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let fetched: TenantQuotaResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(fetched.tenant_id, "demo");
    assert_eq!(fetched.quota, Some(quota));
}

#[tokio::test]
async fn http_lists_peers_from_registry_in_sorted_order() {
    let broker = broker();
    let peers: Arc<dyn PeerRegistry> = Arc::new(TestPeerRegistry::new(vec![5, 2, 3]));
    let app = HttpApi::router_with_peers(broker, Some(peers));

    let response = app
        .oneshot(Request::get("/v1/peers").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let peers: Vec<PeerSummary> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        peers,
        vec![
            PeerSummary {
                node_id: 2,
                rpc_addr: Some("http://127.0.0.1:50062".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 3,
                rpc_addr: Some("http://127.0.0.1:50063".into()),
                connected: true,
            },
            PeerSummary {
                node_id: 5,
                rpc_addr: Some("http://127.0.0.1:50065".into()),
                connected: true,
            },
        ]
    );
}
