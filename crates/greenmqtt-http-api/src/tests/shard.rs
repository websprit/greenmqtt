use super::*;

#[tokio::test]
async fn http_lists_shards_with_optional_filters() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    shards
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::retain("t2"),
            ServiceEndpoint::new(ServiceKind::Retain, 9, "http://127.0.0.1:50090"),
            2,
            11,
            ServiceShardLifecycle::Recovering,
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(broker, None, Some(shards), None);

    let response = app
        .clone()
        .oneshot(Request::get("/v1/shards").body(Body::empty()).unwrap())
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "unexpected status: {} body: {}",
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from_utf8_lossy(&body)
        );
    }
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let assignments: Vec<ServiceShardAssignment> = serde_json::from_slice(&body).unwrap();
    assert_eq!(assignments.len(), 2);

    let response = app
        .oneshot(
            Request::get("/v1/shards?kind=dist&tenant_id=t1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "unexpected status: {} body: {}",
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from_utf8_lossy(&body)
        );
    }
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let assignments: Vec<ServiceShardAssignment> = serde_json::from_slice(&body).unwrap();
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].shard.kind, ServiceShardKind::Dist);
    assert_eq!(assignments[0].shard.tenant_id, "t1");
}

#[tokio::test]
async fn http_gets_single_shard_assignment() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    let assignment = ServiceShardAssignment::new(
        ServiceShardKey::sessiondict("t1", "identity:u1:c1"),
        ServiceEndpoint::new(ServiceKind::SessionDict, 7, "http://127.0.0.1:50070"),
        3,
        12,
        ServiceShardLifecycle::Serving,
    );
    shards.upsert_assignment(assignment.clone()).await.unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(broker, None, Some(shards), None);

    let response = app
        .oneshot(
            Request::get("/v1/shards/sessiondict/t1/identity:u1:c1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "unexpected status: {} body: {}",
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from_utf8_lossy(&body)
        );
    }
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let found: Option<ServiceShardAssignment> = serde_json::from_slice(&body).unwrap();
    assert_eq!(found, Some(assignment));
}

#[tokio::test]
async fn http_can_move_shard_and_record_admin_audit() {
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
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        None,
        Some(shards.clone()),
        None,
    );
    let response = app
        .oneshot(
            Request::post("/v1/shards/dist/t1/*/move")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::shard::ShardActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.current.as_ref().unwrap().owner_node_id(), 9);
    assert_eq!(
        shards
            .resolve_assignment(&ServiceShardKey::dist("t1"))
            .await
            .unwrap()
            .unwrap()
            .owner_node_id(),
        9
    );
    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| entry.action == "shard_move"));
}

#[tokio::test]
async fn http_dry_run_drain_shard_does_not_mutate_or_audit() {
    let broker = broker();
    let shards = Arc::new(TestShardRegistry::default());
    let assignment = ServiceShardAssignment::new(
        ServiceShardKey::retain("t1"),
        ServiceEndpoint::new(ServiceKind::Retain, 7, "http://127.0.0.1:50070"),
        1,
        10,
        ServiceShardLifecycle::Serving,
    );
    shards.upsert_assignment(assignment.clone()).await.unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        None,
        Some(shards.clone()),
        None,
    );
    let response = app
        .oneshot(
            Request::post("/v1/shards/retain/t1/*/drain")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"dry_run":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::shard::ShardActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.previous, Some(assignment.clone()));
    assert_eq!(
        reply.current.as_ref().unwrap().lifecycle,
        ServiceShardLifecycle::Draining
    );
    assert_eq!(
        shards
            .resolve_assignment(&ServiceShardKey::retain("t1"))
            .await
            .unwrap()
            .unwrap(),
        assignment
    );
    assert!(broker.list_admin_audit(None).is_empty());
}

#[tokio::test]
async fn http_failover_shard_records_audit_and_metrics() {
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
            ServiceShardKey::dist("t1"),
            ServiceEndpoint::new(ServiceKind::Dist, 7, "http://127.0.0.1:50070"),
            1,
            10,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_and_metrics(
        broker.clone(),
        None,
        Some(shards.clone()),
        Some(metrics),
    );
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/shards/dist/t1/*/failover")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"target_node_id":9}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| entry.action == "shard_failover"));

    let response = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("mqtt_shard_failover_total"));
}
