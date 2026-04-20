use super::*;
use axum::http::Request;
use async_trait::async_trait;
use greenmqtt_core::{
    RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
};
use greenmqtt_kv_server::{KvRangeHost, ReplicaTransport};
use serde_json::json;

#[derive(Clone, Default)]
struct NoopTransport;

#[async_trait]
impl ReplicaTransport for NoopTransport {
    async fn send(
        &self,
        _from_node_id: u64,
        _target_node_id: u64,
        _range_id: &str,
        _message: &greenmqtt_kv_raft::RaftMessage,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn http_range_control_bootstrap_drain_and_retire() {
    let broker = broker();
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine.clone(),
        })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker.clone(),
        None,
        None,
        Some(runtime.clone()),
        None,
    );

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/bootstrap")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "descriptor": {
                            "id": "range-http",
                            "shard": {"kind": "Retain", "tenant_id": "t1", "scope": "*"},
                            "boundary": {"start_key": null, "end_key": null},
                            "epoch": 1,
                            "config_version": 1,
                            "leader_node_id": 1,
                            "replicas": [{"node_id": 1, "role": "Voter", "sync_state": "Replicating"}],
                            "commit_index": 0,
                            "applied_index": 0,
                            "lifecycle": "Bootstrapping"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::range::RangeActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.status, "bootstrapped");
    assert!(reply.reason.contains("serving"));

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/range-http/drain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::range::RangeActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.status, "draining");
    assert!(reply.reason.contains("migrate"));

    let response = app
        .oneshot(
            Request::delete("/v1/ranges/range-http")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let audit = broker.list_admin_audit(None);
    assert!(audit.iter().any(|entry| entry.action == "range_bootstrap"));
    assert!(audit.iter().any(|entry| entry.action == "range_drain"));
    assert!(audit.iter().any(|entry| entry.action == "range_retire"));
}

#[tokio::test]
async fn http_range_control_can_change_replicas_split_merge_and_list_zombies() {
    let broker = broker();
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine.clone(),
        })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        Some(runtime.clone()),
        None,
    );

    runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-http-ops",
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

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/range-http-ops/change-replicas")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"voters":[1,2], "learners":[]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/range-http-ops/transfer-leadership")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"target_node_id":1}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/range-http-ops/recover")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"new_leader_node_id":1}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/range-http-ops/split")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"split_key":[109]}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::range::RangeActionReply = serde_json::from_slice(&body).unwrap();
    let left = reply.left_range_id.unwrap();
    let right = reply.right_range_id.unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/ranges/merge")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"left_range_id": left, "right_range_id": right}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let merged: crate::range::RangeActionReply = serde_json::from_slice(&body).unwrap();
    let merged_range_id = merged.range_id.unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/ranges/{merged_range_id}/drain"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::get(format!("/v1/ranges?lifecycle=draining&range_id_prefix={merged_range_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let draining: Vec<greenmqtt_kv_server::RangeHealthSnapshot> =
        serde_json::from_slice(&body).unwrap();
    assert!(draining.iter().any(|entry| entry.range_id == merged_range_id));
}

#[tokio::test]
async fn http_range_list_supports_draining_and_zombie_filters() {
    let broker = broker();
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine.clone(),
        })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        Some(runtime.clone()),
        None,
    );

    runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-draining",
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
    runtime.drain_range("range-draining").await.unwrap();

    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "range-zombie".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let zombie_space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine.open_range("range-zombie").await.unwrap(),
    );
    let zombie_raft = Arc::new(MemoryRaftNode::new(
        1,
        "range-zombie",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![1, 2],
            learners: Vec::new(),
        },
    ));
    zombie_raft.recover().await.unwrap();
    host.add_range(HostedRange {
        descriptor: ReplicatedRangeDescriptor::new(
            "range-zombie",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            None,
            vec![RangeReplica::new(
                1,
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

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/ranges?lifecycle=draining")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let draining: Vec<greenmqtt_kv_server::RangeHealthSnapshot> =
        serde_json::from_slice(&body).unwrap();
    assert_eq!(draining.len(), 1);
    assert_eq!(draining[0].range_id, "range-draining");

    let response = app
        .oneshot(
            Request::get("/v1/ranges/zombies?range_id_prefix=range-z")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let zombies: Vec<greenmqtt_kv_server::ZombieRangeSnapshot> =
        serde_json::from_slice(&body).unwrap();
    assert_eq!(zombies.len(), 1);
    assert_eq!(zombies[0].range_id, "range-zombie");
}
