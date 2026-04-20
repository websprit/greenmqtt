use super::*;
use async_trait::async_trait;
use axum::http::Request;
use greenmqtt_core::{
    ControlCommandExecutionState, ControlCommandRecord, ControlCommandReflectionState,
    RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
};
use greenmqtt_kv_server::{KvRangeHost, ReplicaTransport};
use serde_json::json;
use std::collections::BTreeMap;

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
            Request::get(format!(
                "/v1/ranges?lifecycle=draining&range_id_prefix={merged_range_id}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let draining: Vec<greenmqtt_kv_server::RangeHealthSnapshot> =
        serde_json::from_slice(&body).unwrap();
    assert!(draining
        .iter()
        .any(|entry| entry.range_id == merged_range_id));
}

#[tokio::test]
async fn http_range_get_and_action_reply_include_mode_and_command_id() {
    let broker = broker();
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager { engine })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let registry = Arc::new(TestShardRegistry::default());
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker.clone(),
        None,
        None,
        Some(registry.clone()),
        Some(runtime.clone()),
        None,
    );

    runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-http-get",
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
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-http-get",
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
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/ranges/range-http-get")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: Option<crate::range::RangeLookupReply> = serde_json::from_slice(&body).unwrap();
    let reply = reply.unwrap();
    assert_eq!(reply.source, "metadata_fallback");
    assert_eq!(reply.descriptor.unwrap().id, "range-http-get");

    let response = app
        .oneshot(
            Request::post("/v1/ranges/range-http-get/drain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::range::RangeActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.mode, "local");
    assert_eq!(reply.audit_action.as_deref(), Some("range_drain"));
    assert!(reply
        .command_id
        .as_deref()
        .is_some_and(|value| value.starts_with("manual:drain:range-http-get:")));
}

#[tokio::test]
async fn http_range_get_uses_live_source_when_runtime_has_health() {
    let broker = broker();
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager { engine })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-live",
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
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        None,
        Some(runtime),
        None,
    );

    let response = app
        .oneshot(
            Request::get("/v1/ranges/range-live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: Option<crate::range::RangeLookupReply> = serde_json::from_slice(&body).unwrap();
    let reply = reply.unwrap();
    assert_eq!(reply.source, "live");
    assert_eq!(reply.health.unwrap().range_id, "range-live");
}

#[tokio::test]
async fn http_repeated_range_actions_append_manual_command_history() {
    let broker = broker();
    let engine = MemoryKvEngine::default();
    let host = Arc::new(MemoryKvRangeHost::default());
    let runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager { engine })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    let registry = Arc::new(TestShardRegistry::default());
    runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-repeat",
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
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-repeat",
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
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        Some(registry.clone()),
        Some(runtime),
        None,
    );

    for _ in 0..2 {
        let _ = app
            .clone()
            .oneshot(
                Request::post("/v1/ranges/range-repeat/drain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    let commands = registry.list_control_commands().await.unwrap();
    assert_eq!(commands.len(), 2);
    assert_ne!(commands[0].command_id, commands[1].command_id);
}

#[tokio::test]
async fn http_control_command_surfaces_support_filter_retry_fail_and_prune() {
    let broker = broker();
    let registry = Arc::new(TestShardRegistry::default());
    registry
        .upsert_control_command(ControlCommandRecord {
            command_id: "cmd-1".into(),
            command_type: "range_drain".into(),
            target_range_or_shard: "range-a".into(),
            issued_at_ms: 1,
            issued_by: "http".into(),
            attempt_count: 0,
            payload: BTreeMap::new(),
            execution_state: ControlCommandExecutionState::Issued,
            reflection_state: ControlCommandReflectionState::Pending,
            last_error: None,
        })
        .await
        .unwrap();
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker.clone(),
        None,
        None,
        Some(registry.clone()),
        None,
        None,
    );

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/control-commands?issued_by=http")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listed: Vec<ControlCommandRecord> = serde_json::from_slice(&body).unwrap();
    assert_eq!(listed.len(), 1);

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/control-commands/cmd-1/retry")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let retried: Option<ControlCommandRecord> = serde_json::from_slice(&body).unwrap();
    let retried = retried.unwrap();
    assert_eq!(retried.execution_state, ControlCommandExecutionState::Issued);
    assert_eq!(retried.payload.get("retry_of").map(String::as_str), Some("cmd-1"));

    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/control-commands/cmd-1/fail")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"last_error":"manual"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let failed: Option<ControlCommandRecord> = serde_json::from_slice(&body).unwrap();
    let failed = failed.unwrap();
    assert_eq!(failed.execution_state, ControlCommandExecutionState::TerminalFailed);
    assert_eq!(
        failed.payload.get("parent_command_id").map(String::as_str),
        Some("cmd-1")
    );

    let listed = registry.list_control_commands().await.unwrap();
    assert_eq!(listed.len(), 3);

    let response = app
        .oneshot(
            Request::delete("/v1/control-commands?older_than_ms=1000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let pruned: PurgeReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(pruned.removed, 1);
    assert_eq!(registry.list_control_commands().await.unwrap().len(), 2);
}

#[tokio::test]
async fn http_range_filters_use_structured_metadata() {
    let broker = broker();
    let registry = Arc::new(TestShardRegistry::default());
    registry
        .upsert_member(ClusterNodeMembership::new(
            7,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(ServiceKind::Retain, 7, "http://127.0.0.1:50070")],
        ))
        .await
        .unwrap();
    registry
        .upsert_assignment(ServiceShardAssignment::new(
            ServiceShardKey::retain("tenant-1"),
            ServiceEndpoint::new(ServiceKind::Retain, 7, "http://127.0.0.1:50070"),
            1,
            1,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-tenant-1",
            ServiceShardKey::retain("tenant-1"),
            RangeBoundary::full(),
            1,
            1,
            Some(9),
            vec![RangeReplica::new(
                9,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();
    registry
        .upsert_control_command(ControlCommandRecord {
            command_id: "cmd-shard".into(),
            command_type: "range_drain".into(),
            target_range_or_shard: "Retain:tenant-1:*".into(),
            issued_at_ms: 1,
            issued_by: "http".into(),
            attempt_count: 0,
            payload: BTreeMap::new(),
            execution_state: ControlCommandExecutionState::Applied,
            reflection_state: ControlCommandReflectionState::Pending,
            last_error: None,
        })
        .await
        .unwrap();
    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        Some(registry),
        None,
        None,
    );

    let response = app
        .oneshot(
            Request::get(
                "/v1/ranges?leader_node_id=9&owner_node_id=7&tenant_id=tenant-1&service_kind=retain&command_id=cmd-shard",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let ranges: Vec<greenmqtt_kv_server::RangeHealthSnapshot> = serde_json::from_slice(&body).unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].range_id, "range-tenant-1");
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

#[tokio::test]
async fn http_range_control_forwards_to_remote_owner_when_local_runtime_is_wrong_node() {
    let broker = broker();
    let registry = Arc::new(TestShardRegistry::default());

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);

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
    runtime
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-http-forward",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(2),
            vec![RangeReplica::new(
                2,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Bootstrapping,
        ))
        .await
        .unwrap();

    registry
        .upsert_member(ClusterNodeMembership::new(
            2,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                2,
                format!("http://{bind}"),
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_range(ReplicatedRangeDescriptor::new(
            "range-http-forward",
            ServiceShardKey::retain("t1"),
            RangeBoundary::full(),
            1,
            1,
            Some(2),
            vec![RangeReplica::new(
                2,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        ))
        .await
        .unwrap();

    let rpc = tokio::spawn(
        greenmqtt_rpc::RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: Some(host.clone()),
            range_runtime: Some(runtime.clone()),
            inbox_lwt_sink: None,
        }
        .serve(bind),
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        Some(registry),
        None,
        None,
    );

    let response = app
        .oneshot(
            Request::post("/v1/ranges/range-http-forward/drain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reply: crate::range::RangeActionReply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.status, "draining");
    assert!(reply.forwarded);
    assert_eq!(reply.target_node_id, Some(2));
    assert_eq!(
        reply.target_endpoint.as_deref(),
        Some(format!("http://{bind}").as_str())
    );

    let hosted = host
        .open_range("range-http-forward")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hosted.descriptor.lifecycle, ServiceShardLifecycle::Draining);

    rpc.abort();
}

#[tokio::test]
async fn http_range_lists_aggregate_cluster_health_draining_pending_and_zombies() {
    let broker = broker();
    let registry = Arc::new(TestShardRegistry::default());

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let node1_bind = listener.local_addr().unwrap();
    drop(listener);
    let engine1 = MemoryKvEngine::default();
    let host1 = Arc::new(MemoryKvRangeHost::default());
    let runtime1 = Arc::new(ReplicaRuntime::with_config(
        host1.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine1.clone(),
        })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    runtime1
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-draining-cluster",
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
    runtime1
        .drain_range("range-draining-cluster")
        .await
        .unwrap();

    let node1_server = tokio::spawn(
        greenmqtt_rpc::RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: Some(host1.clone()),
            range_runtime: Some(runtime1.clone()),
            inbox_lwt_sink: None,
        }
        .serve(node1_bind),
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let node2_bind = listener.local_addr().unwrap();
    drop(listener);
    let engine2 = MemoryKvEngine::default();
    let host2 = Arc::new(MemoryKvRangeHost::default());
    let runtime2 = Arc::new(ReplicaRuntime::with_config(
        host2.clone(),
        Arc::new(NoopTransport),
        Some(Arc::new(TestRangeLifecycleManager {
            engine: engine2.clone(),
        })),
        std::time::Duration::from_secs(1),
        64,
        64,
        64,
    ));
    runtime2
        .bootstrap_range(ReplicatedRangeDescriptor::new(
            "range-pending-cluster",
            ServiceShardKey::retain("t2"),
            RangeBoundary::full(),
            1,
            1,
            Some(2),
            vec![RangeReplica::new(
                2,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Bootstrapping,
        ))
        .await
        .unwrap();
    runtime2
        .change_replicas("range-pending-cluster", vec![2, 3], Vec::new())
        .await
        .unwrap();

    engine2
        .bootstrap(KvRangeBootstrap {
            range_id: "range-zombie-cluster".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let zombie_space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
        engine2.open_range("range-zombie-cluster").await.unwrap(),
    );
    let zombie_raft = Arc::new(MemoryRaftNode::new(
        2,
        "range-zombie-cluster",
        greenmqtt_kv_raft::RaftClusterConfig {
            voters: vec![2, 3],
            learners: Vec::new(),
        },
    ));
    zombie_raft.recover().await.unwrap();
    host2
        .add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-zombie-cluster",
                ServiceShardKey::retain("t3"),
                RangeBoundary::full(),
                1,
                1,
                None,
                vec![RangeReplica::new(
                    2,
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

    let node2_server = tokio::spawn(
        greenmqtt_rpc::RpcRuntime {
            sessiondict: Arc::new(SessionDictHandle::default()),
            dist: Arc::new(DistHandle::default()),
            inbox: Arc::new(InboxHandle::default()),
            retain: Arc::new(RetainHandle::default()),
            peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
            assignment_registry: Some(registry.clone()),
            range_host: Some(host2.clone()),
            range_runtime: Some(runtime2.clone()),
            inbox_lwt_sink: None,
        }
        .serve(node2_bind),
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    registry
        .upsert_member(ClusterNodeMembership::new(
            1,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                1,
                format!("http://{node1_bind}"),
            )],
        ))
        .await
        .unwrap();
    registry
        .upsert_member(ClusterNodeMembership::new(
            2,
            1,
            ClusterNodeLifecycle::Serving,
            vec![ServiceEndpoint::new(
                ServiceKind::Retain,
                2,
                format!("http://{node2_bind}"),
            )],
        ))
        .await
        .unwrap();

    let app = HttpApi::router_with_peers_shards_metrics_and_ranges(
        broker,
        None,
        None,
        Some(registry),
        None,
        None,
    );

    let response = app
        .clone()
        .oneshot(Request::get("/v1/ranges").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let all: Vec<greenmqtt_kv_server::RangeHealthSnapshot> = serde_json::from_slice(&body).unwrap();
    assert!(all
        .iter()
        .any(|entry| entry.range_id == "range-draining-cluster"));
    assert!(all
        .iter()
        .any(|entry| entry.range_id == "range-pending-cluster"));
    assert!(all
        .iter()
        .any(|entry| entry.range_id == "range-zombie-cluster"));

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
    assert_eq!(draining[0].range_id, "range-draining-cluster");

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/ranges?pending_only=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let pending: Vec<greenmqtt_kv_server::RangeHealthSnapshot> =
        serde_json::from_slice(&body).unwrap();
    assert!(pending.iter().any(|entry| {
        entry.range_id == "range-pending-cluster" && entry.reconfiguration.phase.is_some()
    }));

    let response = app
        .oneshot(
            Request::get("/v1/ranges/zombies")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let zombies: Vec<greenmqtt_kv_server::ZombieRangeSnapshot> =
        serde_json::from_slice(&body).unwrap();
    assert!(zombies
        .iter()
        .any(|entry| entry.range_id == "range-zombie-cluster"));

    node1_server.abort();
    node2_server.abort();
}
