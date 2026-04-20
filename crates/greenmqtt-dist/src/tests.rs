use crate::dist::{
    dist_route_shard, dist_tenant_shard, insert_tenant_route, DistBalanceAction, DistBalancePolicy,
    DistDeliveryReport, DistDeliverySink, DistFanoutRequest, DistFanoutWorker, DistHandle,
    DistMaintenanceWorker, DistRouter, DistTenantStats, PersistentDistHandle, ReplicatedDistHandle,
    TenantRoutes, ThresholdDistBalancePolicy,
};
use crate::trie::select_routes;
use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    PublishProperties, PublishRequest, RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState,
    ReplicatedRangeDescriptor, RouteRecord, ServiceShardKey, ServiceShardLifecycle, SessionKind,
};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, MemoryKvRangeRouter};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, MemoryKvEngine,
};
use greenmqtt_storage::{MemoryRouteStore, RouteStore};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone)]
struct LocalKvRangeExecutor {
    engine: MemoryKvEngine,
}

#[async_trait]
impl KvRangeExecutor for LocalKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let range = self.engine.open_range(range_id).await?;
        range.reader().get(key).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let range = self.engine.open_range(range_id).await?;
        range
            .reader()
            .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
            .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let range = self.engine.open_range(range_id).await?;
        range.writer().apply(mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let range = self.engine.open_range(range_id).await?;
        range.checkpoint(checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let range = self.engine.open_range(range_id).await?;
        range.snapshot().await
    }
}

#[derive(Clone, Default)]
struct RecordingKvRangeExecutor {
    engine: MemoryKvEngine,
    scan_boundaries: Arc<Mutex<Vec<Option<RangeBoundary>>>>,
}

#[async_trait]
impl KvRangeExecutor for RecordingKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let range = self.engine.open_range(range_id).await?;
        range.reader().get(key).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        self.scan_boundaries
            .lock()
            .expect("executor poisoned")
            .push(boundary.clone());
        let range = self.engine.open_range(range_id).await?;
        range
            .reader()
            .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
            .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let range = self.engine.open_range(range_id).await?;
        range.writer().apply(mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let range = self.engine.open_range(range_id).await?;
        range.checkpoint(checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let range = self.engine.open_range(range_id).await?;
        range.snapshot().await
    }
}

#[derive(Default)]
struct RecordingDistSink {
    deliveries: Arc<Mutex<Vec<(String, String, usize)>>>,
}

#[async_trait]
impl DistDeliverySink for RecordingDistSink {
    async fn deliver(
        &self,
        tenant_id: &str,
        fanout: &DistFanoutRequest,
        routes: &[RouteRecord],
    ) -> anyhow::Result<DistDeliveryReport> {
        self.deliveries.lock().expect("sink poisoned").push((
            tenant_id.to_string(),
            fanout.request.topic.clone(),
            routes.len(),
        ));
        Ok(DistDeliveryReport {
            online_deliveries: routes.len(),
            offline_enqueues: 0,
        })
    }
}

#[tokio::test]
async fn shared_subscription_selects_one_route() {
    let dist = DistHandle::default();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "sensors/+/temp".into(),
        session_id: "a".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: Some("workers".into()),
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "sensors/+/temp".into(),
        session_id: "b".into(),
        node_id: 2,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: Some("workers".into()),
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let matches = dist
        .match_topic("t1", &"sensors/1/temp".into())
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
}

#[test]
fn dist_helpers_keep_point_lookup_bulk_and_scan_on_same_tenant_shard() {
    let route = RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    };

    assert_eq!(dist_tenant_shard("t1"), ServiceShardKey::dist("t1"));
    assert_eq!(dist_route_shard(&route), ServiceShardKey::dist("t1"));
}

#[test]
fn wildcard_route_trie_matches_plus_and_hash_filters() {
    let mut routes = TenantRoutes::default();
    insert_tenant_route(
        &mut routes,
        RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "sensors/+/temp".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        },
    );
    insert_tenant_route(
        &mut routes,
        RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "sensors/#".into(),
            session_id: "s2".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        },
    );

    let matched = routes
        .wildcard_trie
        .match_topic(&"sensors/room-1/temp".into());
    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].topic_filter, "sensors/+/temp");
    assert_eq!(matched[1].topic_filter, "sensors/#");
}

#[test]
fn select_routes_preserves_wildcard_insert_order_with_trie() {
    let mut routes = TenantRoutes::default();
    insert_tenant_route(
        &mut routes,
        RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        },
    );
    insert_tenant_route(
        &mut routes,
        RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/#".into(),
            session_id: "s2".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        },
    );

    let selected = select_routes(&routes, &"devices/a/state".into());
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].topic_filter, "devices/+/state");
    assert_eq!(selected[1].topic_filter, "devices/#");
}

#[tokio::test]
async fn exact_and_wildcard_routes_both_match_topic() {
    let dist = DistHandle::default();
    for topic_filter in ["sensors/room-1/temp", "sensors/+/temp"] {
        dist.add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: topic_filter.into(),
            session_id: topic_filter.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    let matches = dist
        .match_topic("t1", &"sensors/room-1/temp".into())
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
}

#[tokio::test]
async fn updating_existing_route_does_not_duplicate_tenant_cache() {
    let dist = DistHandle::default();
    let route = RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/a/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    };
    dist.add_route(route.clone()).await.unwrap();

    let mut updated = route.clone();
    updated.node_id = 7;
    dist.add_route(updated.clone()).await.unwrap();

    assert_eq!(dist.route_count().await.unwrap(), 1);
    assert_eq!(dist.list_routes(Some("t1")).await.unwrap(), vec![updated]);
}

#[tokio::test]
async fn persistent_dist_restores_routes_from_store() {
    let store = Arc::new(MemoryRouteStore::default());
    let dist = PersistentDistHandle::open(store.clone()).await.unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "sensors/+/temp".into(),
        session_id: "a".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: Some("workers".into()),
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let reopened = PersistentDistHandle::open(store).await.unwrap();
    let matches = reopened
        .match_topic("t1", &"sensors/1/temp".into())
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
}

#[tokio::test]
async fn list_routes_can_filter_by_tenant() {
    let dist = DistHandle::default();
    for (tenant_id, session_id) in [("t1", "s1"), ("t2", "s2"), ("t1", "s3")] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: "devices/+/state".into(),
            session_id: session_id.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    let filtered = dist.list_routes(Some("t1")).await.unwrap();
    assert_eq!(filtered.len(), 2);
    assert!(filtered.iter().all(|route| route.tenant_id == "t1"));

    let all = dist.list_routes(None).await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn dist_handle_tracks_session_routes_and_route_count() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id) in [
        ("t1", "devices/+/state", "s1"),
        ("t1", "alerts/#", "s1"),
        ("t2", "metrics/#", "s2"),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    assert_eq!(dist.route_count().await.unwrap(), 3);
    assert_eq!(dist.list_session_routes("s1").await.unwrap().len(), 2);
    assert_eq!(dist.remove_session_routes("s1").await.unwrap(), 2);
    assert_eq!(dist.route_count().await.unwrap(), 1);
    assert!(dist.list_session_routes("s1").await.unwrap().is_empty());
}

#[tokio::test]
async fn dist_handle_remove_session_topic_filter_routes_removes_only_matching_entries() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id, shared_group) in [
        ("t1", "devices/+/state", "s1", None),
        ("t1", "devices/+/state", "s1", Some("workers")),
        ("t1", "alerts/#", "s1", None),
        ("t2", "devices/+/state", "s2", None),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
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

    let removed = dist
        .remove_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(dist.route_count().await.unwrap(), 2);
    assert!(dist
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(dist.list_session_routes("s1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn dist_handle_session_topic_shared_lookup_and_remove_use_exact_index() {
    let dist = DistHandle::default();
    for shared_group in [None, Some("workers")] {
        dist.add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
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

    let matched = dist
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(matched.shared_group.as_deref(), Some("workers"));
    assert_eq!(
        dist.count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
            .await
            .unwrap(),
        1
    );

    let removed = dist
        .remove_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert!(dist
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        dist.list_session_topic_filter_routes("s1", "devices/+/state")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn replicated_dist_match_topic_uses_indexed_scans_and_wildcard_cache() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "dist-range-prefix".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "dist-range-prefix",
            dist_tenant_shard("t1"),
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
    let executor = Arc::new(RecordingKvRangeExecutor {
        engine: engine.clone(),
        ..RecordingKvRangeExecutor::default()
    });
    let dist = ReplicatedDistHandle::new(router, executor.clone());
    for topic_filter in ["devices/a/state", "devices/+/state"] {
        dist.add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: topic_filter.into(),
            session_id: topic_filter.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    executor
        .scan_boundaries
        .lock()
        .expect("executor poisoned")
        .clear();
    let first = dist
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    let first_scan_count = executor
        .scan_boundaries
        .lock()
        .expect("executor poisoned")
        .len();
    assert!(first_scan_count >= 2);

    let second = dist
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(second.len(), 2);
    let second_scan_count = executor
        .scan_boundaries
        .lock()
        .expect("executor poisoned")
        .len();
    assert!(second_scan_count < first_scan_count * 2);
}

#[tokio::test]
async fn replicated_dist_list_session_routes_deduplicates_index_records() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "dist-range-session".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "dist-range-session",
            dist_tenant_shard("t1"),
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
    let executor = Arc::new(RecordingKvRangeExecutor {
        engine: engine.clone(),
        ..RecordingKvRangeExecutor::default()
    });
    let dist = ReplicatedDistHandle::new(router, executor.clone());
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: Some("workers".into()),
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let routes = dist.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);
}

#[tokio::test]
async fn dist_fanout_worker_delivers_matched_routes() {
    let dist: Arc<dyn DistRouter> = Arc::new(DistHandle::default());
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();
    let sink = Arc::new(RecordingDistSink::default());
    let worker = DistFanoutWorker::new(dist.clone());
    let outcome = worker
        .fanout(
            sink.as_ref(),
            "t1",
            &DistFanoutRequest {
                from_session_id: "publisher".into(),
                request: PublishRequest {
                    topic: "devices/a/state".into(),
                    payload: Bytes::from_static(b"payload"),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties::default(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome.matched_routes, 1);
    assert_eq!(outcome.online_deliveries, 1);
    assert_eq!(outcome.offline_enqueues, 0);
    assert_eq!(
        sink.deliveries.lock().expect("sink poisoned").as_slice(),
        [("t1".into(), "devices/a/state".into(), 1)]
    );
}

#[test]
fn threshold_dist_balance_policy_proposes_actions_for_hot_tenant() {
    let policy = ThresholdDistBalancePolicy {
        max_total_routes: 10,
        max_wildcard_routes: 5,
        max_shared_routes: 3,
        desired_voters: 3,
        desired_learners: 1,
    };
    let actions = policy.propose(&[
        DistTenantStats {
            tenant_id: "cool".into(),
            total_routes: 1,
            wildcard_routes: 1,
            shared_routes: 0,
        },
        DistTenantStats {
            tenant_id: "hot".into(),
            total_routes: 20,
            wildcard_routes: 9,
            shared_routes: 4,
        },
    ]);
    assert_eq!(
        actions,
        vec![
            DistBalanceAction::RunTenantCleanup {
                tenant_id: "hot".into(),
            },
            DistBalanceAction::SplitTenantRange {
                tenant_id: "hot".into(),
            },
            DistBalanceAction::ScaleTenantReplicas {
                tenant_id: "hot".into(),
                voters: 3,
                learners: 1,
            },
        ]
    );
}

#[tokio::test]
async fn dist_maintenance_worker_refreshes_tenant_cache() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "dist-range-maint".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "dist-range-maint",
            dist_tenant_shard("t1"),
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
    let dist = Arc::new(ReplicatedDistHandle::new(
        router,
        Arc::new(RecordingKvRangeExecutor {
            engine: engine.clone(),
            ..RecordingKvRangeExecutor::default()
        }),
    ));
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/#".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let worker = DistMaintenanceWorker::new(dist.clone());
    assert_eq!(worker.refresh_tenant("t1").await.unwrap(), 1);
    let task = worker.spawn_tenant_refresh("t1".into(), Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(3)).await;
    task.abort();
}

#[tokio::test]
async fn dist_handle_remove_tenant_routes_removes_only_matching_entries() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id) in [
        ("t1", "devices/+/state", "s1"),
        ("t1", "alerts/#", "s2"),
        ("t2", "metrics/#", "s3"),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    let removed = dist.remove_tenant_routes("t1").await.unwrap();
    assert_eq!(removed, 2);
    assert_eq!(dist.route_count().await.unwrap(), 1);
    assert!(dist.list_routes(Some("t1")).await.unwrap().is_empty());
    assert_eq!(dist.list_routes(Some("t2")).await.unwrap().len(), 1);
    assert!(dist.list_session_routes("s1").await.unwrap().is_empty());
    assert!(dist.list_session_routes("s2").await.unwrap().is_empty());
}

#[tokio::test]
async fn dist_handle_list_topic_filter_routes_matches_wildcard_filters_by_string() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id) in [
        ("t1", "devices/+/state", "s1"),
        ("t1", "alerts/#", "s2"),
        ("t2", "devices/+/state", "s3"),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    let routes = dist
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].session_id, "s1");
}

#[tokio::test]
async fn dist_handle_remove_topic_filter_routes_removes_only_matching_entries() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id) in [
        ("t1", "devices/+/state", "s1"),
        ("t1", "alerts/#", "s2"),
        ("t2", "devices/+/state", "s3"),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    }

    let removed = dist
        .remove_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(dist.route_count().await.unwrap(), 2);
    assert!(dist
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        dist.list_topic_filter_routes("t2", "devices/+/state")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn dist_handle_topic_filter_shared_routes_use_direct_index() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id, shared_group) in [
        ("t1", "devices/+/state", "s1", None),
        ("t1", "devices/+/state", "s2", Some("workers")),
        ("t1", "devices/+/state", "s3", Some("workers")),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
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

    assert_eq!(
        dist.list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        dist.count_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        dist.remove_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(dist.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn replicated_dist_routes_and_matches_topics_over_kv_ranges() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "dist-range-a".into(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"m".to_vec())),
        })
        .await
        .unwrap();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "dist-range-b".into(),
            boundary: RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec())),
        })
        .await
        .unwrap();

    let router = Arc::new(MemoryKvRangeRouter::default());
    let shard = dist_tenant_shard("t1");
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "dist-range-a",
            shard.clone(),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"m".to_vec())),
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
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "dist-range-b",
            shard,
            RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec())),
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

    let dist = ReplicatedDistHandle::new(
        router,
        Arc::new(LocalKvRangeExecutor {
            engine: engine.clone(),
        }),
    );
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "alerts/door".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "metrics/#".into(),
        session_id: "s2".into(),
        node_id: 2,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    assert_eq!(dist.route_count().await.unwrap(), 2);
    assert_eq!(dist.list_session_routes("s1").await.unwrap().len(), 1);
    assert_eq!(
        dist.match_topic("t1", &"metrics/cpu".into())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(dist.remove_session_routes("s1").await.unwrap(), 1);
    assert_eq!(dist.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn replicated_dist_reapplying_same_route_keeps_single_record() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "dist-range-idem".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "dist-range-idem",
            dist_tenant_shard("t1"),
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
    let dist = ReplicatedDistHandle::new(
        router,
        Arc::new(LocalKvRangeExecutor {
            engine: engine.clone(),
        }),
    );
    let route = RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    };

    dist.add_route(route.clone()).await.unwrap();
    dist.add_route(route).await.unwrap();

    assert_eq!(dist.route_count().await.unwrap(), 1);
    assert_eq!(dist.list_routes(Some("t1")).await.unwrap().len(), 1);
}

#[tokio::test]
async fn dist_handle_tenant_shared_routes_use_direct_index() {
    let dist = DistHandle::default();
    for (tenant_id, topic_filter, session_id, shared_group) in [
        ("t1", "devices/+/state", "s1", Some("workers")),
        ("t1", "alerts/#", "s2", Some("workers")),
        ("t1", "metrics/#", "s3", Some("other")),
    ] {
        dist.add_route(RouteRecord {
            tenant_id: tenant_id.into(),
            topic_filter: topic_filter.into(),
            session_id: session_id.into(),
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

    assert_eq!(
        dist.list_tenant_shared_routes("t1", Some("workers"))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        dist.count_tenant_shared_routes("t1", Some("workers"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        dist.remove_tenant_shared_routes("t1", Some("workers"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(dist.route_count().await.unwrap(), 1);
}

#[derive(Default)]
struct CountingRouteStore {
    inner: MemoryRouteStore,
    save_route_calls: AtomicUsize,
    load_session_topic_filter_route_calls: AtomicUsize,
    remove_session_topic_filter_route_calls: AtomicUsize,
    remove_session_routes_calls: AtomicUsize,
    remove_session_topic_filter_routes_calls: AtomicUsize,
    remove_tenant_routes_calls: AtomicUsize,
    remove_tenant_shared_routes_calls: AtomicUsize,
    remove_topic_filter_routes_calls: AtomicUsize,
    remove_topic_filter_shared_routes_calls: AtomicUsize,
    reassign_session_routes_calls: AtomicUsize,
    list_routes_calls: AtomicUsize,
    list_tenant_routes_calls: AtomicUsize,
    count_tenant_routes_calls: AtomicUsize,
    list_tenant_shared_routes_calls: AtomicUsize,
    count_tenant_shared_routes_calls: AtomicUsize,
    list_topic_filter_routes_calls: AtomicUsize,
    list_topic_filter_shared_routes_calls: AtomicUsize,
    count_topic_filter_routes_calls: AtomicUsize,
    count_topic_filter_shared_routes_calls: AtomicUsize,
    list_exact_routes_calls: AtomicUsize,
    list_tenant_wildcard_routes_calls: AtomicUsize,
    list_session_routes_calls: AtomicUsize,
    list_session_topic_filter_routes_calls: AtomicUsize,
    count_session_routes_calls: AtomicUsize,
    count_session_topic_filter_route_calls: AtomicUsize,
    count_session_topic_filter_routes_calls: AtomicUsize,
    count_routes_calls: AtomicUsize,
}

impl CountingRouteStore {
    fn save_route_calls(&self) -> usize {
        self.save_route_calls.load(Ordering::SeqCst)
    }

    fn load_session_topic_filter_route_calls(&self) -> usize {
        self.load_session_topic_filter_route_calls
            .load(Ordering::SeqCst)
    }

    fn remove_session_topic_filter_route_calls(&self) -> usize {
        self.remove_session_topic_filter_route_calls
            .load(Ordering::SeqCst)
    }

    fn remove_session_routes_calls(&self) -> usize {
        self.remove_session_routes_calls.load(Ordering::SeqCst)
    }

    fn remove_session_topic_filter_routes_calls(&self) -> usize {
        self.remove_session_topic_filter_routes_calls
            .load(Ordering::SeqCst)
    }

    fn remove_tenant_routes_calls(&self) -> usize {
        self.remove_tenant_routes_calls.load(Ordering::SeqCst)
    }

    fn remove_tenant_shared_routes_calls(&self) -> usize {
        self.remove_tenant_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn remove_topic_filter_routes_calls(&self) -> usize {
        self.remove_topic_filter_routes_calls.load(Ordering::SeqCst)
    }

    fn remove_topic_filter_shared_routes_calls(&self) -> usize {
        self.remove_topic_filter_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn reassign_session_routes_calls(&self) -> usize {
        self.reassign_session_routes_calls.load(Ordering::SeqCst)
    }

    fn list_routes_calls(&self) -> usize {
        self.list_routes_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_routes_calls(&self) -> usize {
        self.list_tenant_routes_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_routes_calls(&self) -> usize {
        self.count_tenant_routes_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_shared_routes_calls(&self) -> usize {
        self.list_tenant_shared_routes_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_shared_routes_calls(&self) -> usize {
        self.count_tenant_shared_routes_calls.load(Ordering::SeqCst)
    }

    fn list_topic_filter_routes_calls(&self) -> usize {
        self.list_topic_filter_routes_calls.load(Ordering::SeqCst)
    }

    fn list_topic_filter_shared_routes_calls(&self) -> usize {
        self.list_topic_filter_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn count_topic_filter_routes_calls(&self) -> usize {
        self.count_topic_filter_routes_calls.load(Ordering::SeqCst)
    }

    fn count_topic_filter_shared_routes_calls(&self) -> usize {
        self.count_topic_filter_shared_routes_calls
            .load(Ordering::SeqCst)
    }

    fn list_exact_routes_calls(&self) -> usize {
        self.list_exact_routes_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_wildcard_routes_calls(&self) -> usize {
        self.list_tenant_wildcard_routes_calls
            .load(Ordering::SeqCst)
    }

    fn list_session_routes_calls(&self) -> usize {
        self.list_session_routes_calls.load(Ordering::SeqCst)
    }

    fn list_session_topic_filter_routes_calls(&self) -> usize {
        self.list_session_topic_filter_routes_calls
            .load(Ordering::SeqCst)
    }

    fn count_session_routes_calls(&self) -> usize {
        self.count_session_routes_calls.load(Ordering::SeqCst)
    }

    fn count_session_topic_filter_route_calls(&self) -> usize {
        self.count_session_topic_filter_route_calls
            .load(Ordering::SeqCst)
    }

    fn count_session_topic_filter_routes_calls(&self) -> usize {
        self.count_session_topic_filter_routes_calls
            .load(Ordering::SeqCst)
    }

    fn count_routes_calls(&self) -> usize {
        self.count_routes_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RouteStore for CountingRouteStore {
    async fn save_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        self.save_route_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.save_route(route).await
    }

    async fn delete_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        self.inner.delete_route(route).await
    }

    async fn load_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        self.load_session_topic_filter_route_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .load_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn remove_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.remove_session_topic_filter_route_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        self.remove_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.remove_session_routes(session_id).await
    }

    async fn remove_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.remove_session_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.remove_tenant_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.remove_tenant_routes(tenant_id).await
    }

    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.remove_tenant_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn remove_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.remove_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.remove_topic_filter_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .remove_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        self.reassign_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .reassign_session_routes(session_id, node_id)
            .await
    }

    async fn list_routes(&self) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_routes_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_routes().await
    }

    async fn list_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_tenant_routes_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_routes(tenant_id).await
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_routes(tenant_id).await
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_tenant_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_tenant_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_topic_filter_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.count_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_topic_filter_shared_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_exact_routes_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_exact_routes(tenant_id, topic_filter).await
    }

    async fn list_tenant_wildcard_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_tenant_wildcard_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_wildcard_routes(tenant_id).await
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_session_routes(session_id).await
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_session_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_routes(session_id).await
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.count_session_topic_filter_routes_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.count_session_topic_filter_route_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .count_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn count_routes(&self) -> anyhow::Result<usize> {
        self.count_routes_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.count_routes().await
    }
}

#[tokio::test]
async fn persistent_dist_match_topic_uses_cached_tenant_routes() {
    let store = Arc::new(CountingRouteStore::default());
    let dist = PersistentDistHandle::open(store.clone()).await.unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let first = dist
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(store.list_tenant_wildcard_routes_calls(), 1);

    let second = dist
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_exact_routes_calls(), 1);
    assert_eq!(store.list_tenant_wildcard_routes_calls(), 1);
}

#[tokio::test]
async fn persistent_dist_match_topic_cold_exact_lookup_skips_tenant_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/a/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s2".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let matches = lazy
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_exact_routes_calls(), 1);
    assert_eq!(store.list_tenant_wildcard_routes_calls(), 1);
}

#[tokio::test]
async fn persistent_dist_list_exact_routes_uses_exact_index_without_tenant_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/a/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_exact_routes("t1", "devices/a/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_exact_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_topic_filter_routes_reuses_loaded_exact_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/a/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s2".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let matches = lazy
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(store.list_exact_routes_calls(), 1);

    let routes = lazy
        .list_topic_filter_routes("t1", "devices/a/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_exact_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_topic_filter_routes_reuses_loaded_exact_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/a/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s2".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let matches = lazy
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(store.list_exact_routes_calls(), 1);

    let count = lazy
        .count_topic_filter_routes("t1", "devices/a/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.list_exact_routes_calls(), 1);
    assert_eq!(store.count_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_topic_filter_routes_for_wildcards_falls_back_to_tenant_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_exact_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_remove_topic_filter_routes_uses_topic_filter_index() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "alerts/#".into(),
            session_id: "s2".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let removed = lazy
        .remove_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(store.remove_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_open_is_lazy() {
    let store = Arc::new(CountingRouteStore::default());
    let _dist = PersistentDistHandle::open(store.clone()).await.unwrap();
    assert_eq!(store.list_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert_eq!(store.count_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_route_count_uses_store_count() {
    let store = Arc::new(CountingRouteStore::default());
    let dist = PersistentDistHandle::open(store.clone()).await.unwrap();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    assert_eq!(dist.route_count().await.unwrap(), 1);
    assert_eq!(store.count_routes_calls(), 1);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_remove_session_routes_uses_session_index_without_preload() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let removed = lazy.remove_session_routes("s1").await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(store.remove_session_routes_calls(), 1);
    assert_eq!(store.list_routes_calls(), 0);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert!(store.list_routes().await.unwrap().is_empty());
}

#[tokio::test]
async fn persistent_dist_remove_session_topic_routes_uses_session_topic_index_without_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (tenant_id, topic_filter, session_id, shared_group) in [
        ("t1", "devices/+/state", "s1", None),
        ("t1", "devices/+/state", "s1", Some("workers")),
        ("t1", "alerts/#", "s1", None),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let removed = lazy
        .remove_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(store.remove_session_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 0);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_session_topic_shared_lookup_and_remove_use_direct_store_path() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for shared_group in [None, Some("workers")] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: "s1".into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let matched = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(matched.shared_group.as_deref(), Some("workers"));
    assert_eq!(store.load_session_topic_filter_route_calls(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 0);

    let removed = lazy
        .remove_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(store.remove_session_topic_filter_route_calls(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 0);
    assert_eq!(
        lazy.count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_session_topic_shared_lookup_reuses_exact_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    let second = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert!(first.is_some());
    assert!(second.is_some());
    assert_eq!(store.load_session_topic_filter_route_calls(), 1);
}

#[tokio::test]
async fn persistent_dist_missing_session_topic_shared_lookup_reuses_exact_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    let second = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert!(first.is_none());
    assert!(second.is_none());
    assert_eq!(store.load_session_topic_filter_route_calls(), 1);
}

#[tokio::test]
async fn persistent_dist_session_topic_shared_lookup_reuses_loaded_session_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for shared_group in [None, Some("workers")] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: "s1".into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 1);

    let matched = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert!(matched.is_some());
    assert_eq!(store.load_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_missing_shared_lookup_reuses_loaded_session_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 1);

    let missing = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("other"))
        .await
        .unwrap();
    assert!(missing.is_none());
    assert_eq!(store.load_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_session_topic_shared_lookup_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (topic_filter, shared_group) in [("devices/+/state", Some("workers")), ("alerts/#", None)] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: "s1".into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(store.list_session_routes_calls(), 1);

    let matched = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert!(matched.is_some());
    assert_eq!(store.load_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_missing_shared_lookup_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);

    let missing = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("other"))
        .await
        .unwrap();
    assert!(missing.is_none());
    assert_eq!(store.load_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_session_topic_shared_route_reuses_exact_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let route = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert!(route.is_some());
    let count = lazy
        .count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.load_session_topic_filter_route_calls(), 1);
    assert_eq!(store.count_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_missing_count_session_topic_shared_route_reuses_exact_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let route = lazy
        .lookup_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert!(route.is_none());
    let count = lazy
        .count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(store.load_session_topic_filter_route_calls(), 1);
    assert_eq!(store.count_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_session_topic_shared_route_reuses_loaded_session_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 1);

    let count = lazy
        .count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.count_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_session_topic_shared_route_from_topic_cache_populates_exact_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store).await.unwrap();
    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);

    let count = lazy
        .count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(lazy
        .session_topic_shared_cache
        .read()
        .expect("dist poisoned")
        .get(&(
            String::from("s1"),
            String::from("devices/+/state"),
            String::from("workers")
        ))
        .is_some());
}

#[tokio::test]
async fn persistent_dist_count_session_topic_shared_route_from_session_cache_populates_exact_cache()
{
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store).await.unwrap();
    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);

    let count = lazy
        .count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(lazy
        .session_topic_shared_cache
        .read()
        .expect("dist poisoned")
        .get(&(
            String::from("s1"),
            String::from("devices/+/state"),
            String::from("workers")
        ))
        .is_some());
}

#[tokio::test]
async fn persistent_dist_count_session_topic_shared_route_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);

    let count = lazy
        .count_session_topic_filter_route("s1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.count_session_topic_filter_route_calls(), 0);
}

#[tokio::test]
async fn dist_handle_reassign_session_routes_updates_nodes_in_place() {
    let dist = DistHandle::default();
    dist.add_route(RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: Some(7),
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    })
    .await
    .unwrap();

    let reassigned = dist.reassign_session_routes("s1", 9).await.unwrap();
    assert_eq!(reassigned, 1);

    let session_routes = dist.list_session_routes("s1").await.unwrap();
    assert_eq!(session_routes.len(), 1);
    assert_eq!(session_routes[0].node_id, 9);
    let tenant_routes = dist.list_routes(Some("t1")).await.unwrap();
    assert_eq!(tenant_routes.len(), 1);
    assert_eq!(tenant_routes[0].node_id, 9);
}

#[tokio::test]
async fn persistent_dist_reassign_session_routes_uses_session_index_without_global_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let reassigned = lazy.reassign_session_routes("s1", 7).await.unwrap();
    assert_eq!(reassigned, 1);
    assert_eq!(store.reassign_session_routes_calls(), 1);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
    assert_eq!(store.save_route_calls(), 1);

    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].node_id, 7);
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_remove_tenant_routes_uses_tenant_index_without_global_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (tenant_id, topic_filter, session_id) in [
        ("t1", "devices/+/state", "s1"),
        ("t1", "alerts/#", "s2"),
        ("t2", "metrics/#", "s3"),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let removed = lazy.remove_tenant_routes("t1").await.unwrap();
    assert_eq!(removed, 2);
    assert_eq!(store.remove_tenant_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_list_session_routes_uses_session_index_without_tenant_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_session_routes_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy.list_session_routes("s1").await.unwrap();
    let second = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);
}

#[tokio::test]
async fn persistent_dist_count_session_routes_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(routes.len(), 1);
    let count = lazy.count_session_routes("s1").await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.list_session_routes_calls(), 1);
    assert_eq!(store.count_session_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_session_routes_uses_session_index_without_loading_routes() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let count = lazy.count_session_routes("s1").await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.count_session_routes_calls(), 1);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_session_topic_routes_uses_session_index_without_tenant_scan() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_session_topic_routes_reuses_loaded_session_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    let second = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 1);
}

#[tokio::test]
async fn persistent_dist_count_session_topic_routes_reuses_loaded_session_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    let count = lazy
        .count_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 1);
    assert_eq!(store.count_session_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_session_topic_routes_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let warmed = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(warmed.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);

    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);
    assert_eq!(store.list_session_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_session_topic_routes_reuses_loaded_session_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let warmed = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(warmed.len(), 1);
    assert_eq!(store.list_session_routes_calls(), 1);

    let count = lazy
        .count_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.list_session_routes_calls(), 1);
    assert_eq!(store.count_session_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_session_topic_routes_from_session_cache_populates_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store).await.unwrap();
    let warmed = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(warmed.len(), 1);

    let routes = lazy
        .list_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert!(lazy
        .loaded_session_topics
        .read()
        .expect("dist poisoned")
        .contains(&(String::from("s1"), String::from("devices/+/state"))));
    assert_eq!(
        lazy.session_topic_cache
            .read()
            .expect("dist poisoned")
            .get(&(String::from("s1"), String::from("devices/+/state")))
            .map(|routes| routes.len()),
        Some(1)
    );
}

#[tokio::test]
async fn persistent_dist_count_session_topic_routes_from_session_cache_populates_topic_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store).await.unwrap();
    let warmed = lazy.list_session_routes("s1").await.unwrap();
    assert_eq!(warmed.len(), 1);

    let count = lazy
        .count_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(lazy
        .loaded_session_topics
        .read()
        .expect("dist poisoned")
        .contains(&(String::from("s1"), String::from("devices/+/state"))));
    assert_eq!(
        lazy.session_topic_cache
            .read()
            .expect("dist poisoned")
            .get(&(String::from("s1"), String::from("devices/+/state")))
            .map(|routes| routes.len()),
        Some(1)
    );
}

#[tokio::test]
async fn persistent_dist_count_session_topic_routes_uses_session_index_without_loading_routes() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let count = lazy
        .count_session_topic_filter_routes("s1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.count_session_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_session_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_tenant_routes_uses_store_direct_without_listing() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let count = lazy.count_tenant_routes("t1").await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.count_tenant_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_topic_filter_routes_uses_store_direct_without_listing() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let count = lazy
        .count_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.count_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_list_topic_filter_routes_reuses_loaded_filter_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for session_id in ["s1", "s2"] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    let second = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_count_topic_filter_routes_reuses_loaded_filter_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    assert_eq!(
        lazy.list_topic_filter_routes("t1", "devices/+/state")
            .await
            .unwrap()
            .len(),
        1
    );
    let count = lazy
        .count_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);
    assert_eq!(store.count_topic_filter_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_routes_use_direct_store_paths() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, shared_group) in [
        ("s1", None),
        ("s2", Some("workers")),
        ("s3", Some("workers")),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(store.list_topic_filter_shared_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 0);

    let count = lazy
        .count_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(store.count_topic_filter_shared_routes_calls(), 0);
    assert_eq!(store.list_topic_filter_routes_calls(), 0);

    let removed = lazy
        .remove_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(store.remove_topic_filter_shared_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_routes_calls(), 0);
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_routes_reuse_loaded_filter_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, shared_group) in [
        ("s1", None),
        ("s2", Some("workers")),
        ("s3", Some("workers")),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let warmed = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(warmed.len(), 3);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);

    let routes = lazy
        .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_shared_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_count_reuses_loaded_filter_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, shared_group) in [
        ("s1", None),
        ("s2", Some("workers")),
        ("s3", Some("workers")),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let warmed = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(warmed.len(), 3);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);

    let count = lazy
        .count_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(store.list_topic_filter_routes_calls(), 1);
    assert_eq!(store.count_topic_filter_shared_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_list_from_filter_cache_populates_shared_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, shared_group) in [
        ("s1", None),
        ("s2", Some("workers")),
        ("s3", Some("workers")),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store).await.unwrap();
    let warmed = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(warmed.len(), 3);

    let routes = lazy
        .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(routes.len(), 2);
    assert!(lazy
        .inner
        .read()
        .expect("dist poisoned")
        .get("t1")
        .is_some_and(|tenant| tenant
            .loaded_filter_shared
            .contains(&(String::from("devices/+/state"), String::from("workers")))));
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_count_from_filter_cache_populates_shared_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, shared_group) in [
        ("s1", None),
        ("s2", Some("workers")),
        ("s3", Some("workers")),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: "devices/+/state".into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store).await.unwrap();
    let warmed = lazy
        .list_topic_filter_routes("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(warmed.len(), 3);

    let count = lazy
        .count_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert!(lazy
        .inner
        .read()
        .expect("dist poisoned")
        .get("t1")
        .is_some_and(|tenant| tenant
            .loaded_filter_shared
            .contains(&(String::from("devices/+/state"), String::from("workers")))));
}

#[tokio::test]
async fn persistent_dist_tenant_shared_routes_use_direct_store_paths() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (tenant_id, topic_filter, session_id, shared_group) in [
        ("t1", "devices/+/state", "s1", Some("workers")),
        ("t1", "alerts/#", "s2", Some("workers")),
        ("t1", "metrics/#", "s3", Some("other")),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
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

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let routes = lazy
        .list_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(store.list_tenant_shared_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);

    let count = lazy
        .count_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(store.count_tenant_shared_routes_calls(), 0);
    assert_eq!(store.list_tenant_routes_calls(), 0);

    let removed = lazy
        .remove_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(store.remove_tenant_shared_routes_calls(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 0);
    assert_eq!(lazy.route_count().await.unwrap(), 1);
}

#[tokio::test]
async fn persistent_dist_tenant_shared_routes_reuse_loaded_shared_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (topic_filter, session_id) in [("devices/+/state", "s1"), ("alerts/#", "s2")] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: Some("workers".into()),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy
        .list_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    let second = lazy
        .list_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    let count = lazy
        .count_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    assert_eq!(count, 2);
    assert_eq!(store.list_tenant_shared_routes_calls(), 1);
    assert_eq!(store.count_tenant_shared_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_routes_reuse_loaded_shared_group_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (topic_filter, session_id) in [("devices/+/state", "s1"), ("alerts/#", "s2")] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: Some("workers".into()),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let warmed = lazy
        .list_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    assert_eq!(warmed.len(), 2);
    assert_eq!(store.list_tenant_shared_routes_calls(), 1);

    let routes = lazy
        .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(store.list_tenant_shared_routes_calls(), 1);
    assert_eq!(store.list_topic_filter_shared_routes_calls(), 0);
    assert!(lazy
        .inner
        .read()
        .expect("dist poisoned")
        .get("t1")
        .is_some_and(|tenant| tenant
            .loaded_filter_shared
            .contains(&(String::from("devices/+/state"), String::from("workers")))));
}

#[tokio::test]
async fn persistent_dist_topic_filter_shared_count_reuse_loaded_shared_group_cache() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (topic_filter, session_id) in [("devices/+/state", "s1"), ("alerts/#", "s2")] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: Some("workers".into()),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let warmed = lazy
        .list_tenant_shared_routes("t1", Some("workers"))
        .await
        .unwrap();
    assert_eq!(warmed.len(), 2);
    assert_eq!(store.list_tenant_shared_routes_calls(), 1);

    let count = lazy
        .count_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.list_tenant_shared_routes_calls(), 1);
    assert_eq!(store.count_topic_filter_shared_routes_calls(), 0);
    assert!(lazy
        .inner
        .read()
        .expect("dist poisoned")
        .get("t1")
        .is_some_and(|tenant| tenant
            .loaded_filter_shared
            .contains(&(String::from("devices/+/state"), String::from("workers")))));
}

#[tokio::test]
async fn persistent_dist_list_routes_reuses_cached_tenant_view() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    writer
        .add_route(RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first = lazy.list_routes(Some("t1")).await.unwrap();
    let second = lazy.list_routes(Some("t1")).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(store.list_tenant_routes_calls(), 1);
    assert_eq!(store.list_routes_calls(), 0);
}

#[tokio::test]
async fn persistent_partial_match_cache_does_not_hide_full_tenant_route_list() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, topic_filter) in [
        ("s1", "devices/a/state"),
        ("s2", "devices/b/state"),
        ("s3", "devices/+/state"),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first_matches = lazy
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(first_matches.len(), 2);

    let routes = lazy.list_routes(Some("t1")).await.unwrap();
    assert_eq!(routes.len(), 3);
    assert_eq!(store.list_tenant_routes_calls(), 1);
}

#[tokio::test]
async fn persistent_partial_match_cache_fetches_new_exact_topic_on_later_match() {
    let store = Arc::new(CountingRouteStore::default());
    let writer = PersistentDistHandle::open(store.clone()).await.unwrap();
    for (session_id, topic_filter) in [
        ("s1", "devices/a/state"),
        ("s2", "devices/b/state"),
        ("s3", "devices/+/state"),
    ] {
        writer
            .add_route(RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: session_id.into(),
                node_id: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentDistHandle::open(store.clone()).await.unwrap();
    let first_matches = lazy
        .match_topic("t1", &"devices/a/state".into())
        .await
        .unwrap();
    assert_eq!(first_matches.len(), 2);
    let second_matches = lazy
        .match_topic("t1", &"devices/b/state".into())
        .await
        .unwrap();
    assert_eq!(second_matches.len(), 2);
    assert_eq!(store.list_tenant_wildcard_routes_calls(), 1);
    assert_eq!(store.list_exact_routes_calls(), 2);
}
