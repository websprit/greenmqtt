use super::{
    InboxStore, InflightStore, MemoryInboxStore, MemoryInflightStore, MemoryRetainStore,
    MemoryRouteStore, MemorySessionStore, MemorySubscriptionStore, RedisInboxStore,
    RedisInflightStore, RedisRetainStore, RedisRouteStore, RedisSessionStore,
    RedisSubscriptionStore, RetainStore, RocksDbConfig, RocksInboxStore, RocksInflightStore,
    RocksRetainStore, RocksRouteStore, RocksSessionStore, RocksSubscriptionStore, RouteStore,
    SessionStore, SledInboxStore, SledInflightStore, SledRetainStore, SledRouteStore,
    SledSessionStore, SledSubscriptionStore, SubscriptionStore,
};
use greenmqtt_core::{
    ClientIdentity, InflightMessage, InflightPhase, OfflineMessage, PublishProperties,
    RetainedMessage, RouteRecord, SessionKind, SessionRecord, Subscription,
};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn memory_stores_round_trip_data() {
    let session_store = MemorySessionStore::default();
    let subscription_store = MemorySubscriptionStore::default();
    let inbox_store = MemoryInboxStore::default();
    let inflight_store = MemoryInflightStore::default();
    let retain_store = MemoryRetainStore::default();
    let route_store = MemoryRouteStore::default();

    assert_store_roundtrip(
        &session_store,
        &subscription_store,
        &inbox_store,
        &inflight_store,
        &retain_store,
    )
    .await;
    assert_route_store_roundtrip(&route_store).await;
}

#[tokio::test]
async fn sled_stores_round_trip_data() {
    let tempdir = tempfile::tempdir().unwrap();
    let session_store = SledSessionStore::open(tempdir.path().join("sessions")).unwrap();
    let subscription_store =
        SledSubscriptionStore::open(tempdir.path().join("subscriptions")).unwrap();
    let inbox_store = SledInboxStore::open(tempdir.path().join("inbox")).unwrap();
    let inflight_store = SledInflightStore::open(tempdir.path().join("inflight")).unwrap();
    let retain_store = SledRetainStore::open(tempdir.path().join("retain")).unwrap();
    let route_store = SledRouteStore::open(tempdir.path().join("routes")).unwrap();

    assert_store_roundtrip(
        &session_store,
        &subscription_store,
        &inbox_store,
        &inflight_store,
        &retain_store,
    )
    .await;
    assert_route_store_roundtrip(&route_store).await;
}

#[tokio::test]
async fn rocksdb_stores_round_trip_data() {
    let tempdir = tempfile::tempdir().unwrap();
    let session_store = RocksSessionStore::open(tempdir.path().join("sessions")).unwrap();
    let subscription_store =
        RocksSubscriptionStore::open(tempdir.path().join("subscriptions")).unwrap();
    let inbox_store = RocksInboxStore::open(tempdir.path().join("inbox")).unwrap();
    let inflight_store = RocksInflightStore::open(tempdir.path().join("inflight")).unwrap();
    let retain_store = RocksRetainStore::open(tempdir.path().join("retain")).unwrap();
    let route_store = RocksRouteStore::open(tempdir.path().join("routes")).unwrap();

    assert_store_roundtrip(
        &session_store,
        &subscription_store,
        &inbox_store,
        &inflight_store,
        &retain_store,
    )
    .await;
    assert_route_store_roundtrip(&route_store).await;
}

#[test]
fn rocksdb_config_uses_defaults_when_env_is_absent() {
    let _guard = env_lock().lock().unwrap();
    for key in [
        "GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES",
        "GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES",
        "GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES",
        "GREENMQTT_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY",
        "GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS",
        "GREENMQTT_ROCKSDB_PARALLELISM",
        "GREENMQTT_ROCKSDB_BYTES_PER_SYNC",
        "GREENMQTT_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS",
        "GREENMQTT_ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING",
        "GREENMQTT_ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO",
    ] {
        std::env::remove_var(key);
    }

    let config = RocksDbConfig::from_env().unwrap();
    assert_eq!(config.block_cache_bytes, 32 * 1024 * 1024);
    assert_eq!(config.memtable_budget_bytes, 128 * 1024 * 1024);
    assert_eq!(config.write_buffer_bytes, 16 * 1024 * 1024);
    assert_eq!(config.bloom_filter_bits_per_key, 10.0);
    assert_eq!(config.max_background_jobs, 4);
    assert!(config.parallelism >= 1);
    assert_eq!(config.bytes_per_sync, 1 << 20);
    assert!(config.optimize_filters_for_hits);
    assert!(config.memtable_whole_key_filtering);
    assert_eq!(config.memtable_prefix_bloom_ratio, 0.125);
}

#[test]
fn rocksdb_config_reads_env_overrides() {
    let _guard = env_lock().lock().unwrap();
    std::env::set_var("GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES", "1048576");
    std::env::set_var("GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES", "2097152");
    std::env::set_var("GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES", "524288");
    std::env::set_var("GREENMQTT_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY", "7.5");
    std::env::set_var("GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS", "6");
    std::env::set_var("GREENMQTT_ROCKSDB_PARALLELISM", "3");
    std::env::set_var("GREENMQTT_ROCKSDB_BYTES_PER_SYNC", "65536");
    std::env::set_var("GREENMQTT_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS", "false");
    std::env::set_var("GREENMQTT_ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING", "false");
    std::env::set_var("GREENMQTT_ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO", "0.2");

    let config = RocksDbConfig::from_env().unwrap();
    assert_eq!(config.block_cache_bytes, 1_048_576);
    assert_eq!(config.memtable_budget_bytes, 2_097_152);
    assert_eq!(config.write_buffer_bytes, 524_288);
    assert_eq!(config.bloom_filter_bits_per_key, 7.5);
    assert_eq!(config.max_background_jobs, 6);
    assert_eq!(config.parallelism, 3);
    assert_eq!(config.bytes_per_sync, 65_536);
    assert!(!config.optimize_filters_for_hits);
    assert!(!config.memtable_whole_key_filtering);
    assert_eq!(config.memtable_prefix_bloom_ratio, 0.2);

    for key in [
        "GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES",
        "GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES",
        "GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES",
        "GREENMQTT_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY",
        "GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS",
        "GREENMQTT_ROCKSDB_PARALLELISM",
        "GREENMQTT_ROCKSDB_BYTES_PER_SYNC",
        "GREENMQTT_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS",
        "GREENMQTT_ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING",
        "GREENMQTT_ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO",
    ] {
        std::env::remove_var(key);
    }
}

#[tokio::test]
async fn redis_stores_round_trip_data() {
    let server = RedisTestServer::start();
    let session_store = RedisSessionStore::open(&server.url).unwrap();
    let subscription_store = RedisSubscriptionStore::open(&server.url).unwrap();
    let inbox_store = RedisInboxStore::open(&server.url).unwrap();
    let inflight_store = RedisInflightStore::open(&server.url).unwrap();
    let retain_store = RedisRetainStore::open(&server.url).unwrap();
    let route_store = RedisRouteStore::open(&server.url).unwrap();

    assert_store_roundtrip(
        &session_store,
        &subscription_store,
        &inbox_store,
        &inflight_store,
        &retain_store,
    )
    .await;
    assert_route_store_roundtrip(&route_store).await;
}

#[tokio::test]
async fn memory_to_sled_upgrade_preserves_session_route_retain_state() {
    let source_session = MemorySessionStore::default();
    let source_route = MemoryRouteStore::default();
    let source_retain = MemoryRetainStore::default();
    seed_upgrade_state(&source_session, &source_route, &source_retain).await;

    let tempdir = tempfile::tempdir().unwrap();
    let target_session = SledSessionStore::open(tempdir.path().join("sessions")).unwrap();
    let target_route = SledRouteStore::open(tempdir.path().join("routes")).unwrap();
    let target_retain = SledRetainStore::open(tempdir.path().join("retain")).unwrap();

    migrate_upgrade_state(
        &source_session,
        &target_session,
        &source_route,
        &target_route,
        &source_retain,
        &target_retain,
    )
    .await;
    assert_upgrade_state(&target_session, &target_route, &target_retain).await;
}

#[tokio::test]
async fn memory_to_rocksdb_upgrade_preserves_session_route_retain_state() {
    let source_session = MemorySessionStore::default();
    let source_route = MemoryRouteStore::default();
    let source_retain = MemoryRetainStore::default();
    seed_upgrade_state(&source_session, &source_route, &source_retain).await;

    let tempdir = tempfile::tempdir().unwrap();
    let target_session = RocksSessionStore::open(tempdir.path().join("sessions")).unwrap();
    let target_route = RocksRouteStore::open(tempdir.path().join("routes")).unwrap();
    let target_retain = RocksRetainStore::open(tempdir.path().join("retain")).unwrap();

    migrate_upgrade_state(
        &source_session,
        &target_session,
        &source_route,
        &target_route,
        &source_retain,
        &target_retain,
    )
    .await;
    assert_upgrade_state(&target_session, &target_route, &target_retain).await;
}

#[tokio::test]
async fn memory_to_redis_upgrade_preserves_session_route_retain_state() {
    let source_session = MemorySessionStore::default();
    let source_route = MemoryRouteStore::default();
    let source_retain = MemoryRetainStore::default();
    seed_upgrade_state(&source_session, &source_route, &source_retain).await;

    let server = RedisTestServer::start();
    let target_session = RedisSessionStore::open(&server.url).unwrap();
    let target_route = RedisRouteStore::open(&server.url).unwrap();
    let target_retain = RedisRetainStore::open(&server.url).unwrap();

    migrate_upgrade_state(
        &source_session,
        &target_session,
        &source_route,
        &target_route,
        &source_retain,
        &target_retain,
    )
    .await;
    assert_upgrade_state(&target_session, &target_route, &target_retain).await;
}

#[tokio::test]
async fn sled_to_rocksdb_switch_preserves_session_route_retain_state() {
    let source_dir = tempfile::tempdir().unwrap();
    let source_session = SledSessionStore::open(source_dir.path().join("sessions")).unwrap();
    let source_route = SledRouteStore::open(source_dir.path().join("routes")).unwrap();
    let source_retain = SledRetainStore::open(source_dir.path().join("retain")).unwrap();
    seed_upgrade_state(&source_session, &source_route, &source_retain).await;

    let target_dir = tempfile::tempdir().unwrap();
    let target_session = RocksSessionStore::open(target_dir.path().join("sessions")).unwrap();
    let target_route = RocksRouteStore::open(target_dir.path().join("routes")).unwrap();
    let target_retain = RocksRetainStore::open(target_dir.path().join("retain")).unwrap();

    migrate_upgrade_state(
        &source_session,
        &target_session,
        &source_route,
        &target_route,
        &source_retain,
        &target_retain,
    )
    .await;
    assert_upgrade_state(&target_session, &target_route, &target_retain).await;
}

#[tokio::test]
async fn rocksdb_to_redis_switch_preserves_session_route_retain_state() {
    let source_dir = tempfile::tempdir().unwrap();
    let source_session = RocksSessionStore::open(source_dir.path().join("sessions")).unwrap();
    let source_route = RocksRouteStore::open(source_dir.path().join("routes")).unwrap();
    let source_retain = RocksRetainStore::open(source_dir.path().join("retain")).unwrap();
    seed_upgrade_state(&source_session, &source_route, &source_retain).await;

    let server = RedisTestServer::start();
    let target_session = RedisSessionStore::open(&server.url).unwrap();
    let target_route = RedisRouteStore::open(&server.url).unwrap();
    let target_retain = RedisRetainStore::open(&server.url).unwrap();

    migrate_upgrade_state(
        &source_session,
        &target_session,
        &source_route,
        &target_route,
        &source_retain,
        &target_retain,
    )
    .await;
    assert_upgrade_state(&target_session, &target_route, &target_retain).await;
}

#[tokio::test]
async fn redis_to_sled_switch_preserves_session_route_retain_state() {
    let server = RedisTestServer::start();
    let source_session = RedisSessionStore::open(&server.url).unwrap();
    let source_route = RedisRouteStore::open(&server.url).unwrap();
    let source_retain = RedisRetainStore::open(&server.url).unwrap();
    seed_upgrade_state(&source_session, &source_route, &source_retain).await;

    let target_dir = tempfile::tempdir().unwrap();
    let target_session = SledSessionStore::open(target_dir.path().join("sessions")).unwrap();
    let target_route = SledRouteStore::open(target_dir.path().join("routes")).unwrap();
    let target_retain = SledRetainStore::open(target_dir.path().join("retain")).unwrap();

    migrate_upgrade_state(
        &source_session,
        &target_session,
        &source_route,
        &target_route,
        &source_retain,
        &target_retain,
    )
    .await;
    assert_upgrade_state(&target_session, &target_route, &target_retain).await;
}

async fn assert_store_roundtrip<S, U, I, F, R>(
    session_store: &S,
    subscription_store: &U,
    inbox_store: &I,
    inflight_store: &F,
    retain_store: &R,
) where
    S: SessionStore,
    U: SubscriptionStore,
    I: InboxStore,
    F: InflightStore,
    R: RetainStore,
{
    let session = SessionRecord {
        session_id: "s1".into(),
        node_id: 1,
        kind: SessionKind::Persistent,
        identity: ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        },
        session_expiry_interval_secs: None,
        expires_at_ms: None,
    };
    session_store.save_session(&session).await.unwrap();
    assert!(session_store
        .load_session("t1", "c1")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        session_store
            .load_session_by_session_id("s1")
            .await
            .unwrap()
            .unwrap()
            .session_id,
        "s1"
    );
    assert_eq!(session_store.list_sessions().await.unwrap().len(), 1);
    assert_eq!(
        session_store
            .list_tenant_sessions("t1")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(session_store.count_sessions().await.unwrap(), 1);
    session_store
        .save_session(&SessionRecord {
            session_id: "s2".into(),
            node_id: 2,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    assert_eq!(session_store.count_sessions().await.unwrap(), 1);
    assert!(session_store
        .load_session_by_session_id("s1")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        session_store
            .load_session_by_session_id("s2")
            .await
            .unwrap()
            .unwrap()
            .node_id,
        2
    );
    session_store.delete_session("t1", "c1").await.unwrap();
    assert!(session_store
        .load_session("t1", "c1")
        .await
        .unwrap()
        .is_none());
    assert!(session_store
        .load_session_by_session_id("s2")
        .await
        .unwrap()
        .is_none());
    assert_eq!(session_store.count_sessions().await.unwrap(), 0);

    subscription_store
        .save_subscription(&Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "a/+".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    assert_eq!(
        subscription_store
            .list_subscriptions("s1")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        subscription_store
            .load_subscription("s1", "a/+", Some("workers"))
            .await
            .unwrap()
            .unwrap()
            .topic_filter,
        "a/+"
    );
    assert_eq!(subscription_store.count_subscriptions().await.unwrap(), 1);
    subscription_store
        .save_subscription(&Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "a/+".into(),
            qos: 2,
            subscription_identifier: Some(7),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    assert_eq!(subscription_store.count_subscriptions().await.unwrap(), 1);
    assert_eq!(
        subscription_store
            .load_subscription("s1", "a/+", Some("workers"))
            .await
            .unwrap()
            .unwrap()
            .qos,
        2
    );
    assert!(subscription_store
        .delete_subscription("s1", "a/+", Some("workers"))
        .await
        .unwrap());
    assert!(subscription_store
        .load_subscription("s1", "a/+", Some("workers"))
        .await
        .unwrap()
        .is_none());
    assert!(!subscription_store
        .delete_subscription("s1", "a/+", Some("workers"))
        .await
        .unwrap());
    subscription_store
        .save_subscription(&Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "a/#".into(),
            qos: 1,
            subscription_identifier: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            shared_group: None,
            kind: SessionKind::Persistent,
        })
        .await
        .unwrap();
    assert_eq!(
        subscription_store
            .purge_session_subscriptions("s1")
            .await
            .unwrap(),
        1
    );
    assert!(subscription_store
        .list_subscriptions("s1")
        .await
        .unwrap()
        .is_empty());
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "a/+"),
        ("t1", "s1", "c/+"),
        ("t2", "s2", "b/+"),
    ] {
        subscription_store
            .save_subscription(&Subscription {
                session_id: session_id.into(),
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: None,
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }
    assert_eq!(
        subscription_store
            .count_session_subscriptions("s1")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        subscription_store
            .list_session_topic_subscriptions("s1", "a/+")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        subscription_store
            .count_session_topic_subscriptions("s1", "a/+")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        subscription_store
            .count_tenant_subscriptions("t1")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        subscription_store
            .count_tenant_topic_subscriptions("t2", "b/+")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        subscription_store
            .purge_session_topic_subscriptions("s1", "a/+")
            .await
            .unwrap(),
        1
    );
    assert!(subscription_store
        .list_session_topic_subscriptions("s1", "a/+")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        subscription_store
            .purge_tenant_subscriptions("t1")
            .await
            .unwrap(),
        1
    );
    assert_eq!(subscription_store.count_subscriptions().await.unwrap(), 1);
    assert_eq!(
        subscription_store
            .list_tenant_subscriptions("t2")
            .await
            .unwrap()
            .len(),
        1
    );

    inbox_store
        .append_message(&OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "a/b".into(),
            payload: b"v".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    assert_eq!(inbox_store.count_messages().await.unwrap(), 1);
    assert_eq!(inbox_store.count_session_messages("s1").await.unwrap(), 1);
    assert_eq!(inbox_store.count_tenant_messages("t1").await.unwrap(), 1);
    assert_eq!(
        inbox_store.list_tenant_messages("t1").await.unwrap().len(),
        1
    );
    assert_eq!(inbox_store.load_messages("s1").await.unwrap().len(), 1);
    assert_eq!(inbox_store.count_messages().await.unwrap(), 0);
    inbox_store
        .append_message(&OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "a/c".into(),
            payload: b"v2".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    assert_eq!(inbox_store.purge_messages("s1").await.unwrap(), 1);
    assert_eq!(inbox_store.count_messages().await.unwrap(), 0);
    for (tenant_id, session_id, topic) in [("t1", "s1", "a/d"), ("t2", "s2", "a/e")] {
        inbox_store
            .append_message(&OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                topic: topic.into(),
                payload: topic.as_bytes().to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }
    assert_eq!(inbox_store.purge_tenant_messages("t1").await.unwrap(), 1);
    assert_eq!(inbox_store.count_messages().await.unwrap(), 1);
    assert_eq!(inbox_store.count_session_messages("s1").await.unwrap(), 0);
    assert_eq!(inbox_store.count_tenant_messages("t1").await.unwrap(), 0);
    assert_eq!(inbox_store.count_session_messages("s2").await.unwrap(), 1);
    assert_eq!(inbox_store.count_tenant_messages("t2").await.unwrap(), 1);
    assert_eq!(
        inbox_store.list_tenant_messages("t2").await.unwrap().len(),
        1
    );

    inflight_store
        .save_inflight(&InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "a/b".into(),
            payload: b"v".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Release,
        })
        .await
        .unwrap();
    assert_eq!(inflight_store.count_inflight().await.unwrap(), 1);
    assert_eq!(
        inflight_store.count_session_inflight("s1").await.unwrap(),
        1
    );
    assert_eq!(inflight_store.count_tenant_inflight("t1").await.unwrap(), 1);
    assert_eq!(
        inflight_store
            .list_tenant_inflight("t1")
            .await
            .unwrap()
            .len(),
        1
    );
    inflight_store
        .save_inflight(&InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "a/b".into(),
            payload: b"v-updated".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();
    assert_eq!(inflight_store.count_inflight().await.unwrap(), 1);
    assert_eq!(
        inflight_store
            .list_tenant_inflight("t1")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(inflight_store.load_inflight("s1").await.unwrap().len(), 1);
    inflight_store.delete_inflight("s1", 7).await.unwrap();
    assert_eq!(inflight_store.count_inflight().await.unwrap(), 0);
    assert!(inflight_store.load_inflight("s1").await.unwrap().is_empty());
    inflight_store
        .save_inflight(&InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 8,
            topic: "a/c".into(),
            payload: b"v2".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();
    assert_eq!(inflight_store.purge_inflight("s1").await.unwrap(), 1);
    assert_eq!(inflight_store.count_inflight().await.unwrap(), 0);
    for (tenant_id, session_id, packet_id) in [("t1", "s1", 9u16), ("t2", "s2", 10u16)] {
        inflight_store
            .save_inflight(&InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                packet_id,
                topic: format!("a/{packet_id}"),
                payload: format!("v-{packet_id}").into_bytes().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            })
            .await
            .unwrap();
    }
    assert_eq!(inflight_store.purge_tenant_inflight("t1").await.unwrap(), 1);
    assert_eq!(inflight_store.count_inflight().await.unwrap(), 1);
    assert_eq!(
        inflight_store.count_session_inflight("s1").await.unwrap(),
        0
    );
    assert_eq!(inflight_store.count_tenant_inflight("t1").await.unwrap(), 0);
    assert_eq!(
        inflight_store.count_session_inflight("s2").await.unwrap(),
        1
    );
    assert_eq!(inflight_store.count_tenant_inflight("t2").await.unwrap(), 1);
    assert_eq!(
        inflight_store
            .list_tenant_inflight("t2")
            .await
            .unwrap()
            .len(),
        1
    );

    retain_store
        .put_retain(&RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();
    retain_store
        .put_retain(&RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"down".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        retain_store
            .match_retain("t1", "devices/+/state")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(retain_store
        .load_retain("t1", "devices/a/state")
        .await
        .unwrap()
        .is_some());
    assert!(retain_store
        .load_retain("t1", "devices/+/state")
        .await
        .unwrap()
        .is_none());
    assert_eq!(retain_store.count_retained().await.unwrap(), 1);
}

async fn seed_upgrade_state<S, R, T>(session_store: &S, route_store: &R, retain_store: &T)
where
    S: SessionStore,
    R: RouteStore,
    T: RetainStore,
{
    for session in [
        SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1_700_000_001),
        },
        SessionRecord {
            session_id: "s2".into(),
            node_id: 2,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t2".into(),
                user_id: "u2".into(),
                client_id: "c2".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        },
    ] {
        session_store.save_session(&session).await.unwrap();
    }

    for route in [
        RouteRecord {
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
            session_id: "s1".into(),
            node_id: 1,
            subscription_identifier: Some(7),
            no_local: false,
            retain_as_published: true,
            shared_group: Some("workers".into()),
            kind: SessionKind::Persistent,
        },
        RouteRecord {
            tenant_id: "t2".into(),
            topic_filter: "devices/b/status".into(),
            session_id: "s2".into(),
            node_id: 2,
            subscription_identifier: None,
            no_local: true,
            retain_as_published: false,
            shared_group: None,
            kind: SessionKind::Persistent,
        },
    ] {
        route_store.save_route(&route).await.unwrap();
    }

    for retained in [
        RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"online".to_vec().into(),
            qos: 1,
        },
        RetainedMessage {
            tenant_id: "t2".into(),
            topic: "devices/b/status".into(),
            payload: b"healthy".to_vec().into(),
            qos: 0,
        },
    ] {
        retain_store.put_retain(&retained).await.unwrap();
    }
}

async fn migrate_upgrade_state<SS, TS, SR, TR, ST, TT>(
    source_session: &SS,
    target_session: &TS,
    source_route: &SR,
    target_route: &TR,
    source_retain: &ST,
    target_retain: &TT,
) where
    SS: SessionStore,
    TS: SessionStore,
    SR: RouteStore,
    TR: RouteStore,
    ST: RetainStore,
    TT: RetainStore,
{
    for session in source_session.list_sessions().await.unwrap() {
        target_session.save_session(&session).await.unwrap();
    }
    for route in source_route.list_routes().await.unwrap() {
        target_route.save_route(&route).await.unwrap();
    }
    for tenant_id in ["t1", "t2"] {
        for retained in source_retain.list_tenant_retained(tenant_id).await.unwrap() {
            target_retain.put_retain(&retained).await.unwrap();
        }
    }
}

async fn assert_upgrade_state<S, R, T>(session_store: &S, route_store: &R, retain_store: &T)
where
    S: SessionStore,
    R: RouteStore,
    T: RetainStore,
{
    assert_eq!(session_store.count_sessions().await.unwrap(), 2);
    assert_eq!(
        session_store
            .load_session("t1", "c1")
            .await
            .unwrap()
            .unwrap()
            .session_id,
        "s1"
    );
    assert_eq!(
        session_store
            .load_session("t2", "c2")
            .await
            .unwrap()
            .unwrap()
            .node_id,
        2
    );

    assert_eq!(route_store.count_routes().await.unwrap(), 2);
    assert_eq!(
        route_store
            .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        route_store
            .list_exact_routes("t2", "devices/b/status")
            .await
            .unwrap()
            .len(),
        1
    );

    assert_eq!(retain_store.count_retained().await.unwrap(), 2);
    assert_eq!(
        retain_store
            .load_retain("t1", "devices/a/state")
            .await
            .unwrap()
            .unwrap()
            .payload,
        b"online".as_slice()
    );
    assert_eq!(
        retain_store
            .load_retain("t2", "devices/b/status")
            .await
            .unwrap()
            .unwrap()
            .payload,
        b"healthy".as_slice()
    );
}

async fn assert_route_store_roundtrip<R>(route_store: &R)
where
    R: RouteStore,
{
    let route = RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "sensors/+/temp".into(),
        session_id: "s1".into(),
        node_id: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: Some("workers".into()),
        kind: SessionKind::Persistent,
    };
    let exact_route = RouteRecord {
        tenant_id: "t1".into(),
        topic_filter: "sensors/a/temp".into(),
        session_id: "s2".into(),
        node_id: 3,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        shared_group: None,
        kind: SessionKind::Persistent,
    };
    route_store.save_route(&route).await.unwrap();
    route_store
        .save_route(&RouteRecord {
            node_id: 2,
            ..route.clone()
        })
        .await
        .unwrap();
    route_store.save_route(&exact_route).await.unwrap();
    assert_eq!(route_store.list_routes().await.unwrap().len(), 2);
    assert_eq!(route_store.list_tenant_routes("t1").await.unwrap().len(), 2);
    assert_eq!(route_store.count_tenant_routes("t1").await.unwrap(), 2);
    assert_eq!(
        route_store
            .list_topic_filter_routes("t1", "sensors/+/temp")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        route_store
            .count_topic_filter_routes("t1", "sensors/+/temp")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        route_store
            .list_topic_filter_shared_routes("t1", "sensors/+/temp", Some("workers"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        route_store
            .count_topic_filter_shared_routes("t1", "sensors/+/temp", Some("workers"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        route_store
            .list_exact_routes("t1", "sensors/a/temp")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        route_store
            .list_tenant_wildcard_routes("t1")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        route_store.list_session_routes("s1").await.unwrap().len(),
        1
    );
    assert_eq!(
        route_store
            .list_session_topic_filter_routes("s1", "sensors/+/temp")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(route_store
        .load_session_topic_filter_route("s1", "sensors/+/temp", Some("workers"))
        .await
        .unwrap()
        .is_some());
    assert!(route_store
        .load_session_topic_filter_route("s1", "sensors/+/temp", None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(route_store.count_session_routes("s1").await.unwrap(), 1);
    assert_eq!(
        route_store
            .count_session_topic_filter_routes("s1", "sensors/+/temp")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        route_store
            .count_session_topic_filter_route("s1", "sensors/+/temp", Some("workers"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        route_store
            .remove_session_topic_filter_route("s1", "sensors/+/temp", Some("workers"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        route_store
            .remove_topic_filter_shared_routes("t1", "sensors/a/temp", None)
            .await
            .unwrap(),
        1
    );
    assert!(route_store
        .list_session_topic_filter_routes("s1", "sensors/+/temp")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(route_store.count_routes().await.unwrap(), 0);
    assert!(route_store.list_routes().await.unwrap().is_empty());
    route_store.delete_route(&route).await.unwrap();
    route_store.delete_route(&exact_route).await.unwrap();
    assert!(route_store.list_routes().await.unwrap().is_empty());
    assert!(route_store
        .list_session_routes("s1")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(route_store.count_session_routes("s1").await.unwrap(), 0);
    assert_eq!(route_store.count_routes().await.unwrap(), 0);
}

#[tokio::test]
async fn memory_retain_store_count_tracks_replace_and_delete() {
    let retain_store = MemoryRetainStore::default();
    retain_store
        .put_retain(&RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();
    retain_store
        .put_retain(&RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"down".to_vec().into(),
            qos: 1,
        })
        .await
        .unwrap();
    assert_eq!(retain_store.count_retained().await.unwrap(), 1);

    retain_store
        .put_retain(&RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: Vec::new().into(),
            qos: 1,
        })
        .await
        .unwrap();
    assert_eq!(retain_store.count_retained().await.unwrap(), 0);
}

#[tokio::test]
async fn memory_route_store_count_tracks_replace_without_growth() {
    let route_store = MemoryRouteStore::default();
    let mut route = RouteRecord {
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
    route_store.save_route(&route).await.unwrap();
    route.node_id = 7;
    route_store.save_route(&route).await.unwrap();

    assert_eq!(route_store.count_routes().await.unwrap(), 1);
    assert_eq!(
        route_store.list_session_routes("s1").await.unwrap()[0].node_id,
        7
    );
}

#[tokio::test]
async fn rocksdb_route_store_bulk_reassign_and_remove_preserve_indexes() {
    let tempdir = tempfile::tempdir().unwrap();
    let route_store = RocksRouteStore::open(tempdir.path().join("routes")).unwrap();

    for (topic_filter, shared_group) in [
        ("devices/+/state", Some("workers")),
        ("devices/a/status", None),
    ] {
        route_store
            .save_route(&RouteRecord {
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                session_id: "s1".into(),
                node_id: 1,
                subscription_identifier: Some(1),
                no_local: false,
                retain_as_published: false,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    assert_eq!(
        route_store.reassign_session_routes("s1", 9).await.unwrap(),
        2
    );
    assert_eq!(route_store.count_routes().await.unwrap(), 2);
    assert!(route_store
        .list_session_routes("s1")
        .await
        .unwrap()
        .iter()
        .all(|route| route.node_id == 9));
    assert_eq!(
        route_store
            .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
            .await
            .unwrap()[0]
            .node_id,
        9
    );
    assert_eq!(route_store.remove_session_routes("s1").await.unwrap(), 2);
    assert!(route_store
        .list_session_routes("s1")
        .await
        .unwrap()
        .is_empty());
    assert!(route_store
        .list_topic_filter_shared_routes("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(route_store.count_routes().await.unwrap(), 0);
}

#[tokio::test]
async fn memory_session_store_tenant_index_tracks_replace_and_delete() {
    let session_store = MemorySessionStore::default();
    session_store
        .save_session(&SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    session_store
        .save_session(&SessionRecord {
            session_id: "s2".into(),
            node_id: 2,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();

    let sessions = session_store.list_tenant_sessions("t1").await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, "s2");

    session_store.delete_session("t1", "c1").await.unwrap();
    assert!(session_store
        .list_tenant_sessions("t1")
        .await
        .unwrap()
        .is_empty());
}

struct RedisTestServer {
    child: Child,
    _dir: tempfile::TempDir,
    url: String,
}

impl RedisTestServer {
    fn start() -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = tempfile::tempdir().unwrap();
        let child = Command::new(redis_server_binary())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--port")
            .arg(port.to_string())
            .arg("--dir")
            .arg(dir.path())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--protected-mode")
            .arg("no")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            _dir: dir,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        std::thread::sleep(Duration::from_millis(150));
        server
    }
}

fn redis_server_binary() -> String {
    std::env::var("GREENMQTT_REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".to_string())
}

impl Drop for RedisTestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
