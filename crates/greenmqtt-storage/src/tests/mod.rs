use super::{
        InboxStore, InflightStore, MemoryInboxStore, MemoryInflightStore, MemoryRetainStore,
        MemoryRouteStore, MemorySessionStore, MemorySubscriptionStore, RedisInboxStore,
    RedisInflightStore, RedisRetainStore, RedisRouteStore, RedisSessionStore,
    RedisSubscriptionStore, RetainStore, RocksInboxStore, RocksInflightStore, RocksRetainStore,
    RocksRouteStore, RocksSessionStore, RocksSubscriptionStore, RouteStore, SessionStore,
    SledInboxStore, SledInflightStore, SledRetainStore, SledRouteStore, SledSessionStore,
    SledSubscriptionStore, SubscriptionStore,
};
use greenmqtt_core::{
    ClientIdentity, InflightMessage, InflightPhase, OfflineMessage, PublishProperties,
    RetainedMessage, RouteRecord, SessionKind, SessionRecord, Subscription,
};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

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
