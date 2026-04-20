use super::{
    inbox_session_shard, inbox_tenant_scan_shard, inflight_session_shard,
    inflight_tenant_scan_shard, subscription_shard, InboxHandle, InboxService,
    PersistentInboxHandle, ReplicatedInboxHandle, OFFLINE_KEY_PREFIX,
};
use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    InflightMessage, InflightPhase, OfflineMessage, PublishProperties, RangeBoundary, RangeReplica,
    ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, ServiceShardKey,
    ServiceShardLifecycle, SessionKind, Subscription,
};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, MemoryKvRangeRouter};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, MemoryKvEngine,
};
use greenmqtt_storage::{
    InboxStore, InflightStore, MemoryInboxStore, MemoryInflightStore, MemorySubscriptionStore,
    SubscriptionStore,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

#[tokio::test]
async fn fetch_drains_offline_queue() {
    let inbox = InboxHandle::default();
    inbox
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "a/b".into(),
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
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "a/b".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    assert_eq!(inbox.peek(&"s1".into()).await.unwrap().len(), 1);
    assert_eq!(inbox.fetch(&"s1".into()).await.unwrap().len(), 1);
    assert!(inbox.fetch(&"s1".into()).await.unwrap().is_empty());
}

#[test]
fn inbox_helpers_align_point_bulk_and_scan_with_session_shards() {
    let subscription = Subscription {
        session_id: "s1".into(),
        tenant_id: "t1".into(),
        topic_filter: "a/b".into(),
        qos: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        retain_handling: 0,
        shared_group: None,
        kind: SessionKind::Persistent,
    };

    assert_eq!(
        inbox_session_shard("t1", &"s1".to_string()),
        ServiceShardKey::inbox("t1", "s1")
    );
    assert_eq!(
        subscription_shard(&subscription),
        ServiceShardKey::inbox("t1", "s1")
    );
    assert_eq!(
        inflight_session_shard("t1", &"s1".to_string()),
        ServiceShardKey::inflight("t1", "s1")
    );
    assert_eq!(
        inbox_tenant_scan_shard("t1"),
        ServiceShardKey {
            kind: greenmqtt_core::ServiceShardKind::Inbox,
            tenant_id: "t1".into(),
            scope: "*".into(),
        }
    );
    assert_eq!(
        inflight_tenant_scan_shard("t1"),
        ServiceShardKey {
            kind: greenmqtt_core::ServiceShardKind::Inflight,
            tenant_id: "t1".into(),
            scope: "*".into(),
        }
    );
}

#[tokio::test]
async fn persistent_inbox_restores_subscriptions_and_messages() {
    let subscriptions = Arc::new(MemorySubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let inbox =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    inbox
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "a/b".into(),
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
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "a/b".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    assert_eq!(inbox.peek(&"s1".into()).await.unwrap().len(), 1);

    let reopened = PersistentInboxHandle::open(subscriptions, messages, inflight);
    assert_eq!(
        reopened
            .list_subscriptions(&"s1".into())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(reopened.list_all_subscriptions().await.unwrap().len(), 1);
    assert_eq!(reopened.peek(&"s1".into()).await.unwrap().len(), 1);
    assert_eq!(reopened.fetch(&"s1".into()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn persistent_inbox_restores_inflight_messages() {
    let subscriptions = Arc::new(MemorySubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let inbox =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "a/b".into(),
            payload: b"1".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Release,
        })
        .await
        .unwrap();

    let reopened = PersistentInboxHandle::open(subscriptions, messages, inflight);
    let inflight = reopened.fetch_inflight(&"s1".into()).await.unwrap();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].packet_id, 7);
    reopened.ack_inflight(&"s1".into(), 7).await.unwrap();
    assert!(reopened
        .fetch_inflight(&"s1".into())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unsubscribe_returns_whether_subscription_existed() {
    let inbox = InboxHandle::default();
    inbox
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
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

    assert!(inbox
        .unsubscribe(&"s1".into(), "devices/+/state")
        .await
        .unwrap());
    assert!(!inbox
        .unsubscribe(&"s1".into(), "devices/+/state")
        .await
        .unwrap());
}

#[tokio::test]
async fn local_list_tenant_topic_subscriptions_returns_indexed_matches() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t1", "s2", "alerts/#"),
        ("t2", "s3", "devices/+/state"),
    ] {
        inbox
            .subscribe(Subscription {
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

    let subscriptions = inbox
        .list_tenant_topic_subscriptions("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].session_id, "s1");
    assert_eq!(subscriptions[0].tenant_id, "t1");
}

#[tokio::test]
async fn local_purge_tenant_topic_subscriptions_removes_only_matching_entries() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t1", "s2", "alerts/#"),
        ("t2", "s3", "devices/+/state"),
    ] {
        inbox
            .subscribe(Subscription {
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

    let removed = inbox
        .purge_tenant_topic_subscriptions("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let remaining = inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|subscription| {
        !(subscription.tenant_id == "t1" && subscription.topic_filter == "devices/+/state")
    }));
    assert_eq!(
        inbox
            .list_tenant_topic_subscriptions("t2", "devices/+/state")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn local_list_tenant_topic_shared_subscriptions_returns_exact_matches() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter, shared_group) in [
        ("t1", "s1", "devices/+/state", Some("workers")),
        ("t1", "s2", "devices/+/state", None),
        ("t1", "s3", "alerts/#", Some("workers")),
        ("t2", "s4", "devices/+/state", Some("workers")),
    ] {
        inbox
            .subscribe(Subscription {
                session_id: session_id.into(),
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let subscriptions = inbox
        .list_tenant_topic_shared_subscriptions("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].session_id, "s1");
    assert_eq!(subscriptions[0].shared_group.as_deref(), Some("workers"));
}

#[tokio::test]
async fn local_purge_tenant_topic_shared_subscriptions_removes_only_matching_entries() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter, shared_group) in [
        ("t1", "s1", "devices/+/state", Some("workers")),
        ("t1", "s2", "devices/+/state", None),
        ("t1", "s3", "alerts/#", Some("workers")),
        ("t2", "s4", "devices/+/state", Some("workers")),
    ] {
        inbox
            .subscribe(Subscription {
                session_id: session_id.into(),
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let removed = inbox
        .purge_tenant_topic_shared_subscriptions("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let remaining = inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 3);
    assert!(remaining.iter().all(|subscription| {
        !(subscription.tenant_id == "t1"
            && subscription.topic_filter == "devices/+/state"
            && subscription.shared_group.as_deref() == Some("workers"))
    }));
}

#[tokio::test]
async fn local_purge_session_topic_subscriptions_removes_only_matching_entries() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter, shared_group) in [
        ("t1", "s1", "devices/+/state", None),
        ("t1", "s1", "devices/+/state", Some("workers")),
        ("t1", "s1", "alerts/#", None),
        ("t2", "s2", "devices/+/state", None),
    ] {
        inbox
            .subscribe(Subscription {
                session_id: session_id.into(),
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let removed = inbox
        .purge_session_topic_subscriptions(&"s1".into(), "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let remaining = inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|subscription| {
        !(subscription.session_id == "s1" && subscription.topic_filter == "devices/+/state")
    }));
    assert_eq!(
        inbox
            .list_session_topic_subscriptions(&"s1".into(), "devices/+/state")
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        inbox
            .list_tenant_topic_subscriptions("t2", "devices/+/state")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn local_purge_tenant_subscriptions_removes_only_matching_entries() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t1", "s2", "alerts/#"),
        ("t2", "s3", "devices/+/state"),
    ] {
        inbox
            .subscribe(Subscription {
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

    let removed = inbox.purge_tenant_subscriptions("t1").await.unwrap();
    assert_eq!(removed, 2);
    assert_eq!(
        inbox.list_tenant_subscriptions("t1").await.unwrap().len(),
        0
    );
    assert_eq!(
        inbox.list_tenant_subscriptions("t2").await.unwrap().len(),
        1
    );
    assert_eq!(inbox.subscription_count().await.unwrap(), 1);
}

#[tokio::test]
async fn local_purge_tenant_offline_removes_only_matching_entries() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic) in [
        ("t1", "s1", "devices/d1/state"),
        ("t1", "s2", "devices/d2/state"),
        ("t2", "s3", "devices/d3/state"),
    ] {
        inbox
            .enqueue(OfflineMessage {
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

    let removed = inbox.purge_tenant_offline("t1").await.unwrap();
    assert_eq!(removed, 2);
    assert!(inbox.list_tenant_offline("t1").await.unwrap().is_empty());
    let remaining = inbox.list_tenant_offline("t2").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].session_id, "s3");
    assert_eq!(inbox.offline_count().await.unwrap(), 1);
}

#[tokio::test]
async fn local_purge_tenant_inflight_removes_only_matching_entries() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, packet_id) in
        [("t1", "s1", 7u16), ("t1", "s2", 8u16), ("t2", "s3", 9u16)]
    {
        inbox
            .stage_inflight(InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                packet_id,
                topic: format!("devices/{packet_id}/state"),
                payload: format!("payload-{packet_id}").into_bytes().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let removed = inbox.purge_tenant_inflight("t1").await.unwrap();
    assert_eq!(removed, 2);
    assert!(inbox.list_tenant_inflight("t1").await.unwrap().is_empty());
    let remaining = inbox.list_tenant_inflight("t2").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].session_id, "s3");
    assert_eq!(inbox.inflight_count().await.unwrap(), 1);
}

#[tokio::test]
async fn local_offline_and_inflight_counts_track_mutations() {
    let inbox = InboxHandle::default();

    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/d1/state".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/d2/state".into(),
            payload: b"2".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    assert_eq!(inbox.offline_count().await.unwrap(), 2);
    assert_eq!(inbox.fetch(&"s1".into()).await.unwrap().len(), 2);
    assert_eq!(inbox.offline_count().await.unwrap(), 0);

    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"1".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();
    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"2".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Release,
        })
        .await
        .unwrap();
    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 8,
            topic: "devices/d2/state".into(),
            payload: b"3".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();
    assert_eq!(inbox.inflight_count().await.unwrap(), 2);
    inbox.ack_inflight(&"s1".into(), 7).await.unwrap();
    assert_eq!(inbox.inflight_count().await.unwrap(), 1);
    inbox.purge_session(&"s1".into()).await.unwrap();
    assert_eq!(inbox.inflight_count().await.unwrap(), 0);
}

#[derive(Default)]
struct CountingSubscriptionStore {
    inner: MemorySubscriptionStore,
    list_subscriptions_calls: AtomicUsize,
    list_tenant_subscriptions_calls: AtomicUsize,
    list_tenant_shared_subscriptions_calls: AtomicUsize,
    list_tenant_topic_subscriptions_calls: AtomicUsize,
    list_tenant_topic_shared_subscriptions_calls: AtomicUsize,
    load_subscription_calls: AtomicUsize,
    purge_session_subscriptions_calls: AtomicUsize,
    purge_session_topic_subscriptions_calls: AtomicUsize,
    purge_tenant_subscriptions_calls: AtomicUsize,
    purge_tenant_topic_subscriptions_calls: AtomicUsize,
    purge_tenant_topic_shared_subscriptions_calls: AtomicUsize,
}

impl CountingSubscriptionStore {
    fn list_subscriptions_calls(&self) -> usize {
        self.list_subscriptions_calls.load(Ordering::SeqCst)
    }

    fn load_subscription_calls(&self) -> usize {
        self.load_subscription_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_subscriptions_calls(&self) -> usize {
        self.list_tenant_subscriptions_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_topic_subscriptions_calls(&self) -> usize {
        self.list_tenant_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn list_tenant_topic_shared_subscriptions_calls(&self) -> usize {
        self.list_tenant_topic_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn purge_session_subscriptions_calls(&self) -> usize {
        self.purge_session_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn purge_session_topic_subscriptions_calls(&self) -> usize {
        self.purge_session_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn purge_tenant_subscriptions_calls(&self) -> usize {
        self.purge_tenant_subscriptions_calls.load(Ordering::SeqCst)
    }

    fn purge_tenant_topic_subscriptions_calls(&self) -> usize {
        self.purge_tenant_topic_subscriptions_calls
            .load(Ordering::SeqCst)
    }

    fn purge_tenant_topic_shared_subscriptions_calls(&self) -> usize {
        self.purge_tenant_topic_shared_subscriptions_calls
            .load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SubscriptionStore for CountingSubscriptionStore {
    async fn save_subscription(&self, subscription: &Subscription) -> anyhow::Result<()> {
        self.inner.save_subscription(subscription).await
    }

    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        self.load_subscription_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .load_subscription(session_id, topic_filter, shared_group)
            .await
    }

    async fn delete_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.inner
            .delete_subscription(session_id, topic_filter, shared_group)
            .await
    }

    async fn list_subscriptions(&self, session_id: &str) -> anyhow::Result<Vec<Subscription>> {
        self.list_subscriptions_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_subscriptions(session_id).await
    }

    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        self.list_tenant_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_subscriptions(tenant_id).await
    }

    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        self.list_tenant_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_shared_subscriptions(tenant_id, shared_group)
            .await
    }

    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        self.list_tenant_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await
    }

    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        self.list_tenant_topic_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        self.inner.list_all_subscriptions().await
    }

    async fn count_subscriptions(&self) -> anyhow::Result<usize> {
        self.inner.count_subscriptions().await
    }

    async fn purge_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        self.purge_session_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.purge_session_subscriptions(session_id).await
    }

    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.purge_session_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .purge_session_topic_subscriptions(session_id, topic_filter)
            .await
    }

    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.purge_tenant_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.purge_tenant_subscriptions(tenant_id).await
    }

    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        self.purge_tenant_topic_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .purge_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await
    }

    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        self.purge_tenant_topic_shared_subscriptions_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .purge_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await
    }
}

#[derive(Default)]
struct CountingInflightStore {
    inner: MemoryInflightStore,
    save_inflight_batch_calls: AtomicUsize,
    delete_inflight_batch_calls: AtomicUsize,
    load_inflight_calls: AtomicUsize,
    list_tenant_inflight_calls: AtomicUsize,
    count_session_inflight_calls: AtomicUsize,
    count_tenant_inflight_calls: AtomicUsize,
    purge_inflight_calls: AtomicUsize,
    purge_tenant_inflight_calls: AtomicUsize,
}

impl CountingInflightStore {
    fn save_inflight_batch_calls(&self) -> usize {
        self.save_inflight_batch_calls.load(Ordering::SeqCst)
    }

    fn delete_inflight_batch_calls(&self) -> usize {
        self.delete_inflight_batch_calls.load(Ordering::SeqCst)
    }

    fn load_inflight_calls(&self) -> usize {
        self.load_inflight_calls.load(Ordering::SeqCst)
    }

    fn purge_inflight_calls(&self) -> usize {
        self.purge_inflight_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_inflight_calls(&self) -> usize {
        self.list_tenant_inflight_calls.load(Ordering::SeqCst)
    }

    fn count_session_inflight_calls(&self) -> usize {
        self.count_session_inflight_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_inflight_calls(&self) -> usize {
        self.count_tenant_inflight_calls.load(Ordering::SeqCst)
    }

    fn purge_tenant_inflight_calls(&self) -> usize {
        self.purge_tenant_inflight_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl InflightStore for CountingInflightStore {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()> {
        self.inner.save_inflight(message).await
    }

    async fn save_inflight_batch(&self, messages: &[InflightMessage]) -> anyhow::Result<()> {
        self.save_inflight_batch_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.save_inflight_batch(messages).await
    }

    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        self.load_inflight_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.load_inflight(session_id).await
    }

    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        self.list_tenant_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_inflight(tenant_id).await
    }

    async fn count_session_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_inflight(session_id).await
    }

    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_inflight(tenant_id).await
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        self.inner.list_all_inflight().await
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        self.inner.delete_inflight(session_id, packet_id).await
    }

    async fn delete_inflight_batch(
        &self,
        session_id: &str,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        self.delete_inflight_batch_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .delete_inflight_batch(session_id, packet_ids)
            .await
    }

    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        self.purge_inflight_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.purge_inflight(session_id).await
    }

    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.purge_tenant_inflight_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.purge_tenant_inflight(tenant_id).await
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        self.inner.count_inflight().await
    }
}

#[derive(Default)]
struct CountingInboxStore {
    inner: MemoryInboxStore,
    append_messages_calls: AtomicUsize,
    load_messages_calls: AtomicUsize,
    list_tenant_messages_calls: AtomicUsize,
    count_session_messages_calls: AtomicUsize,
    count_tenant_messages_calls: AtomicUsize,
    purge_messages_calls: AtomicUsize,
    purge_tenant_messages_calls: AtomicUsize,
}

impl CountingInboxStore {
    fn append_messages_calls(&self) -> usize {
        self.append_messages_calls.load(Ordering::SeqCst)
    }

    fn load_messages_calls(&self) -> usize {
        self.load_messages_calls.load(Ordering::SeqCst)
    }

    fn purge_messages_calls(&self) -> usize {
        self.purge_messages_calls.load(Ordering::SeqCst)
    }

    fn list_tenant_messages_calls(&self) -> usize {
        self.list_tenant_messages_calls.load(Ordering::SeqCst)
    }

    fn count_session_messages_calls(&self) -> usize {
        self.count_session_messages_calls.load(Ordering::SeqCst)
    }

    fn count_tenant_messages_calls(&self) -> usize {
        self.count_tenant_messages_calls.load(Ordering::SeqCst)
    }

    fn purge_tenant_messages_calls(&self) -> usize {
        self.purge_tenant_messages_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl InboxStore for CountingInboxStore {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        self.inner.append_message(message).await
    }

    async fn append_messages(&self, messages: &[OfflineMessage]) -> anyhow::Result<()> {
        self.append_messages_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.append_messages(messages).await
    }

    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inner.peek_messages(session_id).await
    }

    async fn list_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.list_tenant_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.list_tenant_messages(tenant_id).await
    }

    async fn count_session_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        self.count_session_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_session_messages(session_id).await
    }

    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.count_tenant_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.count_tenant_messages(tenant_id).await
    }

    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        self.inner.list_all_messages().await
    }

    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        self.load_messages_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.load_messages(session_id).await
    }

    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        self.purge_messages_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.purge_messages(session_id).await
    }

    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.purge_tenant_messages_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.purge_tenant_messages(tenant_id).await
    }

    async fn count_messages(&self) -> anyhow::Result<usize> {
        self.inner.count_messages().await
    }
}

#[tokio::test]
async fn persistent_lookup_subscription_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    writer
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
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

    let lazy = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let subscription = lazy
        .lookup_subscription(&"s1".into(), "devices/+/state", None)
        .await
        .unwrap();
    assert!(subscription.is_some());
    let subscription = lazy
        .lookup_subscription(&"s1".into(), "devices/+/state", None)
        .await
        .unwrap();
    assert!(subscription.is_some());
    assert_eq!(subscriptions.load_subscription_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
}

#[tokio::test]
async fn persistent_enqueue_batch_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let inbox = PersistentInboxHandle::open(subscriptions, messages.clone(), inflight);

    inbox
        .enqueue_batch(vec![
            OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                topic: "devices/d1/state".into(),
                payload: b"1".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            },
            OfflineMessage {
                tenant_id: "t1".into(),
                session_id: "s2".into(),
                topic: "devices/d2/state".into(),
                payload: b"2".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            },
        ])
        .await
        .unwrap();

    assert_eq!(messages.append_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
    assert_eq!(inbox.list_tenant_offline("t1").await.unwrap().len(), 2);
}

#[tokio::test]
async fn persistent_purge_session_uses_direct_store_purges() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());

    inbox
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
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
    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();

    let cold =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    cold.purge_session(&"s1".into()).await.unwrap();

    assert_eq!(subscriptions.purge_session_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
    assert_eq!(messages.purge_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
    assert_eq!(inflight.purge_inflight_calls(), 1);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

#[tokio::test]
async fn persistent_stage_inflight_batch_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());

    inbox
        .stage_inflight_batch(vec![
            InflightMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                packet_id: 7,
                topic: "devices/d1/state".into(),
                payload: b"1".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            },
            InflightMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                packet_id: 8,
                topic: "devices/d2/state".into(),
                payload: b"2".to_vec().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            },
        ])
        .await
        .unwrap();

    assert_eq!(inflight.save_inflight_batch_calls(), 1);
    assert_eq!(inbox.fetch_inflight(&"s1".into()).await.unwrap().len(), 2);
}

#[tokio::test]
async fn persistent_ack_inflight_batch_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let inbox = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());

    inbox
        .stage_inflight_batch(vec![
            InflightMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                packet_id: 7,
                topic: "devices/d1/state".into(),
                payload: b"1".to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            },
            InflightMessage {
                tenant_id: "t1".into(),
                session_id: "s1".into(),
                packet_id: 8,
                topic: "devices/d2/state".into(),
                payload: b"2".to_vec().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            },
        ])
        .await
        .unwrap();

    inbox
        .ack_inflight_batch(&"s1".into(), &[7, 8])
        .await
        .unwrap();

    assert_eq!(inflight.delete_inflight_batch_calls(), 1);
    assert!(inbox.fetch_inflight(&"s1".into()).await.unwrap().is_empty());
}

#[tokio::test]
async fn persistent_purge_offline_uses_direct_store_purge() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    writer
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/d1/state".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let cold = PersistentInboxHandle::open(subscriptions, messages.clone(), inflight);
    let removed = cold.purge_offline(&"s1".into()).await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(messages.purge_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_session_subscriptions_uses_direct_store_purge() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    writer
        .subscribe(Subscription {
            session_id: "s1".into(),
            tenant_id: "t1".into(),
            topic_filter: "devices/+/state".into(),
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

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let removed = cold
        .purge_session_subscriptions_only(&"s1".into())
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(subscriptions.purge_session_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_session_topic_subscriptions_uses_direct_store_purge() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (session_id, topic_filter, shared_group) in [
        ("s1", "devices/+/state", None),
        ("s1", "devices/+/state", Some("workers")),
        ("s1", "alerts/#", None),
    ] {
        writer
            .subscribe(Subscription {
                session_id: session_id.into(),
                tenant_id: "t1".into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let removed = cold
        .purge_session_topic_subscriptions(&"s1".into(), "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(subscriptions.purge_session_topic_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
    assert!(cold
        .list_session_topic_subscriptions(&"s1".into(), "devices/+/state")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn persistent_purge_inflight_uses_direct_store_purge() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    writer
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();

    let cold = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());
    let removed = cold.purge_inflight_session(&"s1".into()).await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(inflight.purge_inflight_calls(), 1);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

#[tokio::test]
async fn persistent_list_tenant_offline_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, payload) in
        [("t1", "s1", b"one".to_vec()), ("t2", "s2", b"two".to_vec())]
    {
        writer
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
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

    let lazy = PersistentInboxHandle::open(subscriptions, messages.clone(), inflight);
    let listed = lazy.list_tenant_offline("t1").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_id, "s1");
    assert_eq!(messages.list_tenant_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_tenant_offline_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, payload) in
        [("t1", "s1", b"one".to_vec()), ("t2", "s2", b"two".to_vec())]
    {
        writer
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
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

    let cold = PersistentInboxHandle::open(subscriptions, messages.clone(), inflight);
    let removed = cold.purge_tenant_offline("t1").await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(messages.purge_tenant_messages_calls(), 1);
    assert_eq!(messages.list_tenant_messages_calls(), 0);
    assert_eq!(messages.load_messages_calls(), 0);
}

#[tokio::test]
async fn persistent_count_session_offline_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    writer
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/d1/state".into(),
            payload: b"one".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let lazy = PersistentInboxHandle::open(subscriptions, messages.clone(), inflight);
    let count = lazy.count_session_offline(&"s1".into()).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(messages.count_session_messages_calls(), 1);
    assert_eq!(messages.load_messages_calls(), 0);
}

#[tokio::test]
async fn persistent_count_tenant_offline_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(CountingInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id) in [("t1", "s1"), ("t2", "s2")] {
        writer
            .enqueue(OfflineMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                topic: "devices/d1/state".into(),
                payload: session_id.as_bytes().to_vec().into(),
                qos: 1,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
            })
            .await
            .unwrap();
    }

    let lazy = PersistentInboxHandle::open(subscriptions, messages.clone(), inflight);
    let count = lazy.count_tenant_offline("t1").await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(messages.count_tenant_messages_calls(), 1);
    assert_eq!(messages.list_tenant_messages_calls(), 0);
}

#[tokio::test]
async fn persistent_list_tenant_topic_subscriptions_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t1", "s2", "alerts/#"),
        ("t2", "s3", "devices/+/state"),
    ] {
        writer
            .subscribe(Subscription {
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

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let subscriptions_for_topic = cold
        .list_tenant_topic_subscriptions("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(subscriptions_for_topic.len(), 1);
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 0);
}

#[tokio::test]
async fn persistent_list_tenant_topic_shared_subscriptions_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, topic_filter, shared_group) in [
        ("t1", "s1", "devices/+/state", Some("workers")),
        ("t1", "s2", "devices/+/state", None),
        ("t2", "s3", "devices/+/state", Some("workers")),
    ] {
        writer
            .subscribe(Subscription {
                session_id: session_id.into(),
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let subscriptions_for_topic = cold
        .list_tenant_topic_shared_subscriptions("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(subscriptions_for_topic.len(), 1);
    assert_eq!(
        subscriptions.list_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_tenant_subscriptions_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t1", "s2", "alerts/#"),
        ("t2", "s3", "devices/+/state"),
    ] {
        writer
            .subscribe(Subscription {
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

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let removed = cold.purge_tenant_subscriptions("t1").await.unwrap();
    assert_eq!(removed, 2);
    assert_eq!(subscriptions.purge_tenant_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_tenant_topic_subscriptions_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t1", "s2", "alerts/#"),
        ("t2", "s3", "devices/+/state"),
    ] {
        writer
            .subscribe(Subscription {
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

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let removed = cold
        .purge_tenant_topic_subscriptions("t1", "devices/+/state")
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(subscriptions.purge_tenant_topic_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_tenant_topic_subscriptions_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_tenant_topic_shared_subscriptions_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, topic_filter, shared_group) in [
        ("t1", "s1", "devices/+/state", Some("workers")),
        ("t1", "s2", "devices/+/state", None),
        ("t2", "s3", "devices/+/state", Some("workers")),
    ] {
        writer
            .subscribe(Subscription {
                session_id: session_id.into(),
                tenant_id: tenant_id.into(),
                topic_filter: topic_filter.into(),
                qos: 1,
                subscription_identifier: None,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                shared_group: shared_group.map(str::to_string),
                kind: SessionKind::Persistent,
            })
            .await
            .unwrap();
    }

    let cold = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let removed = cold
        .purge_tenant_topic_shared_subscriptions("t1", "devices/+/state", Some("workers"))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(
        subscriptions.purge_tenant_topic_shared_subscriptions_calls(),
        1
    );
    assert_eq!(
        subscriptions.list_tenant_topic_shared_subscriptions_calls(),
        0
    );
}

#[tokio::test]
async fn persistent_list_tenant_inflight_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, packet_id) in [("t1", "s1", 7u16), ("t2", "s2", 9u16)] {
        writer
            .stage_inflight(InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());
    let listed = lazy.list_tenant_inflight("t1").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_id, "s1");
    assert_eq!(inflight.list_tenant_inflight_calls(), 1);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

#[tokio::test]
async fn persistent_purge_tenant_inflight_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, packet_id) in [("t1", "s1", 7u16), ("t2", "s2", 9u16)] {
        writer
            .stage_inflight(InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let cold = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());
    let removed = cold.purge_tenant_inflight("t1").await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(inflight.purge_tenant_inflight_calls(), 1);
    assert_eq!(inflight.list_tenant_inflight_calls(), 0);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

#[tokio::test]
async fn persistent_count_session_inflight_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    writer
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "devices/d1/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 2,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();

    let lazy = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());
    let count = lazy.count_session_inflight(&"s1".into()).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(inflight.count_session_inflight_calls(), 1);
    assert_eq!(inflight.load_inflight_calls(), 0);
}

#[tokio::test]
async fn persistent_count_tenant_inflight_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(CountingInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, packet_id) in [("t1", "s1", 7u16), ("t2", "s2", 9u16)] {
        writer
            .stage_inflight(InflightMessage {
                tenant_id: tenant_id.into(),
                session_id: session_id.into(),
                packet_id,
                topic: "devices/d1/state".into(),
                payload: format!("inflight-{packet_id}").into_bytes().into(),
                qos: 2,
                retain: false,
                from_session_id: "src".into(),
                properties: PublishProperties::default(),
                phase: InflightPhase::Publish,
            })
            .await
            .unwrap();
    }

    let lazy = PersistentInboxHandle::open(subscriptions, messages, inflight.clone());
    let count = lazy.count_tenant_inflight("t1").await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(inflight.count_tenant_inflight_calls(), 1);
    assert_eq!(inflight.list_tenant_inflight_calls(), 0);
}

#[tokio::test]
async fn list_all_subscriptions_returns_global_view() {
    let inbox = InboxHandle::default();
    for (tenant_id, session_id, topic_filter) in [
        ("t1", "s1", "devices/+/state"),
        ("t2", "s2", "alerts/#"),
        ("t1", "s3", "metrics/#"),
    ] {
        inbox
            .subscribe(Subscription {
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

    let subscriptions = inbox.list_all_subscriptions().await.unwrap();
    assert_eq!(subscriptions.len(), 3);
    assert_eq!(
        subscriptions
            .iter()
            .filter(|subscription| subscription.tenant_id == "t1")
            .count(),
        2
    );
}

#[tokio::test]
async fn persistent_list_tenant_subscriptions_uses_store_direct() {
    let subscriptions = Arc::new(CountingSubscriptionStore::default());
    let messages = Arc::new(MemoryInboxStore::default());
    let inflight = Arc::new(MemoryInflightStore::default());
    let writer =
        PersistentInboxHandle::open(subscriptions.clone(), messages.clone(), inflight.clone());
    for (tenant_id, session_id, topic_filter) in
        [("t1", "s1", "devices/+/state"), ("t2", "s2", "alerts/#")]
    {
        writer
            .subscribe(Subscription {
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

    let lazy = PersistentInboxHandle::open(subscriptions.clone(), messages, inflight);
    let listed = lazy.list_tenant_subscriptions("t1").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].session_id, "s1");
    assert_eq!(subscriptions.list_tenant_subscriptions_calls(), 1);
    assert_eq!(subscriptions.list_subscriptions_calls(), 0);
}

#[tokio::test]
async fn list_all_offline_and_inflight_return_global_views() {
    let inbox = InboxHandle::default();
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "a/b".into(),
            payload: b"1".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t2".into(),
            session_id: "s2".into(),
            topic: "c/d".into(),
            payload: b"2".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 7,
            topic: "a/b".into(),
            payload: b"3".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();

    assert_eq!(inbox.list_all_offline().await.unwrap().len(), 2);
    assert_eq!(inbox.list_all_inflight().await.unwrap().len(), 1);
}

#[tokio::test]
async fn replicated_inbox_handles_subscriptions_offline_and_inflight_over_kv_ranges() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "inbox-range-t1".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "inflight-range-t1".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();

    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "inbox-range-t1",
            inbox_tenant_scan_shard("t1"),
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
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "inflight-range-t1",
            inflight_tenant_scan_shard("t1"),
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

    let inbox = ReplicatedInboxHandle::new(
        router,
        Arc::new(LocalKvRangeExecutor {
            engine: engine.clone(),
        }),
    );
    let subscription = Subscription {
        session_id: "s1".into(),
        tenant_id: "t1".into(),
        topic_filter: "devices/+/state".into(),
        qos: 1,
        subscription_identifier: None,
        no_local: false,
        retain_as_published: false,
        retain_handling: 0,
        shared_group: Some("workers".into()),
        kind: SessionKind::Persistent,
    };
    inbox.subscribe(subscription.clone()).await.unwrap();
    assert_eq!(
        inbox.list_subscriptions(&"s1".into()).await.unwrap(),
        vec![subscription]
    );

    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"offline".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    assert_eq!(inbox.peek(&"s1".into()).await.unwrap().len(), 1);
    assert_eq!(inbox.fetch(&"s1".into()).await.unwrap().len(), 1);
    assert!(inbox.peek(&"s1".into()).await.unwrap().is_empty());

    inbox
        .stage_inflight(InflightMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            packet_id: 42,
            topic: "devices/a/state".into(),
            payload: b"inflight".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
            phase: InflightPhase::Publish,
        })
        .await
        .unwrap();
    assert_eq!(inbox.fetch_inflight(&"s1".into()).await.unwrap().len(), 1);
    inbox.ack_inflight(&"s1".into(), 42).await.unwrap();
    assert!(inbox.fetch_inflight(&"s1".into()).await.unwrap().is_empty());
}

#[tokio::test]
async fn replicated_inbox_resumes_offline_sequence_after_restart() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "inbox-range-t1".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "inbox-range-t1",
            inbox_tenant_scan_shard("t1"),
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

    let executor = Arc::new(LocalKvRangeExecutor {
        engine: engine.clone(),
    });
    let inbox = ReplicatedInboxHandle::new(router.clone(), executor.clone());
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"first".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();
    drop(inbox);

    let reopened = ReplicatedInboxHandle::new(router, executor);
    reopened
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/b/state".into(),
            payload: b"second".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let messages = reopened.peek(&"s1".into()).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert!(messages
        .iter()
        .any(|message| message.payload.as_ref() == b"first"));
    assert!(messages
        .iter()
        .any(|message| message.payload.as_ref() == b"second"));
}

#[tokio::test]
async fn replicated_inbox_replaying_same_offline_mutation_is_idempotent() {
    let engine = MemoryKvEngine::default();
    engine
        .bootstrap(KvRangeBootstrap {
            range_id: "inbox-range-replay".into(),
            boundary: RangeBoundary::full(),
        })
        .await
        .unwrap();
    let router = Arc::new(MemoryKvRangeRouter::default());
    router
        .upsert(ReplicatedRangeDescriptor::new(
            "inbox-range-replay",
            inbox_tenant_scan_shard("t1"),
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

    let executor = Arc::new(LocalKvRangeExecutor {
        engine: engine.clone(),
    });
    let inbox = ReplicatedInboxHandle::new(router, executor.clone());
    inbox
        .enqueue(OfflineMessage {
            tenant_id: "t1".into(),
            session_id: "s1".into(),
            topic: "devices/a/state".into(),
            payload: b"first".to_vec().into(),
            qos: 1,
            retain: false,
            from_session_id: "src".into(),
            properties: PublishProperties::default(),
        })
        .await
        .unwrap();

    let entries = executor
        .scan("inbox-range-replay", None, usize::MAX)
        .await
        .unwrap();
    let (key, value) = entries
        .into_iter()
        .find(|(key, _)| key.starts_with(OFFLINE_KEY_PREFIX))
        .unwrap();
    executor
        .apply(
            "inbox-range-replay",
            vec![KvMutation {
                key,
                value: Some(value),
            }],
        )
        .await
        .unwrap();

    let messages = inbox.peek(&"s1".into()).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload.as_ref(), b"first");
}
