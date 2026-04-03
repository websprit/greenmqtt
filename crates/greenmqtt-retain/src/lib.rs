use async_trait::async_trait;
use greenmqtt_core::{CompiledTopicFilter, Lifecycle, RetainedMessage, ServiceShardKey};
use greenmqtt_storage::RetainStore;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[async_trait]
pub trait RetainService: Send + Sync {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()>;
    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>>;
    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>>;
    async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>>;
    async fn retained_count(&self) -> anyhow::Result<usize>;
}

#[derive(Clone, Default)]
pub struct RetainHandle {
    inner: Arc<RwLock<RetainState>>,
}

#[derive(Default)]
struct RetainState {
    by_tenant: HashMap<String, HashMap<String, RetainedMessage>>,
    total_retained: usize,
}

#[derive(Clone)]
pub struct PersistentRetainHandle {
    store: Arc<dyn RetainStore>,
    inner: Arc<RwLock<PersistentRetainState>>,
}

pub fn retain_tenant_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey::retain(tenant_id.to_string())
}

pub fn retained_message_shard(message: &RetainedMessage) -> ServiceShardKey {
    retain_tenant_shard(&message.tenant_id)
}

#[derive(Default)]
struct PersistentRetainState {
    by_tenant: HashMap<String, HashMap<String, RetainedMessage>>,
    warmed_tenants: HashSet<String>,
    loaded_topics: HashMap<String, HashSet<String>>,
    matched_filters: HashMap<String, HashMap<String, Vec<RetainedMessage>>>,
    all_retained_warmed: bool,
}

impl PersistentRetainHandle {
    pub fn open(store: Arc<dyn RetainStore>) -> Self {
        Self {
            store,
            inner: Arc::new(RwLock::new(PersistentRetainState::default())),
        }
    }

    fn cache_retain(&self, message: RetainedMessage) {
        let tenant_id = message.tenant_id.clone();
        let topic = message.topic.clone();
        let mut guard = self.inner.write().expect("retain poisoned");
        guard
            .by_tenant
            .entry(message.tenant_id.clone())
            .or_default()
            .insert(message.topic.clone(), message);
        guard
            .loaded_topics
            .entry(tenant_id.clone())
            .or_default()
            .insert(topic);
        guard.matched_filters.remove(&tenant_id);
    }

    fn evict_retain(&self, tenant_id: &str, topic: &str) {
        let mut guard = self.inner.write().expect("retain poisoned");
        if let Some(messages) = guard.by_tenant.get_mut(tenant_id) {
            messages.remove(topic);
            if messages.is_empty() {
                guard.by_tenant.remove(tenant_id);
            }
        }
        guard
            .loaded_topics
            .entry(tenant_id.to_string())
            .or_default()
            .insert(topic.to_string());
        guard.matched_filters.remove(tenant_id);
    }

    fn replace_tenant_retained(&self, tenant_id: &str, messages: Vec<RetainedMessage>) {
        let mut guard = self.inner.write().expect("retain poisoned");
        let retained = messages
            .into_iter()
            .map(|message| (message.topic.clone(), message))
            .collect::<HashMap<_, _>>();
        let loaded_topics = retained.keys().cloned().collect::<HashSet<_>>();
        guard.by_tenant.insert(tenant_id.to_string(), retained);
        guard.warmed_tenants.insert(tenant_id.to_string());
        guard.matched_filters.remove(tenant_id);
        guard
            .loaded_topics
            .insert(tenant_id.to_string(), loaded_topics);
    }

    fn cache_match_result(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        messages: &[RetainedMessage],
    ) {
        let mut guard = self.inner.write().expect("retain poisoned");
        let topics = messages
            .iter()
            .map(|message| message.topic.clone())
            .collect::<Vec<_>>();
        let tenant_messages = guard.by_tenant.entry(tenant_id.to_string()).or_default();
        for message in messages {
            tenant_messages.insert(message.topic.clone(), message.clone());
        }
        let loaded_topics = guard
            .loaded_topics
            .entry(tenant_id.to_string())
            .or_default();
        for topic in topics {
            loaded_topics.insert(topic);
        }
        guard
            .matched_filters
            .entry(tenant_id.to_string())
            .or_default()
            .insert(topic_filter.to_string(), messages.to_vec());
    }
}

fn topic_filter_is_exact(topic_filter: &str) -> bool {
    !topic_filter.contains('#') && !topic_filter.contains('+')
}

#[async_trait]
impl RetainService for RetainHandle {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("retain poisoned");
        if message.payload.is_empty() {
            let mut removed = false;
            let mut remove_tenant = false;
            if let Some(messages) = guard.by_tenant.get_mut(&message.tenant_id) {
                removed = messages.remove(&message.topic).is_some();
                remove_tenant = messages.is_empty();
            }
            if removed {
                guard.total_retained -= 1;
            }
            if remove_tenant {
                guard.by_tenant.remove(&message.tenant_id);
            }
        } else {
            let inserted = {
                let messages = guard
                    .by_tenant
                    .entry(message.tenant_id.clone())
                    .or_default();
                messages.insert(message.topic.clone(), message).is_none()
            };
            if inserted {
                guard.total_retained += 1;
            }
        }
        Ok(())
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        Ok(self
            .inner
            .read()
            .expect("retain poisoned")
            .by_tenant
            .get(tenant_id)
            .into_iter()
            .flat_map(|messages| messages.values())
            .cloned()
            .collect())
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self
            .inner
            .read()
            .expect("retain poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|messages| messages.get(topic))
            .cloned())
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        if topic_filter_is_exact(topic_filter) {
            return Ok(self
                .lookup_topic(tenant_id, topic_filter)
                .await?
                .into_iter()
                .collect());
        }
        let compiled = CompiledTopicFilter::new(topic_filter);
        let guard = self.inner.read().expect("retain poisoned");
        let matched = guard
            .by_tenant
            .get(tenant_id)
            .into_iter()
            .flat_map(|messages| messages.values())
            .filter(|message| compiled.matches(&message.topic))
            .cloned()
            .collect();
        Ok(matched)
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        Ok(self.inner.read().expect("retain poisoned").total_retained)
    }
}

#[async_trait]
impl RetainService for PersistentRetainHandle {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        self.store.put_retain(&message).await?;
        if message.payload.is_empty() {
            self.evict_retain(&message.tenant_id, &message.topic);
        } else {
            self.cache_retain(message);
        }
        Ok(())
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        if let Some(messages) = {
            let guard = self.inner.read().expect("retain poisoned");
            if guard.all_retained_warmed || guard.warmed_tenants.contains(tenant_id) {
                Some(
                    guard
                        .by_tenant
                        .get(tenant_id)
                        .into_iter()
                        .flat_map(|messages| messages.values())
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        } {
            return Ok(messages);
        }
        let messages = self.store.list_tenant_retained(tenant_id).await?;
        self.replace_tenant_retained(tenant_id, messages.clone());
        Ok(messages)
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        if let Some(message) = {
            let guard = self.inner.read().expect("retain poisoned");
            if guard.all_retained_warmed
                || guard.warmed_tenants.contains(tenant_id)
                || guard
                    .loaded_topics
                    .get(tenant_id)
                    .is_some_and(|topics| topics.contains(topic))
            {
                Some(
                    guard
                        .by_tenant
                        .get(tenant_id)
                        .and_then(|messages| messages.get(topic))
                        .cloned(),
                )
            } else {
                None
            }
        } {
            return Ok(message);
        }
        let message = self.store.load_retain(tenant_id, topic).await?;
        if let Some(message) = &message {
            self.cache_retain(message.clone());
        } else {
            self.inner
                .write()
                .expect("retain poisoned")
                .loaded_topics
                .entry(tenant_id.to_string())
                .or_default()
                .insert(topic.to_string());
        }
        Ok(message)
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        if topic_filter_is_exact(topic_filter) {
            return Ok(self
                .lookup_topic(tenant_id, topic_filter)
                .await?
                .into_iter()
                .collect());
        }
        if let Some(messages) = {
            let compiled = CompiledTopicFilter::new(topic_filter);
            let guard = self.inner.read().expect("retain poisoned");
            if let Some(messages) = guard
                .matched_filters
                .get(tenant_id)
                .and_then(|filters| filters.get(topic_filter))
            {
                Some(messages.clone())
            } else if guard.all_retained_warmed || guard.warmed_tenants.contains(tenant_id) {
                Some(
                    guard
                        .by_tenant
                        .get(tenant_id)
                        .into_iter()
                        .flat_map(|messages| messages.values())
                        .filter(|message| compiled.matches(&message.topic))
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        } {
            if self
                .inner
                .read()
                .expect("retain poisoned")
                .matched_filters
                .get(tenant_id)
                .and_then(|filters| filters.get(topic_filter))
                .is_none()
            {
                self.cache_match_result(tenant_id, topic_filter, &messages);
            }
            return Ok(messages);
        }
        let messages = self.store.match_retain(tenant_id, topic_filter).await?;
        self.cache_match_result(tenant_id, topic_filter, &messages);
        Ok(messages)
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        {
            let guard = self.inner.read().expect("retain poisoned");
            if guard.all_retained_warmed {
                return Ok(guard.by_tenant.values().map(HashMap::len).sum());
            }
        }
        self.store.count_retained().await
    }
}

#[async_trait]
impl Lifecycle for RetainHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Lifecycle for PersistentRetainHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        retain_tenant_shard, retained_message_shard, PersistentRetainHandle, RetainHandle,
        RetainService,
    };
    use async_trait::async_trait;
    use greenmqtt_core::{RetainedMessage, ServiceShardKey};
    use greenmqtt_storage::{MemoryRetainStore, RetainStore};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn retain_match_is_tenant_scoped() {
        let retain = RetainHandle::default();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        assert_eq!(
            retain
                .match_topic("t1", "devices/+/state")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(retain
            .match_topic("t2", "devices/+/state")
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn retain_helpers_keep_point_lookup_and_scan_on_same_tenant_shard() {
        let message = RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
        };
        assert_eq!(retain_tenant_shard("t1"), ServiceShardKey::retain("t1"));
        assert_eq!(
            retained_message_shard(&message),
            ServiceShardKey::retain("t1")
        );
    }

    #[tokio::test]
    async fn retain_lookup_is_exact() {
        let retain = RetainHandle::default();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        assert!(retain
            .lookup_topic("t1", "devices/a/state")
            .await
            .unwrap()
            .is_some());
        assert!(retain
            .lookup_topic("t1", "devices/+/state")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn persistent_retain_restores_messages() {
        let store = Arc::new(MemoryRetainStore::default());
        let retain = PersistentRetainHandle::open(store.clone());
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store);
        assert_eq!(
            reopened
                .match_topic("t1", "devices/+/state")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[derive(Default)]
    struct CountingRetainStore {
        inner: MemoryRetainStore,
        list_tenant_retained_calls: AtomicUsize,
        match_retain_calls: AtomicUsize,
        load_retain_calls: AtomicUsize,
        count_retained_calls: AtomicUsize,
    }

    impl CountingRetainStore {
        fn list_tenant_retained_calls(&self) -> usize {
            self.list_tenant_retained_calls.load(Ordering::SeqCst)
        }

        fn match_retain_calls(&self) -> usize {
            self.match_retain_calls.load(Ordering::SeqCst)
        }

        fn load_retain_calls(&self) -> usize {
            self.load_retain_calls.load(Ordering::SeqCst)
        }

        fn count_retained_calls(&self) -> usize {
            self.count_retained_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RetainStore for CountingRetainStore {
        async fn put_retain(&self, message: &RetainedMessage) -> anyhow::Result<()> {
            self.inner.put_retain(message).await
        }

        async fn load_retain(
            &self,
            tenant_id: &str,
            topic: &str,
        ) -> anyhow::Result<Option<RetainedMessage>> {
            self.load_retain_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.load_retain(tenant_id, topic).await
        }

        async fn list_tenant_retained(
            &self,
            tenant_id: &str,
        ) -> anyhow::Result<Vec<RetainedMessage>> {
            self.list_tenant_retained_calls
                .fetch_add(1, Ordering::SeqCst);
            self.inner.list_tenant_retained(tenant_id).await
        }

        async fn match_retain(
            &self,
            tenant_id: &str,
            topic_filter: &str,
        ) -> anyhow::Result<Vec<RetainedMessage>> {
            self.match_retain_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.match_retain(tenant_id, topic_filter).await
        }

        async fn count_retained(&self) -> anyhow::Result<usize> {
            self.count_retained_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.count_retained().await
        }
    }

    #[tokio::test]
    async fn persistent_list_tenant_retained_uses_store_direct() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let retained = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(store.list_tenant_retained_calls(), 1);
        assert_eq!(store.match_retain_calls(), 0);
        assert_eq!(store.load_retain_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_list_tenant_retained_reuses_warm_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let first = reopened.list_tenant_retained("t1").await.unwrap();
        let second = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(store.list_tenant_retained_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_exact_match_uses_store_lookup_fast_path() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let retained = reopened.match_topic("t1", "devices/a/state").await.unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(store.load_retain_calls(), 1);
        assert_eq!(store.match_retain_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_lookup_exact_reuses_warm_tenant_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let warmed = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(warmed.len(), 1);

        let retained = reopened
            .lookup_topic("t1", "devices/a/state")
            .await
            .unwrap();
        assert!(retained.is_some());
        assert_eq!(store.list_tenant_retained_calls(), 1);
        assert_eq!(store.load_retain_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_lookup_exact_reuses_loaded_topic_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let first = reopened
            .lookup_topic("t1", "devices/a/state")
            .await
            .unwrap();
        let second = reopened
            .lookup_topic("t1", "devices/a/state")
            .await
            .unwrap();
        assert!(first.is_some());
        assert!(second.is_some());
        assert_eq!(store.load_retain_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_missing_exact_lookup_reuses_loaded_topic_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let reopened = PersistentRetainHandle::open(store.clone());
        let first = reopened
            .lookup_topic("t1", "devices/missing/state")
            .await
            .unwrap();
        let second = reopened
            .lookup_topic("t1", "devices/missing/state")
            .await
            .unwrap();
        assert!(first.is_none());
        assert!(second.is_none());
        assert_eq!(store.load_retain_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_wildcard_match_reuses_warm_tenant_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        for topic in ["devices/a/state", "devices/b/state"] {
            writer
                .retain(RetainedMessage {
                    tenant_id: "t1".into(),
                    topic: topic.into(),
                    payload: b"up".to_vec().into(),
                    qos: 1,
                })
                .await
                .unwrap();
        }

        let reopened = PersistentRetainHandle::open(store.clone());
        let warmed = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(warmed.len(), 2);

        let matched = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        assert_eq!(matched.len(), 2);
        assert_eq!(store.list_tenant_retained_calls(), 1);
        assert_eq!(store.match_retain_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_wildcard_match_on_warm_tenant_populates_filter_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        for topic in ["devices/a/state", "devices/b/state"] {
            writer
                .retain(RetainedMessage {
                    tenant_id: "t1".into(),
                    topic: topic.into(),
                    payload: b"up".to_vec().into(),
                    qos: 1,
                })
                .await
                .unwrap();
        }

        let reopened = PersistentRetainHandle::open(store);
        let warmed = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(warmed.len(), 2);

        let matched = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        assert_eq!(matched.len(), 2);
        assert_eq!(
            reopened
                .inner
                .read()
                .expect("retain poisoned")
                .matched_filters
                .get("t1")
                .and_then(|filters| filters.get("devices/+/state"))
                .map(|messages| messages.len()),
            Some(2)
        );
    }

    #[tokio::test]
    async fn persistent_missing_wildcard_match_on_warm_tenant_populates_filter_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store);
        let warmed = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(warmed.len(), 1);

        let matched = reopened.match_topic("t1", "alerts/#").await.unwrap();
        assert!(matched.is_empty());
        assert_eq!(
            reopened
                .inner
                .read()
                .expect("retain poisoned")
                .matched_filters
                .get("t1")
                .and_then(|filters| filters.get("alerts/#"))
                .map(|messages| messages.len()),
            Some(0)
        );
    }

    #[tokio::test]
    async fn persistent_wildcard_match_reuses_loaded_filter_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        for topic in ["devices/a/state", "devices/b/state"] {
            writer
                .retain(RetainedMessage {
                    tenant_id: "t1".into(),
                    topic: topic.into(),
                    payload: b"up".to_vec().into(),
                    qos: 1,
                })
                .await
                .unwrap();
        }

        let reopened = PersistentRetainHandle::open(store.clone());
        let first = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        let second = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_eq!(store.match_retain_calls(), 1);
        assert_eq!(store.list_tenant_retained_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_missing_wildcard_match_reuses_loaded_filter_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let reopened = PersistentRetainHandle::open(store.clone());
        let first = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        let second = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        assert!(first.is_empty());
        assert!(second.is_empty());
        assert_eq!(store.match_retain_calls(), 1);
        assert_eq!(store.list_tenant_retained_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_wildcard_match_populates_exact_lookup_cache() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let matched = reopened.match_topic("t1", "devices/+/state").await.unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(store.match_retain_calls(), 1);

        let load_calls_before = store.load_retain_calls();
        let exact = reopened
            .lookup_topic("t1", "devices/a/state")
            .await
            .unwrap();
        assert!(exact.is_some());
        assert_eq!(store.load_retain_calls(), load_calls_before);
    }

    #[tokio::test]
    async fn persistent_retained_count_uses_store_when_global_cache_not_warmed() {
        let store = Arc::new(CountingRetainStore::default());
        let writer = PersistentRetainHandle::open(store.clone());
        writer
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let reopened = PersistentRetainHandle::open(store.clone());
        let warmed = reopened.list_tenant_retained("t1").await.unwrap();
        assert_eq!(warmed.len(), 1);
        assert_eq!(reopened.retained_count().await.unwrap(), 1);
        assert_eq!(store.count_retained_calls(), 1);
    }

    #[tokio::test]
    async fn empty_retained_payload_deletes_message() {
        let retain = RetainHandle::default();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: Vec::new().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert!(retain
            .match_topic("t1", "devices/+/state")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn retain_count_tracks_replace_and_delete() {
        let retain = RetainHandle::default();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert_eq!(retain.retained_count().await.unwrap(), 1);

        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"still-up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert_eq!(retain.retained_count().await.unwrap(), 1);

        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: Vec::new().into(),
                qos: 1,
            })
            .await
            .unwrap();
        assert_eq!(retain.retained_count().await.unwrap(), 0);
    }
}
