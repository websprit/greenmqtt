use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{CompiledTopicFilter, Lifecycle, RetainedMessage, ServiceShardKey};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter};
use greenmqtt_kv_engine::KvMutation;
use greenmqtt_storage::RetainStore;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;

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
    async fn preview_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let scanned = self.list_tenant_retained(tenant_id).await?.len();
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned,
            removed: 0,
            refreshed: 0,
        })
    }
    async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        self.preview_tenant_gc(tenant_id).await
    }
    async fn expire_all_tenant(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let retained = self.list_tenant_retained(tenant_id).await?;
        let removed = retained.len();
        for message in retained {
            self.retain(RetainedMessage {
                tenant_id: message.tenant_id,
                topic: message.topic,
                payload: Vec::new().into(),
                qos: 0,
            })
            .await?;
        }
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned: removed,
            removed,
            refreshed: 0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainMaintenanceResult {
    pub tenant_id: String,
    pub scanned: usize,
    pub removed: usize,
    pub refreshed: usize,
}

pub async fn retain_expire_all(
    retain: &dyn RetainService,
    tenant_id: &str,
) -> anyhow::Result<RetainMaintenanceResult> {
    retain.expire_all_tenant(tenant_id).await
}

pub async fn retain_tenant_gc_preview(
    retain: &dyn RetainService,
    tenant_id: &str,
) -> anyhow::Result<RetainMaintenanceResult> {
    retain.preview_tenant_gc(tenant_id).await
}

pub async fn retain_tenant_gc_run(
    retain: &dyn RetainService,
    tenant_id: &str,
) -> anyhow::Result<RetainMaintenanceResult> {
    retain.run_tenant_gc(tenant_id).await
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

#[derive(Clone)]
pub struct ReplicatedRetainHandle {
    router: Arc<dyn KvRangeRouter>,
    executor: Arc<dyn KvRangeExecutor>,
    tenant_cache: Arc<RwLock<HashMap<String, Vec<RetainedMessage>>>>,
    filter_cache: Arc<RwLock<HashMap<(String, String), Vec<RetainedMessage>>>>,
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

impl ReplicatedRetainHandle {
    pub fn new(router: Arc<dyn KvRangeRouter>, executor: Arc<dyn KvRangeExecutor>) -> Self {
        Self {
            router,
            executor,
            tenant_cache: Arc::new(RwLock::new(HashMap::new())),
            filter_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn invalidate_tenant_cache(&self, tenant_id: &str) {
        self.tenant_cache
            .write()
            .expect("retain cache poisoned")
            .remove(tenant_id);
        self.filter_cache
            .write()
            .expect("retain cache poisoned")
            .retain(|(cached_tenant_id, _), _| cached_tenant_id != tenant_id);
    }

    pub fn cache_tenant_count(&self) -> usize {
        self.tenant_cache
            .read()
            .expect("retain cache poisoned")
            .len()
    }

    pub async fn warm_tenant_cache(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.invalidate_tenant_cache(tenant_id);
        Ok(self.list_tenant_retained(tenant_id).await?.len())
    }
}

fn topic_filter_is_exact(topic_filter: &str) -> bool {
    !topic_filter.contains('#') && !topic_filter.contains('+')
}

fn retained_message_key(topic: &str) -> Bytes {
    Bytes::copy_from_slice(topic.as_bytes())
}

fn encode_retained_message(message: &RetainedMessage) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(message)?))
}

fn decode_retained_message(value: &[u8]) -> anyhow::Result<RetainedMessage> {
    Ok(bincode::deserialize(value)?)
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

    async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let scanned = self.list_tenant_retained(tenant_id).await?.len();
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned,
            removed: 0,
            refreshed: scanned,
        })
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

    async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let refreshed = self.list_tenant_retained(tenant_id).await?.len();
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned: refreshed,
            removed: 0,
            refreshed,
        })
    }
}

#[async_trait]
impl RetainService for ReplicatedRetainHandle {
    async fn retain(&self, message: RetainedMessage) -> anyhow::Result<()> {
        let shard = retain_tenant_shard(&message.tenant_id);
        let route = self
            .router
            .route_key(&shard, message.topic.as_bytes())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no retain range available for tenant {}", message.tenant_id)
            })?;
        self.invalidate_tenant_cache(&message.tenant_id);
        self.executor
            .apply(
                &route.descriptor.id,
                vec![KvMutation {
                    key: retained_message_key(&message.topic),
                    value: if message.payload.is_empty() {
                        None
                    } else {
                        Some(encode_retained_message(&message)?)
                    },
                }],
            )
            .await?;
        Ok(())
    }

    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        if let Some(cached) = self
            .tenant_cache
            .read()
            .expect("retain cache poisoned")
            .get(tenant_id)
            .cloned()
        {
            return Ok(cached);
        }
        let shard = retain_tenant_shard(tenant_id);
        let routes = self.router.route_shard(&shard).await?;
        let mut retained = Vec::new();
        for route in routes {
            for (_, value) in self
                .executor
                .scan(&route.descriptor.id, None, usize::MAX)
                .await?
            {
                retained.push(decode_retained_message(&value)?);
            }
        }
        retained.sort_by(|left, right| left.topic.cmp(&right.topic));
        self.tenant_cache
            .write()
            .expect("retain cache poisoned")
            .insert(tenant_id.to_string(), retained.clone());
        Ok(retained)
    }

    async fn lookup_topic(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        let shard = retain_tenant_shard(tenant_id);
        let Some(route) = self.router.route_key(&shard, topic.as_bytes()).await? else {
            return Ok(None);
        };
        self.executor
            .get(&route.descriptor.id, topic.as_bytes())
            .await?
            .map(|value| decode_retained_message(&value))
            .transpose()
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
        if let Some(cached) = self
            .filter_cache
            .read()
            .expect("retain cache poisoned")
            .get(&(tenant_id.to_string(), topic_filter.to_string()))
            .cloned()
        {
            return Ok(cached);
        }
        let compiled = CompiledTopicFilter::new(topic_filter);
        let matched = self
            .list_tenant_retained(tenant_id)
            .await?
            .into_iter()
            .filter(|message| compiled.matches(&message.topic))
            .collect::<Vec<_>>();
        self.filter_cache
            .write()
            .expect("retain cache poisoned")
            .insert(
                (tenant_id.to_string(), topic_filter.to_string()),
                matched.clone(),
            );
        Ok(matched)
    }

    async fn retained_count(&self) -> anyhow::Result<usize> {
        let mut total = 0usize;
        for descriptor in self.router.list().await? {
            if descriptor.shard.kind == greenmqtt_core::ServiceShardKind::Retain {
                total += self
                    .executor
                    .scan(&descriptor.id, None, usize::MAX)
                    .await?
                    .len();
            }
        }
        Ok(total)
    }

    async fn preview_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let scanned = self.list_tenant_retained(tenant_id).await?.len();
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned,
            removed: 0,
            refreshed: 0,
        })
    }

    async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let refreshed = self.warm_tenant_cache(tenant_id).await?;
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned: refreshed,
            removed: 0,
            refreshed,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainTenantStats {
    pub tenant_id: String,
    pub retained_messages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainBalanceAction {
    SplitTenantRange {
        tenant_id: String,
    },
    ScaleTenantReplicas {
        tenant_id: String,
        voters: usize,
        learners: usize,
    },
    RunTenantCleanup {
        tenant_id: String,
    },
}

pub trait RetainBalancePolicy: Send + Sync {
    fn propose(&self, tenants: &[RetainTenantStats]) -> Vec<RetainBalanceAction>;
}

#[derive(Debug, Clone)]
pub struct ThresholdRetainBalancePolicy {
    pub max_retained_messages: usize,
    pub desired_voters: usize,
    pub desired_learners: usize,
}

impl RetainBalancePolicy for ThresholdRetainBalancePolicy {
    fn propose(&self, tenants: &[RetainTenantStats]) -> Vec<RetainBalanceAction> {
        let mut actions = Vec::new();
        for tenant in tenants {
            if tenant.retained_messages > self.max_retained_messages {
                actions.push(RetainBalanceAction::RunTenantCleanup {
                    tenant_id: tenant.tenant_id.clone(),
                });
                actions.push(RetainBalanceAction::SplitTenantRange {
                    tenant_id: tenant.tenant_id.clone(),
                });
                actions.push(RetainBalanceAction::ScaleTenantReplicas {
                    tenant_id: tenant.tenant_id.clone(),
                    voters: self.desired_voters,
                    learners: self.desired_learners,
                });
            }
        }
        actions
    }
}

#[async_trait::async_trait]
pub trait RetainMaintenance: Send + Sync {
    async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult>;
}

#[async_trait::async_trait]
impl RetainMaintenance for ReplicatedRetainHandle {
    async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        let refreshed = self.warm_tenant_cache(tenant_id).await?;
        Ok(RetainMaintenanceResult {
            tenant_id: tenant_id.to_string(),
            scanned: refreshed,
            removed: 0,
            refreshed,
        })
    }
}

#[derive(Clone)]
pub struct RetainMaintenanceWorker {
    maintenance: Arc<dyn RetainMaintenance>,
}

impl RetainMaintenanceWorker {
    pub fn new(maintenance: Arc<dyn RetainMaintenance>) -> Self {
        Self { maintenance }
    }

    pub async fn run_tenant_gc(&self, tenant_id: &str) -> anyhow::Result<RetainMaintenanceResult> {
        self.maintenance.run_tenant_gc(tenant_id).await
    }

    pub fn spawn_tenant_refresh(
        &self,
        tenant_id: String,
        interval: Duration,
    ) -> JoinHandle<anyhow::Result<()>> {
        let worker = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let _ = worker.run_tenant_gc(&tenant_id).await?;
            }
        })
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
        retain_expire_all, retain_tenant_gc_preview, retain_tenant_shard, retained_message_shard,
        PersistentRetainHandle, ReplicatedRetainHandle, RetainBalanceAction, RetainBalancePolicy,
        RetainHandle, RetainMaintenanceWorker, RetainService, RetainTenantStats,
        ThresholdRetainBalancePolicy,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        RetainedMessage, ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, MemoryKvRangeRouter};
    use greenmqtt_kv_engine::{
        KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, MemoryKvEngine,
    };
    use greenmqtt_storage::{MemoryRetainStore, RetainStore};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
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

    #[tokio::test]
    async fn replicated_retain_routes_exact_and_wildcard_access_over_kv_ranges() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-range-a".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"m".to_vec())),
            })
            .await
            .unwrap();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-range-b".into(),
                boundary: RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();

        let router = Arc::new(MemoryKvRangeRouter::default());
        let shard = retain_tenant_shard("t1");
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-range-a",
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
                "retain-range-b",
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

        let retain = ReplicatedRetainHandle::new(
            router,
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "alerts/door".into(),
                payload: b"open".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "metrics/cpu".into(),
                payload: b"high".to_vec().into(),
                qos: 0,
            })
            .await
            .unwrap();

        assert_eq!(
            retain
                .lookup_topic("t1", "alerts/door")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"open".as_slice()
        );
        assert_eq!(retain.match_topic("t1", "#").await.unwrap().len(), 2);
        assert_eq!(retain.retained_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn replicated_retain_reapplying_same_topic_keeps_single_record() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-range-idem".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-range-idem",
                retain_tenant_shard("t1"),
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
        let retain = ReplicatedRetainHandle::new(
            router,
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );

        let message = RetainedMessage {
            tenant_id: "t1".into(),
            topic: "devices/a/state".into(),
            payload: b"up".to_vec().into(),
            qos: 1,
        };
        retain.retain(message.clone()).await.unwrap();
        retain.retain(message).await.unwrap();

        assert_eq!(retain.retained_count().await.unwrap(), 1);
        assert_eq!(retain.match_topic("t1", "#").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn replicated_retain_survives_range_split_and_merge() {
        let engine = MemoryKvEngine::default();
        let router = Arc::new(MemoryKvRangeRouter::default());
        let shard = retain_tenant_shard("t1");
        for descriptor in [
            ReplicatedRangeDescriptor::new(
                "retain-left",
                shard.clone(),
                RangeBoundary::new(None, Some(b"m".to_vec())),
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
            ),
            ReplicatedRangeDescriptor::new(
                "retain-right",
                shard.clone(),
                RangeBoundary::new(Some(b"m".to_vec()), None),
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
            ),
        ] {
            engine
                .bootstrap(KvRangeBootstrap {
                    range_id: descriptor.id.clone(),
                    boundary: descriptor.boundary.clone(),
                })
                .await
                .unwrap();
            router.upsert(descriptor).await.unwrap();
        }

        let retain = ReplicatedRetainHandle::new(
            router.clone(),
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );

        for (topic, payload, qos) in [
            ("alerts/door", b"open".as_slice(), 1),
            ("metrics/cpu", b"high".as_slice(), 0),
        ] {
            retain
                .retain(RetainedMessage {
                    tenant_id: "t1".into(),
                    topic: topic.into(),
                    payload: payload.to_vec().into(),
                    qos,
                })
                .await
                .unwrap();
        }

        assert_eq!(
            retain
                .lookup_topic("t1", "alerts/door")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"open".as_slice()
        );
        assert_eq!(
            retain
                .lookup_topic("t1", "metrics/cpu")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"high".as_slice()
        );

        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-merged".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let left = engine.open_range("retain-left").await.unwrap();
        let right = engine.open_range("retain-right").await.unwrap();
        let merged = engine.open_range("retain-merged").await.unwrap();
        let mut merged_mutations = Vec::new();
        merged_mutations.extend(
            left.reader()
                .scan(&RangeBoundary::full(), usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .map(|(key, value)| KvMutation {
                    key,
                    value: Some(value),
                }),
        );
        merged_mutations.extend(
            right
                .reader()
                .scan(&RangeBoundary::full(), usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .map(|(key, value)| KvMutation {
                    key,
                    value: Some(value),
                }),
        );
        merged.writer().apply(merged_mutations).await.unwrap();

        router.remove("retain-left").await.unwrap();
        router.remove("retain-right").await.unwrap();
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-merged",
                shard,
                RangeBoundary::full(),
                2,
                2,
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

        assert_eq!(retain.match_topic("t1", "#").await.unwrap().len(), 2);
        assert_eq!(
            retain
                .lookup_topic("t1", "alerts/door")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"open".as_slice()
        );
        assert_eq!(
            retain
                .lookup_topic("t1", "metrics/cpu")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"high".as_slice()
        );
    }

    #[tokio::test]
    async fn replicated_retain_can_read_from_restored_snapshot_range() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-source".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();

        let router = Arc::new(MemoryKvRangeRouter::default());
        let shard = retain_tenant_shard("t1");
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-source",
                shard.clone(),
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

        let retain = ReplicatedRetainHandle::new(
            router.clone(),
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "devices/a/state".into(),
                payload: b"restored".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let source = engine.open_range("retain-source").await.unwrap();
        let snapshot = source.snapshot().await.unwrap();
        drop(source);
        engine
            .restore_snapshot(
                KvRangeBootstrap {
                    range_id: "retain-restored".into(),
                    boundary: RangeBoundary::full(),
                },
                &snapshot,
            )
            .await
            .unwrap();
        router.remove("retain-source").await.unwrap();
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-restored",
                shard,
                RangeBoundary::full(),
                2,
                2,
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

        assert_eq!(
            retain
                .lookup_topic("t1", "devices/a/state")
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"restored".as_slice()
        );
    }

    #[tokio::test]
    async fn replicated_retain_caches_tenant_and_filter_matches() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-range-cache".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-range-cache",
                retain_tenant_shard("t1"),
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
        let retain = ReplicatedRetainHandle::new(
            router,
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );
        for topic in ["alerts/a", "alerts/b"] {
            retain
                .retain(RetainedMessage {
                    tenant_id: "t1".into(),
                    topic: topic.into(),
                    payload: b"up".to_vec().into(),
                    qos: 1,
                })
                .await
                .unwrap();
        }

        assert_eq!(retain.match_topic("t1", "alerts/#").await.unwrap().len(), 2);
        assert_eq!(retain.cache_tenant_count(), 1);
        assert_eq!(retain.match_topic("t1", "alerts/#").await.unwrap().len(), 2);
    }

    #[test]
    fn threshold_retain_balance_policy_proposes_actions_for_hot_tenant() {
        let policy = ThresholdRetainBalancePolicy {
            max_retained_messages: 8,
            desired_voters: 3,
            desired_learners: 1,
        };
        let actions = policy.propose(&[
            RetainTenantStats {
                tenant_id: "cool".into(),
                retained_messages: 2,
            },
            RetainTenantStats {
                tenant_id: "hot".into(),
                retained_messages: 12,
            },
        ]);
        assert_eq!(
            actions,
            vec![
                RetainBalanceAction::RunTenantCleanup {
                    tenant_id: "hot".into(),
                },
                RetainBalanceAction::SplitTenantRange {
                    tenant_id: "hot".into(),
                },
                RetainBalanceAction::ScaleTenantReplicas {
                    tenant_id: "hot".into(),
                    voters: 3,
                    learners: 1,
                },
            ]
        );
    }

    #[tokio::test]
    async fn retain_maintenance_worker_refreshes_tenant_cache() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "retain-range-maint".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "retain-range-maint",
                retain_tenant_shard("t1"),
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
        let retain = Arc::new(ReplicatedRetainHandle::new(
            router,
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        ));
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "alerts/a".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let worker = RetainMaintenanceWorker::new(retain.clone());
        let result = worker.run_tenant_gc("t1").await.unwrap();
        assert_eq!(result.scanned, 1);
        assert_eq!(result.refreshed, 1);
        let task = worker.spawn_tenant_refresh("t1".into(), Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(3)).await;
        task.abort();
    }

    #[tokio::test]
    async fn retain_gc_preview_does_not_mutate_state_and_expire_all_clears_tenant() {
        let retain = RetainHandle::default();
        retain
            .retain(RetainedMessage {
                tenant_id: "t1".into(),
                topic: "alerts/a".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
            })
            .await
            .unwrap();

        let preview = retain_tenant_gc_preview(&retain, "t1").await.unwrap();
        assert_eq!(preview.scanned, 1);
        assert_eq!(preview.removed, 0);
        assert_eq!(retain.list_tenant_retained("t1").await.unwrap().len(), 1);

        let expired = retain_expire_all(&retain, "t1").await.unwrap();
        assert_eq!(expired.removed, 1);
        assert!(retain.list_tenant_retained("t1").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn retain_large_wildcard_fanout_remains_correct() {
        let retain = RetainHandle::default();
        for index in 0..256u32 {
            retain
                .retain(RetainedMessage {
                    tenant_id: "t-load".into(),
                    topic: format!("devices/{index}/state"),
                    payload: format!("p-{index}").into_bytes().into(),
                    qos: 1,
                })
                .await
                .unwrap();
        }
        let matched = retain
            .match_topic("t-load", "devices/+/state")
            .await
            .unwrap();
        assert_eq!(matched.len(), 256);
    }
}
