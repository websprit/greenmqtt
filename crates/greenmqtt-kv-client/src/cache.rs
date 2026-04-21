use crate::router::{KvRangeRouter, RangeRoute};
use async_trait::async_trait;
use greenmqtt_core::{ReplicatedRangeDescriptor, ServiceShardKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteCacheConfig {
    pub ttl: Duration,
}

impl Default for RouteCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(5),
        }
    }
}

#[derive(Clone)]
pub struct CachedKvRangeRouter {
    inner: Arc<dyn KvRangeRouter>,
    config: RouteCacheConfig,
    by_id: Arc<RwLock<HashMap<String, TimedRouteEntry<Option<RangeRoute>>>>>,
    by_key: Arc<RwLock<HashMap<RouteKeyCacheKey, TimedRouteEntry<Option<RangeRoute>>>>>,
    by_shard: Arc<RwLock<HashMap<ServiceShardKey, TimedRouteEntry<Vec<RangeRoute>>>>>,
}

#[derive(Debug, Clone)]
struct TimedRouteEntry<T> {
    loaded_at: Instant,
    value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteKeyCacheKey {
    shard: ServiceShardKey,
    key: Vec<u8>,
}

impl Hash for RouteKeyCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.shard.hash(state);
        self.key.hash(state);
    }
}

impl CachedKvRangeRouter {
    pub fn new(inner: Arc<dyn KvRangeRouter>, config: RouteCacheConfig) -> Self {
        Self {
            inner,
            config,
            by_id: Arc::new(RwLock::new(HashMap::new())),
            by_key: Arc::new(RwLock::new(HashMap::new())),
            by_shard: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn fresh<T>(&self, entry: &TimedRouteEntry<T>) -> bool {
        entry.loaded_at.elapsed() <= self.config.ttl
    }

    fn clear_all(&self) {
        self.by_id.write().expect("route cache poisoned").clear();
        self.by_key.write().expect("route cache poisoned").clear();
        self.by_shard
            .write()
            .expect("route cache poisoned")
            .clear();
    }
}

#[async_trait]
impl KvRangeRouter for CachedKvRangeRouter {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        self.inner.upsert(descriptor).await?;
        self.clear_all();
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let removed = self.inner.remove(range_id).await?;
        self.clear_all();
        Ok(removed)
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.inner.list().await
    }

    async fn by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        if let Some(entry) = self
            .by_id
            .read()
            .expect("route cache poisoned")
            .get(range_id)
            .cloned()
            .filter(|entry| self.fresh(entry))
        {
            return Ok(entry.value);
        }
        let value = self.inner.by_id(range_id).await?;
        self.by_id.write().expect("route cache poisoned").insert(
            range_id.to_string(),
            TimedRouteEntry {
                loaded_at: Instant::now(),
                value: value.clone(),
            },
        );
        Ok(value)
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        let cache_key = RouteKeyCacheKey {
            shard: shard.clone(),
            key: key.to_vec(),
        };
        if let Some(entry) = self
            .by_key
            .read()
            .expect("route cache poisoned")
            .get(&cache_key)
            .cloned()
            .filter(|entry| self.fresh(entry))
        {
            return Ok(entry.value);
        }
        let value = self.inner.route_key(shard, key).await?;
        self.by_key.write().expect("route cache poisoned").insert(
            cache_key,
            TimedRouteEntry {
                loaded_at: Instant::now(),
                value: value.clone(),
            },
        );
        Ok(value)
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        if let Some(entry) = self
            .by_shard
            .read()
            .expect("route cache poisoned")
            .get(shard)
            .cloned()
            .filter(|entry| self.fresh(entry))
        {
            return Ok(entry.value);
        }
        let value = self.inner.route_shard(shard).await?;
        self.by_shard.write().expect("route cache poisoned").insert(
            shard.clone(),
            TimedRouteEntry {
                loaded_at: Instant::now(),
                value: value.clone(),
            },
        );
        Ok(value)
    }

    async fn invalidate_range(&self, _range_id: &str) -> anyhow::Result<()> {
        self.clear_all();
        Ok(())
    }

    async fn invalidate_key(&self, _shard: &ServiceShardKey, _key: &[u8]) -> anyhow::Result<()> {
        self.clear_all();
        Ok(())
    }

    async fn invalidate_shard(&self, _shard: &ServiceShardKey) -> anyhow::Result<()> {
        self.clear_all();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedKvRangeRouter, RouteCacheConfig};
    use crate::router::{KvRangeRouter, MemoryKvRangeRouter};
    use async_trait::async_trait;
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Default)]
    struct CountingRouter {
        inner: MemoryKvRangeRouter,
        by_id_calls: AtomicUsize,
    }

    #[async_trait]
    impl KvRangeRouter for CountingRouter {
        async fn upsert(
            &self,
            descriptor: ReplicatedRangeDescriptor,
        ) -> anyhow::Result<()> {
            self.inner.upsert(descriptor).await
        }

        async fn remove(
            &self,
            range_id: &str,
        ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
            self.inner.remove(range_id).await
        }

        async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
            self.inner.list().await
        }

        async fn by_id(
            &self,
            range_id: &str,
        ) -> anyhow::Result<Option<crate::RangeRoute>> {
            self.by_id_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.by_id(range_id).await
        }

        async fn route_key(
            &self,
            shard: &greenmqtt_core::ServiceShardKey,
            key: &[u8],
        ) -> anyhow::Result<Option<crate::RangeRoute>> {
            self.inner.route_key(shard, key).await
        }

        async fn route_shard(
            &self,
            shard: &greenmqtt_core::ServiceShardKey,
        ) -> anyhow::Result<Vec<crate::RangeRoute>> {
            self.inner.route_shard(shard).await
        }
    }

    fn descriptor(id: &str) -> ReplicatedRangeDescriptor {
        ReplicatedRangeDescriptor::new(
            id,
            ServiceShardKey::retain("t1"),
            RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
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
        )
    }

    #[tokio::test]
    async fn cached_router_reuses_fresh_by_id_results_and_refreshes_after_invalidate() {
        let inner = Arc::new(CountingRouter::default());
        inner.upsert(descriptor("range-1")).await.unwrap();
        let router = CachedKvRangeRouter::new(
            inner.clone(),
            RouteCacheConfig {
                ttl: Duration::from_secs(60),
            },
        );

        let first = router.by_id("range-1").await.unwrap();
        let second = router.by_id("range-1").await.unwrap();
        assert!(first.is_some());
        assert!(second.is_some());
        assert_eq!(inner.by_id_calls.load(Ordering::SeqCst), 1);

        router.invalidate_range("range-1").await.unwrap();
        let third = router.by_id("range-1").await.unwrap();
        assert!(third.is_some());
        assert_eq!(inner.by_id_calls.load(Ordering::SeqCst), 2);
    }
}
