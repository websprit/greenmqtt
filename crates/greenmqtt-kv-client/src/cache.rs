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
