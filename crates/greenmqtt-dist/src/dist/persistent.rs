use crate::dist::{
    exact_topic_loaded, insert_tenant_route, remove_tenant_route, retain_tenant_routes,
    rewrite_tenant_route_node, session_topic_shared_identity, shared_group_identity,
    tenant_filter_shared_identity, DistRouter, TenantRoutes,
};
use crate::trie::{select_routes, tenant_routes_from_vec};
use async_trait::async_trait;
use greenmqtt_core::{Lifecycle, RouteRecord, TopicName};
use greenmqtt_storage::RouteStore;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct PersistentDistHandle {
    pub(crate) store: Arc<dyn RouteStore>,
    pub(crate) inner: Arc<RwLock<HashMap<String, TenantRoutes>>>,
    pub(crate) session_topic_shared_cache: Arc<RwLock<SessionTopicSharedCache>>,
    pub(crate) session_routes_cache: Arc<RwLock<HashMap<String, Vec<RouteRecord>>>>,
    pub(crate) loaded_session_ids: Arc<RwLock<HashSet<String>>>,
    pub(crate) session_topic_cache: Arc<RwLock<SessionTopicCache>>,
    pub(crate) loaded_session_topics: Arc<RwLock<HashSet<(String, String)>>>,
}

type SessionTopicSharedCache = HashMap<(String, String, String), Option<RouteRecord>>;
type SessionTopicCache = HashMap<(String, String), Vec<RouteRecord>>;

impl PersistentDistHandle {
    pub async fn open(store: Arc<dyn RouteStore>) -> anyhow::Result<Self> {
        Ok(Self {
            store,
            inner: Arc::new(RwLock::new(HashMap::new())),
            session_topic_shared_cache: Arc::new(RwLock::new(HashMap::new())),
            session_routes_cache: Arc::new(RwLock::new(HashMap::new())),
            loaded_session_ids: Arc::new(RwLock::new(HashSet::new())),
            session_topic_cache: Arc::new(RwLock::new(HashMap::new())),
            loaded_session_topics: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    fn clear_session_route_caches(&self) {
        self.session_topic_shared_cache
            .write()
            .expect("dist poisoned")
            .clear();
        self.session_routes_cache
            .write()
            .expect("dist poisoned")
            .clear();
        self.loaded_session_ids
            .write()
            .expect("dist poisoned")
            .clear();
        self.session_topic_cache
            .write()
            .expect("dist poisoned")
            .clear();
        self.loaded_session_topics
            .write()
            .expect("dist poisoned")
            .clear();
    }
}

#[async_trait]
impl DistRouter for PersistentDistHandle {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        self.store.save_route(&route).await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        insert_tenant_route(guard.entry(route.tenant_id.clone()).or_default(), route);
        Ok(())
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        self.store.delete_route(route).await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        if let Some(routes) = guard.get_mut(&route.tenant_id) {
            remove_tenant_route(routes, route);
            if routes.all.is_empty() {
                guard.remove(&route.tenant_id);
            }
        }
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let removed = self.store.remove_session_routes(session_id).await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        let mut empty_tenants = Vec::new();
        for (tenant_id, routes) in guard.iter_mut() {
            retain_tenant_routes(routes, |route| route.session_id != session_id);
            if routes.all.is_empty() {
                empty_tenants.push(tenant_id.clone());
            }
        }
        for tenant_id in empty_tenants {
            guard.remove(&tenant_id);
        }
        Ok(removed)
    }

    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let removed = self
            .store
            .remove_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        if let Some(routes) = guard.get_mut(tenant_id) {
            retain_tenant_routes(routes, |route| {
                !(route.topic_filter == topic_filter
                    && route.shared_group.as_deref() == shared_group)
            });
            if routes.all.is_empty() {
                guard.remove(tenant_id);
            }
        }
        Ok(removed)
    }

    async fn lookup_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        let key = session_topic_shared_identity(session_id, topic_filter, shared_group);
        if let Some(route) = self
            .session_topic_shared_cache
            .read()
            .expect("dist poisoned")
            .get(&key)
            .cloned()
        {
            return Ok(route);
        }
        let session_topic_key = (session_id.to_string(), topic_filter.to_string());
        if self
            .loaded_session_topics
            .read()
            .expect("dist poisoned")
            .contains(&session_topic_key)
        {
            let route = self
                .session_topic_cache
                .read()
                .expect("dist poisoned")
                .get(&session_topic_key)
                .and_then(|routes| {
                    routes
                        .iter()
                        .find(|route| route.shared_group.as_deref() == shared_group)
                        .cloned()
                });
            self.session_topic_shared_cache
                .write()
                .expect("dist poisoned")
                .insert(key, route.clone());
            return Ok(route);
        }
        if self
            .loaded_session_ids
            .read()
            .expect("dist poisoned")
            .contains(session_id)
        {
            let route = self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .and_then(|routes| {
                    routes
                        .iter()
                        .find(|route| {
                            route.topic_filter == topic_filter
                                && route.shared_group.as_deref() == shared_group
                        })
                        .cloned()
                });
            self.session_topic_shared_cache
                .write()
                .expect("dist poisoned")
                .insert(key, route.clone());
            return Ok(route);
        }
        let route = self
            .store
            .load_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await?;
        self.session_topic_shared_cache
            .write()
            .expect("dist poisoned")
            .insert(key, route.clone());
        Ok(route)
    }

    async fn remove_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let removed = self
            .store
            .remove_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await?;
        if removed == 0 {
            return Ok(0);
        }
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        let mut empty_tenants = Vec::new();
        for (tenant_id, routes) in guard.iter_mut() {
            retain_tenant_routes(routes, |route| {
                !(route.session_id == session_id
                    && route.topic_filter == topic_filter
                    && route.shared_group.as_deref() == shared_group)
            });
            if routes.all.is_empty() {
                empty_tenants.push(tenant_id.clone());
            }
        }
        for tenant_id in empty_tenants {
            guard.remove(&tenant_id);
        }
        Ok(removed)
    }

    async fn remove_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let removed = self
            .store
            .remove_session_topic_filter_routes(session_id, topic_filter)
            .await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        let mut empty_tenants = Vec::new();
        for (tenant_id, routes) in guard.iter_mut() {
            retain_tenant_routes(routes, |route| {
                !(route.session_id == session_id && route.topic_filter == topic_filter)
            });
            if routes.all.is_empty() {
                empty_tenants.push(tenant_id.clone());
            }
        }
        for tenant_id in empty_tenants {
            guard.remove(&tenant_id);
        }
        Ok(removed)
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let removed = self.store.remove_tenant_routes(tenant_id).await?;
        self.clear_session_route_caches();
        self.inner.write().expect("dist poisoned").remove(tenant_id);
        Ok(removed)
    }

    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let removed = self
            .store
            .remove_tenant_shared_routes(tenant_id, shared_group)
            .await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        if let Some(routes) = guard.get_mut(tenant_id) {
            retain_tenant_routes(routes, |route| {
                route.shared_group.as_deref() != shared_group
            });
            if routes.all.is_empty() {
                guard.remove(tenant_id);
            }
        }
        Ok(removed)
    }

    async fn remove_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let removed = self
            .store
            .remove_topic_filter_routes(tenant_id, topic_filter)
            .await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        if let Some(routes) = guard.get_mut(tenant_id) {
            retain_tenant_routes(routes, |route| route.topic_filter != topic_filter);
            if routes.all.is_empty() {
                guard.remove(tenant_id);
            }
        }
        Ok(removed)
    }

    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        let updated = self
            .store
            .reassign_session_routes(session_id, node_id)
            .await?;
        self.clear_session_route_caches();
        let mut guard = self.inner.write().expect("dist poisoned");
        for tenant_routes in guard.values_mut() {
            let routes = tenant_routes
                .all
                .iter()
                .filter(|route| route.session_id == session_id)
                .cloned()
                .collect::<Vec<_>>();
            for route in &routes {
                rewrite_tenant_route_node(tenant_routes, route, node_id);
            }
        }
        Ok(updated)
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        if self
            .loaded_session_ids
            .read()
            .expect("dist poisoned")
            .contains(session_id)
        {
            let mut routes = self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .cloned()
                .unwrap_or_default();
            routes.sort_by(|left, right| {
                left.tenant_id
                    .cmp(&right.tenant_id)
                    .then(left.topic_filter.cmp(&right.topic_filter))
                    .then(left.session_id.cmp(&right.session_id))
            });
            return Ok(routes);
        }
        let mut routes = self.store.list_session_routes(session_id).await?;
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        self.session_routes_cache
            .write()
            .expect("dist poisoned")
            .insert(session_id.to_string(), routes.clone());
        self.loaded_session_ids
            .write()
            .expect("dist poisoned")
            .insert(session_id.to_string());
        Ok(routes)
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let key = (session_id.to_string(), topic_filter.to_string());
        if self
            .loaded_session_topics
            .read()
            .expect("dist poisoned")
            .contains(&key)
        {
            let mut routes = self
                .session_topic_cache
                .read()
                .expect("dist poisoned")
                .get(&key)
                .cloned()
                .unwrap_or_default();
            routes.sort_by(|left, right| {
                left.tenant_id
                    .cmp(&right.tenant_id)
                    .then(left.topic_filter.cmp(&right.topic_filter))
                    .then(left.session_id.cmp(&right.session_id))
            });
            return Ok(routes);
        }
        if self
            .loaded_session_ids
            .read()
            .expect("dist poisoned")
            .contains(session_id)
        {
            let mut routes = self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .into_iter()
                .flat_map(|routes| routes.iter())
                .filter(|route| route.topic_filter == topic_filter)
                .cloned()
                .collect::<Vec<_>>();
            self.session_topic_cache
                .write()
                .expect("dist poisoned")
                .insert(key.clone(), routes.clone());
            self.loaded_session_topics
                .write()
                .expect("dist poisoned")
                .insert(key);
            routes.sort_by(|left, right| {
                left.tenant_id
                    .cmp(&right.tenant_id)
                    .then(left.topic_filter.cmp(&right.topic_filter))
                    .then(left.session_id.cmp(&right.session_id))
            });
            return Ok(routes);
        }
        let mut routes = self
            .store
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await?;
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        self.session_topic_cache
            .write()
            .expect("dist poisoned")
            .insert(key.clone(), routes.clone());
        self.loaded_session_topics
            .write()
            .expect("dist poisoned")
            .insert(key);
        Ok(routes)
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        if self
            .loaded_session_ids
            .read()
            .expect("dist poisoned")
            .contains(session_id)
        {
            return Ok(self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .map(|routes| routes.len())
                .unwrap_or(0));
        }
        self.store.count_session_routes(session_id).await
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let key = (session_id.to_string(), topic_filter.to_string());
        if self
            .loaded_session_topics
            .read()
            .expect("dist poisoned")
            .contains(&key)
        {
            return Ok(self
                .session_topic_cache
                .read()
                .expect("dist poisoned")
                .get(&key)
                .map(|routes| routes.len())
                .unwrap_or(0));
        }
        if self
            .loaded_session_ids
            .read()
            .expect("dist poisoned")
            .contains(session_id)
        {
            let routes = self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .map(|routes| {
                    routes
                        .iter()
                        .filter(|route| route.topic_filter == topic_filter)
                        .count()
                })
                .unwrap_or(0);
            let filtered = self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .into_iter()
                .flat_map(|routes| routes.iter())
                .filter(|route| route.topic_filter == topic_filter)
                .cloned()
                .collect::<Vec<_>>();
            self.session_topic_cache
                .write()
                .expect("dist poisoned")
                .insert(key.clone(), filtered);
            self.loaded_session_topics
                .write()
                .expect("dist poisoned")
                .insert(key);
            return Ok(routes);
        }
        self.store
            .count_session_topic_filter_routes(session_id, topic_filter)
            .await
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let key = session_topic_shared_identity(session_id, topic_filter, shared_group);
        if let Some(route) = self
            .session_topic_shared_cache
            .read()
            .expect("dist poisoned")
            .get(&key)
            .cloned()
        {
            return Ok(usize::from(route.is_some()));
        }
        let session_topic_key = (session_id.to_string(), topic_filter.to_string());
        if self
            .loaded_session_topics
            .read()
            .expect("dist poisoned")
            .contains(&session_topic_key)
        {
            let route = self
                .session_topic_cache
                .read()
                .expect("dist poisoned")
                .get(&session_topic_key)
                .and_then(|routes| {
                    routes
                        .iter()
                        .find(|route| route.shared_group.as_deref() == shared_group)
                        .cloned()
                });
            self.session_topic_shared_cache
                .write()
                .expect("dist poisoned")
                .insert(key, route.clone());
            return Ok(usize::from(route.is_some()));
        }
        if self
            .loaded_session_ids
            .read()
            .expect("dist poisoned")
            .contains(session_id)
        {
            let route = self
                .session_routes_cache
                .read()
                .expect("dist poisoned")
                .get(session_id)
                .and_then(|routes| {
                    routes
                        .iter()
                        .find(|route| {
                            route.topic_filter == topic_filter
                                && route.shared_group.as_deref() == shared_group
                        })
                        .cloned()
                });
            self.session_topic_shared_cache
                .write()
                .expect("dist poisoned")
                .insert(key, route.clone());
            return Ok(usize::from(route.is_some()));
        }
        self.store
            .count_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.store.count_tenant_routes(tenant_id).await
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        if let Some(cached) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cached.fully_loaded {
                let shared_key = shared_group_identity(shared_group);
                let mut routes = cached
                    .by_shared
                    .get(&shared_key)
                    .cloned()
                    .unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
            let shared_key = shared_group_identity(shared_group);
            if cached.loaded_shared_groups.contains(&shared_key) {
                let mut routes = cached
                    .by_shared
                    .get(&shared_key)
                    .cloned()
                    .unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
        }
        let mut routes = self
            .store
            .list_tenant_shared_routes(tenant_id, shared_group)
            .await?;
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        {
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            if !tenant_routes.fully_loaded {
                for route in routes.iter().cloned() {
                    insert_tenant_route(tenant_routes, route);
                }
                tenant_routes
                    .loaded_shared_groups
                    .insert(shared_group_identity(shared_group));
            }
        }
        Ok(routes)
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        if let Some(cached) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cached.fully_loaded {
                return Ok(cached
                    .by_shared
                    .get(&shared_group_identity(shared_group))
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
            let shared_key = shared_group_identity(shared_group);
            if cached.loaded_shared_groups.contains(&shared_key) {
                return Ok(cached
                    .by_shared
                    .get(&shared_key)
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
        }
        self.store
            .count_tenant_shared_routes(tenant_id, shared_group)
            .await
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut populate_from_filter: Option<Vec<RouteRecord>> = None;
        let mut populate_from_shared: Option<Vec<RouteRecord>> = None;
        if let Some(cached) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cached.fully_loaded {
                let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
                let mut routes = cached
                    .by_filter_shared
                    .get(&filter_shared_key)
                    .cloned()
                    .unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
            let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
            if cached.loaded_filter_shared.contains(&filter_shared_key) {
                let mut routes = cached
                    .by_filter_shared
                    .get(&filter_shared_key)
                    .cloned()
                    .unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
            if cached.loaded_filters.contains(topic_filter) {
                let mut routes = cached
                    .by_filter
                    .get(topic_filter)
                    .into_iter()
                    .flat_map(|routes| routes.iter())
                    .filter(|route| route.shared_group.as_deref() == shared_group)
                    .cloned()
                    .collect::<Vec<_>>();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                populate_from_filter = Some(routes.clone());
            } else {
                let shared_key = shared_group_identity(shared_group);
                if cached.loaded_shared_groups.contains(&shared_key) {
                    let mut routes = cached
                        .by_shared
                        .get(&shared_key)
                        .into_iter()
                        .flat_map(|routes| routes.iter())
                        .filter(|route| route.topic_filter == topic_filter)
                        .cloned()
                        .collect::<Vec<_>>();
                    routes.sort_by(|left, right| {
                        left.tenant_id
                            .cmp(&right.tenant_id)
                            .then(left.topic_filter.cmp(&right.topic_filter))
                            .then(left.session_id.cmp(&right.session_id))
                    });
                    populate_from_shared = Some(routes.clone());
                }
            }
        }
        if let Some(routes) = populate_from_filter {
            let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            tenant_routes
                .by_filter_shared
                .insert(filter_shared_key.clone(), routes.clone());
            tenant_routes.loaded_filter_shared.insert(filter_shared_key);
            return Ok(routes);
        }
        if let Some(routes) = populate_from_shared {
            let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            tenant_routes
                .by_filter_shared
                .insert(filter_shared_key.clone(), routes.clone());
            tenant_routes.loaded_filter_shared.insert(filter_shared_key);
            return Ok(routes);
        }
        let mut routes = self
            .store
            .list_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await?;
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        {
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            if !tenant_routes.fully_loaded {
                for route in routes.iter().cloned() {
                    insert_tenant_route(tenant_routes, route);
                }
                tenant_routes
                    .loaded_filter_shared
                    .insert(tenant_filter_shared_identity(topic_filter, shared_group));
            }
        }
        Ok(routes)
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        if let Some(cache) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cache.fully_loaded || (cache.wildcards_loaded && exact_topic_loaded(&cache, topic)) {
                return Ok(select_routes(&cache, topic));
            }
        }
        let existing = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned();
        let need_wildcards = existing
            .as_ref()
            .map(|cache| !cache.fully_loaded && !cache.wildcards_loaded)
            .unwrap_or(true);
        let need_exact = existing
            .as_ref()
            .map(|cache| !exact_topic_loaded(cache, topic.as_str()))
            .unwrap_or(true);
        let wildcard_routes = if need_wildcards {
            self.store.list_tenant_wildcard_routes(tenant_id).await?
        } else {
            Vec::new()
        };
        let exact_routes = if need_exact {
            self.store
                .list_exact_routes(tenant_id, topic.as_str())
                .await?
        } else {
            Vec::new()
        };
        let mut guard = self.inner.write().expect("dist poisoned");
        let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
        if tenant_routes.fully_loaded {
            return Ok(select_routes(tenant_routes, topic));
        }
        if need_wildcards {
            for route in wildcard_routes {
                insert_tenant_route(tenant_routes, route);
            }
            tenant_routes.wildcards_loaded = true;
        }
        if need_exact {
            for route in exact_routes {
                insert_tenant_route(tenant_routes, route);
            }
            tenant_routes
                .loaded_exact_topics
                .insert(topic.as_str().to_string());
        }
        Ok(select_routes(tenant_routes, topic))
    }

    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.list_topic_filter_routes(tenant_id, topic_filter).await
    }

    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        if let Some(cached) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cached.fully_loaded {
                let mut routes = cached
                    .by_filter
                    .get(topic_filter)
                    .cloned()
                    .unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
            if !topic_filter.contains('#')
                && !topic_filter.contains('+')
                && cached.loaded_exact_topics.contains(topic_filter)
            {
                let mut routes = cached.exact.get(topic_filter).cloned().unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
            if cached.loaded_filters.contains(topic_filter) {
                let mut routes = cached
                    .by_filter
                    .get(topic_filter)
                    .cloned()
                    .unwrap_or_default();
                routes.sort_by(|left, right| {
                    left.tenant_id
                        .cmp(&right.tenant_id)
                        .then(left.topic_filter.cmp(&right.topic_filter))
                        .then(left.session_id.cmp(&right.session_id))
                });
                return Ok(routes);
            }
        }
        let mut routes = self
            .store
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await?;
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        {
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            if !tenant_routes.fully_loaded {
                for route in routes.iter().cloned() {
                    insert_tenant_route(tenant_routes, route);
                }
                tenant_routes
                    .loaded_filters
                    .insert(topic_filter.to_string());
                if !topic_filter.contains('#') && !topic_filter.contains('+') {
                    tenant_routes
                        .loaded_exact_topics
                        .insert(topic_filter.to_string());
                }
            }
        }
        Ok(routes)
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        if let Some(cached) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cached.fully_loaded {
                return Ok(cached
                    .by_filter
                    .get(topic_filter)
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
            if !topic_filter.contains('#')
                && !topic_filter.contains('+')
                && cached.loaded_exact_topics.contains(topic_filter)
            {
                return Ok(cached
                    .exact
                    .get(topic_filter)
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
            if cached.loaded_filters.contains(topic_filter) {
                return Ok(cached
                    .by_filter
                    .get(topic_filter)
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
        }
        self.store
            .count_topic_filter_routes(tenant_id, topic_filter)
            .await
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut populate_from_filter: Option<(usize, Vec<RouteRecord>)> = None;
        let mut populate_from_shared: Option<(usize, Vec<RouteRecord>)> = None;
        if let Some(cached) = self
            .inner
            .read()
            .expect("dist poisoned")
            .get(tenant_id)
            .cloned()
        {
            if cached.fully_loaded {
                return Ok(cached
                    .by_filter_shared
                    .get(&tenant_filter_shared_identity(topic_filter, shared_group))
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
            let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
            if cached.loaded_filter_shared.contains(&filter_shared_key) {
                return Ok(cached
                    .by_filter_shared
                    .get(&filter_shared_key)
                    .map(|routes| routes.len())
                    .unwrap_or(0));
            }
            if cached.loaded_filters.contains(topic_filter) {
                let count = cached
                    .by_filter
                    .get(topic_filter)
                    .map(|routes| {
                        routes
                            .iter()
                            .filter(|route| route.shared_group.as_deref() == shared_group)
                            .count()
                    })
                    .unwrap_or(0);
                let filtered = cached
                    .by_filter
                    .get(topic_filter)
                    .into_iter()
                    .flat_map(|routes| routes.iter())
                    .filter(|route| route.shared_group.as_deref() == shared_group)
                    .cloned()
                    .collect::<Vec<_>>();
                populate_from_filter = Some((count, filtered));
            } else {
                let shared_key = shared_group_identity(shared_group);
                if cached.loaded_shared_groups.contains(&shared_key) {
                    let filtered = cached
                        .by_shared
                        .get(&shared_key)
                        .into_iter()
                        .flat_map(|routes| routes.iter())
                        .filter(|route| route.topic_filter == topic_filter)
                        .cloned()
                        .collect::<Vec<_>>();
                    populate_from_shared = Some((filtered.len(), filtered));
                }
            }
        }
        if let Some((count, filtered)) = populate_from_filter {
            let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            tenant_routes
                .by_filter_shared
                .insert(filter_shared_key.clone(), filtered);
            tenant_routes.loaded_filter_shared.insert(filter_shared_key);
            return Ok(count);
        }
        if let Some((count, filtered)) = populate_from_shared {
            let filter_shared_key = tenant_filter_shared_identity(topic_filter, shared_group);
            let mut guard = self.inner.write().expect("dist poisoned");
            let tenant_routes = guard.entry(tenant_id.to_string()).or_default();
            tenant_routes
                .by_filter_shared
                .insert(filter_shared_key.clone(), filtered);
            tenant_routes.loaded_filter_shared.insert(filter_shared_key);
            return Ok(count);
        }
        self.store
            .count_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = match tenant_id {
            Some(tenant_id) => {
                let cached = self
                    .inner
                    .read()
                    .expect("dist poisoned")
                    .get(tenant_id)
                    .cloned();
                if let Some(cached) = cached {
                    if cached.fully_loaded {
                        cached.all
                    } else {
                        let routes = self.store.list_tenant_routes(tenant_id).await?;
                        self.inner.write().expect("dist poisoned").insert(
                            tenant_id.to_string(),
                            tenant_routes_from_vec(routes.clone()),
                        );
                        routes
                    }
                } else {
                    let routes = self.store.list_tenant_routes(tenant_id).await?;
                    self.inner.write().expect("dist poisoned").insert(
                        tenant_id.to_string(),
                        tenant_routes_from_vec(routes.clone()),
                    );
                    routes
                }
            }
            None => self.store.list_routes().await?,
        };
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        Ok(routes)
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        self.store.count_routes().await
    }
}

#[async_trait]
impl Lifecycle for PersistentDistHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
