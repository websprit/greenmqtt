use crate::dist::DistRouter;
use crate::trie::{
    rebuild_wildcard_trie, select_routes, tenant_routes_from_vec, WildcardRouteTrie,
};
use async_trait::async_trait;
use greenmqtt_core::{Lifecycle, RouteRecord, ServiceShardKey, TopicName};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct DistHandle {
    inner: Arc<RwLock<DistState>>,
}

#[derive(Default)]
struct DistState {
    by_tenant: HashMap<String, TenantRoutes>,
    by_session: HashMap<String, Vec<RouteRecord>>,
    by_session_topic: HashMap<(String, String), Vec<RouteRecord>>,
    by_session_topic_shared: HashMap<(String, String, String), RouteRecord>,
    total_routes: usize,
}

#[derive(Clone, Default)]
pub(crate) struct TenantRoutes {
    pub(crate) all: Vec<RouteRecord>,
    pub(crate) by_shared: HashMap<String, Vec<RouteRecord>>,
    pub(crate) by_filter: HashMap<String, Vec<RouteRecord>>,
    pub(crate) by_filter_shared: HashMap<(String, String), Vec<RouteRecord>>,
    pub(crate) exact: HashMap<String, Vec<RouteRecord>>,
    pub(crate) wildcards: Vec<RouteRecord>,
    pub(crate) wildcard_trie: WildcardRouteTrie,
    pub(crate) fully_loaded: bool,
    pub(crate) wildcards_loaded: bool,
    pub(crate) loaded_exact_topics: HashSet<String>,
    pub(crate) loaded_shared_groups: HashSet<String>,
    pub(crate) loaded_filters: HashSet<String>,
    pub(crate) loaded_filter_shared: HashSet<(String, String)>,
}

pub fn dist_tenant_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey::dist(tenant_id.to_string())
}

pub fn dist_route_shard(route: &RouteRecord) -> ServiceShardKey {
    dist_tenant_shard(&route.tenant_id)
}

fn same_route(left: &RouteRecord, right: &RouteRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.session_id == right.session_id
        && left.topic_filter == right.topic_filter
        && left.shared_group == right.shared_group
}

fn route_is_exact(route: &RouteRecord) -> bool {
    !route.topic_filter.contains('#') && !route.topic_filter.contains('+')
}

fn add_route_indexes(cache: &mut TenantRoutes, route: RouteRecord) {
    cache
        .by_shared
        .entry(shared_group_identity(route.shared_group.as_deref()))
        .or_default()
        .push(route.clone());
    cache
        .by_filter
        .entry(route.topic_filter.clone())
        .or_default()
        .push(route.clone());
    cache
        .by_filter_shared
        .entry(tenant_filter_shared_identity(
            &route.topic_filter,
            route.shared_group.as_deref(),
        ))
        .or_default()
        .push(route.clone());
    if route_is_exact(&route) {
        cache
            .exact
            .entry(route.topic_filter.clone())
            .or_default()
            .push(route);
    } else {
        cache.wildcards.push(route);
    }
}

fn remove_route_indexes(cache: &mut TenantRoutes, route: &RouteRecord) {
    let shared_key = shared_group_identity(route.shared_group.as_deref());
    if let Some(routes) = cache.by_shared.get_mut(&shared_key) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            cache.by_shared.remove(&shared_key);
        }
    }
    if let Some(routes) = cache.by_filter.get_mut(&route.topic_filter) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            cache.by_filter.remove(&route.topic_filter);
        }
    }
    let filter_shared_key =
        tenant_filter_shared_identity(&route.topic_filter, route.shared_group.as_deref());
    if let Some(routes) = cache.by_filter_shared.get_mut(&filter_shared_key) {
        routes.retain(|existing| !same_route(existing, route));
        if routes.is_empty() {
            cache.by_filter_shared.remove(&filter_shared_key);
        }
    }
    if route_is_exact(route) {
        if let Some(routes) = cache.exact.get_mut(&route.topic_filter) {
            routes.retain(|existing| !same_route(existing, route));
            if routes.is_empty() {
                cache.exact.remove(&route.topic_filter);
            }
        }
    } else {
        cache
            .wildcards
            .retain(|existing| !same_route(existing, route));
    }
}

fn rewrite_route_node(routes: &mut [RouteRecord], route: &RouteRecord, node_id: u64) {
    for existing in routes {
        if same_route(existing, route) {
            existing.node_id = node_id;
        }
    }
}

pub(crate) fn rewrite_tenant_route_node(
    cache: &mut TenantRoutes,
    route: &RouteRecord,
    node_id: u64,
) {
    rewrite_route_node(&mut cache.all, route, node_id);
    if let Some(routes) = cache
        .by_shared
        .get_mut(&shared_group_identity(route.shared_group.as_deref()))
    {
        rewrite_route_node(routes, route, node_id);
    }
    if let Some(routes) = cache.by_filter.get_mut(&route.topic_filter) {
        rewrite_route_node(routes, route, node_id);
    }
    if let Some(routes) = cache
        .by_filter_shared
        .get_mut(&tenant_filter_shared_identity(
            &route.topic_filter,
            route.shared_group.as_deref(),
        ))
    {
        rewrite_route_node(routes, route, node_id);
    }
    if route_is_exact(route) {
        if let Some(routes) = cache.exact.get_mut(&route.topic_filter) {
            rewrite_route_node(routes, route, node_id);
        }
    } else {
        rewrite_route_node(&mut cache.wildcards, route, node_id);
        rebuild_wildcard_trie(cache);
    }
}

pub(crate) fn shared_group_identity(shared_group: Option<&str>) -> String {
    shared_group.unwrap_or_default().to_string()
}

pub(crate) fn exact_topic_loaded(cache: &TenantRoutes, topic_filter: &str) -> bool {
    cache.fully_loaded || cache.loaded_exact_topics.contains(topic_filter)
}

pub(crate) fn tenant_filter_shared_identity(
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String) {
    (
        topic_filter.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

pub(crate) fn insert_tenant_route(
    cache: &mut TenantRoutes,
    route: RouteRecord,
) -> Option<RouteRecord> {
    let route_exact = route_is_exact(&route);
    if let Some(index) = cache
        .all
        .iter()
        .position(|existing| same_route(existing, &route))
    {
        let previous = cache.all[index].clone();
        cache.all[index] = route.clone();
        remove_route_indexes(cache, &previous);
        add_route_indexes(cache, route);
        if !route_is_exact(&previous) || !route_exact {
            rebuild_wildcard_trie(cache);
        }
        Some(previous)
    } else {
        cache.all.push(route.clone());
        add_route_indexes(cache, route);
        if !route_exact {
            rebuild_wildcard_trie(cache);
        }
        None
    }
}

pub(crate) fn remove_tenant_route(cache: &mut TenantRoutes, route: &RouteRecord) -> bool {
    let before = cache.all.len();
    cache.all.retain(|existing| !same_route(existing, route));
    let removed = before != cache.all.len();
    if !removed {
        return false;
    }

    remove_route_indexes(cache, route);
    if !route_is_exact(route) {
        rebuild_wildcard_trie(cache);
    }
    true
}

pub(crate) fn retain_tenant_routes<F>(cache: &mut TenantRoutes, mut keep: F) -> usize
where
    F: FnMut(&RouteRecord) -> bool,
{
    let previous = cache.all.len();
    let fully_loaded = cache.fully_loaded;
    let wildcards_loaded = cache.wildcards_loaded;
    let loaded_exact_topics = cache.loaded_exact_topics.clone();
    let retained = cache
        .all
        .iter()
        .filter(|route| keep(route))
        .cloned()
        .collect();
    *cache = tenant_routes_from_vec(retained);
    cache.fully_loaded = fully_loaded;
    cache.wildcards_loaded = wildcards_loaded;
    cache.loaded_exact_topics = loaded_exact_topics;
    previous - cache.all.len()
}

fn insert_session_route_indexes(state: &mut DistState, route: RouteRecord) {
    state
        .by_session
        .entry(route.session_id.clone())
        .or_default()
        .push(route.clone());
    state
        .by_session_topic
        .entry((route.session_id.clone(), route.topic_filter.clone()))
        .or_default()
        .push(route.clone());
    state.by_session_topic_shared.insert(
        session_topic_shared_identity(
            &route.session_id,
            &route.topic_filter,
            route.shared_group.as_deref(),
        ),
        route,
    );
}

fn remove_session_route_indexes(state: &mut DistState, route: &RouteRecord) {
    let mut remove_session_entry = false;
    if let Some(routes) = state.by_session.get_mut(&route.session_id) {
        routes.retain(|existing| !same_route(existing, route));
        remove_session_entry = routes.is_empty();
    }
    if remove_session_entry {
        state.by_session.remove(&route.session_id);
    }

    let topic_key = (route.session_id.clone(), route.topic_filter.clone());
    let mut remove_topic_entry = false;
    if let Some(routes) = state.by_session_topic.get_mut(&topic_key) {
        routes.retain(|existing| !same_route(existing, route));
        remove_topic_entry = routes.is_empty();
    }
    if remove_topic_entry {
        state.by_session_topic.remove(&topic_key);
    }
    state
        .by_session_topic_shared
        .remove(&session_topic_shared_identity(
            &route.session_id,
            &route.topic_filter,
            route.shared_group.as_deref(),
        ));
}

fn rewrite_session_route_node(state: &mut DistState, route: &RouteRecord, node_id: u64) {
    if let Some(routes) = state.by_session.get_mut(&route.session_id) {
        rewrite_route_node(routes, route, node_id);
    }
    if let Some(routes) = state
        .by_session_topic
        .get_mut(&(route.session_id.clone(), route.topic_filter.clone()))
    {
        rewrite_route_node(routes, route, node_id);
    }
    if let Some(current) = state
        .by_session_topic_shared
        .get_mut(&session_topic_shared_identity(
            &route.session_id,
            &route.topic_filter,
            route.shared_group.as_deref(),
        ))
    {
        current.node_id = node_id;
    }
}

pub(crate) fn session_topic_shared_identity(
    session_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        session_id.to_string(),
        topic_filter.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

#[async_trait]
impl DistRouter for DistHandle {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let previous = insert_tenant_route(
            guard.by_tenant.entry(route.tenant_id.clone()).or_default(),
            route.clone(),
        );
        if let Some(previous) = previous {
            remove_session_route_indexes(&mut guard, &previous);
            insert_session_route_indexes(&mut guard, route);
        } else {
            insert_session_route_indexes(&mut guard, route);
            guard.total_routes += 1;
        }
        Ok(())
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let mut removed = 0usize;
        let mut remove_tenant_entry = false;
        if let Some(routes) = guard.by_tenant.get_mut(&route.tenant_id) {
            if remove_tenant_route(routes, route) {
                removed = 1;
            }
            remove_tenant_entry = routes.all.is_empty();
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(&route.tenant_id);
        }
        if removed > 0 {
            remove_session_route_indexes(&mut guard, route);
            guard.total_routes -= removed;
        }
        Ok(())
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let Some(routes) = guard.by_session.remove(session_id) else {
            return Ok(0);
        };
        guard
            .by_session_topic
            .retain(|(current_session_id, _), _| current_session_id != session_id);
        guard
            .by_session_topic_shared
            .retain(|(current_session_id, _, _), _| current_session_id != session_id);
        for route in &routes {
            let mut remove_tenant_entry = false;
            if let Some(tenant_routes) = guard.by_tenant.get_mut(&route.tenant_id) {
                remove_tenant_route(tenant_routes, route);
                remove_tenant_entry = tenant_routes.all.is_empty();
            }
            if remove_tenant_entry {
                guard.by_tenant.remove(&route.tenant_id);
            }
        }
        let removed = routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn lookup_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_session_topic_shared
            .get(&session_topic_shared_identity(
                session_id,
                topic_filter,
                shared_group,
            ))
            .cloned())
    }

    async fn remove_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let Some(route) = self
            .lookup_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await?
        else {
            return Ok(0);
        };
        self.remove_route(&route).await?;
        Ok(1)
    }

    async fn remove_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let removed_routes = guard
            .by_session_topic
            .get(&(session_id.to_string(), topic_filter.to_string()))
            .cloned()
            .unwrap_or_default();
        let removed = removed_routes.len();
        if removed == 0 {
            return Ok(0);
        }
        guard
            .by_session_topic
            .remove(&(session_id.to_string(), topic_filter.to_string()));
        for route in &removed_routes {
            guard
                .by_session_topic_shared
                .remove(&session_topic_shared_identity(
                    &route.session_id,
                    &route.topic_filter,
                    route.shared_group.as_deref(),
                ));
        }
        let mut remove_session_entry = false;
        if let Some(session_routes) = guard.by_session.get_mut(session_id) {
            session_routes.retain(|route| route.topic_filter != topic_filter);
            remove_session_entry = session_routes.is_empty();
        }
        if remove_session_entry {
            guard.by_session.remove(session_id);
        }
        for route in &removed_routes {
            let mut remove_tenant_entry = false;
            if let Some(tenant_routes) = guard.by_tenant.get_mut(&route.tenant_id) {
                remove_tenant_route(tenant_routes, route);
                remove_tenant_entry = tenant_routes.all.is_empty();
            }
            if remove_tenant_entry {
                guard.by_tenant.remove(&route.tenant_id);
            }
        }
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let Some(routes) = guard.by_tenant.remove(tenant_id) else {
            return Ok(0);
        };
        for route in &routes.all {
            remove_session_route_indexes(&mut guard, route);
        }
        let removed = routes.all.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let Some(removed_routes) = guard
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| routes.by_shared.get(&shared_group_identity(shared_group)))
            .cloned()
        else {
            return Ok(0);
        };
        let mut remove_tenant_entry = false;
        if let Some(tenant_routes) = guard.by_tenant.get_mut(tenant_id) {
            for route in &removed_routes {
                remove_tenant_route(tenant_routes, route);
            }
            remove_tenant_entry = tenant_routes.all.is_empty();
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(tenant_id);
        }
        for route in &removed_routes {
            remove_session_route_indexes(&mut guard, route);
        }
        let removed = removed_routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let Some(removed_routes) = guard
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| routes.by_filter.get(topic_filter))
            .cloned()
        else {
            return Ok(0);
        };
        let mut remove_tenant_entry = false;
        if let Some(tenant_routes) = guard.by_tenant.get_mut(tenant_id) {
            for route in &removed_routes {
                remove_tenant_route(tenant_routes, route);
            }
            remove_tenant_entry = tenant_routes.all.is_empty();
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(tenant_id);
        }
        for route in &removed_routes {
            remove_session_route_indexes(&mut guard, route);
        }
        let removed = removed_routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let Some(removed_routes) = guard
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| {
                routes
                    .by_filter_shared
                    .get(&tenant_filter_shared_identity(topic_filter, shared_group))
            })
            .cloned()
        else {
            return Ok(0);
        };
        let mut remove_tenant_entry = false;
        if let Some(tenant_routes) = guard.by_tenant.get_mut(tenant_id) {
            for route in &removed_routes {
                remove_tenant_route(tenant_routes, route);
            }
            remove_tenant_entry = tenant_routes.all.is_empty();
        }
        if remove_tenant_entry {
            guard.by_tenant.remove(tenant_id);
        }
        for route in &removed_routes {
            remove_session_route_indexes(&mut guard, route);
        }
        let removed = removed_routes.len();
        guard.total_routes -= removed;
        Ok(removed)
    }

    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        let mut guard = self.inner.write().expect("dist poisoned");
        let previous = guard
            .by_session
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        if previous.is_empty() {
            return Ok(0);
        }
        if let Some(routes) = guard.by_session.get_mut(session_id) {
            for route in routes {
                route.node_id = node_id;
            }
        }
        for route in &previous {
            rewrite_session_route_node(&mut guard, route, node_id);
            if let Some(tenant_routes) = guard.by_tenant.get_mut(&route.tenant_id) {
                rewrite_tenant_route_node(tenant_routes, route, node_id);
            }
        }
        Ok(previous.len())
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        let guard = self.inner.read().expect("dist poisoned");
        let mut routes: Vec<_> = guard
            .by_session
            .get(session_id)
            .into_iter()
            .flat_map(|routes| routes.iter())
            .cloned()
            .collect();
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        Ok(routes)
    }

    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let guard = self.inner.read().expect("dist poisoned");
        let mut routes: Vec<_> = guard
            .by_session_topic
            .get(&(session_id.to_string(), topic_filter.to_string()))
            .into_iter()
            .flat_map(|routes| routes.iter())
            .cloned()
            .collect();
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        Ok(routes)
    }

    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_session
            .get(session_id)
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_session_topic
            .get(&(session_id.to_string(), topic_filter.to_string()))
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(usize::from(
            self.inner
                .read()
                .expect("dist poisoned")
                .by_session_topic_shared
                .contains_key(&session_topic_shared_identity(
                    session_id,
                    topic_filter,
                    shared_group,
                )),
        ))
    }

    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .map(|routes| routes.all.len())
            .unwrap_or(0))
    }

    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| routes.by_shared.get(&shared_group_identity(shared_group)))
            .cloned()
            .unwrap_or_default();
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        Ok(routes)
    }

    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| routes.by_shared.get(&shared_group_identity(shared_group)))
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let guard = self.inner.read().expect("dist poisoned");
        Ok(guard
            .by_tenant
            .get(tenant_id)
            .map(|routes| select_routes(routes, topic))
            .unwrap_or_default())
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
        let mut routes = self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| routes.by_filter.get(topic_filter))
            .cloned()
            .unwrap_or_default();
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        Ok(routes)
    }

    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| {
                routes
                    .by_filter_shared
                    .get(&tenant_filter_shared_identity(topic_filter, shared_group))
            })
            .cloned()
            .unwrap_or_default();
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
        });
        Ok(routes)
    }

    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| routes.by_filter.get(topic_filter))
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("dist poisoned")
            .by_tenant
            .get(tenant_id)
            .and_then(|routes| {
                routes
                    .by_filter_shared
                    .get(&tenant_filter_shared_identity(topic_filter, shared_group))
            })
            .map(|routes| routes.len())
            .unwrap_or(0))
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        let guard = self.inner.read().expect("dist poisoned");
        let mut routes: Vec<_> = if let Some(tenant_id) = tenant_id {
            guard
                .by_tenant
                .get(tenant_id)
                .into_iter()
                .flat_map(|routes| routes.all.iter())
                .cloned()
                .collect()
        } else {
            guard
                .by_tenant
                .values()
                .flat_map(|routes| routes.all.iter())
                .cloned()
                .collect()
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
        Ok(self.inner.read().expect("dist poisoned").total_routes)
    }
}

#[async_trait]
impl Lifecycle for DistHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
