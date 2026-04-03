use crate::dist::{insert_tenant_route, TenantRoutes};
use greenmqtt_core::{
    shared_subscription_key, CompiledTopicFilter, RouteRecord, TopicFilterLevel, TopicName,
};
use std::collections::HashMap;

#[derive(Clone, Default)]
pub(crate) struct WildcardRouteTrie {
    root: WildcardRouteTrieNode,
}

#[derive(Clone, Default)]
pub(crate) struct WildcardRouteTrieNode {
    exact_children: HashMap<String, WildcardRouteTrieNode>,
    single_level_child: Option<Box<WildcardRouteTrieNode>>,
    terminal_routes: Vec<(usize, RouteRecord)>,
    multi_level_routes: Vec<(usize, RouteRecord)>,
}

impl WildcardRouteTrie {
    pub(crate) fn rebuild(&mut self, routes: &[RouteRecord]) {
        self.root = WildcardRouteTrieNode::default();
        for (order, route) in routes.iter().cloned().enumerate() {
            let topic_filter = CompiledTopicFilter::new(&route.topic_filter);
            self.root.insert(&topic_filter, order, route);
        }
    }

    pub(crate) fn match_topic(&self, topic: &TopicName) -> Vec<RouteRecord> {
        let levels: Vec<&str> = topic.split('/').collect();
        let mut matches = Vec::new();
        self.root.collect(&levels, 0, &mut matches);
        matches.sort_by_key(|(order, _)| *order);
        matches.into_iter().map(|(_, route)| route).collect()
    }
}

impl WildcardRouteTrieNode {
    fn insert(&mut self, filter: &CompiledTopicFilter, order: usize, route: RouteRecord) {
        self.insert_levels(filter.levels(), 0, order, route);
    }

    fn insert_levels(
        &mut self,
        levels: &[TopicFilterLevel],
        index: usize,
        order: usize,
        route: RouteRecord,
    ) {
        if index >= levels.len() {
            self.terminal_routes.push((order, route));
            return;
        }

        match &levels[index] {
            TopicFilterLevel::MultiLevelWildcard => {
                self.multi_level_routes.push((order, route));
            }
            TopicFilterLevel::SingleLevelWildcard => {
                let child = self.single_level_child.get_or_insert_with(Default::default);
                child.insert_levels(levels, index + 1, order, route);
            }
            TopicFilterLevel::Literal(level) => {
                self.exact_children
                    .entry(level.clone())
                    .or_default()
                    .insert_levels(levels, index + 1, order, route);
            }
        }
    }

    fn collect(&self, levels: &[&str], index: usize, out: &mut Vec<(usize, RouteRecord)>) {
        out.extend(self.multi_level_routes.iter().cloned());
        if index >= levels.len() {
            out.extend(self.terminal_routes.iter().cloned());
            return;
        }

        if let Some(child) = self.exact_children.get(levels[index]) {
            child.collect(levels, index + 1, out);
        }
        if let Some(child) = &self.single_level_child {
            child.collect(levels, index + 1, out);
        }
    }
}

pub(crate) fn rebuild_wildcard_trie(cache: &mut TenantRoutes) {
    cache.wildcard_trie.rebuild(&cache.wildcards);
}

pub(crate) fn tenant_routes_from_vec(routes: Vec<RouteRecord>) -> TenantRoutes {
    let mut cache = TenantRoutes::default();
    for route in routes {
        insert_tenant_route(&mut cache, route);
    }
    rebuild_wildcard_trie(&mut cache);
    cache.fully_loaded = true;
    cache.wildcards_loaded = true;
    cache
}

pub(crate) fn select_routes(routes: &TenantRoutes, topic: &TopicName) -> Vec<RouteRecord> {
    let wildcard_routes = routes.wildcard_trie.match_topic(topic);
    select_route_parts(
        routes
            .exact
            .get(topic.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]),
        &wildcard_routes,
        topic,
    )
}

fn select_route_parts(
    exact_routes: &[RouteRecord],
    wildcard_routes: &[RouteRecord],
    _topic: &TopicName,
) -> Vec<RouteRecord> {
    let mut shared_routes = HashMap::<String, RouteRecord>::new();
    let mut result = Vec::new();

    for route in exact_routes.iter().chain(wildcard_routes.iter()) {
        if route.shared_group.is_none() {
            result.push(route.clone());
            continue;
        }

        let key = shared_subscription_key(route.shared_group.as_deref(), &route.topic_filter);
        match shared_routes.get(&key) {
            Some(existing) if existing.session_id <= route.session_id => {}
            _ => {
                shared_routes.insert(key, route.clone());
            }
        }
    }

    result.extend(shared_routes.into_values());
    result
}
