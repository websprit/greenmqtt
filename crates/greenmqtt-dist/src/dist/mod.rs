pub(crate) mod handle;
pub(crate) mod persistent;

use crate::trie::tenant_routes_from_vec;
use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::RangeBoundary;
use greenmqtt_core::{dedupe_sessions, PublishOutcome, PublishRequest, RouteRecord, TopicName};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, RangeDataClient, RoutedRangeDataClient};
pub use handle::{dist_route_shard, dist_tenant_shard, DistHandle};
pub(crate) use handle::{
    exact_topic_loaded, insert_tenant_route, remove_tenant_route, retain_tenant_routes,
    rewrite_tenant_route_node, session_topic_shared_identity, shared_group_identity,
    tenant_filter_shared_identity, TenantRoutes,
};
pub use persistent::PersistentDistHandle;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct ReplicatedDistHandle {
    client: Arc<dyn RangeDataClient>,
    tenant_wildcard_cache: Arc<RwLock<HashMap<String, TenantRoutes>>>,
}

impl ReplicatedDistHandle {
    pub fn new(client: Arc<dyn RangeDataClient>) -> Self {
        Self {
            client,
            tenant_wildcard_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_router_executor(
        router: Arc<dyn KvRangeRouter>,
        executor: Arc<dyn KvRangeExecutor>,
    ) -> Self {
        Self::new(Arc::new(RoutedRangeDataClient::new(router, executor)))
    }

    async fn tenant_ranges(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_kv_client::RangeRoute>> {
        self.client.route_shard(&dist_tenant_shard(tenant_id)).await
    }

    async fn scan_tenant_prefix(
        &self,
        tenant_id: &str,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let boundary = prefix_boundary(prefix);
        let mut routes = Vec::new();
        for descriptor in self.tenant_ranges(tenant_id).await? {
            for (_, value) in self
                .client
                .scan(
                    &descriptor.descriptor.id,
                    Some(boundary.clone()),
                    usize::MAX,
                )
                .await?
            {
                routes.push(decode_route_record(&value)?);
            }
        }
        Ok(routes)
    }

    fn invalidate_tenant_cache(&self, tenant_id: &str) {
        self.tenant_wildcard_cache
            .write()
            .expect("dist cache poisoned")
            .remove(tenant_id);
    }

    pub async fn warm_tenant_cache(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.invalidate_tenant_cache(tenant_id);
        let cache = self.cached_wildcard_routes(tenant_id).await?;
        Ok(cache.wildcards.len())
    }

    async fn cached_wildcard_routes(&self, tenant_id: &str) -> anyhow::Result<TenantRoutes> {
        if let Some(routes) = self
            .tenant_wildcard_cache
            .read()
            .expect("dist cache poisoned")
            .get(tenant_id)
            .cloned()
        {
            return Ok(routes);
        }
        let wildcard = self
            .list_routes(Some(tenant_id))
            .await?
            .into_iter()
            .filter(|route| route.topic_filter.contains('#') || route.topic_filter.contains('+'))
            .collect::<Vec<_>>();
        let routes = tenant_routes_from_vec(wildcard);
        self.tenant_wildcard_cache
            .write()
            .expect("dist cache poisoned")
            .insert(tenant_id.to_string(), routes.clone());
        Ok(routes)
    }
}

fn route_record_key(route: &RouteRecord) -> Bytes {
    let mut key = Vec::with_capacity(
        route.topic_filter.len()
            + route.session_id.len()
            + route.shared_group.as_deref().unwrap_or_default().len()
            + 2,
    );
    key.extend_from_slice(route.topic_filter.as_bytes());
    key.push(0);
    key.extend_from_slice(route.session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(route.shared_group.as_deref().unwrap_or_default().as_bytes());
    Bytes::from(key)
}

fn encode_route_record(route: &RouteRecord) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(route)?))
}

fn decode_route_record(value: &[u8]) -> anyhow::Result<RouteRecord> {
    Ok(bincode::deserialize(value)?)
}

const EXACT_INDEX_TAG: &[u8] = b"\0\xffexact\0";

fn exact_index_key(route: &RouteRecord) -> Bytes {
    let mut key = Vec::with_capacity(
        route.topic_filter.len()
            + EXACT_INDEX_TAG.len()
            + route.session_id.len()
            + route.shared_group.as_deref().unwrap_or_default().len()
            + 2,
    );
    key.extend_from_slice(route.topic_filter.as_bytes());
    key.extend_from_slice(EXACT_INDEX_TAG);
    key.extend_from_slice(route.session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(route.shared_group.as_deref().unwrap_or_default().as_bytes());
    Bytes::from(key)
}

fn exact_index_prefix(topic: &str) -> Bytes {
    let mut key = Vec::with_capacity(topic.len() + EXACT_INDEX_TAG.len());
    key.extend_from_slice(topic.as_bytes());
    key.extend_from_slice(EXACT_INDEX_TAG);
    Bytes::from(key)
}

fn route_mutations(
    route: &RouteRecord,
    value: Option<Bytes>,
) -> anyhow::Result<Vec<greenmqtt_kv_engine::KvMutation>> {
    let mut mutations = vec![greenmqtt_kv_engine::KvMutation {
        key: route_record_key(route),
        value: value.clone(),
    }];
    if !route.topic_filter.contains('#') && !route.topic_filter.contains('+') {
        mutations.push(greenmqtt_kv_engine::KvMutation {
            key: exact_index_key(route),
            value,
        });
    }
    Ok(mutations)
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] = end[index].saturating_add(1);
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn prefix_boundary(prefix: &[u8]) -> RangeBoundary {
    RangeBoundary::new(Some(prefix.to_vec()), prefix_upper_bound(prefix))
}

#[async_trait]
pub trait DistRouter: Send + Sync {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()>;
    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()>;
    async fn lookup_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<RouteRecord>> {
        Ok(self
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await?
            .into_iter()
            .find(|route| route.shared_group.as_deref() == shared_group))
    }
    async fn remove_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        if let Some(route) = self
            .lookup_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await?
        {
            self.remove_route(&route).await?;
            Ok(1)
        } else {
            Ok(0)
        }
    }
    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let routes = self.list_routes(Some(tenant_id)).await?;
        for route in &routes {
            self.remove_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn remove_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let routes = self
            .list_tenant_shared_routes(tenant_id, shared_group)
            .await?;
        for route in &routes {
            self.remove_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize>;
    async fn remove_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let routes = self
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await?;
        for route in &routes {
            self.remove_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn remove_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let routes = self
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await?;
        for route in &routes {
            self.remove_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn remove_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let routes = self
            .list_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await?;
        for route in &routes {
            self.remove_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn reassign_session_routes(
        &self,
        session_id: &str,
        node_id: u64,
    ) -> anyhow::Result<usize> {
        let routes = self.list_session_routes(session_id).await?;
        for route in &routes {
            self.remove_route(route).await?;
            let mut updated = route.clone();
            updated.node_id = node_id;
            self.add_route(updated).await?;
        }
        Ok(routes.len())
    }
    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>>;
    async fn list_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_session_routes(session_id)
            .await?
            .into_iter()
            .filter(|route| route.topic_filter == topic_filter)
            .collect())
    }
    async fn count_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_session_routes(session_id).await?.len())
    }
    async fn count_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await?
            .len())
    }
    async fn count_session_topic_filter_route(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(usize::from(
            self.lookup_session_topic_filter_route(session_id, topic_filter, shared_group)
                .await?
                .is_some(),
        ))
    }
    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_routes(Some(tenant_id)).await?.len())
    }
    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_routes(Some(tenant_id))
            .await?
            .into_iter()
            .filter(|route| route.shared_group.as_deref() == shared_group)
            .collect())
    }
    async fn count_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_shared_routes(tenant_id, shared_group)
            .await?
            .len())
    }
    async fn list_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        if !topic_filter.contains('#') && !topic_filter.contains('+') {
            self.list_exact_routes(tenant_id, topic_filter).await
        } else {
            Ok(self
                .list_routes(Some(tenant_id))
                .await?
                .into_iter()
                .filter(|route| route.topic_filter == topic_filter)
                .collect())
        }
    }
    async fn list_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await?
            .into_iter()
            .filter(|route| route.shared_group.as_deref() == shared_group)
            .collect())
    }
    async fn count_topic_filter_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await?
            .len())
    }
    async fn count_topic_filter_shared_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_topic_filter_shared_routes(tenant_id, topic_filter, shared_group)
            .await?
            .len())
    }
    async fn list_exact_routes(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_routes(Some(tenant_id))
            .await?
            .into_iter()
            .filter(|route| route.topic_filter == topic_filter)
            .collect())
    }
    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>>;
    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>>;
    async fn route_count(&self) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait DistDeliverySink: Send + Sync {
    async fn deliver(
        &self,
        tenant_id: &str,
        fanout: &DistFanoutRequest,
        routes: &[RouteRecord],
    ) -> anyhow::Result<DistDeliveryReport>;
}

#[derive(Clone)]
pub struct DistFanoutWorker {
    router: Arc<dyn DistRouter>,
}

impl DistFanoutWorker {
    pub fn new(router: Arc<dyn DistRouter>) -> Self {
        Self { router }
    }

    pub async fn fanout(
        &self,
        sink: &dyn DistDeliverySink,
        tenant_id: &str,
        fanout: &DistFanoutRequest,
    ) -> anyhow::Result<PublishOutcome> {
        let routes = self
            .router
            .match_topic(tenant_id, &fanout.request.topic)
            .await?;
        let report = sink.deliver(tenant_id, fanout, &routes).await?;
        Ok(PublishOutcome {
            matched_routes: dedupe_sessions(&routes).len(),
            online_deliveries: report.online_deliveries,
            offline_enqueues: report.offline_enqueues,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistFanoutRequest {
    pub from_session_id: String,
    pub request: PublishRequest,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DistDeliveryReport {
    pub online_deliveries: usize,
    pub offline_enqueues: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistTenantStats {
    pub tenant_id: String,
    pub total_routes: usize,
    pub wildcard_routes: usize,
    pub shared_routes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistBalanceAction {
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

pub trait DistBalancePolicy: Send + Sync {
    fn propose(&self, tenants: &[DistTenantStats]) -> Vec<DistBalanceAction>;
}

#[derive(Debug, Clone)]
pub struct ThresholdDistBalancePolicy {
    pub max_total_routes: usize,
    pub max_wildcard_routes: usize,
    pub max_shared_routes: usize,
    pub desired_voters: usize,
    pub desired_learners: usize,
}

impl DistBalancePolicy for ThresholdDistBalancePolicy {
    fn propose(&self, tenants: &[DistTenantStats]) -> Vec<DistBalanceAction> {
        let mut actions = Vec::new();
        for tenant in tenants {
            let overloaded = tenant.total_routes > self.max_total_routes
                || tenant.wildcard_routes > self.max_wildcard_routes
                || tenant.shared_routes > self.max_shared_routes;
            if overloaded {
                actions.push(DistBalanceAction::RunTenantCleanup {
                    tenant_id: tenant.tenant_id.clone(),
                });
                actions.push(DistBalanceAction::SplitTenantRange {
                    tenant_id: tenant.tenant_id.clone(),
                });
                actions.push(DistBalanceAction::ScaleTenantReplicas {
                    tenant_id: tenant.tenant_id.clone(),
                    voters: self.desired_voters,
                    learners: self.desired_learners,
                });
            }
        }
        actions
    }
}

#[async_trait]
pub trait DistMaintenance: Send + Sync {
    async fn refresh_tenant(&self, tenant_id: &str) -> anyhow::Result<usize>;
}

#[async_trait]
impl DistMaintenance for ReplicatedDistHandle {
    async fn refresh_tenant(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.warm_tenant_cache(tenant_id).await
    }
}

#[derive(Clone)]
pub struct DistMaintenanceWorker {
    maintenance: Arc<dyn DistMaintenance>,
}

impl DistMaintenanceWorker {
    pub fn new(maintenance: Arc<dyn DistMaintenance>) -> Self {
        Self { maintenance }
    }

    pub async fn refresh_tenant(&self, tenant_id: &str) -> anyhow::Result<usize> {
        self.maintenance.refresh_tenant(tenant_id).await
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
                let _ = worker.refresh_tenant(&tenant_id).await?;
            }
        })
    }
}

#[async_trait]
impl DistRouter for ReplicatedDistHandle {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        let shard = dist_tenant_shard(&route.tenant_id);
        let range = self
            .client
            .route_key(&shard, route.topic_filter.as_bytes())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no dist range available for tenant {}", route.tenant_id)
            })?;
        self.invalidate_tenant_cache(&route.tenant_id);
        self.client
            .apply(
                &range.descriptor.id,
                route_mutations(&route, Some(encode_route_record(&route)?))?,
            )
            .await
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let shard = dist_tenant_shard(&route.tenant_id);
        let range = self
            .client
            .route_key(&shard, route.topic_filter.as_bytes())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no dist range available for tenant {}", route.tenant_id)
            })?;
        self.invalidate_tenant_cache(&route.tenant_id);
        self.client
            .apply(&range.descriptor.id, route_mutations(route, None)?)
            .await
    }

    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let routes = self.list_session_routes(session_id).await?;
        for route in &routes {
            self.remove_route(route).await?;
        }
        Ok(routes.len())
    }

    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_routes(None)
            .await?
            .into_iter()
            .filter(|route| route.session_id == session_id)
            .collect())
    }

    async fn match_topic(
        &self,
        tenant_id: &str,
        topic: &TopicName,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        let mut routes = self
            .scan_tenant_prefix(tenant_id, &exact_index_prefix(topic))
            .await?;
        let wildcard = self.cached_wildcard_routes(tenant_id).await?;
        routes.extend(crate::trie::select_routes(&wildcard, topic));
        Ok(routes)
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        let descriptors = match tenant_id {
            Some(tenant_id) => {
                self.client
                    .route_shard(&dist_tenant_shard(tenant_id))
                    .await?
            }
            None => self
                .client
                .list_ranges()
                .await?
                .into_iter()
                .filter(|descriptor| {
                    descriptor.shard.kind == greenmqtt_core::ServiceShardKind::Dist
                })
                .map(greenmqtt_kv_client::RangeRoute::from_descriptor)
                .collect(),
        };
        let mut routes = Vec::new();
        for descriptor in descriptors {
            for (_, value) in self
                .client
                .scan(&descriptor.descriptor.id, None, usize::MAX)
                .await?
            {
                if value.is_empty() {
                    continue;
                }
                let route = decode_route_record(&value)?;
                if tenant_id
                    .map(|tenant_id| route.tenant_id == tenant_id)
                    .unwrap_or(true)
                {
                    routes.push(route);
                }
            }
        }
        routes.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.topic_filter.cmp(&right.topic_filter))
                .then(left.session_id.cmp(&right.session_id))
                .then(left.shared_group.cmp(&right.shared_group))
        });
        routes.dedup_by(|left, right| {
            left.tenant_id == right.tenant_id
                && left.topic_filter == right.topic_filter
                && left.session_id == right.session_id
                && left.shared_group == right.shared_group
        });
        Ok(routes)
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_routes(None).await?.len())
    }
}
