pub(crate) mod handle;
pub(crate) mod persistent;

use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{RouteRecord, TopicName};
pub use handle::{dist_route_shard, dist_tenant_shard, DistHandle};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter};
pub(crate) use handle::{
    exact_topic_loaded, insert_tenant_route, remove_tenant_route, retain_tenant_routes,
    rewrite_tenant_route_node, session_topic_shared_identity, shared_group_identity,
    tenant_filter_shared_identity, TenantRoutes,
};
pub use persistent::PersistentDistHandle;
use crate::trie::tenant_routes_from_vec;
use std::sync::Arc;

#[derive(Clone)]
pub struct ReplicatedDistHandle {
    router: Arc<dyn KvRangeRouter>,
    executor: Arc<dyn KvRangeExecutor>,
}

impl ReplicatedDistHandle {
    pub fn new(router: Arc<dyn KvRangeRouter>, executor: Arc<dyn KvRangeExecutor>) -> Self {
        Self { router, executor }
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
impl DistRouter for ReplicatedDistHandle {
    async fn add_route(&self, route: RouteRecord) -> anyhow::Result<()> {
        let shard = dist_tenant_shard(&route.tenant_id);
        let range = self
            .router
            .route_key(&shard, route.topic_filter.as_bytes())
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist range available for tenant {}", route.tenant_id))?;
        self.executor
            .apply(
                &range.descriptor.id,
                vec![greenmqtt_kv_engine::KvMutation {
                    key: route_record_key(&route),
                    value: Some(encode_route_record(&route)?),
                }],
            )
            .await
    }

    async fn remove_route(&self, route: &RouteRecord) -> anyhow::Result<()> {
        let shard = dist_tenant_shard(&route.tenant_id);
        let range = self
            .router
            .route_key(&shard, route.topic_filter.as_bytes())
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist range available for tenant {}", route.tenant_id))?;
        self.executor
            .apply(
                &range.descriptor.id,
                vec![greenmqtt_kv_engine::KvMutation {
                    key: route_record_key(route),
                    value: None,
                }],
            )
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
        let routes = self.list_routes(Some(tenant_id)).await?;
        let cache = tenant_routes_from_vec(routes);
        Ok(crate::trie::select_routes(&cache, topic))
    }

    async fn list_routes(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<RouteRecord>> {
        let descriptors = match tenant_id {
            Some(tenant_id) => self.router.route_shard(&dist_tenant_shard(tenant_id)).await?,
            None => self
                .router
                .list()
                .await?
                .into_iter()
                .filter(|descriptor| descriptor.shard.kind == greenmqtt_core::ServiceShardKind::Dist)
                .map(greenmqtt_kv_client::RangeRoute::from_descriptor)
                .collect(),
        };
        let mut routes = Vec::new();
        for descriptor in descriptors {
            for (_, value) in self
                .executor
                .scan(&descriptor.descriptor.id, None, usize::MAX)
                .await?
            {
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
        Ok(routes)
    }

    async fn route_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_routes(None).await?.len())
    }
}
