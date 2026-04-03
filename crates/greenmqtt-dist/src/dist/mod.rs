pub(crate) mod handle;
pub(crate) mod persistent;

use async_trait::async_trait;
use greenmqtt_core::{RouteRecord, TopicName};
pub use handle::{dist_route_shard, dist_tenant_shard, DistHandle};
pub(crate) use handle::{
    exact_topic_loaded, insert_tenant_route, remove_tenant_route, retain_tenant_routes,
    rewrite_tenant_route_node, session_topic_shared_identity, shared_group_identity,
    tenant_filter_shared_identity, TenantRoutes,
};
pub use persistent::PersistentDistHandle;

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
