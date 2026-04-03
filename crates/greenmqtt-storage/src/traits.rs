use crate::route_topic_filter_is_exact;
use async_trait::async_trait;
use greenmqtt_core::{
    InflightMessage, OfflineMessage, RetainedMessage, RouteRecord, SessionRecord, Subscription,
};

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn save_session(&self, session: &SessionRecord) -> anyhow::Result<()>;
    async fn load_session(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>>;
    async fn load_session_by_session_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self
            .list_sessions()
            .await?
            .into_iter()
            .find(|record| record.session_id == session_id))
    }
    async fn delete_session(&self, tenant_id: &str, client_id: &str) -> anyhow::Result<()>;
    async fn list_sessions(&self) -> anyhow::Result<Vec<SessionRecord>>;
    async fn list_tenant_sessions(&self, tenant_id: &str) -> anyhow::Result<Vec<SessionRecord>> {
        Ok(self
            .list_sessions()
            .await?
            .into_iter()
            .filter(|record| record.identity.tenant_id == tenant_id)
            .collect())
    }
    async fn count_sessions(&self) -> anyhow::Result<usize> {
        Ok(self.list_sessions().await?.len())
    }
}

#[async_trait]
pub trait SubscriptionStore: Send + Sync {
    async fn save_subscription(&self, subscription: &Subscription) -> anyhow::Result<()>;
    async fn load_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        Ok(self
            .list_subscriptions(session_id)
            .await?
            .into_iter()
            .find(|subscription| {
                subscription.topic_filter == topic_filter
                    && subscription.shared_group.as_deref() == shared_group
            }))
    }
    async fn delete_subscription(
        &self,
        session_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool>;
    async fn purge_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        let subscriptions = self.list_subscriptions(session_id).await?;
        let mut removed = 0usize;
        for subscription in subscriptions {
            if self
                .delete_subscription(
                    session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await?;
        let mut removed = 0usize;
        for subscription in subscriptions {
            if self
                .delete_subscription(
                    session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let subscriptions = self.list_tenant_subscriptions(tenant_id).await?;
        let mut removed = 0usize;
        for subscription in subscriptions {
            if self
                .delete_subscription(
                    &subscription.session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    async fn purge_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?;
        let mut removed = 0usize;
        for subscription in subscriptions {
            if self
                .delete_subscription(
                    &subscription.session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    async fn purge_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await?;
        let mut removed = 0usize;
        for subscription in subscriptions {
            if self
                .delete_subscription(
                    &subscription.session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                )
                .await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    async fn list_tenant_subscriptions(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_all_subscriptions()
            .await?
            .into_iter()
            .filter(|subscription| subscription.tenant_id == tenant_id)
            .collect())
    }
    async fn list_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_tenant_subscriptions(tenant_id)
            .await?
            .into_iter()
            .filter(|subscription| subscription.shared_group.as_deref() == shared_group)
            .collect())
    }
    async fn count_session_subscriptions(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_subscriptions(session_id).await?.len())
    }
    async fn list_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_subscriptions(session_id)
            .await?
            .into_iter()
            .filter(|subscription| subscription.topic_filter == topic_filter)
            .collect())
    }
    async fn count_session_topic_subscriptions(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await?
            .len())
    }
    async fn count_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_subscriptions(tenant_id).await?.len())
    }
    async fn count_tenant_shared_subscriptions(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_shared_subscriptions(tenant_id, shared_group)
            .await?
            .len())
    }
    async fn list_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_tenant_subscriptions(tenant_id)
            .await?
            .into_iter()
            .filter(|subscription| subscription.topic_filter == topic_filter)
            .collect())
    }
    async fn list_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<Subscription>> {
        Ok(self
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?
            .into_iter()
            .filter(|subscription| subscription.shared_group.as_deref() == shared_group)
            .collect())
    }
    async fn count_tenant_topic_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_topic_subscriptions(tenant_id, topic_filter)
            .await?
            .len())
    }
    async fn count_tenant_topic_shared_subscriptions(
        &self,
        tenant_id: &str,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<usize> {
        Ok(self
            .list_tenant_topic_shared_subscriptions(tenant_id, topic_filter, shared_group)
            .await?
            .len())
    }
    async fn list_subscriptions(&self, session_id: &str) -> anyhow::Result<Vec<Subscription>>;
    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>>;
    async fn count_subscriptions(&self) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait InboxStore: Send + Sync {
    async fn append_message(&self, message: &OfflineMessage) -> anyhow::Result<()>;
    async fn append_messages(&self, messages: &[OfflineMessage]) -> anyhow::Result<()> {
        for message in messages {
            self.append_message(message).await?;
        }
        Ok(())
    }
    async fn peek_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn count_session_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self.peek_messages(session_id).await?.len())
    }
    async fn list_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .list_all_messages()
            .await?
            .into_iter()
            .filter(|message| message.tenant_id == tenant_id)
            .collect())
    }
    async fn count_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_messages(tenant_id).await?.len())
    }
    async fn list_all_messages(&self) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn load_messages(&self, session_id: &str) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn purge_messages(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self.load_messages(session_id).await?.len())
    }
    async fn purge_tenant_messages(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let messages = self.list_tenant_messages(tenant_id).await?;
        let removed = messages.len();
        for session_id in messages.into_iter().map(|message| message.session_id) {
            let _ = self.purge_messages(&session_id).await?;
        }
        Ok(removed)
    }
    async fn count_messages(&self) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait InflightStore: Send + Sync {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()>;
    async fn save_inflight_batch(&self, messages: &[InflightMessage]) -> anyhow::Result<()> {
        for message in messages {
            self.save_inflight(message).await?;
        }
        Ok(())
    }
    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>>;
    async fn count_session_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        Ok(self.load_inflight(session_id).await?.len())
    }
    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        Ok(self
            .list_all_inflight()
            .await?
            .into_iter()
            .filter(|message| message.tenant_id == tenant_id)
            .collect())
    }
    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_inflight(tenant_id).await?.len())
    }
    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>>;
    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()>;
    async fn delete_inflight_batch(
        &self,
        session_id: &str,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        for packet_id in packet_ids {
            self.delete_inflight(session_id, *packet_id).await?;
        }
        Ok(())
    }
    async fn purge_inflight(&self, session_id: &str) -> anyhow::Result<usize> {
        let inflight = self.load_inflight(session_id).await?;
        let removed = inflight.len();
        for message in inflight {
            self.delete_inflight(session_id, message.packet_id).await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let inflight = self.list_tenant_inflight(tenant_id).await?;
        let removed = inflight.len();
        for message in inflight {
            self.delete_inflight(&message.session_id, message.packet_id)
                .await?;
        }
        Ok(removed)
    }
    async fn count_inflight(&self) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait RetainStore: Send + Sync {
    async fn put_retain(&self, message: &RetainedMessage) -> anyhow::Result<()>;
    async fn load_retain(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Option<RetainedMessage>> {
        Ok(self
            .match_retain(tenant_id, topic)
            .await?
            .into_iter()
            .find(|message| message.topic == topic))
    }
    async fn list_tenant_retained(&self, tenant_id: &str) -> anyhow::Result<Vec<RetainedMessage>> {
        self.match_retain(tenant_id, "#").await
    }
    async fn match_retain(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>>;
    async fn count_retained(&self) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait RouteStore: Send + Sync {
    async fn save_route(&self, route: &RouteRecord) -> anyhow::Result<()>;
    async fn delete_route(&self, route: &RouteRecord) -> anyhow::Result<()>;
    async fn load_session_topic_filter_route(
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
            .load_session_topic_filter_route(session_id, topic_filter, shared_group)
            .await?
        {
            self.delete_route(&route).await?;
            Ok(1)
        } else {
            Ok(0)
        }
    }
    async fn remove_session_routes(&self, session_id: &str) -> anyhow::Result<usize> {
        let routes = self.list_session_routes(session_id).await?;
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn remove_session_topic_filter_routes(
        &self,
        session_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let routes = self
            .list_session_topic_filter_routes(session_id, topic_filter)
            .await?;
        for route in &routes {
            self.delete_route(route).await?;
        }
        Ok(routes.len())
    }
    async fn remove_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let routes = self.list_tenant_routes(tenant_id).await?;
        for route in &routes {
            self.delete_route(route).await?;
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
            self.delete_route(route).await?;
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
            self.delete_route(route).await?;
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
            self.delete_route(route).await?;
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
            let mut updated = route.clone();
            updated.node_id = node_id;
            self.save_route(&updated).await?;
        }
        Ok(routes.len())
    }
    async fn list_routes(&self) -> anyhow::Result<Vec<RouteRecord>>;
    async fn list_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<Vec<RouteRecord>>;
    async fn count_tenant_routes(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_routes(tenant_id).await?.len())
    }
    async fn list_tenant_shared_routes(
        &self,
        tenant_id: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_tenant_routes(tenant_id)
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
        Ok(self
            .list_tenant_routes(tenant_id)
            .await?
            .into_iter()
            .filter(|route| route.topic_filter == topic_filter)
            .collect())
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
            .list_topic_filter_routes(tenant_id, topic_filter)
            .await?
            .into_iter()
            .filter(|route| route_topic_filter_is_exact(&route.topic_filter))
            .collect())
    }
    async fn list_tenant_wildcard_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_tenant_routes(tenant_id)
            .await?
            .into_iter()
            .filter(|route| !route_topic_filter_is_exact(&route.topic_filter))
            .collect())
    }
    async fn list_session_routes(&self, session_id: &str) -> anyhow::Result<Vec<RouteRecord>> {
        Ok(self
            .list_routes()
            .await?
            .into_iter()
            .filter(|route| route.session_id == session_id)
            .collect())
    }
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
            self.load_session_topic_filter_route(session_id, topic_filter, shared_group)
                .await?
                .is_some(),
        ))
    }
    async fn count_routes(&self) -> anyhow::Result<usize> {
        Ok(self.list_routes().await?.len())
    }
}
