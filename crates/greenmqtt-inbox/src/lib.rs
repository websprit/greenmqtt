use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{
    InflightMessage, InflightPhase, OfflineMessage, PublishProperties, PublishRequest,
    RangeBoundary, ServiceShardKey, ServiceShardKind, SessionId, Subscription,
};
use greenmqtt_kv_client::RangeDataClient;
use greenmqtt_kv_engine::KvMutation;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;

#[async_trait]
pub trait InboxService: Send + Sync {
    async fn attach(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn detach(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn attach_batch(&self, session_ids: &[SessionId]) -> anyhow::Result<()> {
        for session_id in session_ids {
            self.attach(session_id).await?;
        }
        Ok(())
    }
    async fn detach_batch(&self, session_ids: &[SessionId]) -> anyhow::Result<()> {
        for session_id in session_ids {
            self.detach(session_id).await?;
        }
        Ok(())
    }
    async fn register_delayed_lwt(
        &self,
        generation: u64,
        publish: DelayedLwtPublish,
    ) -> anyhow::Result<()>;
    async fn clear_delayed_lwt(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn send_delayed_lwt(
        &self,
        session_id: &SessionId,
        generation: u64,
        sink: &dyn DelayedLwtSink,
    ) -> anyhow::Result<InboxLwtResult>;
    async fn purge_session(&self, session_id: &SessionId) -> anyhow::Result<()>;
    async fn purge_session_subscriptions_only(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<usize> {
        let subscriptions = self.list_subscriptions(session_id).await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn purge_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<usize> {
        let subscriptions = self
            .list_session_topic_subscriptions(session_id, topic_filter)
            .await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_subscriptions(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let subscriptions = self.list_tenant_subscriptions(tenant_id).await?;
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
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
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
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
        let removed = subscriptions.len();
        for subscription in subscriptions {
            self.unsubscribe_shared(
                &subscription.session_id,
                &subscription.topic_filter,
                subscription.shared_group.as_deref(),
            )
            .await?;
        }
        Ok(removed)
    }
    async fn count_session_subscriptions(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.list_subscriptions(session_id).await?.len())
    }
    async fn list_session_topic_subscriptions(
        &self,
        session_id: &SessionId,
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
        session_id: &SessionId,
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
    async fn purge_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.fetch(session_id).await?.len())
    }
    async fn purge_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let messages = self.list_tenant_offline(tenant_id).await?;
        let removed = messages.len();
        let session_ids: std::collections::BTreeSet<_> = messages
            .into_iter()
            .map(|message| message.session_id)
            .collect();
        for session_id in session_ids {
            let _ = self.purge_offline(&session_id).await?;
        }
        Ok(removed)
    }
    async fn count_session_offline(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.peek(session_id).await?.len())
    }
    async fn count_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_offline(tenant_id).await?.len())
    }
    async fn purge_inflight_session(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        let messages = self.fetch_inflight(session_id).await?;
        let removed = messages.len();
        for message in messages {
            self.ack_inflight(session_id, message.packet_id).await?;
        }
        Ok(removed)
    }
    async fn purge_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let messages = self.list_tenant_inflight(tenant_id).await?;
        let removed = messages.len();
        let session_ids: std::collections::BTreeSet<_> = messages
            .into_iter()
            .map(|message| message.session_id)
            .collect();
        for session_id in session_ids {
            let _ = self.purge_inflight_session(&session_id).await?;
        }
        Ok(removed)
    }
    async fn count_session_inflight(&self, session_id: &SessionId) -> anyhow::Result<usize> {
        Ok(self.fetch_inflight(session_id).await?.len())
    }
    async fn count_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<usize> {
        Ok(self.list_tenant_inflight(tenant_id).await?.len())
    }
    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()>;
    async fn lookup_subscription(
        &self,
        session_id: &SessionId,
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
    async fn unsubscribe(&self, session_id: &SessionId, topic_filter: &str)
        -> anyhow::Result<bool>;
    async fn unsubscribe_shared(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool>;
    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()>;
    async fn enqueue_batch(&self, messages: Vec<OfflineMessage>) -> anyhow::Result<()> {
        for message in messages {
            self.enqueue(message).await?;
        }
        Ok(())
    }
    async fn fetch_batch(
        &self,
        session_ids: &[SessionId],
    ) -> anyhow::Result<BTreeMap<SessionId, Vec<OfflineMessage>>> {
        let mut fetched = BTreeMap::new();
        for session_id in session_ids {
            fetched.insert(session_id.clone(), self.fetch(session_id).await?);
        }
        Ok(fetched)
    }
    async fn list_subscriptions(&self, session_id: &SessionId)
        -> anyhow::Result<Vec<Subscription>>;
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
    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>>;
    async fn peek(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn list_tenant_offline(&self, tenant_id: &str) -> anyhow::Result<Vec<OfflineMessage>> {
        Ok(self
            .list_all_offline()
            .await?
            .into_iter()
            .filter(|message| message.tenant_id == tenant_id)
            .collect())
    }
    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn fetch(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>>;
    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()>;
    async fn stage_inflight_batch(&self, messages: Vec<InflightMessage>) -> anyhow::Result<()> {
        for message in messages {
            self.stage_inflight(message).await?;
        }
        Ok(())
    }
    async fn commit_batch(&self, commits: &[InboxInflightCommit]) -> anyhow::Result<()> {
        for commit in commits {
            self.ack_inflight_batch(&commit.session_id, &commit.packet_ids)
                .await?;
        }
        Ok(())
    }
    async fn promote_inflight_release(
        &self,
        session_id: &SessionId,
        packet_id: u16,
    ) -> anyhow::Result<bool> {
        let Some(mut message) = self
            .fetch_inflight(session_id)
            .await?
            .into_iter()
            .find(|message| message.packet_id == packet_id)
        else {
            return Ok(false);
        };
        if message.qos != 2 {
            return Ok(false);
        }
        if message.phase == InflightPhase::Release {
            return Ok(true);
        }
        message.phase = InflightPhase::Release;
        self.stage_inflight(message).await?;
        Ok(true)
    }
    async fn ack_inflight(&self, session_id: &SessionId, packet_id: u16) -> anyhow::Result<()>;
    async fn ack_inflight_batch(
        &self,
        session_id: &SessionId,
        packet_ids: &[u16],
    ) -> anyhow::Result<()> {
        for packet_id in packet_ids {
            self.ack_inflight(session_id, *packet_id).await?;
        }
        Ok(())
    }
    async fn fetch_inflight(&self, session_id: &SessionId) -> anyhow::Result<Vec<InflightMessage>>;
    async fn list_tenant_inflight(&self, tenant_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        Ok(self
            .list_all_inflight()
            .await?
            .into_iter()
            .filter(|message| message.tenant_id == tenant_id)
            .collect())
    }
    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>>;
    async fn subscription_count(&self) -> anyhow::Result<usize>;
    async fn offline_count(&self) -> anyhow::Result<usize>;
    async fn inflight_count(&self) -> anyhow::Result<usize>;
    async fn subscribe_batch(&self, subscriptions: Vec<Subscription>) -> anyhow::Result<()> {
        for subscription in subscriptions {
            self.subscribe(subscription).await?;
        }
        Ok(())
    }
    async fn unsubscribe_batch(
        &self,
        requests: &[InboxUnsubscribeSpec],
    ) -> anyhow::Result<usize> {
        let mut removed = 0usize;
        for request in requests {
            if self
                .unsubscribe_shared(
                    &request.session_id,
                    &request.topic_filter,
                    request.shared_group.as_deref(),
                )
                .await?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    async fn expire_messages(&self, now_ms: u64) -> anyhow::Result<InboxExpiryStats> {
        let _ = now_ms;
        Ok(InboxExpiryStats::default())
    }
    async fn expire_tenant_messages(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats> {
        let _ = tenant_id;
        let _ = now_ms;
        Ok(InboxExpiryStats::default())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InboxExpiryStats {
    pub offline_messages: usize,
    pub inflight_messages: usize,
}

impl InboxExpiryStats {
    pub fn total(self) -> usize {
        self.offline_messages + self.inflight_messages
    }

    pub fn is_empty(self) -> bool {
        self.total() == 0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InboxTenantGcPreview {
    pub subscriptions: usize,
    pub offline_messages: usize,
    pub inflight_messages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayedLwtPublish {
    pub tenant_id: String,
    pub session_id: SessionId,
    pub publish: PublishRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredDelayedLwt {
    pub generation: u64,
    pub publish: DelayedLwtPublish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxLwtResult {
    Ok,
    NoInbox,
    Conflict,
    NoDetach,
    NoLwt,
    DistTryLater,
    RetainTryLater,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxDelayedTask {
    ExpireSession { session_id: SessionId, now_ms: u64 },
    ExpireTenant { tenant_id: String, now_ms: u64 },
    SendLwt(DelayedLwtPublish),
}

#[async_trait]
pub trait InboxDelayedTaskHandler: Send + Sync {
    async fn expire_session(
        &self,
        session_id: &SessionId,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats>;
    async fn expire_tenant(&self, tenant_id: &str, now_ms: u64)
        -> anyhow::Result<InboxExpiryStats>;
    async fn send_lwt(&self, publish: &DelayedLwtPublish) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct InboxDelayTaskRunner {
    handler: Arc<dyn InboxDelayedTaskHandler>,
    max_retries: usize,
    retry_delay: Duration,
}

impl InboxDelayTaskRunner {
    pub fn new(
        handler: Arc<dyn InboxDelayedTaskHandler>,
        max_retries: usize,
        retry_delay: Duration,
    ) -> Self {
        Self {
            handler,
            max_retries,
            retry_delay,
        }
    }

    async fn run_once(&self, task: &InboxDelayedTask) -> anyhow::Result<()> {
        match task {
            InboxDelayedTask::ExpireSession { session_id, now_ms } => {
                let _ = self.handler.expire_session(session_id, *now_ms).await?;
            }
            InboxDelayedTask::ExpireTenant { tenant_id, now_ms } => {
                let _ = self.handler.expire_tenant(tenant_id, *now_ms).await?;
            }
            InboxDelayedTask::SendLwt(publish) => {
                self.handler.send_lwt(publish).await?;
            }
        }
        Ok(())
    }

    pub async fn run_with_retry(&self, task: InboxDelayedTask) -> anyhow::Result<()> {
        let mut attempts = 0usize;
        loop {
            match self.run_once(&task).await {
                Ok(()) => return Ok(()),
                Err(error) if attempts < self.max_retries => {
                    attempts += 1;
                    tokio::time::sleep(self.retry_delay).await;
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn schedule(
        &self,
        task: InboxDelayedTask,
        delay: Duration,
    ) -> JoinHandle<anyhow::Result<()>> {
        let runner = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            runner.run_with_retry(task).await
        })
    }
}

#[derive(Clone)]
pub struct InboxServiceDelayHandler {
    inbox: Arc<dyn InboxService>,
    lwt_sink: Arc<dyn DelayedLwtSink>,
}

impl InboxServiceDelayHandler {
    pub fn new(inbox: Arc<dyn InboxService>, lwt_sink: Arc<dyn DelayedLwtSink>) -> Self {
        Self { inbox, lwt_sink }
    }
}

#[async_trait]
impl InboxDelayedTaskHandler for InboxServiceDelayHandler {
    async fn expire_session(
        &self,
        session_id: &SessionId,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats> {
        let _ = session_id;
        self.inbox.expire_messages(now_ms).await
    }

    async fn expire_tenant(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats> {
        self.inbox.expire_tenant_messages(tenant_id, now_ms).await
    }

    async fn send_lwt(&self, publish: &DelayedLwtPublish) -> anyhow::Result<()> {
        self.lwt_sink.send_lwt(publish).await
    }
}

#[async_trait]
pub trait DelayedLwtSink: Send + Sync {
    async fn send_lwt(&self, publish: &DelayedLwtPublish) -> anyhow::Result<()>;
}

pub async fn inbox_send_lwt(
    inbox: &dyn InboxService,
    session_id: &SessionId,
    generation: u64,
    sink: &dyn DelayedLwtSink,
) -> anyhow::Result<InboxLwtResult> {
    inbox.send_delayed_lwt(session_id, generation, sink).await
}

pub(crate) fn delayed_lwt_result_from_error(error: &anyhow::Error) -> InboxLwtResult {
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("dist") && message.contains("try later") {
        InboxLwtResult::DistTryLater
    } else if message.contains("retain") && message.contains("try later") {
        InboxLwtResult::RetainTryLater
    } else {
        InboxLwtResult::Error
    }
}

pub async fn inbox_expire_all(
    inbox: &dyn InboxService,
    tenant_id: &str,
    now_ms: u64,
) -> anyhow::Result<InboxExpiryStats> {
    inbox.expire_tenant_messages(tenant_id, now_ms).await
}

pub async fn inbox_tenant_gc_preview(
    inbox: &dyn InboxService,
    tenant_id: &str,
) -> anyhow::Result<InboxTenantGcPreview> {
    Ok(InboxTenantGcPreview {
        subscriptions: inbox.count_tenant_subscriptions(tenant_id).await?,
        offline_messages: inbox.count_tenant_offline(tenant_id).await?,
        inflight_messages: inbox.count_tenant_inflight(tenant_id).await?,
    })
}

pub async fn inbox_tenant_gc_run(
    inbox: &dyn InboxService,
    tenant_id: &str,
    now_ms: u64,
) -> anyhow::Result<InboxExpiryStats> {
    inbox.expire_tenant_messages(tenant_id, now_ms).await
}

#[derive(Clone)]
pub struct TenantInboxGcRunner {
    inbox: Arc<dyn InboxService>,
}

impl TenantInboxGcRunner {
    pub fn new(inbox: Arc<dyn InboxService>) -> Self {
        Self { inbox }
    }

    pub async fn run_tenant(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats> {
        self.inbox.expire_tenant_messages(tenant_id, now_ms).await
    }

    pub async fn run_all(&self, now_ms: u64) -> anyhow::Result<InboxExpiryStats> {
        self.inbox.expire_messages(now_ms).await
    }

    pub fn spawn_tenant(
        &self,
        tenant_id: String,
        interval: Duration,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let runner = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let _ = runner.run_tenant(&tenant_id, now_ms()).await?;
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxUnsubscribeSpec {
    pub session_id: SessionId,
    pub topic_filter: String,
    pub shared_group: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxInflightCommit {
    pub session_id: SessionId,
    pub packet_ids: Vec<u16>,
}

#[derive(Clone)]
pub struct InboxBatchScheduler {
    inbox: Arc<dyn InboxService>,
}

impl InboxBatchScheduler {
    pub fn new(inbox: Arc<dyn InboxService>) -> Self {
        Self { inbox }
    }

    pub async fn attach_all(&self, session_ids: &[SessionId]) -> anyhow::Result<()> {
        self.inbox.attach_batch(session_ids).await
    }

    pub async fn detach_all(&self, session_ids: &[SessionId]) -> anyhow::Result<()> {
        self.inbox.detach_batch(session_ids).await
    }

    pub async fn fetch_all(
        &self,
        session_ids: &[SessionId],
    ) -> anyhow::Result<BTreeMap<SessionId, Vec<OfflineMessage>>> {
        self.inbox.fetch_batch(session_ids).await
    }

    pub async fn commit_all(&self, commits: &[InboxInflightCommit]) -> anyhow::Result<()> {
        self.inbox.commit_batch(commits).await
    }

    pub async fn insert_all(&self, messages: Vec<OfflineMessage>) -> anyhow::Result<()> {
        self.inbox.enqueue_batch(messages).await
    }

    pub async fn subscribe_all(&self, subscriptions: Vec<Subscription>) -> anyhow::Result<()> {
        self.inbox.subscribe_batch(subscriptions).await
    }

    pub async fn unsubscribe_all(
        &self,
        requests: &[InboxUnsubscribeSpec],
    ) -> anyhow::Result<usize> {
        self.inbox.unsubscribe_batch(requests).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxTenantStats {
    pub tenant_id: String,
    pub subscriptions: usize,
    pub offline_messages: usize,
    pub inflight_messages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxBalanceAction {
    SplitTenantRange {
        tenant_id: String,
    },
    ScaleTenantReplicas {
        tenant_id: String,
        voters: usize,
        learners: usize,
    },
    RunTenantMaintenance {
        tenant_id: String,
    },
}

pub trait InboxBalancePolicy: Send + Sync {
    fn propose(&self, tenants: &[InboxTenantStats]) -> Vec<InboxBalanceAction>;
}

#[derive(Debug, Clone)]
pub struct ThresholdInboxBalancePolicy {
    pub max_subscriptions: usize,
    pub max_offline_messages: usize,
    pub max_inflight_messages: usize,
    pub desired_voters: usize,
    pub desired_learners: usize,
}

impl InboxBalancePolicy for ThresholdInboxBalancePolicy {
    fn propose(&self, tenants: &[InboxTenantStats]) -> Vec<InboxBalanceAction> {
        let mut actions = Vec::new();
        for tenant in tenants {
            let overloaded = tenant.subscriptions > self.max_subscriptions
                || tenant.offline_messages > self.max_offline_messages
                || tenant.inflight_messages > self.max_inflight_messages;
            if overloaded {
                actions.push(InboxBalanceAction::RunTenantMaintenance {
                    tenant_id: tenant.tenant_id.clone(),
                });
                actions.push(InboxBalanceAction::SplitTenantRange {
                    tenant_id: tenant.tenant_id.clone(),
                });
                actions.push(InboxBalanceAction::ScaleTenantReplicas {
                    tenant_id: tenant.tenant_id.clone(),
                    voters: self.desired_voters,
                    learners: self.desired_learners,
                });
            }
        }
        actions
    }
}

#[derive(Clone)]
pub struct InboxMaintenanceWorker {
    gc_runner: TenantInboxGcRunner,
    delay_runner: InboxDelayTaskRunner,
}

impl InboxMaintenanceWorker {
    pub fn new(gc_runner: TenantInboxGcRunner, delay_runner: InboxDelayTaskRunner) -> Self {
        Self {
            gc_runner,
            delay_runner,
        }
    }

    pub async fn run_tenant_gc(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats> {
        self.gc_runner.run_tenant(tenant_id, now_ms).await
    }

    pub fn schedule_tenant_gc(
        &self,
        tenant_id: String,
        interval: Duration,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> JoinHandle<anyhow::Result<()>> {
        self.gc_runner.spawn_tenant(tenant_id, interval, now_ms)
    }

    pub fn schedule_delayed_task(
        &self,
        task: InboxDelayedTask,
        delay: Duration,
    ) -> JoinHandle<anyhow::Result<()>> {
        self.delay_runner.schedule(task, delay)
    }
}

fn publish_properties_expired_at(properties: &PublishProperties, now_ms: u64) -> bool {
    let Some(expiry_secs) = properties.message_expiry_interval_secs else {
        return false;
    };
    let Some(stored_at_ms) = properties.stored_at_ms else {
        return false;
    };
    now_ms.saturating_sub(stored_at_ms) >= u64::from(expiry_secs) * 1000
}

#[derive(Clone)]
pub struct ReplicatedInboxHandle {
    client: Arc<dyn RangeDataClient>,
    next_seq: Arc<AtomicU64>,
    active_sessions: Arc<RwLock<HashMap<String, bool>>>,
    delayed_lwts: Arc<RwLock<HashMap<String, RegisteredDelayedLwt>>>,
}

impl ReplicatedInboxHandle {
    pub fn new(client: Arc<dyn RangeDataClient>) -> Self {
        Self {
            client,
            next_seq: Arc::new(AtomicU64::new(0)),
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
            delayed_lwts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn remove_offline_messages(&self, messages: &[OfflineMessage]) -> anyhow::Result<usize> {
        let mut removed = 0usize;
        for message in messages {
            let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
            let entries = self.client.scan(&range_id, None, usize::MAX).await?;
            let mut removals = Vec::new();
            for (key, value) in entries {
                if !key.starts_with(OFFLINE_KEY_PREFIX) {
                    continue;
                }
                let candidate = decode_offline(&value)?;
                if candidate.session_id == message.session_id
                    && candidate.topic == message.topic
                    && candidate.from_session_id == message.from_session_id
                    && candidate.payload == message.payload
                    && candidate.properties == message.properties
                {
                    removals.push(KvMutation { key, value: None });
                }
            }
            if !removals.is_empty() {
                removed += removals.len();
                self.client.apply(&range_id, removals).await?;
            }
        }
        Ok(removed)
    }

    async fn remove_inflight_messages(
        &self,
        messages: &[InflightMessage],
    ) -> anyhow::Result<usize> {
        let mut removed = 0usize;
        let mut grouped: BTreeMap<String, Vec<KvMutation>> = BTreeMap::new();
        for message in messages {
            let range_id = self
                .inflight_range_id_for_tenant(&message.tenant_id)
                .await?;
            grouped.entry(range_id).or_default().push(KvMutation {
                key: inflight_key(&message.session_id, message.packet_id),
                value: None,
            });
            removed += 1;
        }
        for (range_id, removals) in grouped {
            self.client.apply(&range_id, removals).await?;
        }
        Ok(removed)
    }

    async fn inbox_range_id_for_tenant(&self, tenant_id: &str) -> anyhow::Result<String> {
        let shard = inbox_tenant_scan_shard(tenant_id);
        self.client
            .route_shard(&shard)
            .await?
            .into_iter()
            .next()
            .map(|route| route.descriptor.id)
            .ok_or_else(|| anyhow::anyhow!("no inbox range available for tenant {tenant_id}"))
    }

    async fn inflight_range_id_for_tenant(&self, tenant_id: &str) -> anyhow::Result<String> {
        let shard = inflight_tenant_scan_shard(tenant_id);
        self.client
            .route_shard(&shard)
            .await?
            .into_iter()
            .next()
            .map(|route| route.descriptor.id)
            .ok_or_else(|| anyhow::anyhow!("no inflight range available for tenant {tenant_id}"))
    }

    async fn scan_inbox_kind(&self, kind: ServiceShardKind) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut entries = Vec::new();
        for descriptor in self.client.list_ranges().await? {
            if descriptor.shard.kind != kind {
                continue;
            }
            entries.extend(self.client.scan(&descriptor.id, None, usize::MAX).await?);
        }
        Ok(entries)
    }

    async fn scan_kind_prefix(
        &self,
        kind: ServiceShardKind,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut entries = Vec::new();
        let boundary = prefix_boundary(prefix);
        for descriptor in self.client.list_ranges().await? {
            if descriptor.shard.kind != kind {
                continue;
            }
            entries.extend(
                self.client
                    .scan(&descriptor.id, Some(boundary.clone()), usize::MAX)
                    .await?,
            );
        }
        Ok(entries)
    }

    async fn get_from_kind_ranges(
        &self,
        kind: ServiceShardKind,
        key: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        for descriptor in self.client.list_ranges().await? {
            if descriptor.shard.kind != kind {
                continue;
            }
            if let Some(value) = self.client.get(&descriptor.id, key).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    async fn next_offline_seq(&self) -> anyhow::Result<u64> {
        let current = self.next_seq.load(Ordering::Relaxed);
        if current > 0 {
            return Ok(self.next_seq.fetch_add(1, Ordering::Relaxed));
        }
        let mut max_seq = 0u64;
        for (key, _) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Some(seq) = offline_seq_from_key(&key) {
                max_seq = max_seq.max(seq);
            }
        }
        let next = max_seq.saturating_add(1);
        let _ = self
            .next_seq
            .compare_exchange(0, next, Ordering::SeqCst, Ordering::SeqCst);
        Ok(self.next_seq.fetch_add(1, Ordering::Relaxed))
    }
}

const SUBSCRIPTION_KEY_PREFIX: &[u8] = b"sub\0";
const OFFLINE_KEY_PREFIX: &[u8] = b"offline\0";
const INFLIGHT_KEY_PREFIX: &[u8] = b"inflight\0";

fn subscription_key(
    session_id: &SessionId,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> Bytes {
    let mut key = Vec::with_capacity(
        SUBSCRIPTION_KEY_PREFIX.len()
            + session_id.len()
            + topic_filter.len()
            + shared_group.unwrap_or_default().len()
            + 2,
    );
    key.extend_from_slice(SUBSCRIPTION_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(shared_group.unwrap_or_default().as_bytes());
    key.push(0);
    key.extend_from_slice(topic_filter.as_bytes());
    Bytes::from(key)
}

fn offline_key(session_id: &SessionId, seq: u64) -> Bytes {
    let mut key = Vec::with_capacity(OFFLINE_KEY_PREFIX.len() + session_id.len() + 9);
    key.extend_from_slice(OFFLINE_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&seq.to_be_bytes());
    Bytes::from(key)
}

fn offline_seq_from_key(key: &[u8]) -> Option<u64> {
    if !key.starts_with(OFFLINE_KEY_PREFIX) || key.len() < 8 {
        return None;
    }
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&key[key.len().saturating_sub(8)..]);
    Some(u64::from_be_bytes(raw))
}

fn inflight_key(session_id: &SessionId, packet_id: u16) -> Bytes {
    let mut key = Vec::with_capacity(INFLIGHT_KEY_PREFIX.len() + session_id.len() + 3);
    key.extend_from_slice(INFLIGHT_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&packet_id.to_be_bytes());
    Bytes::from(key)
}

fn subscription_session_prefix(session_id: &SessionId) -> Bytes {
    let mut key = Vec::with_capacity(SUBSCRIPTION_KEY_PREFIX.len() + session_id.len() + 1);
    key.extend_from_slice(SUBSCRIPTION_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    Bytes::from(key)
}

fn offline_session_prefix(session_id: &SessionId) -> Bytes {
    let mut key = Vec::with_capacity(OFFLINE_KEY_PREFIX.len() + session_id.len() + 1);
    key.extend_from_slice(OFFLINE_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    Bytes::from(key)
}

fn inflight_session_prefix(session_id: &SessionId) -> Bytes {
    let mut key = Vec::with_capacity(INFLIGHT_KEY_PREFIX.len() + session_id.len() + 1);
    key.extend_from_slice(INFLIGHT_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key.push(0);
    Bytes::from(key)
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

fn encode_subscription(subscription: &Subscription) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(subscription)?))
}

fn decode_subscription(value: &[u8]) -> anyhow::Result<Subscription> {
    Ok(bincode::deserialize(value)?)
}

fn encode_offline(message: &OfflineMessage) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(message)?))
}

fn decode_offline(value: &[u8]) -> anyhow::Result<OfflineMessage> {
    Ok(bincode::deserialize(value)?)
}

fn encode_inflight(message: &InflightMessage) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(message)?))
}

fn decode_inflight(value: &[u8]) -> anyhow::Result<InflightMessage> {
    Ok(bincode::deserialize(value)?)
}

pub fn inbox_session_shard(tenant_id: &str, session_id: &SessionId) -> ServiceShardKey {
    ServiceShardKey::inbox(tenant_id.to_string(), session_id.clone())
}

pub fn inflight_session_shard(tenant_id: &str, session_id: &SessionId) -> ServiceShardKey {
    ServiceShardKey::inflight(tenant_id.to_string(), session_id.clone())
}

pub fn subscription_shard(subscription: &Subscription) -> ServiceShardKey {
    inbox_session_shard(&subscription.tenant_id, &subscription.session_id)
}

pub fn inbox_tenant_scan_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::Inbox,
        tenant_id: tenant_id.to_string(),
        scope: "*".into(),
    }
}

pub fn inflight_tenant_scan_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::Inflight,
        tenant_id: tenant_id.to_string(),
        scope: "*".into(),
    }
}

#[async_trait]
impl InboxService for ReplicatedInboxHandle {
    async fn attach(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.active_sessions
            .write()
            .expect("replicated inbox poisoned")
            .insert(session_id.clone(), true);
        Ok(())
    }

    async fn detach(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.active_sessions
            .write()
            .expect("replicated inbox poisoned")
            .insert(session_id.clone(), false);
        Ok(())
    }

    async fn attach_batch(&self, session_ids: &[SessionId]) -> anyhow::Result<()> {
        let mut guard = self
            .active_sessions
            .write()
            .expect("replicated inbox poisoned");
        for session_id in session_ids {
            guard.insert(session_id.clone(), true);
        }
        Ok(())
    }

    async fn detach_batch(&self, session_ids: &[SessionId]) -> anyhow::Result<()> {
        let mut guard = self
            .active_sessions
            .write()
            .expect("replicated inbox poisoned");
        for session_id in session_ids {
            guard.insert(session_id.clone(), false);
        }
        Ok(())
    }

    async fn register_delayed_lwt(
        &self,
        generation: u64,
        publish: DelayedLwtPublish,
    ) -> anyhow::Result<()> {
        let session_id = publish.session_id.clone();
        self.active_sessions
            .write()
            .expect("replicated inbox poisoned")
            .entry(session_id.clone())
            .or_insert(true);
        self.delayed_lwts
            .write()
            .expect("replicated inbox poisoned")
            .insert(
                session_id,
                RegisteredDelayedLwt {
                    generation,
                    publish,
                },
            );
        Ok(())
    }

    async fn clear_delayed_lwt(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.delayed_lwts
            .write()
            .expect("replicated inbox poisoned")
            .remove(session_id);
        Ok(())
    }

    async fn send_delayed_lwt(
        &self,
        session_id: &SessionId,
        generation: u64,
        sink: &dyn DelayedLwtSink,
    ) -> anyhow::Result<InboxLwtResult> {
        let attached = self
            .active_sessions
            .read()
            .expect("replicated inbox poisoned")
            .get(session_id)
            .copied();
        let registered = self
            .delayed_lwts
            .read()
            .expect("replicated inbox poisoned")
            .get(session_id)
            .cloned();
        let registered = match (attached, registered) {
            (None, None) => return Ok(InboxLwtResult::NoInbox),
            (_, None) => return Ok(InboxLwtResult::NoLwt),
            (_, Some(registered)) if registered.generation != generation => {
                return Ok(InboxLwtResult::Conflict)
            }
            (Some(true), Some(_)) => return Ok(InboxLwtResult::NoDetach),
            (_, Some(registered)) => registered,
        };
        match sink.send_lwt(&registered.publish).await {
            Ok(()) => {
                self.delayed_lwts
                    .write()
                    .expect("replicated inbox poisoned")
                    .remove(session_id);
                Ok(InboxLwtResult::Ok)
            }
            Err(error) => Ok(delayed_lwt_result_from_error(&error)),
        }
    }

    async fn purge_session(&self, session_id: &SessionId) -> anyhow::Result<()> {
        self.active_sessions
            .write()
            .expect("replicated inbox poisoned")
            .remove(session_id);
        self.delayed_lwts
            .write()
            .expect("replicated inbox poisoned")
            .remove(session_id);
        let subscriptions = self.list_subscriptions(session_id).await?;
        let offline = self.peek(session_id).await?;
        let inflight = self.fetch_inflight(session_id).await?;

        for subscription in subscriptions {
            let range_id = self
                .inbox_range_id_for_tenant(&subscription.tenant_id)
                .await?;
            self.client
                .apply(
                    &range_id,
                    vec![KvMutation {
                        key: subscription_key(
                            &subscription.session_id,
                            &subscription.topic_filter,
                            subscription.shared_group.as_deref(),
                        ),
                        value: None,
                    }],
                )
                .await?;
        }

        for message in offline {
            let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
            let entries = self.client.scan(&range_id, None, usize::MAX).await?;
            let mut removals = Vec::new();
            for (key, value) in entries {
                if !key.starts_with(OFFLINE_KEY_PREFIX) {
                    continue;
                }
                let candidate = decode_offline(&value)?;
                if candidate.session_id == *session_id
                    && candidate.topic == message.topic
                    && candidate.from_session_id == message.from_session_id
                {
                    removals.push(KvMutation { key, value: None });
                }
            }
            if !removals.is_empty() {
                self.client.apply(&range_id, removals).await?;
            }
        }

        for message in inflight {
            let range_id = self
                .inflight_range_id_for_tenant(&message.tenant_id)
                .await?;
            self.client
                .apply(
                    &range_id,
                    vec![KvMutation {
                        key: inflight_key(session_id, message.packet_id),
                        value: None,
                    }],
                )
                .await?;
        }
        Ok(())
    }

    async fn subscribe(&self, subscription: Subscription) -> anyhow::Result<()> {
        let range_id = self
            .inbox_range_id_for_tenant(&subscription.tenant_id)
            .await?;
        self.client
            .apply(
                &range_id,
                vec![KvMutation {
                    key: subscription_key(
                        &subscription.session_id,
                        &subscription.topic_filter,
                        subscription.shared_group.as_deref(),
                    ),
                    value: Some(encode_subscription(&subscription)?),
                }],
            )
            .await
    }

    async fn subscribe_batch(&self, subscriptions: Vec<Subscription>) -> anyhow::Result<()> {
        let mut grouped: BTreeMap<String, Vec<KvMutation>> = BTreeMap::new();
        for subscription in subscriptions {
            let range_id = self
                .inbox_range_id_for_tenant(&subscription.tenant_id)
                .await?;
            grouped.entry(range_id).or_default().push(KvMutation {
                key: subscription_key(
                    &subscription.session_id,
                    &subscription.topic_filter,
                    subscription.shared_group.as_deref(),
                ),
                value: Some(encode_subscription(&subscription)?),
            });
        }
        for (range_id, mutations) in grouped {
            self.client.apply(&range_id, mutations).await?;
        }
        Ok(())
    }

    async fn unsubscribe(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
    ) -> anyhow::Result<bool> {
        self.unsubscribe_shared(session_id, topic_filter, None)
            .await
    }

    async fn unsubscribe_shared(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<bool> {
        let Some(subscription) = self
            .lookup_subscription(session_id, topic_filter, shared_group)
            .await?
        else {
            return Ok(false);
        };
        let range_id = self
            .inbox_range_id_for_tenant(&subscription.tenant_id)
            .await?;
        self.client
            .apply(
                &range_id,
                vec![KvMutation {
                    key: subscription_key(session_id, topic_filter, shared_group),
                    value: None,
                }],
            )
            .await?;
        Ok(true)
    }

    async fn unsubscribe_batch(
        &self,
        requests: &[InboxUnsubscribeSpec],
    ) -> anyhow::Result<usize> {
        let mut grouped: BTreeMap<String, Vec<KvMutation>> = BTreeMap::new();
        let mut removed = 0usize;
        for request in requests {
            let Some(subscription) = self
                .lookup_subscription(
                    &request.session_id,
                    &request.topic_filter,
                    request.shared_group.as_deref(),
                )
                .await?
            else {
                continue;
            };
            let range_id = self
                .inbox_range_id_for_tenant(&subscription.tenant_id)
                .await?;
            grouped.entry(range_id).or_default().push(KvMutation {
                key: subscription_key(
                    &request.session_id,
                    &request.topic_filter,
                    request.shared_group.as_deref(),
                ),
                value: None,
            });
            removed += 1;
        }
        for (range_id, mutations) in grouped {
            self.client.apply(&range_id, mutations).await?;
        }
        Ok(removed)
    }

    async fn lookup_subscription(
        &self,
        session_id: &SessionId,
        topic_filter: &str,
        shared_group: Option<&str>,
    ) -> anyhow::Result<Option<Subscription>> {
        let key = subscription_key(session_id, topic_filter, shared_group);
        Ok(self
            .get_from_kind_ranges(ServiceShardKind::Inbox, &key)
            .await?
            .map(|value| decode_subscription(&value))
            .transpose()?)
    }

    async fn enqueue(&self, message: OfflineMessage) -> anyhow::Result<()> {
        let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
        let seq = self.next_offline_seq().await?;
        self.client
            .apply(
                &range_id,
                vec![KvMutation {
                    key: offline_key(&message.session_id, seq),
                    value: Some(encode_offline(&message)?),
                }],
            )
            .await
    }

    async fn enqueue_batch(&self, messages: Vec<OfflineMessage>) -> anyhow::Result<()> {
        let mut grouped: BTreeMap<String, Vec<KvMutation>> = BTreeMap::new();
        for message in messages {
            let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
            let seq = self.next_offline_seq().await?;
            grouped.entry(range_id).or_default().push(KvMutation {
                key: offline_key(&message.session_id, seq),
                value: Some(encode_offline(&message)?),
            });
        }
        for (range_id, mutations) in grouped {
            self.client.apply(&range_id, mutations).await?;
        }
        Ok(())
    }

    async fn list_subscriptions(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for (_, value) in self
            .scan_kind_prefix(
                ServiceShardKind::Inbox,
                &subscription_session_prefix(session_id),
            )
            .await?
        {
            if let Ok(subscription) = decode_subscription(&value) {
                if subscription.session_id == *session_id {
                    subscriptions.push(subscription);
                }
            }
        }
        subscriptions.sort_by(|left, right| {
            left.topic_filter
                .cmp(&right.topic_filter)
                .then(left.shared_group.cmp(&right.shared_group))
        });
        Ok(subscriptions)
    }

    async fn list_all_subscriptions(&self) -> anyhow::Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Ok(subscription) = decode_subscription(&value) {
                subscriptions.push(subscription);
            }
        }
        subscriptions.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.topic_filter.cmp(&right.topic_filter))
        });
        Ok(subscriptions)
    }

    async fn peek(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self
            .scan_kind_prefix(ServiceShardKind::Inbox, &offline_session_prefix(session_id))
            .await?
        {
            if let Ok(message) = decode_offline(&value) {
                if message.session_id == *session_id {
                    messages.push(message);
                }
            }
        }
        Ok(messages)
    }

    async fn list_all_offline(&self) -> anyhow::Result<Vec<OfflineMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inbox).await? {
            if let Ok(message) = decode_offline(&value) {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    async fn fetch(&self, session_id: &SessionId) -> anyhow::Result<Vec<OfflineMessage>> {
        let messages = self.peek(session_id).await?;
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        for message in &messages {
            let range_id = self.inbox_range_id_for_tenant(&message.tenant_id).await?;
            let entries = self
                .client
                .scan(
                    &range_id,
                    Some(prefix_boundary(&offline_session_prefix(session_id))),
                    usize::MAX,
                )
                .await?;
            let mut removals = Vec::new();
            for (key, value) in entries {
                if !key.starts_with(OFFLINE_KEY_PREFIX) {
                    continue;
                }
                let candidate = decode_offline(&value)?;
                if candidate.session_id == *session_id
                    && candidate.topic == message.topic
                    && candidate.from_session_id == message.from_session_id
                {
                    removals.push(KvMutation { key, value: None });
                }
            }
            if !removals.is_empty() {
                self.client.apply(&range_id, removals).await?;
            }
        }
        Ok(messages)
    }

    async fn stage_inflight(&self, message: InflightMessage) -> anyhow::Result<()> {
        let range_id = self
            .inflight_range_id_for_tenant(&message.tenant_id)
            .await?;
        self.client
            .apply(
                &range_id,
                vec![KvMutation {
                    key: inflight_key(&message.session_id, message.packet_id),
                    value: Some(encode_inflight(&message)?),
                }],
            )
            .await
    }

    async fn stage_inflight_batch(&self, messages: Vec<InflightMessage>) -> anyhow::Result<()> {
        let mut grouped: BTreeMap<String, Vec<KvMutation>> = BTreeMap::new();
        for message in messages {
            let range_id = self
                .inflight_range_id_for_tenant(&message.tenant_id)
                .await?;
            grouped.entry(range_id).or_default().push(KvMutation {
                key: inflight_key(&message.session_id, message.packet_id),
                value: Some(encode_inflight(&message)?),
            });
        }
        for (range_id, mutations) in grouped {
            self.client.apply(&range_id, mutations).await?;
        }
        Ok(())
    }

    async fn ack_inflight(&self, session_id: &SessionId, packet_id: u16) -> anyhow::Result<()> {
        let Some(message) = self
            .fetch_inflight(session_id)
            .await?
            .into_iter()
            .find(|message| message.packet_id == packet_id)
        else {
            return Ok(());
        };
        let range_id = self
            .inflight_range_id_for_tenant(&message.tenant_id)
            .await?;
        self.client
            .apply(
                &range_id,
                vec![KvMutation {
                    key: inflight_key(session_id, packet_id),
                    value: None,
                }],
            )
            .await
    }

    async fn fetch_inflight(&self, session_id: &SessionId) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self
            .scan_kind_prefix(
                ServiceShardKind::Inflight,
                &inflight_session_prefix(session_id),
            )
            .await?
        {
            if let Ok(message) = decode_inflight(&value) {
                if message.session_id == *session_id {
                    messages.push(message);
                }
            }
        }
        messages.sort_by_key(|message| message.packet_id);
        Ok(messages)
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        let mut messages = Vec::new();
        for (_, value) in self.scan_inbox_kind(ServiceShardKind::Inflight).await? {
            if let Ok(message) = decode_inflight(&value) {
                messages.push(message);
            }
        }
        messages.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then(left.session_id.cmp(&right.session_id))
                .then(left.packet_id.cmp(&right.packet_id))
        });
        Ok(messages)
    }

    async fn subscription_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_all_subscriptions().await?.len())
    }

    async fn offline_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_all_offline().await?.len())
    }

    async fn inflight_count(&self) -> anyhow::Result<usize> {
        Ok(self.list_all_inflight().await?.len())
    }

    async fn expire_messages(&self, now_ms: u64) -> anyhow::Result<InboxExpiryStats> {
        let expired_offline = self
            .list_all_offline()
            .await?
            .into_iter()
            .filter(|message| publish_properties_expired_at(&message.properties, now_ms))
            .collect::<Vec<_>>();
        let expired_inflight = self
            .list_all_inflight()
            .await?
            .into_iter()
            .filter(|message| publish_properties_expired_at(&message.properties, now_ms))
            .collect::<Vec<_>>();
        Ok(InboxExpiryStats {
            offline_messages: self.remove_offline_messages(&expired_offline).await?,
            inflight_messages: self.remove_inflight_messages(&expired_inflight).await?,
        })
    }

    async fn expire_tenant_messages(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxExpiryStats> {
        let expired_offline = self
            .list_tenant_offline(tenant_id)
            .await?
            .into_iter()
            .filter(|message| publish_properties_expired_at(&message.properties, now_ms))
            .collect::<Vec<_>>();
        let expired_inflight = self
            .list_tenant_inflight(tenant_id)
            .await?
            .into_iter()
            .filter(|message| publish_properties_expired_at(&message.properties, now_ms))
            .collect::<Vec<_>>();
        Ok(InboxExpiryStats {
            offline_messages: self.remove_offline_messages(&expired_offline).await?,
            inflight_messages: self.remove_inflight_messages(&expired_inflight).await?,
        })
    }
}

mod handle;
mod state;

pub use handle::{InboxHandle, PersistentInboxHandle};

#[cfg(test)]
mod tests;
