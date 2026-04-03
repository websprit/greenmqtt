use crate::{BridgeEventHook, EventHook, TopicRewriteEventHook, WebHookEventHook};
use async_trait::async_trait;
use greenmqtt_core::{
    ClientIdentity, OfflineMessage, PublishOutcome, PublishRequest, RetainedMessage, SessionRecord,
    Subscription,
};

#[derive(Clone, Default)]
pub struct NoopEventHook;

#[async_trait]
impl EventHook for NoopEventHook {}

pub(crate) fn rewrite_topic(topic: &str, filter: &str, rewrite_to: &str) -> Option<String> {
    let topic_parts = topic.split('/').collect::<Vec<_>>();
    let filter_parts = filter.split('/').collect::<Vec<_>>();
    let mut captures = Vec::new();
    let mut topic_index = 0usize;

    for filter_part in filter_parts {
        match filter_part {
            "+" => {
                let part = topic_parts.get(topic_index)?;
                captures.push((*part).to_string());
                topic_index += 1;
            }
            "#" => {
                captures.push(topic_parts[topic_index..].join("/"));
                topic_index = topic_parts.len();
                break;
            }
            literal => {
                let part = topic_parts.get(topic_index)?;
                if literal != *part {
                    return None;
                }
                topic_index += 1;
            }
        }
    }

    if topic_index != topic_parts.len() {
        return None;
    }

    let mut rewritten = rewrite_to.to_string();
    for (index, capture) in captures.iter().enumerate() {
        rewritten = rewritten.replace(&format!("{{{}}}", index + 1), capture);
    }
    Some(rewritten)
}

#[derive(Clone)]
pub enum HookTarget {
    Bridge(BridgeEventHook),
    TopicRewrite(TopicRewriteEventHook),
    WebHook(WebHookEventHook),
}

#[async_trait]
impl EventHook for HookTarget {
    async fn rewrite_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
    ) -> anyhow::Result<PublishRequest> {
        match self {
            Self::Bridge(hook) => hook.rewrite_publish(identity, request).await,
            Self::TopicRewrite(hook) => hook.rewrite_publish(identity, request).await,
            Self::WebHook(hook) => hook.rewrite_publish(identity, request).await,
        }
    }

    async fn on_connect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_connect(session).await,
            Self::TopicRewrite(hook) => hook.on_connect(session).await,
            Self::WebHook(hook) => hook.on_connect(session).await,
        }
    }

    async fn on_disconnect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_disconnect(session).await,
            Self::TopicRewrite(hook) => hook.on_disconnect(session).await,
            Self::WebHook(hook) => hook.on_disconnect(session).await,
        }
    }

    async fn on_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_subscribe(identity, subscription).await,
            Self::TopicRewrite(hook) => hook.on_subscribe(identity, subscription).await,
            Self::WebHook(hook) => hook.on_subscribe(identity, subscription).await,
        }
    }

    async fn on_unsubscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_unsubscribe(identity, subscription).await,
            Self::TopicRewrite(hook) => hook.on_unsubscribe(identity, subscription).await,
            Self::WebHook(hook) => hook.on_unsubscribe(identity, subscription).await,
        }
    }

    async fn on_offline_enqueue(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_offline_enqueue(message).await,
            Self::TopicRewrite(hook) => hook.on_offline_enqueue(message).await,
            Self::WebHook(hook) => hook.on_offline_enqueue(message).await,
        }
    }

    async fn on_retain_write(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_retain_write(message).await,
            Self::TopicRewrite(hook) => hook.on_retain_write(message).await,
            Self::WebHook(hook) => hook.on_retain_write(message).await,
        }
    }

    async fn on_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
        outcome: &PublishOutcome,
    ) -> anyhow::Result<()> {
        match self {
            Self::Bridge(hook) => hook.on_publish(identity, request, outcome).await,
            Self::TopicRewrite(hook) => hook.on_publish(identity, request, outcome).await,
            Self::WebHook(hook) => hook.on_publish(identity, request, outcome).await,
        }
    }
}

#[derive(Clone, Default)]
pub struct ConfiguredEventHook {
    hooks: Vec<HookTarget>,
}

impl ConfiguredEventHook {
    pub fn new(hooks: Vec<HookTarget>) -> Self {
        Self { hooks }
    }
}

#[async_trait]
impl EventHook for ConfiguredEventHook {
    async fn rewrite_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
    ) -> anyhow::Result<PublishRequest> {
        let mut rewritten = request.clone();
        for hook in &self.hooks {
            rewritten = hook.rewrite_publish(identity, &rewritten).await?;
        }
        Ok(rewritten)
    }

    async fn on_connect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_connect(session).await?;
        }
        Ok(())
    }

    async fn on_disconnect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_disconnect(session).await?;
        }
        Ok(())
    }

    async fn on_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_subscribe(identity, subscription).await?;
        }
        Ok(())
    }

    async fn on_unsubscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_unsubscribe(identity, subscription).await?;
        }
        Ok(())
    }

    async fn on_offline_enqueue(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_offline_enqueue(message).await?;
        }
        Ok(())
    }

    async fn on_retain_write(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_retain_write(message).await?;
        }
        Ok(())
    }

    async fn on_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
        outcome: &PublishOutcome,
    ) -> anyhow::Result<()> {
        for hook in &self.hooks {
            hook.on_publish(identity, request, outcome).await?;
        }
        Ok(())
    }
}
