use crate::{EventHook, PublishRequest};
use async_trait::async_trait;
use greenmqtt_core::ClientIdentity;

use crate::hook::rewrite_topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRewriteRule {
    pub tenant_id: Option<String>,
    pub topic_filter: String,
    pub rewrite_to: String,
}

#[derive(Clone)]
pub struct TopicRewriteEventHook {
    rules: Vec<TopicRewriteRule>,
}

impl TopicRewriteEventHook {
    pub fn new(rules: Vec<TopicRewriteRule>) -> Self {
        Self { rules }
    }

    pub fn rules(&self) -> &[TopicRewriteRule] {
        &self.rules
    }
}

#[async_trait]
impl EventHook for TopicRewriteEventHook {
    async fn rewrite_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
    ) -> anyhow::Result<PublishRequest> {
        let mut rewritten = request.clone();
        for rule in &self.rules {
            if let Some(tenant_id) = &rule.tenant_id {
                if tenant_id != &identity.tenant_id {
                    continue;
                }
            }
            if let Some(topic) =
                rewrite_topic(&rewritten.topic, &rule.topic_filter, &rule.rewrite_to)
            {
                rewritten.topic = topic;
            }
        }
        Ok(rewritten)
    }
}
