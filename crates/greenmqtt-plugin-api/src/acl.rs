use crate::{parse_http_url, read_http_status_line, AclProvider, IdentityMatcher};
use async_trait::async_trait;
use greenmqtt_core::{topic_matches, ClientIdentity, Subscription};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAclConfig {
    pub url: String,
}

#[derive(Clone)]
pub struct HttpAclProvider {
    config: HttpAclConfig,
}

#[derive(Debug, Serialize)]
struct HttpAclRequest<'a> {
    action: &'a str,
    identity: &'a ClientIdentity,
    topic: Option<&'a str>,
    subscription: Option<&'a Subscription>,
}

#[derive(Debug, Deserialize)]
struct HttpAclDecision {
    allow: bool,
}

impl HttpAclProvider {
    pub fn new(config: HttpAclConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &HttpAclConfig {
        &self.config
    }

    async fn evaluate(&self, request: &HttpAclRequest<'_>) -> anyhow::Result<bool> {
        let target = parse_http_url(&self.config.url)?;
        let payload = serde_json::to_vec(request)?;
        let decision = timeout(Duration::from_secs(5), async {
            let mut stream = TcpStream::connect(&target.address).await?;
            let request = format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                target.path,
                target.host_header,
                payload.len()
            );
            stream.write_all(request.as_bytes()).await?;
            stream.write_all(&payload).await?;
            let status_line = read_http_status_line(&mut stream).await?;
            anyhow::ensure!(
                status_line.contains(" 200 "),
                "http acl returned non-success status: {status_line}"
            );
            let mut body = Vec::new();
            stream.read_to_end(&mut body).await?;
            Ok::<_, anyhow::Error>(serde_json::from_slice::<HttpAclDecision>(&body)?)
        })
        .await??;
        Ok(decision.allow)
    }
}

#[derive(Clone, Default)]
pub struct AllowAllAcl;

#[async_trait]
impl AclProvider for AllowAllAcl {
    async fn can_subscribe(
        &self,
        _identity: &ClientIdentity,
        _subscription: &Subscription,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn can_publish(&self, _identity: &ClientIdentity, _topic: &str) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl AclProvider for HttpAclProvider {
    async fn can_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<bool> {
        self.evaluate(&HttpAclRequest {
            action: "subscribe",
            identity,
            topic: None,
            subscription: Some(subscription),
        })
        .await
    }

    async fn can_publish(&self, identity: &ClientIdentity, topic: &str) -> anyhow::Result<bool> {
        self.evaluate(&HttpAclRequest {
            action: "publish",
            identity,
            topic: Some(topic),
            subscription: None,
        })
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclAction {
    Subscribe,
    Publish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclRule {
    pub decision: AclDecision,
    pub action: AclAction,
    pub identity: IdentityMatcher,
    pub topic_filter: String,
}

#[derive(Clone, Default)]
pub struct StaticAclProvider {
    rules: Vec<AclRule>,
    default_allow: bool,
}

impl StaticAclProvider {
    pub fn new(rules: Vec<AclRule>) -> Self {
        Self::with_default_decision(rules, false)
    }

    pub fn with_default_decision(rules: Vec<AclRule>, default_allow: bool) -> Self {
        Self {
            rules,
            default_allow,
        }
    }

    fn evaluate(&self, action: AclAction, identity: &ClientIdentity, topic: &str) -> bool {
        for rule in &self.rules {
            if rule.action != action {
                continue;
            }
            if !rule.identity.matches(identity) {
                continue;
            }
            if !topic_matches(&rule.topic_filter, topic) {
                continue;
            }
            return matches!(rule.decision, AclDecision::Allow);
        }
        self.default_allow
    }
}

#[async_trait]
impl AclProvider for StaticAclProvider {
    async fn can_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<bool> {
        Ok(self.evaluate(AclAction::Subscribe, identity, &subscription.topic_filter))
    }

    async fn can_publish(&self, identity: &ClientIdentity, topic: &str) -> anyhow::Result<bool> {
        Ok(self.evaluate(AclAction::Publish, identity, topic))
    }
}

#[derive(Clone)]
pub enum ConfiguredAcl {
    AllowAll(AllowAllAcl),
    Static(StaticAclProvider),
    Http(HttpAclProvider),
}

impl Default for ConfiguredAcl {
    fn default() -> Self {
        Self::AllowAll(AllowAllAcl)
    }
}

#[async_trait]
impl AclProvider for ConfiguredAcl {
    async fn can_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<bool> {
        match self {
            Self::AllowAll(provider) => provider.can_subscribe(identity, subscription).await,
            Self::Static(provider) => provider.can_subscribe(identity, subscription).await,
            Self::Http(provider) => provider.can_subscribe(identity, subscription).await,
        }
    }

    async fn can_publish(&self, identity: &ClientIdentity, topic: &str) -> anyhow::Result<bool> {
        match self {
            Self::AllowAll(provider) => provider.can_publish(identity, topic).await,
            Self::Static(provider) => provider.can_publish(identity, topic).await,
            Self::Http(provider) => provider.can_publish(identity, topic).await,
        }
    }
}
