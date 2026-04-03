use crate::{parse_http_url, read_http_status_line};
use async_trait::async_trait;
use greenmqtt_core::ClientIdentity;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

use crate::{AuthProvider, EnhancedAuthResult};

#[derive(Clone, Default)]
pub struct AllowAllAuth;

#[async_trait]
impl AuthProvider for AllowAllAuth {
    async fn authenticate(&self, _identity: &ClientIdentity) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityMatcher {
    pub tenant_id: String,
    pub user_id: String,
    pub client_id: String,
}

impl IdentityMatcher {
    pub fn matches(&self, identity: &ClientIdentity) -> bool {
        super::matches_pattern(&self.tenant_id, &identity.tenant_id)
            && super::matches_pattern(&self.user_id, &identity.user_id)
            && super::matches_pattern(&self.client_id, &identity.client_id)
    }
}

#[derive(Clone, Default)]
pub struct StaticAuthProvider {
    allowed_identities: Vec<IdentityMatcher>,
    denied_identities: Vec<IdentityMatcher>,
}

impl StaticAuthProvider {
    pub fn new(allowed_identities: Vec<IdentityMatcher>) -> Self {
        Self::with_denied(allowed_identities, Vec::new())
    }

    pub fn with_denied(
        allowed_identities: Vec<IdentityMatcher>,
        denied_identities: Vec<IdentityMatcher>,
    ) -> Self {
        Self {
            allowed_identities,
            denied_identities,
        }
    }
}

#[async_trait]
impl AuthProvider for StaticAuthProvider {
    async fn authenticate(&self, identity: &ClientIdentity) -> anyhow::Result<bool> {
        if self
            .denied_identities
            .iter()
            .any(|matcher| matcher.matches(identity))
        {
            return Ok(false);
        }
        Ok(self
            .allowed_identities
            .iter()
            .any(|matcher| matcher.matches(identity)))
    }
}

#[derive(Clone)]
pub struct StaticEnhancedAuthProvider {
    allowed_identities: Vec<IdentityMatcher>,
    denied_identities: Vec<IdentityMatcher>,
    method: String,
    challenge_data: Vec<u8>,
    response_data: Vec<u8>,
}

impl StaticEnhancedAuthProvider {
    pub fn new(
        allowed_identities: Vec<IdentityMatcher>,
        method: impl Into<String>,
        challenge_data: impl Into<Vec<u8>>,
        response_data: impl Into<Vec<u8>>,
    ) -> Self {
        Self::with_denied(
            allowed_identities,
            Vec::new(),
            method,
            challenge_data,
            response_data,
        )
    }

    pub fn with_denied(
        allowed_identities: Vec<IdentityMatcher>,
        denied_identities: Vec<IdentityMatcher>,
        method: impl Into<String>,
        challenge_data: impl Into<Vec<u8>>,
        response_data: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            allowed_identities,
            denied_identities,
            method: method.into(),
            challenge_data: challenge_data.into(),
            response_data: response_data.into(),
        }
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn challenge_data(&self) -> &[u8] {
        &self.challenge_data
    }

    pub fn response_data(&self) -> &[u8] {
        &self.response_data
    }

    fn identity_allowed(&self, identity: &ClientIdentity) -> bool {
        if self
            .denied_identities
            .iter()
            .any(|matcher| matcher.matches(identity))
        {
            return false;
        }
        self.allowed_identities
            .iter()
            .any(|matcher| matcher.matches(identity))
    }
}

#[async_trait]
impl AuthProvider for StaticEnhancedAuthProvider {
    async fn authenticate(&self, _identity: &ClientIdentity) -> anyhow::Result<bool> {
        Ok(false)
    }

    async fn begin_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        _auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        anyhow::ensure!(method == self.method, "unsupported authentication method");
        anyhow::ensure!(self.identity_allowed(identity), "authentication failed");
        Ok(EnhancedAuthResult::Continue {
            auth_data: Some(self.challenge_data.clone()),
            reason_string: Some("continue authentication".to_string()),
        })
    }

    async fn continue_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        anyhow::ensure!(method == self.method, "unsupported authentication method");
        anyhow::ensure!(self.identity_allowed(identity), "authentication failed");
        anyhow::ensure!(
            auth_data == Some(self.response_data.as_slice()),
            "authentication failed"
        );
        Ok(EnhancedAuthResult::Success)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAuthConfig {
    pub url: String,
}

#[derive(Clone)]
pub struct HttpAuthProvider {
    config: HttpAuthConfig,
}

#[derive(Debug, Deserialize)]
struct HttpAuthDecision {
    #[serde(default)]
    allow: bool,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    auth_data: Option<Vec<u8>>,
    #[serde(default)]
    reason_string: Option<String>,
}

#[derive(Debug, Serialize)]
struct HttpEnhancedAuthRequest<'a> {
    phase: &'a str,
    identity: &'a ClientIdentity,
    method: &'a str,
    auth_data: Option<&'a [u8]>,
}

impl HttpAuthProvider {
    pub fn new(config: HttpAuthConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &HttpAuthConfig {
        &self.config
    }

    async fn post_json<T, R>(&self, payload: &T) -> anyhow::Result<R>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let target = parse_http_url(&self.config.url)?;
        let payload = serde_json::to_vec(payload)?;
        timeout(Duration::from_secs(5), async {
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
                "http auth returned non-success status: {status_line}"
            );
            let mut body = Vec::new();
            stream.read_to_end(&mut body).await?;
            Ok::<_, anyhow::Error>(serde_json::from_slice::<R>(&body)?)
        })
        .await?
    }

    fn enhanced_auth_result(
        &self,
        decision: HttpAuthDecision,
    ) -> anyhow::Result<EnhancedAuthResult> {
        match decision.result.as_deref() {
            Some("success") => Ok(EnhancedAuthResult::Success),
            Some("continue") => Ok(EnhancedAuthResult::Continue {
                auth_data: decision.auth_data,
                reason_string: decision.reason_string,
            }),
            Some("deny") => anyhow::bail!(
                "{}",
                decision
                    .reason_string
                    .unwrap_or_else(|| "authentication failed".to_string())
            ),
            Some(other) => anyhow::bail!("invalid http auth result {other}"),
            None if decision.allow => Ok(EnhancedAuthResult::Success),
            None => anyhow::bail!("authentication failed"),
        }
    }
}

#[async_trait]
impl AuthProvider for HttpAuthProvider {
    async fn authenticate(&self, identity: &ClientIdentity) -> anyhow::Result<bool> {
        let decision: HttpAuthDecision = self.post_json(identity).await?;
        Ok(decision.allow || matches!(decision.result.as_deref(), Some("success")))
    }

    async fn begin_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        let decision: HttpAuthDecision = self
            .post_json(&HttpEnhancedAuthRequest {
                phase: "begin",
                identity,
                method,
                auth_data,
            })
            .await?;
        self.enhanced_auth_result(decision)
    }

    async fn continue_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        let decision: HttpAuthDecision = self
            .post_json(&HttpEnhancedAuthRequest {
                phase: "continue",
                identity,
                method,
                auth_data,
            })
            .await?;
        self.enhanced_auth_result(decision)
    }
}

#[derive(Clone)]
pub enum ConfiguredAuth {
    AllowAll(AllowAllAuth),
    Static(StaticAuthProvider),
    EnhancedStatic(StaticEnhancedAuthProvider),
    Http(HttpAuthProvider),
}

impl Default for ConfiguredAuth {
    fn default() -> Self {
        Self::AllowAll(AllowAllAuth)
    }
}

#[async_trait]
impl AuthProvider for ConfiguredAuth {
    async fn authenticate(&self, identity: &ClientIdentity) -> anyhow::Result<bool> {
        match self {
            Self::AllowAll(provider) => provider.authenticate(identity).await,
            Self::Static(provider) => provider.authenticate(identity).await,
            Self::EnhancedStatic(provider) => provider.authenticate(identity).await,
            Self::Http(provider) => provider.authenticate(identity).await,
        }
    }

    async fn begin_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        match self {
            Self::AllowAll(provider) => {
                provider
                    .begin_enhanced_auth(identity, method, auth_data)
                    .await
            }
            Self::Static(provider) => {
                provider
                    .begin_enhanced_auth(identity, method, auth_data)
                    .await
            }
            Self::EnhancedStatic(provider) => {
                provider
                    .begin_enhanced_auth(identity, method, auth_data)
                    .await
            }
            Self::Http(provider) => {
                provider
                    .begin_enhanced_auth(identity, method, auth_data)
                    .await
            }
        }
    }

    async fn continue_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        match self {
            Self::AllowAll(provider) => {
                provider
                    .continue_enhanced_auth(identity, method, auth_data)
                    .await
            }
            Self::Static(provider) => {
                provider
                    .continue_enhanced_auth(identity, method, auth_data)
                    .await
            }
            Self::EnhancedStatic(provider) => {
                provider
                    .continue_enhanced_auth(identity, method, auth_data)
                    .await
            }
            Self::Http(provider) => {
                provider
                    .continue_enhanced_auth(identity, method, auth_data)
                    .await
            }
        }
    }
}
