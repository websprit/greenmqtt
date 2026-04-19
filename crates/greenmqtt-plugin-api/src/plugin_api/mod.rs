use async_trait::async_trait;
use greenmqtt_core::{
    ClientIdentity, OfflineMessage, PublishOutcome, PublishRequest, RetainedMessage, SessionRecord,
    Subscription,
};
use std::future::Future;

tokio::task_local! {
    static LISTENER_PROFILE: String;
}

pub const BRIDGE_CLIENT_ID_PREFIX: &str = "greenmqtt-bridge-";
pub const BRIDGE_TRACE_ID_PROPERTY: &str = "greenmqtt-bridge-trace-id";
pub const BRIDGE_HOP_COUNT_PROPERTY: &str = "greenmqtt-bridge-hop-count";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnhancedAuthResult {
    Success,
    Continue {
        auth_data: Option<Vec<u8>>,
        reason_string: Option<String>,
    },
}

#[async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(&self, identity: &ClientIdentity) -> anyhow::Result<bool>;

    async fn begin_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        let _ = (method, auth_data);
        if self.authenticate(identity).await? {
            Ok(EnhancedAuthResult::Success)
        } else {
            anyhow::bail!("authentication failed");
        }
    }

    async fn continue_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        self.begin_enhanced_auth(identity, method, auth_data).await
    }
}

#[async_trait]
pub trait AclProvider: Send + Sync {
    async fn can_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<bool>;
    async fn can_publish(&self, identity: &ClientIdentity, topic: &str) -> anyhow::Result<bool>;
}

#[async_trait]
pub trait EventHook: Send + Sync {
    async fn rewrite_publish(
        &self,
        _identity: &ClientIdentity,
        request: &PublishRequest,
    ) -> anyhow::Result<PublishRequest> {
        Ok(request.clone())
    }

    async fn on_connect(&self, _session: &SessionRecord) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_disconnect(&self, _session: &SessionRecord) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_subscribe(
        &self,
        _identity: &ClientIdentity,
        _subscription: &Subscription,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_unsubscribe(
        &self,
        _identity: &ClientIdentity,
        _subscription: &Subscription,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_offline_enqueue(&self, _message: &OfflineMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_retain_write(&self, _message: &RetainedMessage) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_publish(
        &self,
        _identity: &ClientIdentity,
        _request: &PublishRequest,
        _outcome: &PublishOutcome,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub async fn with_listener_profile<F, R>(profile: String, future: F) -> R
where
    F: Future<Output = R>,
{
    LISTENER_PROFILE.scope(profile, future).await
}

pub fn current_listener_profile() -> Option<String> {
    LISTENER_PROFILE.try_with(|profile| profile.clone()).ok()
}
