use crate::{parse_http_url, read_http_status_line, EventHook, PublishRequest};
use async_trait::async_trait;
use greenmqtt_core::{
    ClientIdentity, OfflineMessage, PublishOutcome, RetainedMessage, SessionRecord, Subscription,
};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebHookConfig {
    pub url: String,
    pub connect: bool,
    pub disconnect: bool,
    pub subscribe: bool,
    pub unsubscribe: bool,
    pub publish: bool,
    pub offline: bool,
    pub retain: bool,
}

#[derive(Clone)]
pub struct WebHookEventHook {
    pub(crate) config: WebHookConfig,
}

impl WebHookEventHook {
    pub fn new(config: WebHookConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &WebHookConfig {
        &self.config
    }

    pub(crate) async fn post_json<T: Serialize>(
        &self,
        event: &str,
        body: &T,
    ) -> anyhow::Result<()> {
        let target = parse_http_url(&self.config.url)?;
        let payload = serde_json::json!({
            "event": event,
            "body": body,
        });
        let payload = serde_json::to_vec(&payload)?;
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
                status_line.contains(" 200 ")
                    || status_line.contains(" 202 ")
                    || status_line.contains(" 204 "),
                "webhook returned non-success status: {status_line}"
            );
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }
}

#[async_trait]
impl EventHook for WebHookEventHook {
    async fn on_connect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        if self.config.connect {
            self.post_json("connect", session).await?;
        }
        Ok(())
    }

    async fn on_disconnect(&self, session: &SessionRecord) -> anyhow::Result<()> {
        if self.config.disconnect {
            self.post_json("disconnect", session).await?;
        }
        Ok(())
    }

    async fn on_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<()> {
        if self.config.subscribe {
            self.post_json(
                "subscribe",
                &serde_json::json!({
                    "identity": identity,
                    "subscription": subscription,
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn on_unsubscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &Subscription,
    ) -> anyhow::Result<()> {
        if self.config.unsubscribe {
            self.post_json(
                "unsubscribe",
                &serde_json::json!({
                    "identity": identity,
                    "subscription": subscription,
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn on_offline_enqueue(&self, message: &OfflineMessage) -> anyhow::Result<()> {
        if self.config.offline {
            self.post_json("offline_enqueue", message).await?;
        }
        Ok(())
    }

    async fn on_retain_write(&self, message: &RetainedMessage) -> anyhow::Result<()> {
        if self.config.retain {
            self.post_json("retain_write", message).await?;
        }
        Ok(())
    }

    async fn on_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
        outcome: &PublishOutcome,
    ) -> anyhow::Result<()> {
        if self.config.publish {
            self.post_json(
                "publish",
                &serde_json::json!({
                    "identity": identity,
                    "request": request,
                    "outcome": outcome,
                }),
            )
            .await?;
        }
        Ok(())
    }
}
