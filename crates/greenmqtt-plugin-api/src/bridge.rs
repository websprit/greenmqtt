use crate::{
    connect_packet_v5, publish_packet_v5, read_packet_body, read_packet_type, EventHook,
    PublishRequest, BRIDGE_CLIENT_ID_PREFIX, BRIDGE_HOP_COUNT_PROPERTY, BRIDGE_TRACE_ID_PROPERTY,
};
use async_trait::async_trait;
use greenmqtt_core::{
    topic_matches, ClientIdentity, PublishOutcome, PublishProperties, UserProperty,
};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};

use crate::hook::rewrite_topic;

#[derive(Clone)]
pub struct BridgeRule {
    pub tenant_id: Option<String>,
    pub topic_filter: String,
    pub rewrite_to: Option<String>,
    pub remote_addr: String,
}

#[derive(Clone)]
pub struct BridgeEventHook {
    client_id_prefix: String,
    rules: Vec<BridgeRule>,
    timeout: Duration,
    fail_open: bool,
    retries: u32,
    retry_delay: Duration,
    max_inflight: usize,
    inflight_limit: Option<Arc<Semaphore>>,
}

impl BridgeEventHook {
    pub fn new(client_id_prefix: impl Into<String>, rules: Vec<BridgeRule>) -> Self {
        Self {
            client_id_prefix: client_id_prefix.into(),
            rules,
            timeout: Duration::from_secs(5),
            fail_open: true,
            retries: 0,
            retry_delay: Duration::from_millis(0),
            max_inflight: 16,
            inflight_limit: Some(Arc::new(Semaphore::new(16))),
        }
    }

    pub fn with_options(
        client_id_prefix: impl Into<String>,
        rules: Vec<BridgeRule>,
        timeout: Duration,
        fail_open: bool,
        retries: u32,
        retry_delay: Duration,
        max_inflight: usize,
    ) -> Self {
        Self {
            client_id_prefix: client_id_prefix.into(),
            rules,
            timeout,
            fail_open,
            retries,
            retry_delay,
            max_inflight,
            inflight_limit: (max_inflight > 0).then(|| Arc::new(Semaphore::new(max_inflight))),
        }
    }

    fn is_bridged_identity(&self, identity: &ClientIdentity) -> bool {
        identity.client_id.starts_with(BRIDGE_CLIENT_ID_PREFIX)
    }

    fn is_bridged_request(&self, request: &PublishRequest) -> bool {
        request.properties.user_properties.iter().any(|property| {
            property.key == BRIDGE_TRACE_ID_PROPERTY
                || (property.key == BRIDGE_HOP_COUNT_PROPERTY
                    && property.value.parse::<u32>().unwrap_or_default() > 0)
        })
    }

    fn bridged_properties(&self, rule_index: usize, request: &PublishRequest) -> PublishProperties {
        let mut properties = request.properties.clone();
        let existing_trace = properties
            .user_properties
            .iter()
            .find(|property| property.key == BRIDGE_TRACE_ID_PROPERTY)
            .map(|property| property.value.clone());
        let existing_hops = properties
            .user_properties
            .iter()
            .find(|property| property.key == BRIDGE_HOP_COUNT_PROPERTY)
            .and_then(|property| property.value.parse::<u32>().ok())
            .unwrap_or_default();
        properties.user_properties.retain(|property| {
            property.key != BRIDGE_TRACE_ID_PROPERTY && property.key != BRIDGE_HOP_COUNT_PROPERTY
        });
        properties.user_properties.push(UserProperty {
            key: BRIDGE_TRACE_ID_PROPERTY.into(),
            value: existing_trace
                .unwrap_or_else(|| format!("{}-{rule_index}", self.client_id_prefix)),
        });
        properties.user_properties.push(UserProperty {
            key: BRIDGE_HOP_COUNT_PROPERTY.into(),
            value: (existing_hops + 1).to_string(),
        });
        properties
    }

    async fn forward_publish(
        &self,
        rule_index: usize,
        remote_addr: &str,
        request: &PublishRequest,
    ) -> anyhow::Result<()> {
        let mut attempts = 0u32;
        loop {
            let result = timeout(self.timeout, async {
                let mut stream = TcpStream::connect(remote_addr).await?;
                let client_id = format!("{}-{rule_index}", self.client_id_prefix);
                stream.write_all(&connect_packet_v5(&client_id)).await?;
                let packet_type = read_packet_type(&mut stream).await?;
                anyhow::ensure!(
                    packet_type == 2,
                    "bridge expected CONNACK, got {packet_type}"
                );
                let _ = read_packet_body(&mut stream).await?;
                let publish = publish_packet_v5(
                    &request.topic,
                    &request.payload,
                    request.qos.min(1),
                    request.retain,
                    &self.bridged_properties(rule_index, request),
                )?;
                stream.write_all(&publish).await?;
                if request.qos.min(1) == 1 {
                    let packet_type = read_packet_type(&mut stream).await?;
                    anyhow::ensure!(
                        packet_type == 4,
                        "bridge expected PUBACK, got {packet_type}"
                    );
                    let _ = read_packet_body(&mut stream).await?;
                }
                stream.write_all(&[0xE0, 0x00]).await?;
                anyhow::Ok(())
            })
            .await;
            match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) if attempts < self.retries => {
                    attempts += 1;
                    eprintln!(
                        "greenmqtt bridge retry {attempts}/{} after error: {error:#}",
                        self.retries
                    );
                    if !self.retry_delay.is_zero() {
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
                Err(error) if attempts < self.retries => {
                    attempts += 1;
                    eprintln!(
                        "greenmqtt bridge retry {attempts}/{} after timeout: {error:#}",
                        self.retries
                    );
                    if !self.retry_delay.is_zero() {
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[async_trait]
impl EventHook for BridgeEventHook {
    async fn on_publish(
        &self,
        identity: &ClientIdentity,
        request: &PublishRequest,
        _outcome: &PublishOutcome,
    ) -> anyhow::Result<()> {
        if self.is_bridged_identity(identity) || self.is_bridged_request(request) {
            return Ok(());
        }
        for (index, rule) in self.rules.iter().enumerate() {
            if let Some(tenant_id) = &rule.tenant_id {
                if tenant_id != &identity.tenant_id {
                    continue;
                }
            }
            if !topic_matches(&rule.topic_filter, &request.topic) {
                continue;
            }
            let rewritten_request = if let Some(rewrite_to) = &rule.rewrite_to {
                let mut rewritten = request.clone();
                if let Some(topic) = rewrite_topic(&request.topic, &rule.topic_filter, rewrite_to) {
                    rewritten.topic = topic;
                }
                rewritten
            } else {
                request.clone()
            };
            let _permit = if let Some(limit) = &self.inflight_limit {
                match limit.clone().try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(_) if self.fail_open => {
                        eprintln!(
                            "greenmqtt bridge inflight limit {} reached for {}",
                            self.max_inflight, rule.remote_addr
                        );
                        continue;
                    }
                    Err(_) => {
                        anyhow::bail!(
                            "bridge inflight limit {} reached for {}",
                            self.max_inflight,
                            rule.remote_addr
                        );
                    }
                }
            } else {
                None
            };
            self.forward_publish(index, &rule.remote_addr, &rewritten_request)
                .await
                .or_else(|error| {
                    if self.fail_open {
                        eprintln!("greenmqtt bridge forward error: {error:#}");
                        Ok(())
                    } else {
                        Err(error)
                    }
                })?;
        }
        Ok(())
    }
}
