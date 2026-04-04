mod failure_detector;
mod membership;

use async_trait::async_trait;
use foca::Config as FocaConfig;
use greenmqtt_core::{ClusterNodeMembership, Lifecycle, NodeId};
pub use failure_detector::{
    FailureDetector, FailureDetectorConfig, FailureDetectorSnapshot,
};
pub use membership::MemberList;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub bind_addr: String,
    pub seeds: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterEngine {
    Foca,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub members: Vec<ClusterNodeMembership>,
}

#[async_trait]
pub trait ClusterControl: Send + Sync {
    async fn snapshot(&self) -> anyhow::Result<ClusterSnapshot>;
}

pub struct ClusterRuntime {
    pub config: ClusterConfig,
    pub engine: ClusterEngine,
}

impl ClusterRuntime {
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            engine: ClusterEngine::Foca,
        }
    }

    pub fn engine_config(&self) -> FocaConfig {
        let cluster_size = NonZeroU32::new((self.config.seeds.len() as u32).saturating_add(1))
            .unwrap_or_else(|| NonZeroU32::new(1).expect("non-zero"));
        FocaConfig::new_lan(cluster_size)
    }
}

#[async_trait]
impl Lifecycle for ClusterRuntime {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
