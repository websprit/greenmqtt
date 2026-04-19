use async_trait::async_trait;
use greenmqtt_core::{NodeId, ReplicatedRangeDescriptor, ServiceShardLifecycle};
use greenmqtt_kv_engine::KvRangeSpace;
use greenmqtt_kv_raft::{RaftNode, RaftStatusSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct HostedRange {
    pub descriptor: ReplicatedRangeDescriptor,
    pub raft: Arc<dyn RaftNode>,
    pub space: Arc<dyn KvRangeSpace>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostedRangeStatus {
    pub descriptor: ReplicatedRangeDescriptor,
    pub raft: RaftStatusSnapshot,
}

#[async_trait]
pub trait KvRangeHost: Send + Sync {
    async fn add_range(&self, range: HostedRange) -> anyhow::Result<()>;
    async fn remove_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>>;
    async fn open_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>>;
    async fn range(&self, range_id: &str) -> anyhow::Result<Option<HostedRangeStatus>>;
    async fn list_ranges(&self) -> anyhow::Result<Vec<HostedRangeStatus>>;
    async fn local_leader_ranges(&self, local_node_id: NodeId) -> anyhow::Result<Vec<HostedRangeStatus>>;
}

#[derive(Clone, Default)]
pub struct MemoryKvRangeHost {
    ranges: Arc<RwLock<BTreeMap<String, HostedRange>>>,
}

#[async_trait]
impl KvRangeHost for MemoryKvRangeHost {
    async fn add_range(&self, range: HostedRange) -> anyhow::Result<()> {
        self.ranges
            .write()
            .expect("range host poisoned")
            .insert(range.descriptor.id.clone(), range);
        Ok(())
    }

    async fn remove_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>> {
        Ok(self
            .ranges
            .write()
            .expect("range host poisoned")
            .remove(range_id))
    }

    async fn open_range(&self, range_id: &str) -> anyhow::Result<Option<HostedRange>> {
        Ok(self
            .ranges
            .read()
            .expect("range host poisoned")
            .get(range_id)
            .cloned())
    }

    async fn range(&self, range_id: &str) -> anyhow::Result<Option<HostedRangeStatus>> {
        let maybe_range = self
            .ranges
            .read()
            .expect("range host poisoned")
            .get(range_id)
            .cloned();
        match maybe_range {
            Some(range) => Ok(Some(HostedRangeStatus {
                descriptor: range.descriptor,
                raft: range.raft.status().await?,
            })),
            None => Ok(None),
        }
    }

    async fn list_ranges(&self) -> anyhow::Result<Vec<HostedRangeStatus>> {
        let ranges: Vec<_> = self
            .ranges
            .read()
            .expect("range host poisoned")
            .values()
            .cloned()
            .collect();
        let mut statuses = Vec::with_capacity(ranges.len());
        for range in ranges {
            statuses.push(HostedRangeStatus {
                descriptor: range.descriptor,
                raft: range.raft.status().await?,
            });
        }
        Ok(statuses)
    }

    async fn local_leader_ranges(&self, local_node_id: NodeId) -> anyhow::Result<Vec<HostedRangeStatus>> {
        let ranges: Vec<_> = self
            .ranges
            .read()
            .expect("range host poisoned")
            .values()
            .cloned()
            .collect();
        let mut statuses = Vec::new();
        for range in ranges {
            let raft = range.raft.status().await?;
            if raft.leader_node_id == Some(local_node_id)
                && range.descriptor.lifecycle == ServiceShardLifecycle::Serving
            {
                statuses.push(HostedRangeStatus {
                    descriptor: range.descriptor,
                    raft,
                });
            }
        }
        Ok(statuses)
    }
}

#[cfg(test)]
mod tests {
    use super::{HostedRange, KvRangeHost, MemoryKvRangeHost};
    use greenmqtt_core::{
        RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor,
        ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_engine::{KvEngine, MemoryKvEngine};
    use greenmqtt_kv_raft::{MemoryRaftNode, RaftClusterConfig, RaftNode};
    use std::sync::Arc;

    #[tokio::test]
    async fn memory_range_host_tracks_ranges_and_reports_local_leaders() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(greenmqtt_kv_engine::KvRangeBootstrap {
                range_id: "range-1".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let space = Arc::<dyn greenmqtt_kv_engine::KvRangeSpace>::from(
            engine.open_range("range-1").await.unwrap(),
        );
        let raft = Arc::new(MemoryRaftNode::new(
            1,
            "range-1",
            RaftClusterConfig {
                voters: vec![1, 2, 3],
                learners: Vec::new(),
            },
        ));
        raft.recover().await.unwrap();
        let host = MemoryKvRangeHost::default();
        host.add_range(HostedRange {
            descriptor: ReplicatedRangeDescriptor::new(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
                1,
                1,
                Some(1),
                vec![
                    RangeReplica::new(1, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                    RangeReplica::new(2, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                    RangeReplica::new(3, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                ],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ),
            raft,
            space,
        })
        .await
        .unwrap();

        assert_eq!(host.list_ranges().await.unwrap().len(), 1);
        assert_eq!(host.local_leader_ranges(1).await.unwrap().len(), 1);
        assert_eq!(host.local_leader_ranges(2).await.unwrap().len(), 0);
        assert!(host.range("range-1").await.unwrap().is_some());
    }
}
