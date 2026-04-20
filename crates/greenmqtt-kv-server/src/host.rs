use crate::{HostedRange, HostedRangeStatus, KvRangeHost, MemoryKvRangeHost};
use async_trait::async_trait;
use greenmqtt_core::{NodeId, ServiceShardLifecycle};

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

    async fn local_leader_ranges(
        &self,
        local_node_id: NodeId,
    ) -> anyhow::Result<Vec<HostedRangeStatus>> {
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
