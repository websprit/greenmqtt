mod cache;
mod client;
mod consistency;
mod control;
mod error;
mod executor;
mod router;

pub use client::{
    LeaderRoutedKvRangeExecutor, MutationRedirect, QuerySequencer, RangeDataClient,
    RoutedMutation, RoutedRangeDataClient,
};
pub use cache::{CachedKvRangeRouter, RouteCacheConfig};
pub use consistency::{RangeReadOptions, ReadConsistency};
pub use control::{RangeControlClient, RangeSplitResult};
pub use error::{classify_kv_error, KvRangeErrorKind, RetryDirective};
pub use executor::{KvRangeExecutor, KvRangeExecutorFactory};
pub use router::{KvRangeRouter, MemoryKvRangeRouter, RangeRoute};

#[cfg(test)]
mod tests {
    use crate::{
        KvRangeExecutor, KvRangeExecutorFactory, KvRangeRouter, LeaderRoutedKvRangeExecutor,
        MemoryKvRangeRouter, MutationRedirect, QuerySequencer, RetryDirective,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use greenmqtt_core::{
        ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership, RangeBoundary,
        RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, ServiceEndpoint,
        ServiceKind, ServiceShardKey, ServiceShardLifecycle,
    };
    use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};

    fn descriptor(
        id: &str,
        shard: ServiceShardKey,
        start: &[u8],
        end: &[u8],
        leader: u64,
    ) -> ReplicatedRangeDescriptor {
        ReplicatedRangeDescriptor::new(
            id,
            shard,
            RangeBoundary::new(Some(start.to_vec()), Some(end.to_vec())),
            1,
            1,
            Some(leader),
            vec![
                RangeReplica::new(leader, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                RangeReplica::new(
                    leader + 1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                ),
            ],
            0,
            0,
            ServiceShardLifecycle::Serving,
        )
    }

    #[tokio::test]
    async fn memory_router_routes_keys_to_matching_range_and_leader() {
        let router = MemoryKvRangeRouter::default();
        let shard = ServiceShardKey::retain("tenant-a");
        router
            .upsert(descriptor("range-1", shard.clone(), b"a", b"m", 1))
            .await
            .unwrap();
        router
            .upsert(descriptor("range-2", shard.clone(), b"m", b"z", 3))
            .await
            .unwrap();

        let route = router.route_key(&shard, b"melon").await.unwrap().unwrap();
        assert_eq!(route.descriptor.id, "range-2");
        assert_eq!(route.leader_node_id, Some(3));
        assert_eq!(route.query_ready_nodes, vec![3, 4]);
    }

    #[tokio::test]
    async fn router_lists_ranges_by_shard_and_supports_redirect_hints() {
        let router = MemoryKvRangeRouter::default();
        let shard = ServiceShardKey::dist("tenant-a");
        router
            .upsert(descriptor("range-1", shard.clone(), b"a", b"z", 7))
            .await
            .unwrap();
        let routes = router.route_shard(&shard).await.unwrap();
        assert_eq!(routes.len(), 1);
        let redirect = MutationRedirect::from_route(&routes[0]);
        assert_eq!(redirect.range_id, "range-1");
        assert_eq!(redirect.directive, RetryDirective::RetryWithLeader(7));
    }

    #[test]
    fn query_sequencer_round_robins_query_ready_replicas() {
        let route = crate::RangeRoute {
            descriptor: descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                1,
            ),
            leader_node_id: Some(1),
            query_ready_nodes: vec![1, 2],
        };
        let sequencer = QuerySequencer::default();
        assert_eq!(sequencer.select(&route), Some(1));
        assert_eq!(sequencer.select(&route), Some(2));
        assert_eq!(sequencer.select(&route), Some(1));
    }

    #[derive(Default)]
    struct TestMembershipRegistry {
        members: Arc<RwLock<BTreeMap<u64, ClusterNodeMembership>>>,
    }

    #[async_trait]
    impl ClusterMembershipRegistry for TestMembershipRegistry {
        async fn upsert_member(
            &self,
            member: ClusterNodeMembership,
        ) -> anyhow::Result<Option<ClusterNodeMembership>> {
            Ok(self
                .members
                .write()
                .expect("test membership poisoned")
                .insert(member.node_id, member))
        }

        async fn resolve_member(
            &self,
            node_id: u64,
        ) -> anyhow::Result<Option<ClusterNodeMembership>> {
            Ok(self
                .members
                .read()
                .expect("test membership poisoned")
                .get(&node_id)
                .cloned())
        }

        async fn remove_member(
            &self,
            node_id: u64,
        ) -> anyhow::Result<Option<ClusterNodeMembership>> {
            Ok(self
                .members
                .write()
                .expect("test membership poisoned")
                .remove(&node_id))
        }

        async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
            Ok(self
                .members
                .read()
                .expect("test membership poisoned")
                .values()
                .cloned()
                .collect())
        }
    }

    #[derive(Default)]
    struct TestExecutor {
        writes: Arc<RwLock<HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>>>,
    }

    #[async_trait]
    impl KvRangeExecutor for TestExecutor {
        async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            Ok(self
                .writes
                .read()
                .expect("test executor poisoned")
                .get(range_id)
                .and_then(|entries| entries.get(key).cloned())
                .map(Bytes::from))
        }

        async fn scan(
            &self,
            range_id: &str,
            _boundary: Option<greenmqtt_core::RangeBoundary>,
            limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            Ok(self
                .writes
                .read()
                .expect("test executor poisoned")
                .get(range_id)
                .into_iter()
                .flat_map(|entries| entries.iter().take(limit))
                .map(|(key, value)| (Bytes::from(key.clone()), Bytes::from(value.clone())))
                .collect())
        }

        async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
            let mut guard = self.writes.write().expect("test executor poisoned");
            let entries = guard.entry(range_id.to_string()).or_default();
            for mutation in mutations {
                if let Some(value) = mutation.value {
                    entries.insert(mutation.key.to_vec(), value.to_vec());
                } else {
                    entries.remove(mutation.key.as_ref());
                }
            }
            Ok(())
        }

        async fn checkpoint(
            &self,
            range_id: &str,
            checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            Ok(KvRangeCheckpoint {
                range_id: range_id.to_string(),
                checkpoint_id: checkpoint_id.to_string(),
                path: format!("/tmp/{range_id}-{checkpoint_id}"),
            })
        }

        async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            Ok(KvRangeSnapshot {
                range_id: range_id.to_string(),
                boundary: RangeBoundary::full(),
                term: 0,
                index: 0,
                checksum: 0,
                layout_version: 1,
                data_path: format!("/tmp/{range_id}-snapshot"),
            })
        }
    }

    #[derive(Default)]
    struct TestExecutorFactory {
        executors: Arc<RwLock<HashMap<String, Arc<dyn KvRangeExecutor>>>>,
    }

    impl TestExecutorFactory {
        fn register(&self, endpoint: &str, executor: Arc<dyn KvRangeExecutor>) {
            self.executors
                .write()
                .expect("test executor factory poisoned")
                .insert(endpoint.to_string(), executor);
        }
    }

    #[async_trait]
    impl KvRangeExecutorFactory for TestExecutorFactory {
        async fn connect(&self, endpoint: &str) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
            self.executors
                .read()
                .expect("test executor factory poisoned")
                .get(endpoint)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no executor registered for endpoint {endpoint}"))
        }
    }

    #[tokio::test]
    async fn leader_routed_executor_sends_mutations_to_current_leader() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback_impl = Arc::new(TestExecutor::default());
        let node7_impl = Arc::new(TestExecutor::default());
        let node9_impl = Arc::new(TestExecutor::default());
        let fallback: Arc<dyn KvRangeExecutor> = fallback_impl.clone();
        let factory = Arc::new(TestExecutorFactory::default());
        factory.register("node7", node7_impl.clone());
        factory.register("node9", node9_impl.clone());
        membership
            .upsert_member(ClusterNodeMembership::new(
                7,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(ServiceKind::Retain, 7, "node7")],
            ))
            .await
            .unwrap();
        membership
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(ServiceKind::Retain, 9, "node9")],
            ))
            .await
            .unwrap();
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                7,
            ))
            .await
            .unwrap();
        let executor =
            LeaderRoutedKvRangeExecutor::new(router.clone(), membership.clone(), fallback, factory);

        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"apple"),
                    value: Some(Bytes::from_static(b"red")),
                }],
            )
            .await
            .unwrap();

        assert_eq!(
            node7_impl.get("range-1", b"apple").await.unwrap().unwrap(),
            Bytes::from_static(b"red")
        );
        assert!(node9_impl.get("range-1", b"apple").await.unwrap().is_none());
        assert!(fallback_impl
            .get("range-1", b"apple")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn leader_routed_executor_follows_leader_changes() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback: Arc<dyn KvRangeExecutor> = Arc::new(TestExecutor::default());
        let node7_impl = Arc::new(TestExecutor::default());
        let node9_impl = Arc::new(TestExecutor::default());
        let factory = Arc::new(TestExecutorFactory::default());
        factory.register("node7", node7_impl.clone());
        factory.register("node9", node9_impl.clone());
        for (node_id, endpoint) in [(7, "node7"), (9, "node9")] {
            membership
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(ServiceKind::Retain, node_id, endpoint)],
                ))
                .await
                .unwrap();
        }
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                7,
            ))
            .await
            .unwrap();
        let executor =
            LeaderRoutedKvRangeExecutor::new(router.clone(), membership, fallback, factory);

        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"alpha"),
                    value: Some(Bytes::from_static(b"one")),
                }],
            )
            .await
            .unwrap();
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                9,
            ))
            .await
            .unwrap();
        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"beta"),
                    value: Some(Bytes::from_static(b"two")),
                }],
            )
            .await
            .unwrap();

        assert!(node7_impl.get("range-1", b"alpha").await.unwrap().is_some());
        assert!(node7_impl.get("range-1", b"beta").await.unwrap().is_none());
        assert!(node9_impl.get("range-1", b"alpha").await.unwrap().is_none());
        assert_eq!(
            node9_impl.get("range-1", b"beta").await.unwrap().unwrap(),
            Bytes::from_static(b"two")
        );
    }

    struct FlippingLeaderErrorExecutor {
        failed: AtomicBool,
    }

    #[async_trait]
    impl KvRangeExecutor for FlippingLeaderErrorExecutor {
        async fn get(&self, _range_id: &str, _key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                anyhow::bail!("kv/not-leader leader=9");
            }
            Ok(Some(Bytes::from_static(b"ok")))
        }
        async fn scan(
            &self,
            _range_id: &str,
            _boundary: Option<greenmqtt_core::RangeBoundary>,
            _limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            unreachable!()
        }
        async fn apply(
            &self,
            _range_id: &str,
            _mutations: Vec<KvMutation>,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn checkpoint(
            &self,
            _range_id: &str,
            _checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            unreachable!()
        }
        async fn snapshot(&self, _range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn leader_routed_executor_retries_with_leader_hint() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback: Arc<dyn KvRangeExecutor> = Arc::new(FlippingLeaderErrorExecutor {
            failed: AtomicBool::new(false),
        });
        let node9_impl = Arc::new(TestExecutor::default());
        node9_impl
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"k"),
                    value: Some(Bytes::from_static(b"v")),
                }],
            )
            .await
            .unwrap();
        let factory = Arc::new(TestExecutorFactory::default());
        factory.register("node9", node9_impl.clone());
        membership
            .upsert_member(ClusterNodeMembership::new(
                9,
                1,
                ClusterNodeLifecycle::Serving,
                vec![ServiceEndpoint::new(ServiceKind::Retain, 9, "node9")],
            ))
            .await
            .unwrap();
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                7,
            ))
            .await
            .unwrap();
        let executor = LeaderRoutedKvRangeExecutor::new(router, membership, fallback, factory);
        let value = executor.get("range-1", b"k").await.unwrap().unwrap();
        assert_eq!(value, Bytes::from_static(b"v"));
    }

    struct TransportErrorExecutor;

    #[async_trait]
    impl KvRangeExecutor for TransportErrorExecutor {
        async fn get(&self, _range_id: &str, _key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            anyhow::bail!("transport error")
        }
        async fn scan(
            &self,
            _range_id: &str,
            _boundary: Option<greenmqtt_core::RangeBoundary>,
            _limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            unreachable!()
        }
        async fn apply(
            &self,
            _range_id: &str,
            _mutations: Vec<KvMutation>,
        ) -> anyhow::Result<()> {
            anyhow::bail!("transport error")
        }
        async fn checkpoint(
            &self,
            _range_id: &str,
            _checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            unreachable!()
        }
        async fn snapshot(&self, _range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn leader_routed_executor_falls_back_to_other_replicas_when_leader_transport_fails() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback: Arc<dyn KvRangeExecutor> = Arc::new(TestExecutor::default());
        let leader_impl: Arc<dyn KvRangeExecutor> = Arc::new(TransportErrorExecutor);
        let replica_impl = Arc::new(TestExecutor::default());
        let factory = Arc::new(TestExecutorFactory::default());
        factory.register("node7", leader_impl);
        factory.register("node8", replica_impl.clone());
        for (node_id, endpoint) in [(7, "node7"), (8, "node8")] {
            membership
                .upsert_member(ClusterNodeMembership::new(
                    node_id,
                    1,
                    ClusterNodeLifecycle::Serving,
                    vec![ServiceEndpoint::new(ServiceKind::Retain, node_id, endpoint)],
                ))
                .await
                .unwrap();
        }
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
                1,
                1,
                Some(7),
                vec![
                    RangeReplica::new(7, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                    RangeReplica::new(8, ReplicaRole::Voter, ReplicaSyncState::Replicating),
                ],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();

        let executor = LeaderRoutedKvRangeExecutor::new(router, membership, fallback, factory);
        executor
            .apply(
                "range-1",
                vec![KvMutation {
                    key: Bytes::from_static(b"beta"),
                    value: Some(Bytes::from_static(b"two")),
                }],
            )
            .await
            .unwrap();

        assert_eq!(
            replica_impl.get("range-1", b"beta").await.unwrap().unwrap(),
            Bytes::from_static(b"two")
        );
    }

    struct EpochMismatchExecutor {
        failed: AtomicBool,
    }

    #[async_trait]
    impl KvRangeExecutor for EpochMismatchExecutor {
        async fn get(&self, _range_id: &str, _key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                anyhow::bail!("kv/epoch-mismatch");
            }
            Ok(Some(Bytes::from_static(b"fresh")))
        }
        async fn scan(
            &self,
            _range_id: &str,
            _boundary: Option<greenmqtt_core::RangeBoundary>,
            _limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            unreachable!()
        }
        async fn apply(
            &self,
            _range_id: &str,
            _mutations: Vec<KvMutation>,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn checkpoint(
            &self,
            _range_id: &str,
            _checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            unreachable!()
        }
        async fn snapshot(&self, _range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn leader_routed_executor_refreshes_route_after_epoch_mismatch() {
        let router = Arc::new(MemoryKvRangeRouter::default());
        let membership = Arc::new(TestMembershipRegistry::default());
        let fallback: Arc<dyn KvRangeExecutor> = Arc::new(EpochMismatchExecutor {
            failed: AtomicBool::new(false),
        });
        router
            .upsert(descriptor(
                "range-1",
                ServiceShardKey::retain("tenant-a"),
                b"a",
                b"z",
                1,
            ))
            .await
            .unwrap();
        let factory = Arc::new(TestExecutorFactory::default());
        let executor = LeaderRoutedKvRangeExecutor::new(router, membership, fallback, factory);
        let value = executor.get("range-1", b"k").await.unwrap().unwrap();
        assert_eq!(value, Bytes::from_static(b"fresh"));
    }
}
