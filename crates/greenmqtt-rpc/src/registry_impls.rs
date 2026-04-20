use super::*;

impl StaticPeerForwarder {
    pub fn push_delivery_calls(&self) -> usize {
        self.push_delivery_calls.load(Ordering::SeqCst)
    }

    pub fn push_deliveries_calls(&self) -> usize {
        self.push_deliveries_calls.load(Ordering::SeqCst)
    }
}

impl DynamicServiceEndpointRegistry {
    async fn validate_current_assignment(
        &self,
        expected: &ServiceShardAssignment,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&expected.shard)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no assignment registered for shard {:?}", expected.shard)
            })?;
        anyhow::ensure!(
            current.matches_owner_epoch_fence(expected),
            "stale shard assignment for {:?}: expected owner={} epoch={} fence={}, current owner={} epoch={} fence={}",
            expected.shard,
            expected.owner_node_id(),
            expected.epoch,
            expected.fencing_token,
            current.owner_node_id(),
            current.epoch,
            current.fencing_token,
        );
        Ok(current)
    }

    pub async fn sync_members(&self, members: Vec<ClusterNodeMembership>) -> anyhow::Result<usize> {
        let mut guard = self.members.write().expect("service registry poisoned");
        let mut next = BTreeMap::new();
        let mut changed = 0usize;
        for member in members {
            if guard.get(&member.node_id) != Some(&member) {
                changed += 1;
            }
            next.insert(member.node_id, member);
        }
        changed += guard
            .keys()
            .filter(|node_id| !next.contains_key(node_id))
            .count();
        *guard = next;
        Ok(changed)
    }

    pub async fn transition_shard_to_member(
        &self,
        kind: ServiceShardTransitionKind,
        shard: ServiceShardKey,
        source_node_id: Option<NodeId>,
        target_node_id: NodeId,
        lifecycle: ServiceShardLifecycle,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let member = self
            .resolve_member(target_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target member {target_node_id} not found"))?;
        let service_kind = shard.service_kind();
        let endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == service_kind)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("target member {target_node_id} has no {service_kind:?} endpoint")
            })?;
        let current = self.resolve_assignment(&shard).await?;
        let next_epoch = current
            .as_ref()
            .map(|assignment| assignment.epoch + 1)
            .unwrap_or(1);
        let next_fencing_token = current
            .as_ref()
            .map(|assignment| assignment.fencing_token + 1)
            .unwrap_or(1);
        let assignment = ServiceShardAssignment::new(
            shard.clone(),
            ServiceEndpoint::new(service_kind, target_node_id, endpoint.endpoint),
            next_epoch,
            next_fencing_token,
            lifecycle,
        );
        self.apply_transition(ServiceShardTransition::new(
            kind,
            shard,
            source_node_id,
            assignment.clone(),
        ))
        .await?;
        Ok(assignment)
    }

    fn select_replacement_member(
        &self,
        service_kind: ServiceKind,
        excluded_node_id: NodeId,
    ) -> Option<NodeId> {
        let members = self.members.read().expect("service registry poisoned");
        members
            .values()
            .filter(|member| {
                member.node_id != excluded_node_id
                    && matches!(
                        member.lifecycle,
                        greenmqtt_core::ClusterNodeLifecycle::Serving
                    )
                    && member
                        .endpoints
                        .iter()
                        .any(|endpoint| endpoint.kind == service_kind)
            })
            .map(|member| member.node_id)
            .min()
    }

    pub async fn move_shard_to_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no assignment registered for shard {:?}", shard))?;
        self.move_shard_to_member_with_fencing(&current, target_node_id)
            .await
    }

    pub async fn move_shard_to_member_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.validate_current_assignment(expected_source).await?;
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Migration,
            expected_source.shard.clone(),
            Some(expected_source.owner_node_id()),
            target_node_id,
            ServiceShardLifecycle::Draining,
        )
        .await
    }

    pub async fn failover_shard_to_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_node_id = self
            .resolve_assignment(&shard)
            .await?
            .map(|assignment| assignment.owner_node_id());
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Failover,
            shard,
            source_node_id,
            target_node_id,
            ServiceShardLifecycle::Recovering,
        )
        .await
    }

    pub async fn bootstrap_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.transition_shard_to_member(
            ServiceShardTransitionKind::Bootstrap,
            shard,
            None,
            target_node_id,
            ServiceShardLifecycle::Bootstrapping,
        )
        .await
    }

    pub async fn catch_up_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no assignment registered for shard {:?}", shard))?;
        self.catch_up_shard_on_member_with_fencing(&current, target_node_id)
            .await
    }

    pub async fn catch_up_shard_on_member_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.validate_current_assignment(expected_source).await?;
        self.transition_shard_to_member(
            ServiceShardTransitionKind::CatchUp,
            expected_source.shard.clone(),
            Some(expected_source.owner_node_id()),
            target_node_id,
            ServiceShardLifecycle::Recovering,
        )
        .await
    }

    pub async fn repair_shard_on_member(
        &self,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let current = self
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no assignment registered for shard {:?}", shard))?;
        self.repair_shard_on_member_with_fencing(&current, target_node_id)
            .await
    }

    pub async fn repair_shard_on_member_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        self.validate_current_assignment(expected_source).await?;
        self.transition_shard_to_member(
            ServiceShardTransitionKind::AntiEntropy,
            expected_source.shard.clone(),
            Some(expected_source.owner_node_id()),
            target_node_id,
            ServiceShardLifecycle::Serving,
        )
        .await
    }

    pub async fn drain_shard(
        &self,
        shard: ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let Some(current) = self.resolve_assignment(&shard).await? else {
            return Ok(None);
        };
        self.drain_shard_with_fencing(&current).await
    }

    pub async fn drain_shard_with_fencing(
        &self,
        expected_source: &ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let current = self.validate_current_assignment(expected_source).await?;
        let drained = ServiceShardAssignment::new(
            current.shard.clone(),
            current.endpoint.clone(),
            current.epoch + 1,
            current.fencing_token + 1,
            ServiceShardLifecycle::Draining,
        );
        self.apply_transition(ServiceShardTransition::new(
            ServiceShardTransitionKind::Migration,
            current.shard.clone(),
            Some(current.owner_node_id()),
            drained.clone(),
        ))
        .await?;
        Ok(Some(drained))
    }

    pub async fn handle_member_lifecycle_transition(
        &self,
        node_id: NodeId,
        lifecycle: greenmqtt_core::ClusterNodeLifecycle,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let _ = self
            .set_member_lifecycle(node_id, lifecycle.clone())
            .await?;
        let owned: Vec<_> = self
            .list_assignments(None)
            .await?
            .into_iter()
            .filter(|assignment| assignment.owner_node_id() == node_id)
            .collect();
        let mut transitioned = Vec::new();
        for assignment in owned {
            let Some(target_node_id) =
                self.select_replacement_member(assignment.shard.service_kind(), node_id)
            else {
                continue;
            };
            let next = match lifecycle {
                greenmqtt_core::ClusterNodeLifecycle::Leaving => {
                    self.move_shard_to_member_with_fencing(&assignment, target_node_id)
                        .await?
                }
                greenmqtt_core::ClusterNodeLifecycle::Offline => {
                    self.failover_shard_to_member(assignment.shard.clone(), target_node_id)
                        .await?
                }
                _ => continue,
            };
            transitioned.push(next);
        }
        Ok(transitioned)
    }
}

impl PersistentMetadataRegistry {
    pub async fn open(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let engine = Arc::new(RocksDbKvEngine::open(path.into())?);
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: METADATA_RANGE_ID.to_string(),
                boundary: greenmqtt_core::RangeBoundary::full(),
            })
            .await?;
        Ok(Self {
            engine,
            range_id: METADATA_RANGE_ID.to_string(),
        })
    }

    async fn read_value<T: DeserializeOwned>(&self, key: Bytes) -> anyhow::Result<Option<T>> {
        let range = self.engine.open_range(&self.range_id).await?;
        range
            .reader()
            .get(&key)
            .await?
            .map(|value| decode_metadata_value(&value))
            .transpose()
    }

    async fn write_value<T: Serialize>(&self, key: Bytes, value: &T) -> anyhow::Result<()> {
        let range = self.engine.open_range(&self.range_id).await?;
        range
            .writer()
            .apply(vec![KvMutation {
                key,
                value: Some(encode_metadata_value(value)?),
            }])
            .await
    }

    async fn delete_value<T: DeserializeOwned>(&self, key: Bytes) -> anyhow::Result<Option<T>> {
        let previous = self.read_value::<T>(key.clone()).await?;
        if previous.is_some() {
            let range = self.engine.open_range(&self.range_id).await?;
            range
                .writer()
                .apply(vec![KvMutation { key, value: None }])
                .await?;
        }
        Ok(previous)
    }

    async fn scan_prefix<T: DeserializeOwned>(&self, prefix: &[u8]) -> anyhow::Result<Vec<T>> {
        let range = self.engine.open_range(&self.range_id).await?;
        Ok(range
            .reader()
            .scan(&greenmqtt_core::RangeBoundary::full(), usize::MAX)
            .await?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(_, value)| decode_metadata_value(&value))
            .collect::<Result<Vec<_>, _>>()?)
    }
}

impl ReplicatedMetadataRegistry {
    pub fn new(executor: Arc<dyn KvRangeExecutor>, range_id: impl Into<String>) -> Self {
        Self {
            executor,
            layout: MetadataPlaneLayout::single(range_id),
        }
    }

    pub fn with_layout(executor: Arc<dyn KvRangeExecutor>, layout: MetadataPlaneLayout) -> Self {
        Self { executor, layout }
    }

    pub fn sharded(executor: Arc<dyn KvRangeExecutor>, base_range_id: impl Into<String>) -> Self {
        Self {
            executor,
            layout: MetadataPlaneLayout::sharded(base_range_id),
        }
    }

    async fn read_value<T: DeserializeOwned>(&self, key: Bytes) -> anyhow::Result<Option<T>> {
        let range_id = self.layout.range_for_key(&key).to_string();
        self.executor
            .get(&range_id, &key)
            .await?
            .map(|value| decode_metadata_value(&value))
            .transpose()
    }

    async fn write_value<T: Serialize>(&self, key: Bytes, value: &T) -> anyhow::Result<()> {
        let range_id = self.layout.range_for_key(&key).to_string();
        self.executor
            .apply(
                &range_id,
                vec![KvMutation {
                    key,
                    value: Some(encode_metadata_value(value)?),
                }],
            )
            .await
    }

    async fn delete_value<T: DeserializeOwned>(&self, key: Bytes) -> anyhow::Result<Option<T>> {
        let previous = self.read_value::<T>(key.clone()).await?;
        if previous.is_some() {
            let range_id = self.layout.range_for_key(&key).to_string();
            self.executor
                .apply(&range_id, vec![KvMutation { key, value: None }])
                .await?;
        }
        Ok(previous)
    }

    async fn scan_prefix<T: DeserializeOwned>(&self, prefix: &[u8]) -> anyhow::Result<Vec<T>> {
        let range_id = self.layout.range_for_prefix(prefix).to_string();
        Ok(self
            .executor
            .scan(
                &range_id,
                Some(greenmqtt_core::RangeBoundary::full()),
                usize::MAX,
            )
            .await?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(_, value)| decode_metadata_value(&value))
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn scan_entries(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let range_id = self.layout.range_for_prefix(prefix).to_string();
        Ok(self
            .executor
            .scan(
                &range_id,
                Some(greenmqtt_core::RangeBoundary::full()),
                usize::MAX,
            )
            .await?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .collect())
    }
}

#[async_trait]
impl ServiceEndpointRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("service registry poisoned")
            .insert(assignment.shard.clone(), assignment))
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .read()
            .expect("service registry poisoned")
            .get(shard)
            .cloned())
    }

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        Ok(self
            .assignments
            .write()
            .expect("service registry poisoned")
            .remove(shard))
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments: Vec<_> = self
            .assignments
            .read()
            .expect("service registry poisoned")
            .values()
            .filter(|assignment| {
                kind.as_ref()
                    .map(|kind| assignment.shard.kind == *kind)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        assignments.sort_by(|left, right| left.shard.cmp(&right.shard));
        Ok(assignments)
    }
}

#[async_trait]
impl ServiceEndpointRegistry for PersistentMetadataRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let key = assignment_key(&assignment.shard)?;
        let previous = self
            .read_value::<ServiceShardAssignment>(key.clone())
            .await?;
        self.write_value(key, &assignment).await?;
        Ok(previous)
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.read_value(assignment_key(shard)?).await
    }

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.delete_value(assignment_key(shard)?).await
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments = self
            .scan_prefix::<ServiceShardAssignment>(ASSIGNMENT_PREFIX)
            .await?;
        if let Some(kind) = kind {
            assignments.retain(|assignment| assignment.shard.kind == kind);
        }
        assignments.sort_by(|left, right| left.shard.cmp(&right.shard));
        Ok(assignments)
    }
}

#[async_trait]
impl ServiceEndpointRegistry for ReplicatedMetadataRegistry {
    async fn upsert_assignment(
        &self,
        assignment: ServiceShardAssignment,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let key = assignment_key(&assignment.shard)?;
        let previous = self
            .read_value::<ServiceShardAssignment>(key.clone())
            .await?;
        self.write_value(key, &assignment).await?;
        Ok(previous)
    }

    async fn resolve_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.read_value(assignment_key(shard)?).await
    }

    async fn remove_assignment(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.delete_value(assignment_key(shard)?).await
    }

    async fn list_assignments(
        &self,
        kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ServiceShardAssignment>> {
        let mut assignments = self
            .scan_prefix::<ServiceShardAssignment>(ASSIGNMENT_PREFIX)
            .await?;
        if let Some(kind) = kind {
            assignments.retain(|assignment| assignment.shard.kind == kind);
        }
        assignments.sort_by(|left, right| left.shard.cmp(&right.shard));
        Ok(assignments)
    }
}

#[async_trait]
impl ReplicatedRangeRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .ranges
            .write()
            .expect("service registry poisoned")
            .insert(descriptor.id.clone(), descriptor))
    }

    async fn resolve_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .ranges
            .read()
            .expect("service registry poisoned")
            .get(range_id)
            .cloned())
    }

    async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .ranges
            .write()
            .expect("service registry poisoned")
            .remove(range_id))
    }

    async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut ranges: Vec<_> = self
            .ranges
            .read()
            .expect("service registry poisoned")
            .values()
            .filter(|descriptor| {
                shard_kind
                    .as_ref()
                    .map(|kind| descriptor.shard.kind == *kind)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        ranges.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(ranges)
    }
}

#[async_trait]
impl ReplicatedRangeRegistry for PersistentMetadataRegistry {
    async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let key = range_key(&descriptor.id);
        let previous = self
            .read_value::<ReplicatedRangeDescriptor>(key.clone())
            .await?;
        self.write_value(key, &descriptor).await?;
        Ok(previous)
    }

    async fn resolve_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.read_value(range_key(range_id)).await
    }

    async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.delete_value(range_key(range_id)).await
    }

    async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut ranges = self
            .scan_prefix::<ReplicatedRangeDescriptor>(RANGE_PREFIX)
            .await?;
        if let Some(kind) = shard_kind {
            ranges.retain(|descriptor| descriptor.shard.kind == kind);
        }
        ranges.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(ranges)
    }
}

#[async_trait]
impl ReplicatedRangeRegistry for ReplicatedMetadataRegistry {
    async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let key = range_key(&descriptor.id);
        let previous = self
            .read_value::<ReplicatedRangeDescriptor>(key.clone())
            .await?;
        self.write_value(key, &descriptor).await?;
        Ok(previous)
    }

    async fn resolve_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.read_value(range_key(range_id)).await
    }

    async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.delete_value(range_key(range_id)).await
    }

    async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut ranges = self
            .scan_prefix::<ReplicatedRangeDescriptor>(RANGE_PREFIX)
            .await?;
        if let Some(kind) = shard_kind {
            ranges.retain(|descriptor| descriptor.shard.kind == kind);
        }
        ranges.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(ranges)
    }
}

#[async_trait]
impl BalancerStateRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_balancer_state(
        &self,
        balancer_name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        Ok(self
            .balancer_states
            .write()
            .expect("service registry poisoned")
            .insert(balancer_name.to_string(), state))
    }

    async fn resolve_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        Ok(self
            .balancer_states
            .read()
            .expect("service registry poisoned")
            .get(balancer_name)
            .cloned())
    }

    async fn remove_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        Ok(self
            .balancer_states
            .write()
            .expect("service registry poisoned")
            .remove(balancer_name))
    }

    async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        Ok(self
            .balancer_states
            .read()
            .expect("service registry poisoned")
            .clone())
    }
}

#[async_trait]
impl BalancerStateRegistry for PersistentMetadataRegistry {
    async fn upsert_balancer_state(
        &self,
        balancer_name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        let key = balancer_key(balancer_name);
        let previous = self.read_value::<BalancerState>(key.clone()).await?;
        self.write_value(key, &state).await?;
        Ok(previous)
    }

    async fn resolve_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.read_value(balancer_key(balancer_name)).await
    }

    async fn remove_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.delete_value(balancer_key(balancer_name)).await
    }

    async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        let range = self.engine.open_range(&self.range_id).await?;
        let mut states = BTreeMap::new();
        for (key, value) in range
            .reader()
            .scan(&greenmqtt_core::RangeBoundary::full(), usize::MAX)
            .await?
        {
            if !key.starts_with(BALANCER_PREFIX) {
                continue;
            }
            let name = std::str::from_utf8(&key[BALANCER_PREFIX.len()..])?.to_string();
            states.insert(name, decode_metadata_value(&value)?);
        }
        Ok(states)
    }
}

#[async_trait]
impl BalancerStateRegistry for ReplicatedMetadataRegistry {
    async fn upsert_balancer_state(
        &self,
        balancer_name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        let key = balancer_key(balancer_name);
        let previous = self.read_value::<BalancerState>(key.clone()).await?;
        self.write_value(key, &state).await?;
        Ok(previous)
    }

    async fn resolve_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.read_value(balancer_key(balancer_name)).await
    }

    async fn remove_balancer_state(
        &self,
        balancer_name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.delete_value(balancer_key(balancer_name)).await
    }

    async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        let mut states = BTreeMap::new();
        for (key, value) in self.scan_entries(BALANCER_PREFIX).await? {
            let name = std::str::from_utf8(&key[BALANCER_PREFIX.len()..])?.to_string();
            states.insert(name, decode_metadata_value(&value)?);
        }
        Ok(states)
    }
}

#[async_trait]
impl RangeReconfigurationRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_reconfiguration_state(
        &self,
        state: RangeReconfigurationState,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        Ok(self
            .reconfiguration_states
            .write()
            .expect("service registry poisoned")
            .insert(state.range_id.clone(), state))
    }

    async fn resolve_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        Ok(self
            .reconfiguration_states
            .read()
            .expect("service registry poisoned")
            .get(range_id)
            .cloned())
    }

    async fn remove_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        Ok(self
            .reconfiguration_states
            .write()
            .expect("service registry poisoned")
            .remove(range_id))
    }

    async fn list_reconfiguration_states(&self) -> anyhow::Result<Vec<RangeReconfigurationState>> {
        let mut states: Vec<_> = self
            .reconfiguration_states
            .read()
            .expect("service registry poisoned")
            .values()
            .cloned()
            .collect();
        states.sort_by(|left, right| left.range_id.cmp(&right.range_id));
        Ok(states)
    }
}

#[async_trait]
impl RangeReconfigurationRegistry for PersistentMetadataRegistry {
    async fn upsert_reconfiguration_state(
        &self,
        state: RangeReconfigurationState,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        let key = reconfiguration_key(&state.range_id);
        let previous = self
            .read_value::<RangeReconfigurationState>(key.clone())
            .await?;
        self.write_value(key, &state).await?;
        Ok(previous)
    }

    async fn resolve_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        self.read_value(reconfiguration_key(range_id)).await
    }

    async fn remove_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        self.delete_value(reconfiguration_key(range_id)).await
    }

    async fn list_reconfiguration_states(&self) -> anyhow::Result<Vec<RangeReconfigurationState>> {
        let mut states = self
            .scan_prefix::<RangeReconfigurationState>(RECONFIG_PREFIX)
            .await?;
        states.sort_by(|left, right| left.range_id.cmp(&right.range_id));
        Ok(states)
    }
}

#[async_trait]
impl RangeReconfigurationRegistry for ReplicatedMetadataRegistry {
    async fn upsert_reconfiguration_state(
        &self,
        state: RangeReconfigurationState,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        let key = reconfiguration_key(&state.range_id);
        let previous = self
            .read_value::<RangeReconfigurationState>(key.clone())
            .await?;
        self.write_value(key, &state).await?;
        Ok(previous)
    }

    async fn resolve_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        self.read_value(reconfiguration_key(range_id)).await
    }

    async fn remove_reconfiguration_state(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeReconfigurationState>> {
        self.delete_value(reconfiguration_key(range_id)).await
    }

    async fn list_reconfiguration_states(&self) -> anyhow::Result<Vec<RangeReconfigurationState>> {
        let mut states = self
            .scan_prefix::<RangeReconfigurationState>(RECONFIG_PREFIX)
            .await?;
        states.sort_by(|left, right| left.range_id.cmp(&right.range_id));
        Ok(states)
    }
}

#[async_trait]
impl ControlCommandRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_control_command(
        &self,
        record: ControlCommandRecord,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        Ok(self
            .control_commands
            .write()
            .expect("service registry poisoned")
            .insert(record.command_id.clone(), record))
    }

    async fn resolve_control_command(
        &self,
        command_id: &str,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        Ok(self
            .control_commands
            .read()
            .expect("service registry poisoned")
            .get(command_id)
            .cloned())
    }

    async fn remove_control_command(
        &self,
        command_id: &str,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        Ok(self
            .control_commands
            .write()
            .expect("service registry poisoned")
            .remove(command_id))
    }

    async fn list_control_commands(&self) -> anyhow::Result<Vec<ControlCommandRecord>> {
        let mut commands: Vec<_> = self
            .control_commands
            .read()
            .expect("service registry poisoned")
            .values()
            .cloned()
            .collect();
        commands.sort_by(|left, right| left.command_id.cmp(&right.command_id));
        Ok(commands)
    }
}

#[async_trait]
impl ControlCommandRegistry for PersistentMetadataRegistry {
    async fn upsert_control_command(
        &self,
        record: ControlCommandRecord,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        let key = command_key(&record.command_id);
        let previous = self.read_value::<ControlCommandRecord>(key.clone()).await?;
        self.write_value(key, &record).await?;
        Ok(previous)
    }

    async fn resolve_control_command(
        &self,
        command_id: &str,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        self.read_value(command_key(command_id)).await
    }

    async fn remove_control_command(
        &self,
        command_id: &str,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        self.delete_value(command_key(command_id)).await
    }

    async fn list_control_commands(&self) -> anyhow::Result<Vec<ControlCommandRecord>> {
        let mut commands = self
            .scan_prefix::<ControlCommandRecord>(COMMAND_PREFIX)
            .await?;
        commands.sort_by(|left, right| left.command_id.cmp(&right.command_id));
        Ok(commands)
    }
}

#[async_trait]
impl ControlCommandRegistry for ReplicatedMetadataRegistry {
    async fn upsert_control_command(
        &self,
        record: ControlCommandRecord,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        let key = command_key(&record.command_id);
        let previous = self.read_value::<ControlCommandRecord>(key.clone()).await?;
        self.write_value(key, &record).await?;
        Ok(previous)
    }

    async fn resolve_control_command(
        &self,
        command_id: &str,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        self.read_value(command_key(command_id)).await
    }

    async fn remove_control_command(
        &self,
        command_id: &str,
    ) -> anyhow::Result<Option<ControlCommandRecord>> {
        self.delete_value(command_key(command_id)).await
    }

    async fn list_control_commands(&self) -> anyhow::Result<Vec<ControlCommandRecord>> {
        let mut commands = self
            .scan_prefix::<ControlCommandRecord>(COMMAND_PREFIX)
            .await?;
        commands.sort_by(|left, right| left.command_id.cmp(&right.command_id));
        Ok(commands)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for DynamicServiceEndpointRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("service registry poisoned")
            .insert(member.node_id, member))
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .read()
            .expect("service registry poisoned")
            .get(&node_id)
            .cloned())
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Ok(self
            .members
            .write()
            .expect("service registry poisoned")
            .remove(&node_id))
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut members: Vec<_> = self
            .members
            .read()
            .expect("service registry poisoned")
            .values()
            .cloned()
            .collect();
        members.sort_by_key(|member| member.node_id);
        Ok(members)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for PersistentMetadataRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let key = member_key(member.node_id);
        let previous = self
            .read_value::<ClusterNodeMembership>(key.clone())
            .await?;
        self.write_value(key, &member).await?;
        Ok(previous)
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.read_value(member_key(node_id)).await
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.delete_value(member_key(node_id)).await
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut members = self
            .scan_prefix::<ClusterNodeMembership>(MEMBER_PREFIX)
            .await?;
        members.sort_by_key(|member| member.node_id);
        Ok(members)
    }
}

#[async_trait]
impl ClusterMembershipRegistry for ReplicatedMetadataRegistry {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let key = member_key(member.node_id);
        let previous = self
            .read_value::<ClusterNodeMembership>(key.clone())
            .await?;
        self.write_value(key, &member).await?;
        Ok(previous)
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.read_value(member_key(node_id)).await
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.delete_value(member_key(node_id)).await
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut members = self
            .scan_prefix::<ClusterNodeMembership>(MEMBER_PREFIX)
            .await?;
        members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(members)
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for DynamicServiceEndpointRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        let mut assignments = self.assignments.write().expect("service registry poisoned");
        if let Some(source_node_id) = transition.source_node_id {
            if let Some(current) = assignments.get(&transition.shard) {
                anyhow::ensure!(
                    current.owner_node_id() == source_node_id,
                    "source owner mismatch for shard transition"
                );
            }
        }
        Ok(assignments.insert(transition.shard.clone(), transition.target_assignment))
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for PersistentMetadataRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        if let Some(source_node_id) = transition.source_node_id {
            if let Some(current) = self.resolve_assignment(&transition.shard).await? {
                anyhow::ensure!(
                    current.owner_node_id() == source_node_id,
                    "source owner mismatch for shard transition"
                );
            }
        }
        self.upsert_assignment(transition.target_assignment).await
    }
}

#[async_trait]
impl ServiceShardRecoveryControl for ReplicatedMetadataRegistry {
    async fn apply_transition(
        &self,
        transition: ServiceShardTransition,
    ) -> anyhow::Result<Option<ServiceShardAssignment>> {
        self.upsert_assignment(transition.target_assignment).await
    }
}
