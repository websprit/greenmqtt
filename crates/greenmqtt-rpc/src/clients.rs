use super::*;

impl SessionDictGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                SessionDictServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Self> {
        let shard = session_identity_shard(identity);
        let assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict endpoint registered for shard {shard:?}")
        })?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn export_shard_snapshot(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<SessionDictShardSnapshot> {
        anyhow::ensure!(
            shard.kind == ServiceShardKind::SessionDict,
            "sessiondict snapshot requires SessionDict shard"
        );
        let sessions = self
            .list_sessions(Some(&shard.tenant_id))
            .await?
            .into_iter()
            .filter(|record| shard_matches_sessiondict_record(shard, record))
            .collect();
        Ok(SessionDictShardSnapshot { sessions })
    }

    pub async fn stream_shard_snapshot(
        &self,
        assignment: &ServiceShardAssignment,
    ) -> anyhow::Result<SessionDictShardSnapshot> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .stream_shard_snapshot(shard_snapshot_request_for_assignment(assignment))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        let mut expected_checksum = None;
        while let Some(chunk) = stream.message().await? {
            if let Some(checksum) = expected_checksum {
                anyhow::ensure!(
                    checksum == chunk.checksum,
                    "sessiondict stream checksum drift"
                );
            } else {
                expected_checksum = Some(chunk.checksum);
            }
            bytes.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        let snapshot = SessionDictShardSnapshot::decode(&bytes)?;
        if let Some(checksum) = expected_checksum {
            anyhow::ensure!(
                snapshot.checksum()? == checksum,
                "sessiondict stream checksum mismatch"
            );
        }
        Ok(snapshot)
    }

    pub async fn import_shard_snapshot(
        &self,
        snapshot: &SessionDictShardSnapshot,
    ) -> anyhow::Result<usize> {
        for session in &snapshot.sessions {
            self.register(session.clone()).await?;
        }
        Ok(snapshot.sessions.len())
    }

    pub async fn move_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict assignment registered for shard {shard:?}")
        })?;
        let source =
            SessionDictGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .move_shard_to_member(shard.clone(), target_node_id)
            .await?;
        let target = SessionDictGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;

        for session in snapshot.sessions {
            source.unregister(&session.session_id).await?;
        }

        Ok(assignment)
    }

    pub async fn catch_up_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict assignment registered for shard {shard:?}")
        })?;
        let source =
            SessionDictGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let expected_checksum = snapshot.checksum()?;

        let assignment = registry
            .catch_up_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = SessionDictGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        let restored = target.export_shard_snapshot(&shard).await?;
        anyhow::ensure!(
            restored.checksum()? == expected_checksum,
            "sessiondict shard catch-up checksum mismatch"
        );
        Ok(assignment)
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        let shard = greenmqtt_sessiondict::session_scan_shard(tenant_id);
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no sessiondict assignment registered for shard {shard:?}")
        })?;
        let source =
            SessionDictGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::SessionDict)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no sessiondict endpoint")
            })?;
        let replica = SessionDictGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_snapshot = replica.export_shard_snapshot(&shard).await?;
        if source_snapshot.checksum()? != replica_snapshot.checksum()? {
            for record in &replica_snapshot.sessions {
                if !source_snapshot
                    .sessions
                    .iter()
                    .any(|candidate| candidate.session_id == record.session_id)
                {
                    replica.unregister(&record.session_id).await?;
                }
            }
            for record in &source_snapshot.sessions {
                if !replica_snapshot.sessions.iter().any(|candidate| {
                    candidate.session_id == record.session_id && candidate == record
                }) {
                    replica.register(record.clone()).await?;
                }
            }
            let repaired = replica.export_shard_snapshot(&shard).await?;
            anyhow::ensure!(
                repaired.checksum()? == source_snapshot.checksum()?,
                "sessiondict shard replica anti-entropy checksum mismatch"
            );
        }
        source_snapshot.checksum()
    }
}

impl DistGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                DistServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = dist_tenant_shard(tenant_id);
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn export_shard_snapshot(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<DistShardSnapshot> {
        Ok(DistShardSnapshot {
            routes: self.list_routes(Some(tenant_id)).await?,
        })
    }

    pub async fn stream_shard_snapshot(
        &self,
        assignment: &ServiceShardAssignment,
    ) -> anyhow::Result<DistShardSnapshot> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .stream_shard_snapshot(shard_snapshot_request_for_assignment(assignment))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        let mut expected_checksum = None;
        while let Some(chunk) = stream.message().await? {
            if let Some(checksum) = expected_checksum {
                anyhow::ensure!(checksum == chunk.checksum, "dist stream checksum drift");
            } else {
                expected_checksum = Some(chunk.checksum);
            }
            bytes.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        let snapshot = DistShardSnapshot::decode(&bytes)?;
        if let Some(checksum) = expected_checksum {
            anyhow::ensure!(
                snapshot.checksum()? == checksum,
                "dist stream checksum mismatch"
            );
        }
        Ok(snapshot)
    }

    pub async fn import_shard_snapshot(
        &self,
        snapshot: &DistShardSnapshot,
    ) -> anyhow::Result<usize> {
        for route in &snapshot.routes {
            self.add_route(route.clone()).await?;
        }
        Ok(snapshot.routes.len())
    }

    pub async fn move_tenant_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .move_shard_to_member(shard.clone(), target_node_id)
            .await?;
        let target = DistGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;

        for route in snapshot.routes {
            let mut migrated = route.clone();
            migrated.node_id = target_node_id;
            target.add_route(migrated.clone()).await?;
            source.remove_route(&route).await?;
        }

        Ok(assignment)
    }

    pub async fn catch_up_tenant_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let expected_checksum = snapshot.checksum()?;

        let assignment = registry
            .catch_up_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = DistGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        let restored = target.export_shard_snapshot(tenant_id).await?;
        anyhow::ensure!(
            restored.checksum()? == expected_checksum,
            "dist shard catch-up checksum mismatch"
        );
        Ok(assignment)
    }

    pub async fn anti_entropy_repair_tenant_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .repair_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = DistGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        let target_snapshot = target.stream_shard_snapshot(&assignment).await?;
        if source_snapshot.checksum()? != target_snapshot.checksum()? {
            for route in &target_snapshot.routes {
                if !source_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    target.remove_route(route).await?;
                }
            }
            for route in &source_snapshot.routes {
                if !target_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    target.add_route(route.clone()).await?;
                }
            }
            let repaired = target.stream_shard_snapshot(&assignment).await?;
            anyhow::ensure!(
                repaired.checksum()? == source_snapshot.checksum()?,
                "dist shard anti-entropy checksum mismatch"
            );
        }
        Ok(assignment)
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        let shard = dist_tenant_shard(tenant_id);
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no dist assignment registered for shard {shard:?}"))?;
        let source = DistGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::Dist)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no dist endpoint")
            })?;
        let replica = DistGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_snapshot = replica.export_shard_snapshot(tenant_id).await?;
        if source_snapshot.checksum()? != replica_snapshot.checksum()? {
            for route in &replica_snapshot.routes {
                if !source_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    replica.remove_route(route).await?;
                }
            }
            for route in &source_snapshot.routes {
                if !replica_snapshot
                    .routes
                    .iter()
                    .any(|candidate| same_route_identity(candidate, route))
                {
                    replica.add_route(route.clone()).await?;
                }
            }
            let repaired = replica.export_shard_snapshot(tenant_id).await?;
            anyhow::ensure!(
                repaired.checksum()? == source_snapshot.checksum()?,
                "dist shard replica anti-entropy checksum mismatch"
            );
        }
        source_snapshot.checksum()
    }
}

impl PeriodicAntiEntropyReconciler {
    pub fn new(
        registry: Arc<DynamicServiceEndpointRegistry>,
        interval: std::time::Duration,
    ) -> Self {
        Self { registry, interval }
    }

    pub async fn reconcile_dist_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        DistGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn reconcile_sessiondict_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        SessionDictGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn reconcile_inbox_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<(u64, u64)> {
        InboxGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn reconcile_retain_tenant_replica(
        &self,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        RetainGrpcClient::anti_entropy_sync_tenant_replica_via_registry(
            &self.registry,
            tenant_id,
            replica_node_id,
        )
        .await
    }

    pub async fn run_dist_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_dist_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_sessiondict_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_sessiondict_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_inbox_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_inbox_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_retain_tenant_replica_until_cancelled(
        &self,
        tenant_id: String,
        replica_node_id: NodeId,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.changed() => {
                    if *cancel.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.reconcile_retain_tenant_replica(&tenant_id, replica_node_id).await?;
                }
            }
        }
        Ok(())
    }
}

impl InboxGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                InboxServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
        session_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = inbox_session_shard(tenant_id, &session_id.to_string());
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn export_shard_snapshot(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<InboxShardSnapshot> {
        anyhow::ensure!(
            matches!(
                shard.kind,
                ServiceShardKind::Inbox | ServiceShardKind::Inflight
            ),
            "inbox snapshot requires Inbox or Inflight shard"
        );
        let subscriptions = if shard.kind == ServiceShardKind::Inflight {
            Vec::new()
        } else {
            self.list_all_subscriptions()
                .await?
                .into_iter()
                .filter(|subscription| {
                    shard_matches_inbox_session(
                        shard,
                        &subscription.tenant_id,
                        &subscription.session_id,
                    )
                })
                .collect()
        };
        let offline_messages = if shard.kind == ServiceShardKind::Inflight {
            Vec::new()
        } else {
            self.list_all_offline()
                .await?
                .into_iter()
                .filter(|message| {
                    shard_matches_inbox_session(shard, &message.tenant_id, &message.session_id)
                })
                .collect()
        };
        let inflight_messages = self
            .list_all_inflight()
            .await?
            .into_iter()
            .filter(|message| {
                shard_matches_inbox_session(shard, &message.tenant_id, &message.session_id)
            })
            .collect();
        Ok(InboxShardSnapshot {
            subscriptions,
            offline_messages,
            inflight_messages,
        })
    }

    pub async fn stream_shard_snapshot(
        &self,
        assignment: &ServiceShardAssignment,
    ) -> anyhow::Result<InboxShardSnapshot> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .stream_shard_snapshot(shard_snapshot_request_for_assignment(assignment))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        let mut expected_checksum = None;
        while let Some(chunk) = stream.message().await? {
            if let Some(checksum) = expected_checksum {
                anyhow::ensure!(checksum == chunk.checksum, "inbox stream checksum drift");
            } else {
                expected_checksum = Some(chunk.checksum);
            }
            bytes.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }
        let snapshot = InboxShardSnapshot::decode(&bytes)?;
        if let Some(checksum) = expected_checksum {
            anyhow::ensure!(
                snapshot.checksum()? == checksum,
                "inbox stream checksum mismatch"
            );
        }
        Ok(snapshot)
    }

    pub async fn import_shard_snapshot(
        &self,
        snapshot: &InboxShardSnapshot,
    ) -> anyhow::Result<usize> {
        for subscription in &snapshot.subscriptions {
            self.subscribe(subscription.clone()).await?;
        }
        for message in &snapshot.offline_messages {
            self.enqueue(message.clone()).await?;
        }
        for message in &snapshot.inflight_messages {
            self.stage_inflight(message.clone()).await?;
        }
        Ok(snapshot.subscriptions.len()
            + snapshot.offline_messages.len()
            + snapshot.inflight_messages.len())
    }

    pub async fn send_lwt(&self, publish: DelayedLwtPublish) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .send_lwt(InboxSendLwtRequest {
                tenant_id: publish.tenant_id,
                session_id: publish.session_id,
                topic: publish.publish.topic,
                payload: publish.publish.payload.to_vec(),
                qos: publish.publish.qos as u32,
                retain: publish.publish.retain,
                properties: Some(greenmqtt_proto::to_proto_publish_properties(
                    &publish.publish.properties,
                )),
            })
            .await?;
        Ok(())
    }

    pub async fn expire_all(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxMaintenanceReply> {
        let mut client = self.inner.lock().await;
        Ok(client
            .expire_all(InboxExpireAllRequest {
                tenant_id: tenant_id.to_string(),
                now_ms,
            })
            .await?
            .into_inner())
    }

    pub async fn tenant_gc_preview(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<InboxMaintenanceReply> {
        let mut client = self.inner.lock().await;
        Ok(client
            .tenant_gc_preview(InboxTenantGcRequest {
                tenant_id: tenant_id.to_string(),
                now_ms: 0,
            })
            .await?
            .into_inner())
    }

    pub async fn tenant_gc_run(
        &self,
        tenant_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<InboxMaintenanceReply> {
        let mut client = self.inner.lock().await;
        Ok(client
            .tenant_gc_run(InboxTenantGcRequest {
                tenant_id: tenant_id.to_string(),
                now_ms,
            })
            .await?
            .into_inner())
    }

    pub async fn move_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        anyhow::ensure!(
            matches!(
                shard.kind,
                ServiceShardKind::Inbox | ServiceShardKind::Inflight
            ),
            "inbox move requires Inbox or Inflight shard"
        );
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox assignment registered for shard {shard:?}"))?;
        let source = InboxGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;

        let assignment = registry
            .move_shard_to_member(shard.clone(), target_node_id)
            .await?;
        let target = InboxGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        #[cfg(test)]
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let late_snapshot = source.export_shard_snapshot(&shard).await?;
        target.import_shard_snapshot(&late_snapshot).await?;

        let mut session_ids = BTreeMap::<String, ()>::new();
        for subscription in &snapshot.subscriptions {
            session_ids.insert(subscription.session_id.clone(), ());
        }
        for message in &snapshot.offline_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for message in &snapshot.inflight_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for subscription in &late_snapshot.subscriptions {
            session_ids.insert(subscription.session_id.clone(), ());
        }
        for message in &late_snapshot.offline_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for message in &late_snapshot.inflight_messages {
            session_ids.insert(message.session_id.clone(), ());
        }
        for session_id in session_ids.into_keys() {
            source.purge_session(&session_id).await?;
        }

        Ok(assignment)
    }

    pub async fn catch_up_shard_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        shard: ServiceShardKey,
        target_node_id: NodeId,
    ) -> anyhow::Result<ServiceShardAssignment> {
        anyhow::ensure!(
            matches!(
                shard.kind,
                ServiceShardKind::Inbox | ServiceShardKind::Inflight
            ),
            "inbox catch-up requires Inbox or Inflight shard"
        );
        let source_assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no inbox assignment registered for shard {shard:?}"))?;
        let source = InboxGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let snapshot = source.stream_shard_snapshot(&source_assignment).await?;
        let expected_checksum = snapshot.checksum()?;

        let assignment = registry
            .catch_up_shard_on_member(shard.clone(), target_node_id)
            .await?;
        let target = InboxGrpcClient::connect(assignment.endpoint.endpoint.clone()).await?;
        target.import_shard_snapshot(&snapshot).await?;
        let restored = target.export_shard_snapshot(&shard).await?;
        anyhow::ensure!(
            restored.checksum()? == expected_checksum,
            "inbox shard catch-up checksum mismatch"
        );
        Ok(assignment)
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<(u64, u64)> {
        let source_assignment = registry
            .resolve_assignment(&inbox_tenant_scan_shard(tenant_id))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("no inbox assignment registered for tenant {tenant_id}")
            })?;
        let source = InboxGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_inbox_snapshot = source
            .export_shard_snapshot(&inbox_tenant_scan_shard(tenant_id))
            .await?;
        let source_inflight_snapshot = source
            .export_shard_snapshot(&inflight_tenant_scan_shard(tenant_id))
            .await?;

        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::Inbox)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no inbox endpoint")
            })?;
        let replica = InboxGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_inbox_snapshot = replica
            .export_shard_snapshot(&inbox_tenant_scan_shard(tenant_id))
            .await?;
        let replica_inflight_snapshot = replica
            .export_shard_snapshot(&inflight_tenant_scan_shard(tenant_id))
            .await?;

        if source_inbox_snapshot.checksum()? != replica_inbox_snapshot.checksum()? {
            let mut session_ids = BTreeMap::<String, ()>::new();
            for subscription in &source_inbox_snapshot.subscriptions {
                session_ids.insert(subscription.session_id.clone(), ());
            }
            for subscription in &replica_inbox_snapshot.subscriptions {
                session_ids.insert(subscription.session_id.clone(), ());
            }
            for message in &source_inbox_snapshot.offline_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for message in &replica_inbox_snapshot.offline_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for session_id in session_ids.keys() {
                let _ = replica.purge_session_subscriptions_only(session_id).await?;
                let _ = replica.purge_offline(session_id).await?;
            }
            for subscription in &source_inbox_snapshot.subscriptions {
                replica.subscribe(subscription.clone()).await?;
            }
            for message in &source_inbox_snapshot.offline_messages {
                replica.enqueue(message.clone()).await?;
            }
        }

        if source_inflight_snapshot.checksum()? != replica_inflight_snapshot.checksum()? {
            let mut session_ids = BTreeMap::<String, ()>::new();
            for message in &source_inflight_snapshot.inflight_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for message in &replica_inflight_snapshot.inflight_messages {
                session_ids.insert(message.session_id.clone(), ());
            }
            for session_id in session_ids.keys() {
                let _ = replica.purge_inflight_session(session_id).await?;
            }
            for message in &source_inflight_snapshot.inflight_messages {
                replica.stage_inflight(message.clone()).await?;
            }
        }

        let repaired_inbox_snapshot = replica
            .export_shard_snapshot(&inbox_tenant_scan_shard(tenant_id))
            .await?;
        let repaired_inflight_snapshot = replica
            .export_shard_snapshot(&inflight_tenant_scan_shard(tenant_id))
            .await?;
        anyhow::ensure!(
            repaired_inbox_snapshot.checksum()? == source_inbox_snapshot.checksum()?,
            "inbox tenant replica anti-entropy checksum mismatch"
        );
        anyhow::ensure!(
            repaired_inflight_snapshot.checksum()? == source_inflight_snapshot.checksum()?,
            "inflight tenant replica anti-entropy checksum mismatch"
        );
        Ok((
            source_inbox_snapshot.checksum()?,
            source_inflight_snapshot.checksum()?,
        ))
    }
}

impl RetainGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RetainServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn connect_via_registry(
        registry: &dyn ServiceEndpointRegistry,
        tenant_id: &str,
    ) -> anyhow::Result<Self> {
        let shard = retain_tenant_shard(tenant_id);
        let assignment = registry
            .resolve_assignment(&shard)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no retain endpoint registered for shard {shard:?}"))?;
        Self::connect(assignment.endpoint.endpoint).await
    }

    pub async fn anti_entropy_sync_tenant_replica_via_registry(
        registry: &DynamicServiceEndpointRegistry,
        tenant_id: &str,
        replica_node_id: NodeId,
    ) -> anyhow::Result<u64> {
        let shard = retain_tenant_shard(tenant_id);
        let source_assignment = registry.resolve_assignment(&shard).await?.ok_or_else(|| {
            anyhow::anyhow!("no retain assignment registered for shard {shard:?}")
        })?;
        let source = RetainGrpcClient::connect(source_assignment.endpoint.endpoint.clone()).await?;
        let source_snapshot = source.list_tenant_retained(tenant_id).await?;
        let source_checksum = snapshot_checksum(&source_snapshot)?;

        let member = registry
            .resolve_member(replica_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replica member {replica_node_id} not found"))?;
        let replica_endpoint = member
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == ServiceKind::Retain)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("replica member {replica_node_id} has no retain endpoint")
            })?;
        let replica = RetainGrpcClient::connect(replica_endpoint.endpoint).await?;
        let replica_snapshot = replica.list_tenant_retained(tenant_id).await?;
        if source_checksum != snapshot_checksum(&replica_snapshot)? {
            for message in &replica_snapshot {
                if !source_snapshot
                    .iter()
                    .any(|candidate| candidate.topic == message.topic)
                {
                    let mut delete = message.clone();
                    delete.payload = Bytes::new();
                    replica.retain(delete).await?;
                }
            }
            for message in &source_snapshot {
                if !replica_snapshot
                    .iter()
                    .any(|candidate| candidate.topic == message.topic && candidate == message)
                {
                    replica.retain(message.clone()).await?;
                }
            }
            let repaired = replica.list_tenant_retained(tenant_id).await?;
            anyhow::ensure!(
                snapshot_checksum(&repaired)? == source_checksum,
                "retain shard replica anti-entropy checksum mismatch"
            );
        }
        Ok(source_checksum)
    }

    pub async fn expire_all(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client
            .expire_all(RetainExpireAllRequest {
                tenant_id: tenant_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.count as usize)
    }

    pub async fn tenant_gc_preview(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client
            .tenant_gc_preview(RetainTenantGcRequest {
                tenant_id: tenant_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.count as usize)
    }

    pub async fn tenant_gc_run(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let mut client = self.inner.lock().await;
        let reply = client
            .tenant_gc_run(RetainTenantGcRequest {
                tenant_id: tenant_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.count as usize)
    }
}

impl MetadataGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                MetadataServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .upsert_member(MemberRecordReply {
                member: Some(to_proto_cluster_node_membership(&member)),
            })
            .await?
            .into_inner();
        Ok(reply.member.map(from_proto_cluster_node_membership))
    }

    pub async fn lookup_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_member(MemberLookupRequest { node_id })
            .await?
            .into_inner();
        Ok(reply.member.map(from_proto_cluster_node_membership))
    }

    pub async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_member(MemberLookupRequest { node_id })
            .await?
            .into_inner();
        Ok(reply.member.map(from_proto_cluster_node_membership))
    }

    pub async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        let mut client = self.inner.lock().await;
        let reply = client.list_members(()).await?.into_inner();
        Ok(reply
            .members
            .into_iter()
            .map(from_proto_cluster_node_membership)
            .collect())
    }

    pub async fn upsert_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .upsert_range(RangeUpsertRequest {
                descriptor: Some(to_proto_replicated_range(&descriptor)),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn lookup_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_range(RangeLookupRequest {
                range_id: range_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn remove_range(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_range(RangeLookupRequest {
                range_id: range_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn list_ranges(
        &self,
        shard_kind: Option<ServiceShardKind>,
        tenant_id: Option<&str>,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .list_ranges(RangeListRequest {
                shard_kind: shard_kind
                    .as_ref()
                    .map(to_proto_shard_kind)
                    .unwrap_or_default(),
                tenant_id: tenant_id.unwrap_or_default().to_string(),
                scope: scope.unwrap_or_default().to_string(),
            })
            .await?
            .into_inner();
        Ok(reply
            .descriptors
            .into_iter()
            .map(from_proto_replicated_range)
            .collect())
    }

    pub async fn route_range(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .route_range(RouteRangeRequest {
                shard_kind: to_proto_shard_kind(&shard.kind),
                tenant_id: shard.tenant_id.clone(),
                scope: shard.scope.clone(),
                key: key.to_vec(),
            })
            .await?
            .into_inner();
        Ok(reply.descriptor.map(from_proto_replicated_range))
    }

    pub async fn upsert_balancer_state(
        &self,
        name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .upsert_balancer_state(BalancerStateUpsertRequest {
                name: name.to_string(),
                state: Some(to_proto_balancer_state(&state)),
            })
            .await?
            .into_inner();
        Ok(reply.previous.map(from_proto_balancer_state))
    }

    pub async fn lookup_balancer_state(&self, name: &str) -> anyhow::Result<Option<BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .lookup_balancer_state(BalancerStateRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.state.map(from_proto_balancer_state))
    }

    pub async fn remove_balancer_state(&self, name: &str) -> anyhow::Result<Option<BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .remove_balancer_state(BalancerStateRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        Ok(reply.state.map(from_proto_balancer_state))
    }

    pub async fn list_balancer_states(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        let mut client = self.inner.lock().await;
        let reply = client.list_balancer_states(()).await?.into_inner();
        Ok(reply
            .entries
            .into_iter()
            .filter_map(|entry| {
                entry
                    .state
                    .map(|state| (entry.name, from_proto_balancer_state(state)))
            })
            .collect())
    }
}

impl KvRangeGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                KvRangeServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.get_fenced(range_id, key, None).await
    }

    pub async fn get_fenced(
        &self,
        range_id: &str,
        key: &[u8],
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<Option<Bytes>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .get(KvRangeGetRequest {
                range_id: range_id.to_string(),
                key: key.to_vec(),
                expected_epoch: expected_epoch.unwrap_or_default(),
            })
            .await?
            .into_inner();
        Ok(reply.found.then_some(Bytes::from(reply.value)))
    }

    pub async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        self.scan_fenced(range_id, boundary, limit, None).await
    }

    pub async fn scan_fenced(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut client = self.inner.lock().await;
        let reply = client
            .scan(KvRangeScanRequest {
                range_id: range_id.to_string(),
                boundary: boundary
                    .as_ref()
                    .map(greenmqtt_proto::to_proto_range_boundary),
                limit: limit as u32,
                expected_epoch: expected_epoch.unwrap_or_default(),
            })
            .await?
            .into_inner();
        Ok(reply
            .entries
            .into_iter()
            .map(greenmqtt_proto::from_proto_kv_entry)
            .collect())
    }

    pub async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        self.apply_fenced(range_id, mutations, None).await
    }

    pub async fn apply_fenced(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .apply(KvRangeApplyRequest {
                range_id: range_id.to_string(),
                mutations: mutations
                    .iter()
                    .map(greenmqtt_proto::to_proto_kv_mutation)
                    .collect(),
                expected_epoch: expected_epoch.unwrap_or_default(),
            })
            .await?;
        Ok(())
    }

    pub async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        self.checkpoint_fenced(range_id, checkpoint_id, None).await
    }

    pub async fn checkpoint_fenced(
        &self,
        range_id: &str,
        checkpoint_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let mut client = self.inner.lock().await;
        let reply = client
            .checkpoint(KvRangeCheckpointRequest {
                range_id: range_id.to_string(),
                checkpoint_id: checkpoint_id.to_string(),
                expected_epoch: expected_epoch.unwrap_or_default(),
            })
            .await?
            .into_inner();
        Ok(from_proto_kv_range_checkpoint(reply))
    }

    pub async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        self.snapshot_fenced(range_id, None).await
    }

    pub async fn snapshot_fenced(
        &self,
        range_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<KvRangeSnapshot> {
        let mut client = self.inner.lock().await;
        let reply = client
            .snapshot(KvRangeSnapshotRequest {
                range_id: range_id.to_string(),
                expected_epoch: expected_epoch.unwrap_or_default(),
            })
            .await?
            .into_inner();
        Ok(from_proto_kv_range_snapshot(reply))
    }
}

impl RaftTransportGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RaftTransportServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn send(
        &self,
        range_id: &str,
        from_node_id: NodeId,
        message: &RaftMessage,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        if matches!(message, RaftMessage::InstallSnapshot(_)) {
            counter!("kv_raft_snapshot_send_total", "range_id" => range_id.to_string())
                .increment(1);
        }
        client
            .send(to_proto_raft_transport_request(
                range_id,
                from_node_id,
                message,
            ))
            .await?;
        Ok(())
    }
}

impl RangeAdminGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RangeAdminServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn list_range_health(&self) -> anyhow::Result<RangeHealthListReply> {
        let mut client = self.inner.lock().await;
        Ok(client.list_range_health(()).await?.into_inner())
    }

    pub async fn list_range_health_snapshots(&self) -> anyhow::Result<Vec<RangeHealthSnapshot>> {
        Ok(self
            .list_range_health()
            .await?
            .entries
            .into_iter()
            .map(from_proto_range_health_record)
            .collect())
    }

    pub async fn get_range_health(&self, range_id: &str) -> anyhow::Result<RangeHealthReply> {
        let mut client = self.inner.lock().await;
        Ok(client
            .get_range_health(RangeHealthRequest {
                range_id: range_id.to_string(),
            })
            .await?
            .into_inner())
    }

    pub async fn get_range_health_snapshot(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeHealthSnapshot>> {
        Ok(self
            .get_range_health(range_id)
            .await?
            .health
            .map(from_proto_range_health_record))
    }

    pub async fn debug_dump(&self) -> anyhow::Result<String> {
        let mut client = self.inner.lock().await;
        Ok(client.debug_dump(()).await?.into_inner().text)
    }
}

fn from_proto_reconfiguration_record(
    record: greenmqtt_proto::internal::RangeReconfigurationRecord,
    range_id: String,
) -> RangeReconfigurationState {
    let phase = if record.has_phase {
        match record.phase.as_str() {
            "staging_learners" => Some(ReconfigurationPhase::StagingLearners),
            "joint_consensus" => Some(ReconfigurationPhase::JointConsensus),
            "finalizing" => Some(ReconfigurationPhase::Finalizing),
            _ => None,
        }
    } else {
        None
    };
    RangeReconfigurationState {
        range_id,
        old_voters: record.old_voters,
        old_learners: record.old_learners,
        current_voters: record.current_voters,
        current_learners: record.current_learners,
        joint_voters: record.joint_voters,
        joint_learners: record.joint_learners,
        pending_voters: record.pending_voters,
        pending_learners: record.pending_learners,
        phase,
        blocked_on_catch_up: record.blocked_on_catch_up,
    }
}

fn from_proto_range_health_record(
    record: greenmqtt_proto::internal::RangeHealthRecord,
) -> RangeHealthSnapshot {
    let range_id = record.range_id.clone();
    RangeHealthSnapshot {
        range_id: range_id.clone(),
        lifecycle: match record.lifecycle.as_str() {
            "bootstrapping" => ServiceShardLifecycle::Bootstrapping,
            "draining" => ServiceShardLifecycle::Draining,
            "recovering" => ServiceShardLifecycle::Recovering,
            "offline" => ServiceShardLifecycle::Offline,
            _ => ServiceShardLifecycle::Serving,
        },
        role: match record.role.as_str() {
            "candidate" => greenmqtt_kv_raft::RaftNodeRole::Candidate,
            "leader" => greenmqtt_kv_raft::RaftNodeRole::Leader,
            _ => greenmqtt_kv_raft::RaftNodeRole::Follower,
        },
        current_term: record.current_term,
        leader_node_id: record.has_leader_node_id.then_some(record.leader_node_id),
        commit_index: record.commit_index,
        applied_index: record.applied_index,
        latest_snapshot_index: record
            .has_latest_snapshot_index
            .then_some(record.latest_snapshot_index),
        replica_lag: record
            .replica_lag
            .into_iter()
            .map(|replica| ReplicaLagSnapshot {
                node_id: replica.node_id,
                lag: replica.lag,
                match_index: replica.match_index,
                next_index: replica.next_index,
            })
            .collect(),
        reconfiguration: record
            .reconfiguration
            .map(|reconfig| from_proto_reconfiguration_record(reconfig, range_id.clone()))
            .unwrap_or_else(|| RangeReconfigurationState {
                range_id,
                ..Default::default()
            }),
    }
}

impl RangeControlGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                RangeControlServiceClient::connect(endpoint.into()).await?,
            )),
        })
    }

    pub async fn bootstrap_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        let mut client = self.inner.lock().await;
        Ok(client
            .bootstrap_range(RangeBootstrapRequest {
                descriptor: Some(to_proto_replicated_range(&descriptor)),
            })
            .await?
            .into_inner()
            .range_id)
    }

    pub async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .change_replicas(RangeChangeReplicasRequest {
                range_id: range_id.to_string(),
                voters,
                learners,
            })
            .await?;
        Ok(())
    }

    pub async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .transfer_leadership(RangeTransferLeadershipRequest {
                range_id: range_id.to_string(),
                target_node_id,
            })
            .await?;
        Ok(())
    }

    pub async fn recover_range(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .recover_range(RangeRecoverRequest {
                range_id: range_id.to_string(),
                new_leader_node_id,
            })
            .await?;
        Ok(())
    }

    pub async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<(String, String)> {
        let mut client = self.inner.lock().await;
        let reply = client
            .split_range(RangeSplitRequest {
                range_id: range_id.to_string(),
                split_key,
            })
            .await?
            .into_inner();
        Ok((reply.left_range_id, reply.right_range_id))
    }

    pub async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        let mut client = self.inner.lock().await;
        Ok(client
            .merge_ranges(RangeMergeRequest {
                left_range_id: left_range_id.to_string(),
                right_range_id: right_range_id.to_string(),
            })
            .await?
            .into_inner()
            .range_id)
    }

    pub async fn drain_range(&self, range_id: &str) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .drain_range(RangeDrainRequest {
                range_id: range_id.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .retire_range(RangeRetireRequest {
                range_id: range_id.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn list_zombie_ranges(&self) -> anyhow::Result<Vec<ZombieRangeSnapshot>> {
        let mut client = self.inner.lock().await;
        Ok(client
            .list_zombie_ranges(())
            .await?
            .into_inner()
            .entries
            .into_iter()
            .map(|entry| ZombieRangeSnapshot {
                range_id: entry.range_id,
                lifecycle: match entry.lifecycle.as_str() {
                    "bootstrapping" => ServiceShardLifecycle::Bootstrapping,
                    "draining" => ServiceShardLifecycle::Draining,
                    "recovering" => ServiceShardLifecycle::Recovering,
                    "offline" => ServiceShardLifecycle::Offline,
                    _ => ServiceShardLifecycle::Serving,
                },
                leader_node_id: entry.has_leader_node_id.then_some(entry.leader_node_id),
            })
            .collect())
    }
}

impl RoutedRangeControlGrpcClient {
    pub async fn connect(
        metadata_endpoint: impl Into<String>,
        fallback_endpoint: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Self::connect_with_backoff(
            metadata_endpoint,
            fallback_endpoint,
            Duration::from_millis(DEFAULT_RPC_RETRY_AFTER_MS),
        )
        .await
    }

    pub async fn connect_with_backoff(
        metadata_endpoint: impl Into<String>,
        fallback_endpoint: impl Into<String>,
        overload_backoff: Duration,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            metadata: MetadataGrpcClient::connect(metadata_endpoint.into()).await?,
            fallback_endpoint: fallback_endpoint.into(),
            endpoint_backoff: Arc::new(Mutex::new(HashMap::new())),
            overload_backoff,
        })
    }

    fn operator_endpoint_for_member(
        member: &ClusterNodeMembership,
        preferred_kind: Option<ServiceKind>,
    ) -> anyhow::Result<String> {
        if let Some(kind) = preferred_kind {
            if let Some(endpoint) = member
                .endpoints
                .iter()
                .find(|endpoint| endpoint.kind == kind)
            {
                return Ok(endpoint.endpoint.clone());
            }
        }
        member
            .endpoints
            .first()
            .map(|endpoint| endpoint.endpoint.clone())
            .ok_or_else(|| {
                anyhow::anyhow!("member {} has no registered RPC endpoints", member.node_id)
            })
    }

    fn target_node_for_descriptor(descriptor: &ReplicatedRangeDescriptor) -> Option<NodeId> {
        descriptor
            .leader_node_id
            .or_else(|| {
                descriptor
                    .replicas
                    .iter()
                    .find(|replica| replica.role == greenmqtt_core::ReplicaRole::Voter)
                    .map(|replica| replica.node_id)
            })
            .or_else(|| descriptor.replicas.first().map(|replica| replica.node_id))
    }

    pub async fn resolve_range_target(
        &self,
        range_id: &str,
    ) -> anyhow::Result<RoutedRangeControlTarget> {
        let descriptor = self
            .metadata
            .lookup_range(range_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found in metadata"))?;
        let target_node_id = Self::target_node_for_descriptor(&descriptor)
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` has no target replica"))?;
        let member = self
            .metadata
            .lookup_member(target_node_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("range target node {target_node_id} not found"))?;
        Ok(RoutedRangeControlTarget {
            range_id: range_id.to_string(),
            node_id: target_node_id,
            endpoint: Self::operator_endpoint_for_member(
                &member,
                Some(descriptor.shard.service_kind()),
            )?,
        })
    }

    pub async fn resolve_shard_target(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<RoutedRangeControlTarget> {
        let descriptors = self
            .metadata
            .list_ranges(
                Some(shard.kind.clone()),
                Some(&shard.tenant_id),
                Some(&shard.scope),
            )
            .await?;
        anyhow::ensure!(
            !descriptors.is_empty(),
            "no range found for shard {:?}",
            shard
        );
        anyhow::ensure!(
            descriptors.len() == 1,
            "shard {:?} resolves to {} ranges; specify a concrete range_id",
            shard,
            descriptors.len()
        );
        self.resolve_range_target(&descriptors[0].id).await
    }

    async fn control_client_for_descriptor(
        &self,
        descriptor: &ReplicatedRangeDescriptor,
    ) -> anyhow::Result<RangeControlGrpcClient> {
        if let Some(node_id) = Self::target_node_for_descriptor(descriptor) {
            if let Some(member) = self.metadata.lookup_member(node_id).await? {
                let endpoint = Self::operator_endpoint_for_member(
                    &member,
                    Some(descriptor.shard.service_kind()),
                )?;
                self.ensure_endpoint_available(&endpoint).await?;
                return RangeControlGrpcClient::connect(endpoint).await;
            }
        }
        self.ensure_endpoint_available(&self.fallback_endpoint)
            .await?;
        RangeControlGrpcClient::connect(self.fallback_endpoint.clone()).await
    }

    async fn ensure_endpoint_available(&self, endpoint: &str) -> anyhow::Result<()> {
        let mut backoff = self.endpoint_backoff.lock().await;
        if let Some(until) = backoff.get(endpoint).copied() {
            if until > Instant::now() {
                anyhow::bail!("rpc/endpoint-backed-off endpoint={endpoint}");
            }
            backoff.remove(endpoint);
        }
        Ok(())
    }

    async fn note_endpoint_overload(&self, endpoint: &str, error: &anyhow::Error) {
        let Some(status) = error.downcast_ref::<tonic::Status>() else {
            return;
        };
        if status.code() != tonic::Code::ResourceExhausted {
            return;
        }
        let retry_after_ms = status
            .metadata()
            .get("x-greenmqtt-retry-after-ms")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(self.overload_backoff.as_millis() as u64);
        self.endpoint_backoff.lock().await.insert(
            endpoint.to_string(),
            Instant::now() + Duration::from_millis(retry_after_ms),
        );
    }

    pub async fn bootstrap_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        self.control_client_for_descriptor(&descriptor)
            .await?
            .bootstrap_range(descriptor)
            .await
    }

    pub async fn change_replicas(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        let target = self.resolve_range_target(range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.change_replicas(range_id, voters, learners).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }

    pub async fn transfer_leadership(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        let target = self.resolve_range_target(range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.transfer_leadership(range_id, target_node_id).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }

    pub async fn recover_range(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        let target = self.resolve_range_target(range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.recover_range(range_id, new_leader_node_id).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }

    pub async fn split_range(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<(String, String)> {
        let target = self.resolve_range_target(range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.split_range(range_id, split_key).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }

    pub async fn merge_ranges(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        let target = self.resolve_range_target(left_range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.merge_ranges(left_range_id, right_range_id).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }

    pub async fn drain_range(&self, range_id: &str) -> anyhow::Result<()> {
        let target = self.resolve_range_target(range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.drain_range(range_id).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }

    pub async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        let target = self.resolve_range_target(range_id).await?;
        self.ensure_endpoint_available(&target.endpoint).await?;
        let client = RangeControlGrpcClient::connect(target.endpoint.clone()).await?;
        let result = client.retire_range(range_id).await;
        if let Err(error) = &result {
            self.note_endpoint_overload(&target.endpoint, error).await;
        }
        result
    }
}

#[async_trait]
impl ReplicaTransport for RaftTransportGrpcClient {
    async fn send(
        &self,
        from_node_id: NodeId,
        target_node_id: NodeId,
        range_id: &str,
        message: &RaftMessage,
    ) -> anyhow::Result<()> {
        let _ = target_node_id;
        Self::send(self, range_id, from_node_id, message).await
    }
}

#[async_trait]
impl KvRangeExecutor for KvRangeGrpcClient {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        Self::get(self, range_id, key).await
    }

    async fn get_fenced(
        &self,
        range_id: &str,
        key: &[u8],
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<Option<Bytes>> {
        Self::get_fenced(self, range_id, key, expected_epoch).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        Self::scan(self, range_id, boundary, limit).await
    }

    async fn scan_fenced(
        &self,
        range_id: &str,
        boundary: Option<greenmqtt_core::RangeBoundary>,
        limit: usize,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        Self::scan_fenced(self, range_id, boundary, limit, expected_epoch).await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        Self::apply(self, range_id, mutations).await
    }

    async fn apply_fenced(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<()> {
        Self::apply_fenced(self, range_id, mutations, expected_epoch).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        Self::checkpoint(self, range_id, checkpoint_id).await
    }

    async fn checkpoint_fenced(
        &self,
        range_id: &str,
        checkpoint_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        Self::checkpoint_fenced(self, range_id, checkpoint_id, expected_epoch).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        Self::snapshot(self, range_id).await
    }

    async fn snapshot_fenced(
        &self,
        range_id: &str,
        expected_epoch: Option<u64>,
    ) -> anyhow::Result<KvRangeSnapshot> {
        Self::snapshot_fenced(self, range_id, expected_epoch).await
    }
}

#[async_trait]
impl KvRangeExecutorFactory for KvRangeGrpcExecutorFactory {
    async fn connect(&self, endpoint: &str) -> anyhow::Result<Arc<dyn KvRangeExecutor>> {
        Ok(Arc::new(
            KvRangeGrpcClient::connect(endpoint.to_string()).await?,
        ))
    }
}

#[async_trait]
impl ClusterMembershipRegistry for MetadataGrpcClient {
    async fn upsert_member(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Self::upsert_member(self, member).await
    }

    async fn resolve_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Self::lookup_member(self, node_id).await
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        Self::remove_member(self, node_id).await
    }

    async fn list_members(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        Self::list_members(self).await
    }
}

#[async_trait]
impl KvRangeRouter for MetadataGrpcClient {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        let _ = self.upsert_range(descriptor).await?;
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.remove_range(range_id).await
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        self.list_ranges(None, None, None).await
    }

    async fn by_id(&self, range_id: &str) -> anyhow::Result<Option<RangeRoute>> {
        Ok(self
            .lookup_range(range_id)
            .await?
            .map(RangeRoute::from_descriptor))
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<RangeRoute>> {
        Ok(self
            .route_range(shard, key)
            .await?
            .map(RangeRoute::from_descriptor))
    }

    async fn route_shard(&self, shard: &ServiceShardKey) -> anyhow::Result<Vec<RangeRoute>> {
        Ok(self
            .list_ranges(
                Some(shard.kind.clone()),
                Some(&shard.tenant_id),
                Some(&shard.scope),
            )
            .await?
            .into_iter()
            .map(RangeRoute::from_descriptor)
            .collect())
    }
}

impl StaticPeerForwarder {
    pub async fn connect_node(
        &self,
        node_id: NodeId,
        endpoint: impl Into<String>,
    ) -> anyhow::Result<()> {
        let endpoint = endpoint.into();
        let client = BrokerPeerServiceClient::connect(endpoint.clone()).await?;
        self.peers.write().expect("peer forwarder poisoned").insert(
            node_id,
            StaticPeerClient {
                endpoint,
                client: Arc::new(Mutex::new(client)),
            },
        );
        Ok(())
    }

    pub fn configured_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<_> = self
            .peers
            .read()
            .expect("peer forwarder poisoned")
            .keys()
            .copied()
            .collect();
        nodes.sort_unstable();
        nodes
    }

    pub fn disconnect_node(&self, node_id: NodeId) -> bool {
        self.peers
            .write()
            .expect("peer forwarder poisoned")
            .remove(&node_id)
            .is_some()
    }
}

#[async_trait]
impl PeerRegistry for StaticPeerForwarder {
    fn list_peer_nodes(&self) -> Vec<NodeId> {
        self.configured_nodes()
    }

    fn list_peer_endpoints(&self) -> BTreeMap<NodeId, String> {
        self.peers
            .read()
            .expect("peer forwarder poisoned")
            .iter()
            .map(|(node_id, peer)| (*node_id, peer.endpoint.clone()))
            .collect()
    }

    fn remove_peer_node(&self, node_id: NodeId) -> bool {
        self.disconnect_node(node_id)
    }

    async fn add_peer_node(&self, node_id: NodeId, endpoint: String) -> anyhow::Result<()> {
        self.connect_node(node_id, endpoint).await
    }
}
