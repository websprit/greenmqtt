use super::*;
use greenmqtt_core::RangeBoundary;
use greenmqtt_proto::internal::{
    broker_peer_service_server::BrokerPeerService, dist_service_server::DistService,
    inbox_service_server::InboxService as ProtoInboxService,
    kv_range_service_server::KvRangeService, metadata_service_server::MetadataService,
    raft_transport_service_server::RaftTransportService,
    range_admin_service_server::RangeAdminService,
    range_control_service_server::RangeControlService, retain_service_server::RetainService,
    session_dict_service_server::SessionDictService,
};

#[tonic::async_trait]
impl SessionDictService for SessionDictRpc {
    type StreamShardSnapshotStream = tonic::codegen::BoxStream<ShardSnapshotChunk>;

    async fn register_session(
        &self,
        request: Request<RegisterSessionRequest>,
    ) -> Result<Response<RegisterSessionReply>, Status> {
        let record = request
            .into_inner()
            .record
            .ok_or_else(|| Status::invalid_argument("missing session record"))?;
        let replaced = <Self as crate::SessionDictTransportService>::register_session_record(
            self,
            from_proto_session(record),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RegisterSessionReply {
            replaced: replaced.as_ref().map(to_proto_session),
        }))
    }

    async fn unregister_session(
        &self,
        request: Request<UnregisterSessionRequest>,
    ) -> Result<Response<RegisterSessionReply>, Status> {
        let replaced = <Self as crate::SessionDictTransportService>::unregister_session_record(
            self,
            &request.into_inner().session_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RegisterSessionReply {
            replaced: replaced.as_ref().map(to_proto_session),
        }))
    }

    async fn lookup_session(
        &self,
        request: Request<LookupSessionRequest>,
    ) -> Result<Response<LookupSessionReply>, Status> {
        let identity = request
            .into_inner()
            .identity
            .ok_or_else(|| Status::invalid_argument("missing client identity"))?;
        let record =
            <Self as crate::SessionDictTransportService>::lookup_session_record_by_identity(
                self,
                &from_proto_client_identity(identity),
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(LookupSessionReply {
            record: record.as_ref().map(to_proto_session),
        }))
    }

    async fn lookup_session_by_id(
        &self,
        request: Request<LookupSessionByIdRequest>,
    ) -> Result<Response<LookupSessionReply>, Status> {
        let record = <Self as crate::SessionDictTransportService>::lookup_session_record_by_id(
            self,
            &request.into_inner().session_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(LookupSessionReply {
            record: record.as_ref().map(to_proto_session),
        }))
    }

    async fn count_sessions(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.session_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsReply>, Status> {
        let request = request.into_inner();
        let records = <Self as crate::SessionDictTransportService>::list_session_records(
            self,
            if request.tenant_id.is_empty() {
                None
            } else {
                Some(request.tenant_id.as_str())
            },
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(ListSessionsReply {
            records: records.iter().map(to_proto_session).collect(),
        }))
    }

    async fn session_exists(
        &self,
        request: Request<SessionExistRequest>,
    ) -> Result<Response<SessionExistReply>, Status> {
        let scheduler = SessionOnlineCheckScheduler::new(self.inner.clone());
        let checks = scheduler
            .check_sessions(&request.into_inner().session_ids)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(SessionExistReply {
            entries: checks
                .into_iter()
                .map(|(session_id, exists)| SessionExistRecord { session_id, exists })
                .collect(),
        }))
    }

    async fn identity_exists(
        &self,
        request: Request<IdentityExistRequest>,
    ) -> Result<Response<IdentityExistReply>, Status> {
        let identities = request
            .into_inner()
            .identities
            .into_iter()
            .map(from_proto_client_identity)
            .collect::<Vec<_>>();
        let scheduler = SessionOnlineCheckScheduler::new(self.inner.clone());
        let checks = scheduler
            .check_identities(&identities)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(IdentityExistReply {
            entries: identities
                .into_iter()
                .map(|identity| {
                    let key = (
                        identity.tenant_id.clone(),
                        identity.user_id.clone(),
                        identity.client_id.clone(),
                    );
                    IdentityExistRecord {
                        identity: Some(to_proto_client_identity(&identity)),
                        exists: checks.get(&key).copied().unwrap_or(false),
                    }
                })
                .collect(),
        }))
    }

    async fn stream_shard_snapshot(
        &self,
        request: Request<ShardSnapshotRequest>,
    ) -> Result<Response<Self::StreamShardSnapshotStream>, Status> {
        let request = request.into_inner();
        validate_snapshot_assignment(
            &self.assignment_registry,
            assignment_from_snapshot_request(
                ServiceShardKind::SessionDict,
                &request,
                ServiceKind::SessionDict,
            ),
        )
        .await?;
        let shard = sessiondict_shard_from_request(request);
        let sessions = self
            .inner
            .list_sessions(Some(&shard.tenant_id))
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|record| shard_matches_sessiondict_record(&shard, record))
            .collect();
        let snapshot = SessionDictShardSnapshot { sessions };
        let bytes = snapshot.encode().map_err(internal_status)?;
        let checksum = snapshot.checksum().map_err(internal_status)?;
        let output = tonic::codegen::tokio_stream::iter(
            snapshot_chunks(bytes, checksum).into_iter().map(Ok),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl DistService for DistRpc {
    type StreamShardSnapshotStream = tonic::codegen::BoxStream<ShardSnapshotChunk>;

    async fn add_route(&self, request: Request<AddRouteRequest>) -> Result<Response<()>, Status> {
        let route = request
            .into_inner()
            .route
            .ok_or_else(|| Status::invalid_argument("missing route"))?;
        <Self as crate::DistTransportService>::add_route_record(self, from_proto_route(route))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn remove_route(
        &self,
        request: Request<RemoveRouteRequest>,
    ) -> Result<Response<()>, Status> {
        let route = request
            .into_inner()
            .route
            .ok_or_else(|| Status::invalid_argument("missing route"))?;
        <Self as crate::DistTransportService>::remove_route_record(self, &from_proto_route(route))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn remove_session_routes(
        &self,
        request: Request<RemoveSessionRoutesRequest>,
    ) -> Result<Response<RemoveSessionRoutesReply>, Status> {
        let removed = self
            .inner
            .remove_session_routes(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RemoveSessionRoutesReply {
            removed: removed as u32,
        }))
    }

    async fn list_session_routes(
        &self,
        request: Request<ListSessionRoutesRequest>,
    ) -> Result<Response<ListSessionRoutesReply>, Status> {
        let routes = <Self as crate::DistTransportService>::list_session_route_records(
            self,
            &request.into_inner().session_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(ListSessionRoutesReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn match_topic(
        &self,
        request: Request<MatchTopicRequest>,
    ) -> Result<Response<MatchTopicReply>, Status> {
        let request = request.into_inner();
        let routes = <Self as crate::DistTransportService>::match_route_records(
            self,
            &request.tenant_id,
            &request.topic,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(MatchTopicReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn list_routes(
        &self,
        request: Request<ListRoutesRequest>,
    ) -> Result<Response<ListRoutesReply>, Status> {
        let request = request.into_inner();
        let routes = <Self as crate::DistTransportService>::list_route_records(
            self,
            if request.tenant_id.is_empty() {
                None
            } else {
                Some(request.tenant_id.as_str())
            },
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(ListRoutesReply {
            routes: routes.iter().map(to_proto_route).collect(),
        }))
    }

    async fn count_routes(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.route_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }

    async fn stream_shard_snapshot(
        &self,
        request: Request<ShardSnapshotRequest>,
    ) -> Result<Response<Self::StreamShardSnapshotStream>, Status> {
        let request = request.into_inner();
        validate_snapshot_assignment(
            &self.assignment_registry,
            assignment_from_snapshot_request(ServiceShardKind::Dist, &request, ServiceKind::Dist),
        )
        .await?;
        let tenant_id = request.tenant_id;
        let snapshot = DistShardSnapshot {
            routes: self
                .inner
                .list_routes(Some(&tenant_id))
                .await
                .map_err(internal_status)?,
        };
        let bytes = snapshot.encode().map_err(internal_status)?;
        let checksum = snapshot.checksum().map_err(internal_status)?;
        let output = tonic::codegen::tokio_stream::iter(
            snapshot_chunks(bytes, checksum).into_iter().map(Ok),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl ProtoInboxService for InboxRpc {
    type StreamShardSnapshotStream = tonic::codegen::BoxStream<ShardSnapshotChunk>;

    async fn attach(&self, request: Request<InboxAttachRequest>) -> Result<Response<()>, Status> {
        <Self as crate::InboxTransportService>::attach_session(
            self,
            &request.into_inner().session_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn detach(&self, request: Request<InboxDetachRequest>) -> Result<Response<()>, Status> {
        <Self as crate::InboxTransportService>::detach_session(
            self,
            &request.into_inner().session_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn purge_session(
        &self,
        request: Request<InboxPurgeSessionRequest>,
    ) -> Result<Response<()>, Status> {
        <Self as crate::InboxTransportService>::purge_session_state(
            self,
            &request.into_inner().session_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn subscribe(
        &self,
        request: Request<InboxSubscribeRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.inner
            .subscribe(Subscription {
                session_id: request.session_id,
                tenant_id: request.tenant_id,
                topic_filter: request.topic_filter,
                qos: request.qos as u8,
                subscription_identifier: if request.subscription_identifier == 0 {
                    None
                } else {
                    Some(request.subscription_identifier)
                },
                no_local: request.no_local,
                retain_as_published: request.retain_as_published,
                retain_handling: request.retain_handling as u8,
                shared_group: if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group)
                },
                kind: greenmqtt_proto::from_proto_session_kind(&request.kind),
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn unsubscribe(
        &self,
        request: Request<InboxUnsubscribeRequest>,
    ) -> Result<Response<InboxUnsubscribeReply>, Status> {
        let request = request.into_inner();
        let removed = self
            .inner
            .unsubscribe_shared(
                &request.session_id,
                &request.topic_filter,
                if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group.as_str())
                },
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxUnsubscribeReply { removed }))
    }

    async fn enqueue(&self, request: Request<InboxEnqueueRequest>) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing offline message"))?;
        self.inner
            .enqueue(from_proto_offline(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn list_subscriptions(
        &self,
        request: Request<InboxListSubscriptionsRequest>,
    ) -> Result<Response<InboxListSubscriptionsReply>, Status> {
        let subscriptions = self
            .inner
            .list_subscriptions(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxListSubscriptionsReply {
            subscriptions: subscriptions.iter().map(to_proto_subscription).collect(),
        }))
    }

    async fn lookup_subscription(
        &self,
        request: Request<InboxLookupSubscriptionRequest>,
    ) -> Result<Response<InboxLookupSubscriptionReply>, Status> {
        let request = request.into_inner();
        let subscription = self
            .inner
            .lookup_subscription(
                &request.session_id,
                &request.topic_filter,
                if request.shared_group.is_empty() {
                    None
                } else {
                    Some(request.shared_group.as_str())
                },
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxLookupSubscriptionReply {
            subscription: subscription.as_ref().map(to_proto_subscription),
        }))
    }

    async fn list_all_subscriptions(
        &self,
        request: Request<InboxListAllSubscriptionsRequest>,
    ) -> Result<Response<InboxListSubscriptionsReply>, Status> {
        let request = request.into_inner();
        let mut subscriptions = self
            .inner
            .list_all_subscriptions()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            subscriptions.retain(|subscription| subscription.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            subscriptions.retain(|subscription| subscription.session_id == request.session_id);
        }
        Ok(Response::new(InboxListSubscriptionsReply {
            subscriptions: subscriptions.iter().map(to_proto_subscription).collect(),
        }))
    }

    async fn fetch(
        &self,
        request: Request<InboxFetchRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let messages = self
            .inner
            .fetch(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn peek(
        &self,
        request: Request<InboxFetchRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let messages = self
            .inner
            .peek(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn list_offline(
        &self,
        request: Request<InboxListMessagesRequest>,
    ) -> Result<Response<InboxFetchReply>, Status> {
        let request = request.into_inner();
        let mut messages = self
            .inner
            .list_all_offline()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            messages.retain(|message| message.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            messages.retain(|message| message.session_id == request.session_id);
        }
        Ok(Response::new(InboxFetchReply {
            messages: messages.iter().map(to_proto_offline).collect(),
        }))
    }

    async fn stage_inflight(
        &self,
        request: Request<InboxStageInflightRequest>,
    ) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing inflight message"))?;
        self.inner
            .stage_inflight(from_proto_inflight(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn ack_inflight(
        &self,
        request: Request<InboxAckInflightRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        <Self as crate::InboxTransportService>::ack_inflight_packet(
            self,
            &request.session_id,
            request.packet_id as u16,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn fetch_inflight(
        &self,
        request: Request<InboxFetchInflightRequest>,
    ) -> Result<Response<InboxFetchInflightReply>, Status> {
        let messages = self
            .inner
            .fetch_inflight(&request.into_inner().session_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(InboxFetchInflightReply {
            messages: messages.iter().map(to_proto_inflight).collect(),
        }))
    }

    async fn list_inflight(
        &self,
        request: Request<InboxListMessagesRequest>,
    ) -> Result<Response<InboxFetchInflightReply>, Status> {
        let request = request.into_inner();
        let mut messages = self
            .inner
            .list_all_inflight()
            .await
            .map_err(internal_status)?;
        if !request.tenant_id.is_empty() {
            messages.retain(|message| message.tenant_id == request.tenant_id);
        }
        if !request.session_id.is_empty() {
            messages.retain(|message| message.session_id == request.session_id);
        }
        Ok(Response::new(InboxFetchInflightReply {
            messages: messages.iter().map(to_proto_inflight).collect(),
        }))
    }

    async fn stats(&self, _request: Request<()>) -> Result<Response<InboxStatsReply>, Status> {
        let subscriptions = self
            .inner
            .subscription_count()
            .await
            .map_err(internal_status)?;
        let offline_messages = self.inner.offline_count().await.map_err(internal_status)?;
        let inflight_messages = self.inner.inflight_count().await.map_err(internal_status)?;
        Ok(Response::new(InboxStatsReply {
            subscriptions: subscriptions as u64,
            offline_messages: offline_messages as u64,
            inflight_messages: inflight_messages as u64,
        }))
    }

    async fn send_lwt(
        &self,
        request: Request<InboxSendLwtRequest>,
    ) -> Result<Response<InboxSendLwtReply>, Status> {
        let sink = self
            .lwt_sink
            .as_ref()
            .ok_or_else(|| Status::unavailable("delayed lwt sink unavailable"))?;
        let request = request.into_inner();
        self.inner
            .register_delayed_lwt(
                request.generation,
                DelayedLwtPublish {
                    tenant_id: request.tenant_id,
                    session_id: request.session_id.clone(),
                    publish: greenmqtt_core::PublishRequest {
                        topic: request.topic,
                        payload: request.payload.into(),
                        qos: request.qos as u8,
                        retain: request.retain,
                        properties: greenmqtt_proto::from_proto_publish_properties(
                            request.properties.unwrap_or_default(),
                        ),
                    },
                },
            )
            .await
            .map_err(internal_status)?;
        let result = inbox_send_lwt(
            self.inner.as_ref(),
            &request.session_id,
            request.generation,
            sink.as_ref(),
        )
        .await
        .map_err(internal_status)?;
        counter!(
            "greenmqtt_inbox_delayed_lwt_dispatch_total",
            "code" => format!("{result:?}")
        )
        .increment(1);
        Ok(Response::new(InboxSendLwtReply {
            code: format!("{result:?}"),
            message: format!("delayed lwt result: {result:?}"),
        }))
    }

    async fn expire_all(
        &self,
        request: Request<InboxExpireAllRequest>,
    ) -> Result<Response<InboxMaintenanceReply>, Status> {
        let request = request.into_inner();
        let stats = inbox_expire_all(self.inner.as_ref(), &request.tenant_id, request.now_ms)
            .await
            .map_err(internal_status)?;
        counter!("greenmqtt_inbox_tenant_gc_total", "action" => "expire_all").increment(1);
        Ok(Response::new(inbox_maintenance_reply(
            0,
            stats.offline_messages,
            stats.inflight_messages,
        )))
    }

    async fn tenant_gc_preview(
        &self,
        request: Request<InboxTenantGcRequest>,
    ) -> Result<Response<InboxMaintenanceReply>, Status> {
        let preview = inbox_tenant_gc_preview(self.inner.as_ref(), &request.into_inner().tenant_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(inbox_preview_reply(preview)))
    }

    async fn tenant_gc_run(
        &self,
        request: Request<InboxTenantGcRequest>,
    ) -> Result<Response<InboxMaintenanceReply>, Status> {
        let request = request.into_inner();
        let stats = inbox_tenant_gc_run(self.inner.as_ref(), &request.tenant_id, request.now_ms)
            .await
            .map_err(internal_status)?;
        counter!("greenmqtt_inbox_tenant_gc_total", "action" => "run").increment(1);
        Ok(Response::new(inbox_maintenance_reply(
            0,
            stats.offline_messages,
            stats.inflight_messages,
        )))
    }

    async fn stream_shard_snapshot(
        &self,
        request: Request<ShardSnapshotRequest>,
    ) -> Result<Response<Self::StreamShardSnapshotStream>, Status> {
        let request = request.into_inner();
        validate_snapshot_assignment(
            &self.assignment_registry,
            assignment_from_snapshot_request(ServiceShardKind::Inbox, &request, ServiceKind::Inbox),
        )
        .await?;
        let shard = inbox_shard_from_request(request);
        let subscriptions = self
            .inner
            .list_all_subscriptions()
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|subscription| {
                shard_matches_inbox_session(
                    &shard,
                    &subscription.tenant_id,
                    &subscription.session_id,
                )
            })
            .collect();
        let offline_messages = self
            .inner
            .list_all_offline()
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|message| {
                shard_matches_inbox_session(&shard, &message.tenant_id, &message.session_id)
            })
            .collect();
        let inflight_messages = self
            .inner
            .list_all_inflight()
            .await
            .map_err(internal_status)?
            .into_iter()
            .filter(|message| {
                shard_matches_inbox_session(&shard, &message.tenant_id, &message.session_id)
            })
            .collect();
        let snapshot = InboxShardSnapshot {
            subscriptions,
            offline_messages,
            inflight_messages,
        };
        let bytes = snapshot.encode().map_err(internal_status)?;
        let checksum = snapshot.checksum().map_err(internal_status)?;
        let output = tonic::codegen::tokio_stream::iter(
            snapshot_chunks(bytes, checksum).into_iter().map(Ok),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl RetainService for RetainRpc {
    async fn write(&self, request: Request<RetainWriteRequest>) -> Result<Response<()>, Status> {
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("missing retained message"))?;
        <Self as crate::RetainTransportService>::retain_message(self, from_proto_retain(message))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn r#match(
        &self,
        request: Request<RetainMatchRequest>,
    ) -> Result<Response<RetainMatchReply>, Status> {
        let request = request.into_inner();
        let messages = <Self as crate::RetainTransportService>::match_retained(
            self,
            &request.tenant_id,
            &request.topic_filter,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RetainMatchReply {
            messages: messages.iter().map(to_proto_retain).collect(),
        }))
    }

    async fn count_retained(&self, _request: Request<()>) -> Result<Response<CountReply>, Status> {
        let count = self.inner.retained_count().await.map_err(internal_status)?;
        Ok(Response::new(CountReply {
            count: count as u64,
        }))
    }

    async fn expire_all(
        &self,
        request: Request<RetainExpireAllRequest>,
    ) -> Result<Response<RetainMaintenanceReply>, Status> {
        let result = retain_expire_all(self.inner.as_ref(), &request.into_inner().tenant_id)
            .await
            .map_err(internal_status)?;
        counter!("greenmqtt_retain_expire_sweep_total", "action" => "expire_all").increment(1);
        Ok(Response::new(retain_maintenance_reply(&result)))
    }

    async fn tenant_gc_preview(
        &self,
        request: Request<RetainTenantGcRequest>,
    ) -> Result<Response<RetainMaintenanceReply>, Status> {
        let result = retain_tenant_gc_preview(self.inner.as_ref(), &request.into_inner().tenant_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(retain_maintenance_reply(&result)))
    }

    async fn tenant_gc_run(
        &self,
        request: Request<RetainTenantGcRequest>,
    ) -> Result<Response<RetainMaintenanceReply>, Status> {
        let result = retain_tenant_gc_run(self.inner.as_ref(), &request.into_inner().tenant_id)
            .await
            .map_err(internal_status)?;
        counter!("greenmqtt_retain_expire_sweep_total", "action" => "tenant_gc").increment(1);
        Ok(Response::new(retain_maintenance_reply(&result)))
    }
}

#[tonic::async_trait]
impl BrokerPeerService for BrokerPeerRpc {
    async fn push_delivery(
        &self,
        request: Request<PushDeliveryRequest>,
    ) -> Result<Response<PushDeliveryReply>, Status> {
        let delivery = request
            .into_inner()
            .delivery
            .ok_or_else(|| Status::invalid_argument("missing delivery"))?;
        let delivered = self
            .inner
            .push_delivery(from_proto_delivery(delivery))
            .await
            .map_err(internal_status)?;
        Ok(Response::new(PushDeliveryReply { delivered }))
    }

    async fn push_deliveries(
        &self,
        request: Request<PushDeliveriesRequest>,
    ) -> Result<Response<PushDeliveriesReply>, Status> {
        let deliveries: Vec<_> = request
            .into_inner()
            .deliveries
            .into_iter()
            .map(from_proto_delivery)
            .collect();
        let undelivered = self
            .inner
            .push_deliveries(deliveries.clone())
            .await
            .map_err(internal_status)?;
        let mut remaining = undelivered;
        let mut undelivered_indices = Vec::new();
        for (index, delivery) in deliveries.into_iter().enumerate() {
            if let Some(position) = remaining.iter().position(|current| *current == delivery) {
                remaining.remove(position);
                undelivered_indices.push(index as u32);
            }
        }
        Ok(Response::new(PushDeliveriesReply {
            undelivered_indices,
        }))
    }
}

#[tonic::async_trait]
impl MetadataService for MetadataRpc {
    async fn upsert_member(
        &self,
        request: Request<MemberRecordReply>,
    ) -> Result<Response<MemberRecordReply>, Status> {
        let member = request
            .into_inner()
            .member
            .ok_or_else(|| Status::invalid_argument("missing cluster member"))?;
        let previous = <Self as crate::MetadataTransportService>::upsert_member_record(
            self,
            from_proto_cluster_node_membership(member),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(MemberRecordReply {
            member: previous.as_ref().map(to_proto_cluster_node_membership),
        }))
    }

    async fn lookup_member(
        &self,
        request: Request<MemberLookupRequest>,
    ) -> Result<Response<MemberRecordReply>, Status> {
        let member = <Self as crate::MetadataTransportService>::lookup_member_record(
            self,
            request.into_inner().node_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(MemberRecordReply {
            member: member.as_ref().map(to_proto_cluster_node_membership),
        }))
    }

    async fn remove_member(
        &self,
        request: Request<MemberLookupRequest>,
    ) -> Result<Response<MemberRecordReply>, Status> {
        let member = <Self as crate::MetadataTransportService>::remove_member_record(
            self,
            request.into_inner().node_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(MemberRecordReply {
            member: member.as_ref().map(to_proto_cluster_node_membership),
        }))
    }

    async fn list_members(
        &self,
        _request: Request<()>,
    ) -> Result<Response<MemberListReply>, Status> {
        let members = <Self as crate::MetadataTransportService>::list_member_records(self)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(MemberListReply {
            members: members
                .iter()
                .map(to_proto_cluster_node_membership)
                .collect(),
        }))
    }

    async fn upsert_range(
        &self,
        request: Request<RangeUpsertRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let descriptor = request
            .into_inner()
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing replicated range descriptor"))?;
        let previous = <Self as crate::MetadataTransportService>::upsert_range_record(
            self,
            from_proto_replicated_range(descriptor),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: previous.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn lookup_range(
        &self,
        request: Request<RangeLookupRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let descriptor = <Self as crate::MetadataTransportService>::lookup_range_record(
            self,
            &request.into_inner().range_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: descriptor.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn remove_range(
        &self,
        request: Request<RangeLookupRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let descriptor = <Self as crate::MetadataTransportService>::remove_range_record(
            self,
            &request.into_inner().range_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: descriptor.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn list_ranges(
        &self,
        request: Request<RangeListRequest>,
    ) -> Result<Response<RangeListReply>, Status> {
        let request = request.into_inner();
        let shard_kind = if request.shard_kind.is_empty() {
            None
        } else {
            Some(from_proto_shard_kind(&request.shard_kind))
        };
        let descriptors = <Self as crate::MetadataTransportService>::list_range_records(
            self,
            shard_kind,
            (!request.tenant_id.is_empty()).then_some(request.tenant_id.as_str()),
            (!request.scope.is_empty()).then_some(request.scope.as_str()),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RangeListReply {
            descriptors: descriptors.iter().map(to_proto_replicated_range).collect(),
        }))
    }

    async fn route_range(
        &self,
        request: Request<RouteRangeRequest>,
    ) -> Result<Response<RangeRecordReply>, Status> {
        let request = request.into_inner();
        let descriptor = <Self as crate::MetadataTransportService>::route_range_record(
            self,
            &ServiceShardKey {
                kind: from_proto_shard_kind(&request.shard_kind),
                tenant_id: request.tenant_id,
                scope: request.scope,
            },
            &request.key,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RangeRecordReply {
            descriptor: descriptor.as_ref().map(to_proto_replicated_range),
        }))
    }

    async fn upsert_balancer_state(
        &self,
        request: Request<BalancerStateUpsertRequest>,
    ) -> Result<Response<BalancerStateUpsertReply>, Status> {
        let request = request.into_inner();
        let state = request
            .state
            .ok_or_else(|| Status::invalid_argument("missing balancer state"))?;
        let previous = <Self as crate::MetadataTransportService>::upsert_balancer_state_record(
            self,
            &request.name,
            from_proto_balancer_state(state),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(BalancerStateUpsertReply {
            previous: previous.as_ref().map(to_proto_balancer_state),
        }))
    }

    async fn lookup_balancer_state(
        &self,
        request: Request<BalancerStateRequest>,
    ) -> Result<Response<BalancerStateReply>, Status> {
        let state = <Self as crate::MetadataTransportService>::lookup_balancer_state_record(
            self,
            &request.into_inner().name,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(BalancerStateReply {
            state: state.as_ref().map(to_proto_balancer_state),
        }))
    }

    async fn remove_balancer_state(
        &self,
        request: Request<BalancerStateRequest>,
    ) -> Result<Response<BalancerStateReply>, Status> {
        let state = <Self as crate::MetadataTransportService>::remove_balancer_state_record(
            self,
            &request.into_inner().name,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(BalancerStateReply {
            state: state.as_ref().map(to_proto_balancer_state),
        }))
    }

    async fn list_balancer_states(
        &self,
        _request: Request<()>,
    ) -> Result<Response<BalancerStateListReply>, Status> {
        let states = <Self as crate::MetadataTransportService>::list_balancer_state_records(self)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(BalancerStateListReply {
            entries: states
                .into_iter()
                .map(|(name, state)| NamedBalancerStateRecord {
                    name,
                    state: Some(to_proto_balancer_state(&state)),
                })
                .collect(),
        }))
    }
}

#[tonic::async_trait]
impl KvRangeService for KvRangeRpc {
    async fn get(
        &self,
        request: Request<KvRangeGetRequest>,
    ) -> Result<Response<KvRangeGetReply>, Status> {
        let request = request.into_inner();
        let value = <Self as crate::KvRangeTransportService>::get_value(
            self,
            &request.range_id,
            &request.key,
            request.expected_epoch,
        )
        .await
        .map_err(crate::kv_range_transport_status)?;
        Ok(Response::new(KvRangeGetReply {
            value: value.clone().unwrap_or_default().to_vec(),
            found: value.is_some(),
        }))
    }

    async fn scan(
        &self,
        request: Request<KvRangeScanRequest>,
    ) -> Result<Response<KvRangeScanReply>, Status> {
        let request = request.into_inner();
        let boundary = request
            .boundary
            .map(greenmqtt_proto::from_proto_range_boundary)
            .unwrap_or_else(greenmqtt_core::RangeBoundary::full);
        let entries = <Self as crate::KvRangeTransportService>::scan_entries(
            self,
            &request.range_id,
            Some(boundary),
            request.limit as usize,
            request.expected_epoch,
        )
        .await
        .map_err(crate::kv_range_transport_status)?;
        Ok(Response::new(KvRangeScanReply {
            entries: entries.iter().map(to_proto_kv_entry).collect(),
        }))
    }

    async fn apply(&self, request: Request<KvRangeApplyRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let mutations = request
            .mutations
            .into_iter()
            .map(from_proto_kv_mutation)
            .collect::<Vec<_>>();
        <Self as crate::KvRangeTransportService>::apply_mutations(
            self,
            &request.range_id,
            mutations,
            request.expected_epoch,
        )
        .await
        .map_err(crate::kv_range_transport_status)?;
        Ok(Response::new(()))
    }

    async fn checkpoint(
        &self,
        request: Request<KvRangeCheckpointRequest>,
    ) -> Result<Response<KvRangeCheckpointReply>, Status> {
        let request = request.into_inner();
        let checkpoint = <Self as crate::KvRangeTransportService>::checkpoint_range(
            self,
            &request.range_id,
            &request.checkpoint_id,
            request.expected_epoch,
        )
        .await
        .map_err(crate::kv_range_transport_status)?;
        Ok(Response::new(to_proto_kv_range_checkpoint(&checkpoint)))
    }

    async fn snapshot(
        &self,
        request: Request<KvRangeSnapshotRequest>,
    ) -> Result<Response<KvRangeSnapshotReply>, Status> {
        let request = request.into_inner();
        let snapshot = <Self as crate::KvRangeTransportService>::snapshot_metadata(
            self,
            &request.range_id,
            request.expected_epoch,
        )
        .await
        .map_err(crate::kv_range_transport_status)?;
        Ok(Response::new(to_proto_kv_range_snapshot(&snapshot)))
    }
}

#[tonic::async_trait]
impl RaftTransportService for RaftTransportRpc {
    async fn send(&self, request: Request<RaftTransportRequest>) -> Result<Response<()>, Status> {
        let host = self
            .inner
            .as_ref()
            .ok_or_else(|| Status::unavailable("kv range host unavailable"))?;
        let (range_id, from_node_id, message) =
            from_proto_raft_transport_request(request.into_inner()).map_err(internal_status)?;
        let hosted = host
            .open_range(&range_id)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("range not found"))?;
        if matches!(message, RaftMessage::InstallSnapshot(_)) {
            counter!("kv_raft_snapshot_receive_total", "range_id" => range_id.clone()).increment(1);
        }
        hosted
            .raft
            .receive(from_node_id, message)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }
}

#[tonic::async_trait]
impl RangeAdminService for RangeAdminRpc {
    async fn list_range_health(
        &self,
        _request: Request<()>,
    ) -> Result<Response<RangeHealthListReply>, Status> {
        let health = <Self as crate::RangeAdminTransportService>::list_range_health_snapshots(self)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeHealthListReply {
            entries: health.iter().map(to_proto_range_health).collect(),
        }))
    }

    async fn get_range_health(
        &self,
        request: Request<RangeHealthRequest>,
    ) -> Result<Response<RangeHealthReply>, Status> {
        let range_id = request.into_inner().range_id;
        let health =
            <Self as crate::RangeAdminTransportService>::get_range_health_snapshot(self, &range_id)
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("range not found"))?;
        Ok(Response::new(RangeHealthReply {
            health: Some(to_proto_range_health(&health)),
        }))
    }

    async fn debug_dump(&self, _request: Request<()>) -> Result<Response<RangeDebugReply>, Status> {
        let text = <Self as crate::RangeAdminTransportService>::debug_dump_text(self)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeDebugReply { text }))
    }
}

#[tonic::async_trait]
impl RangeControlService for RangeControlRpc {
    async fn bootstrap_range(
        &self,
        request: Request<RangeBootstrapRequest>,
    ) -> Result<Response<RangeBootstrapReply>, Status> {
        let descriptor = request
            .into_inner()
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing descriptor"))
            .map(from_proto_replicated_range)?;
        let range_id =
            <Self as crate::RangeControlTransportService>::bootstrap_range_action(self, descriptor)
                .await
                .map_err(internal_status)?;
        Ok(Response::new(RangeBootstrapReply { range_id }))
    }

    async fn change_replicas(
        &self,
        request: Request<RangeChangeReplicasRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        <Self as crate::RangeControlTransportService>::change_replicas_action(
            self,
            &request.range_id,
            request.voters,
            request.learners,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn transfer_leadership(
        &self,
        request: Request<RangeTransferLeadershipRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        <Self as crate::RangeControlTransportService>::transfer_leadership_action(
            self,
            &request.range_id,
            request.target_node_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn recover_range(
        &self,
        request: Request<RangeRecoverRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        <Self as crate::RangeControlTransportService>::recover_range_action(
            self,
            &request.range_id,
            request.new_leader_node_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn split_range(
        &self,
        request: Request<RangeSplitRequest>,
    ) -> Result<Response<RangeSplitReply>, Status> {
        let request = request.into_inner();
        let (left_range_id, right_range_id) =
            <Self as crate::RangeControlTransportService>::split_range_action(
                self,
                &request.range_id,
                request.split_key,
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RangeSplitReply {
            left_range_id,
            right_range_id,
        }))
    }

    async fn merge_ranges(
        &self,
        request: Request<RangeMergeRequest>,
    ) -> Result<Response<RangeMergeReply>, Status> {
        let request = request.into_inner();
        let range_id = <Self as crate::RangeControlTransportService>::merge_ranges_action(
            self,
            &request.left_range_id,
            &request.right_range_id,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(RangeMergeReply { range_id }))
    }

    async fn drain_range(
        &self,
        request: Request<RangeDrainRequest>,
    ) -> Result<Response<()>, Status> {
        let range_id = request.into_inner().range_id;
        <Self as crate::RangeControlTransportService>::drain_range_action(self, &range_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn retire_range(
        &self,
        request: Request<RangeRetireRequest>,
    ) -> Result<Response<()>, Status> {
        let range_id = request.into_inner().range_id;
        <Self as crate::RangeControlTransportService>::retire_range_action(self, &range_id)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn list_zombie_ranges(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ZombieRangeListReply>, Status> {
        let entries =
            <Self as crate::RangeControlTransportService>::list_zombie_range_snapshots(self)
                .await
                .map_err(internal_status)?;
        Ok(Response::new(ZombieRangeListReply {
            entries: entries.iter().map(to_proto_zombie_range).collect(),
        }))
    }
}

pub(crate) fn internal_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("range not found") {
        return Status::not_found(format!("kv/range-not-found {message}"));
    }
    if message.contains("range epoch mismatch") {
        return Status::failed_precondition(format!("kv/epoch-mismatch {message}"));
    }
    if message.contains("propose requires local leader ownership") {
        return Status::failed_precondition(format!("kv/not-leader {message}"));
    }
    if message.contains("read index requires local leader ownership") {
        return Status::failed_precondition(format!("kv/not-leader {message}"));
    }
    if message.contains("range lifecycle is not serving") {
        return Status::failed_precondition(format!("kv/config-changing {message}"));
    }
    if message.contains("blocked on catch-up")
        || message.contains("joint catch-up")
        || message.contains("dual-majority catch-up")
        || message.contains("entering joint config")
        || message.contains("joint consensus change already in progress")
    {
        return Status::failed_precondition(format!("kv/config-changing {message}"));
    }
    if message.contains("active leader lease") {
        return Status::failed_precondition(format!("kv/config-changing {message}"));
    }
    if message.contains("timed out waiting for raft command to apply") {
        return Status::deadline_exceeded(format!("kv/config-changing {message}"));
    }
    Status::internal(format!("kv/internal {message}"))
}

impl RangeControlRpc {
    fn runtime(&self) -> anyhow::Result<Arc<greenmqtt_kv_server::ReplicaRuntime>> {
        self.inner
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("range runtime unavailable"))
    }

    async fn sync_reconfiguration_state(&self, range_id: &str) -> Result<(), Status> {
        let Some(runtime) = self.inner.as_ref() else {
            return Ok(());
        };
        let Some(registry) = self.registry.as_ref() else {
            return Ok(());
        };
        if let Some(state) = runtime
            .reconfiguration_state(range_id)
            .await
            .map_err(internal_status)?
        {
            registry
                .upsert_reconfiguration_state(state)
                .await
                .map_err(internal_status)?;
        } else {
            let _ = registry
                .remove_reconfiguration_state(range_id)
                .await
                .map_err(internal_status)?;
        }
        Ok(())
    }

    async fn sync_reconfiguration_state_anyhow(&self, range_id: &str) -> anyhow::Result<()> {
        self.sync_reconfiguration_state(range_id)
            .await
            .map_err(anyhow::Error::from)
    }

    pub(crate) async fn bootstrap_range_action(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        let range_id = self.runtime()?.bootstrap_range(descriptor).await?;
        self.sync_reconfiguration_state_anyhow(&range_id).await?;
        Ok(range_id)
    }

    pub(crate) async fn change_replicas_action(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        let result = self
            .runtime()?
            .change_replicas(range_id, voters, learners)
            .await;
        if result
            .as_ref()
            .err()
            .map(|error| {
                let message = error.to_string();
                message.contains("blocked on catch-up")
                    || message.contains("joint catch-up")
                    || message.contains("entering joint config")
            })
            .unwrap_or(false)
            || result.is_ok()
        {
            self.sync_reconfiguration_state_anyhow(range_id).await?;
        }
        result?;
        Ok(())
    }

    pub(crate) async fn transfer_leadership_action(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.runtime()?
            .transfer_leadership(range_id, target_node_id)
            .await?;
        self.sync_reconfiguration_state_anyhow(range_id).await?;
        Ok(())
    }

    pub(crate) async fn recover_range_action(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.runtime()?
            .recover_range(range_id, new_leader_node_id)
            .await?;
        self.sync_reconfiguration_state_anyhow(range_id).await?;
        Ok(())
    }

    pub(crate) async fn split_range_action(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<(String, String)> {
        let source_descriptor = if let Some(registry) = self.registry.as_ref() {
            registry.resolve_range(range_id).await?
        } else {
            None
        };
        let (left_range_id, right_range_id) = self
            .runtime()?
            .split_range(range_id, split_key.clone())
            .await?;
        if let (Some(registry), Some(source)) = (self.registry.as_ref(), source_descriptor) {
            let child_epoch = source.epoch + 2;
            let left = ReplicatedRangeDescriptor::new(
                left_range_id.clone(),
                source.shard.clone(),
                RangeBoundary::new(source.boundary.start_key.clone(), Some(split_key.clone())),
                child_epoch,
                source.config_version,
                source.leader_node_id,
                source.replicas.clone(),
                source.commit_index,
                source.applied_index,
                ServiceShardLifecycle::Serving,
            );
            let right = ReplicatedRangeDescriptor::new(
                right_range_id.clone(),
                source.shard,
                RangeBoundary::new(Some(split_key), source.boundary.end_key.clone()),
                child_epoch,
                source.config_version,
                source.leader_node_id,
                source.replicas,
                source.commit_index,
                source.applied_index,
                ServiceShardLifecycle::Serving,
            );
            let _ = registry.remove_range(range_id).await?;
            let _ = registry.upsert_range(left).await?;
            let _ = registry.upsert_range(right).await?;
        }
        self.sync_reconfiguration_state_anyhow(range_id).await?;
        self.sync_reconfiguration_state_anyhow(&left_range_id)
            .await?;
        self.sync_reconfiguration_state_anyhow(&right_range_id)
            .await?;
        Ok((left_range_id, right_range_id))
    }

    pub(crate) async fn merge_ranges_action(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        let range_id = self
            .runtime()?
            .merge_ranges(left_range_id, right_range_id)
            .await?;
        self.sync_reconfiguration_state_anyhow(left_range_id)
            .await?;
        self.sync_reconfiguration_state_anyhow(right_range_id)
            .await?;
        self.sync_reconfiguration_state_anyhow(&range_id).await?;
        Ok(range_id)
    }

    pub(crate) async fn drain_range_action(&self, range_id: &str) -> anyhow::Result<()> {
        self.runtime()?.drain_range(range_id).await?;
        self.sync_reconfiguration_state_anyhow(range_id).await?;
        Ok(())
    }

    pub(crate) async fn retire_range_action(&self, range_id: &str) -> anyhow::Result<()> {
        self.runtime()?.retire_range(range_id).await?;
        self.sync_reconfiguration_state_anyhow(range_id).await?;
        Ok(())
    }

    pub(crate) async fn list_zombie_range_snapshots(
        &self,
    ) -> anyhow::Result<Vec<ZombieRangeSnapshot>> {
        self.runtime()?.list_zombie_ranges().await
    }
}

impl RangeAdminRpc {
    fn runtime(&self) -> anyhow::Result<Arc<greenmqtt_kv_server::ReplicaRuntime>> {
        if let Some(runtime) = self.runtime.as_ref() {
            return Ok(runtime.clone());
        }
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("kv range host unavailable"))?;
        Ok(Arc::new(greenmqtt_kv_server::ReplicaRuntime::new(
            host.clone(),
            Arc::new(crate::NoopReplicaTransport),
        )))
    }

    pub(crate) async fn list_range_health_snapshots(
        &self,
    ) -> anyhow::Result<Vec<RangeHealthSnapshot>> {
        self.runtime()?.health_snapshot().await
    }

    pub(crate) async fn get_range_health_snapshot(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeHealthSnapshot>> {
        Ok(self
            .list_range_health_snapshots()
            .await?
            .into_iter()
            .find(|entry| entry.range_id == range_id))
    }

    pub(crate) async fn debug_dump_text(&self) -> anyhow::Result<String> {
        self.runtime()?.debug_dump().await
    }
}

impl SessionDictRpc {
    pub(crate) async fn register_session_record(
        &self,
        record: SessionRecord,
    ) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.register(record).await
    }

    pub(crate) async fn unregister_session_record(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.unregister(session_id).await
    }

    pub(crate) async fn lookup_session_record_by_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.lookup_identity(identity).await
    }

    pub(crate) async fn lookup_session_record_by_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.lookup_session(session_id).await
    }

    pub(crate) async fn list_session_records(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<SessionRecord>> {
        self.inner.list_sessions(tenant_id).await
    }
}

impl DistRpc {
    pub(crate) async fn add_route_record(&self, route: RouteRecord) -> anyhow::Result<()> {
        self.inner.add_route(route).await
    }

    pub(crate) async fn remove_route_record(&self, route: &RouteRecord) -> anyhow::Result<()> {
        self.inner.remove_route(route).await
    }

    pub(crate) async fn list_session_route_records(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.inner.list_session_routes(session_id).await
    }

    pub(crate) async fn match_route_records(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.inner.match_topic(tenant_id, &topic.to_string()).await
    }

    pub(crate) async fn list_route_records(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        self.inner.list_routes(tenant_id).await
    }
}

impl InboxRpc {
    pub(crate) async fn attach_session(&self, session_id: &str) -> anyhow::Result<()> {
        self.inner.attach(&session_id.to_string()).await
    }

    pub(crate) async fn detach_session(&self, session_id: &str) -> anyhow::Result<()> {
        self.inner.detach(&session_id.to_string()).await
    }

    pub(crate) async fn purge_session_state(&self, session_id: &str) -> anyhow::Result<()> {
        self.inner.purge_session(&session_id.to_string()).await
    }

    pub(crate) async fn ack_inflight_packet(
        &self,
        session_id: &str,
        packet_id: u16,
    ) -> anyhow::Result<()> {
        self.inner
            .ack_inflight(&session_id.to_string(), packet_id)
            .await
    }
}

impl KvRangeRpc {
    pub(crate) async fn get_value(
        &self,
        range_id: &str,
        key: &[u8],
        expected_epoch: u64,
    ) -> Result<Option<Bytes>, crate::KvRangeTransportError> {
        let host = self.inner.as_ref().ok_or_else(|| {
            crate::KvRangeTransportError::Unavailable("kv range host unavailable".into())
        })?;
        let hosted = host
            .open_range(range_id)
            .await
            .map_err(crate::KvRangeTransportError::Internal)?
            .ok_or_else(|| crate::KvRangeTransportError::NotFound("range not found".into()))?;
        validate_kv_request_fence(&hosted, expected_epoch).map_err(|status| {
            crate::KvRangeTransportError::Internal(anyhow::anyhow!(status.to_string()))
        })?;
        hosted
            .raft
            .read_index()
            .await
            .map_err(crate::KvRangeTransportError::Internal)?;
        hosted
            .space
            .reader()
            .get(key)
            .await
            .map_err(crate::KvRangeTransportError::Internal)
    }

    pub(crate) async fn scan_entries(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
        expected_epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>, crate::KvRangeTransportError> {
        let host = self.inner.as_ref().ok_or_else(|| {
            crate::KvRangeTransportError::Unavailable("kv range host unavailable".into())
        })?;
        let hosted = host
            .open_range(range_id)
            .await
            .map_err(crate::KvRangeTransportError::Internal)?
            .ok_or_else(|| crate::KvRangeTransportError::NotFound("range not found".into()))?;
        validate_kv_request_fence(&hosted, expected_epoch).map_err(|status| {
            crate::KvRangeTransportError::Internal(anyhow::anyhow!(status.to_string()))
        })?;
        hosted
            .raft
            .read_index()
            .await
            .map_err(crate::KvRangeTransportError::Internal)?;
        hosted
            .space
            .reader()
            .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
            .await
            .map_err(crate::KvRangeTransportError::Internal)
    }

    pub(crate) async fn apply_mutations(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
        expected_epoch: u64,
    ) -> Result<(), crate::KvRangeTransportError> {
        let host = self.inner.as_ref().ok_or_else(|| {
            crate::KvRangeTransportError::Unavailable("kv range host unavailable".into())
        })?;
        let hosted = host
            .open_range(range_id)
            .await
            .map_err(crate::KvRangeTransportError::Internal)?
            .ok_or_else(|| crate::KvRangeTransportError::NotFound("range not found".into()))?;
        validate_kv_request_fence(&hosted, expected_epoch).map_err(|status| {
            crate::KvRangeTransportError::Internal(anyhow::anyhow!(status.to_string()))
        })?;
        let proposed_index = hosted
            .raft
            .propose(Bytes::from(
                bincode::serialize(&mutations)
                    .map_err(anyhow::Error::from)
                    .map_err(crate::KvRangeTransportError::Internal)?,
            ))
            .await
            .map_err(crate::KvRangeTransportError::Internal)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let _ = apply_committed_entries_for_range(&hosted)
                .await
                .map_err(crate::KvRangeTransportError::Internal)?;
            if hosted
                .raft
                .status()
                .await
                .map_err(crate::KvRangeTransportError::Internal)?
                .applied_index
                >= proposed_index
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(crate::KvRangeTransportError::DeadlineExceeded(
                    "timed out waiting for raft command to apply".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    pub(crate) async fn checkpoint_range(
        &self,
        range_id: &str,
        checkpoint_id: &str,
        expected_epoch: u64,
    ) -> Result<KvRangeCheckpoint, crate::KvRangeTransportError> {
        let host = self.inner.as_ref().ok_or_else(|| {
            crate::KvRangeTransportError::Unavailable("kv range host unavailable".into())
        })?;
        let hosted = host
            .open_range(range_id)
            .await
            .map_err(crate::KvRangeTransportError::Internal)?
            .ok_or_else(|| crate::KvRangeTransportError::NotFound("range not found".into()))?;
        validate_kv_request_fence(&hosted, expected_epoch).map_err(|status| {
            crate::KvRangeTransportError::Internal(anyhow::anyhow!(status.to_string()))
        })?;
        hosted
            .raft
            .read_index()
            .await
            .map_err(crate::KvRangeTransportError::Internal)?;
        hosted
            .space
            .checkpoint(checkpoint_id)
            .await
            .map_err(crate::KvRangeTransportError::Internal)
    }

    pub(crate) async fn snapshot_metadata(
        &self,
        range_id: &str,
        expected_epoch: u64,
    ) -> Result<KvRangeSnapshot, crate::KvRangeTransportError> {
        let host = self.inner.as_ref().ok_or_else(|| {
            crate::KvRangeTransportError::Unavailable("kv range host unavailable".into())
        })?;
        let hosted = host
            .open_range(range_id)
            .await
            .map_err(crate::KvRangeTransportError::Internal)?
            .ok_or_else(|| crate::KvRangeTransportError::NotFound("range not found".into()))?;
        validate_kv_request_fence(&hosted, expected_epoch).map_err(|status| {
            crate::KvRangeTransportError::Internal(anyhow::anyhow!(status.to_string()))
        })?;
        hosted
            .raft
            .read_index()
            .await
            .map_err(crate::KvRangeTransportError::Internal)?;
        hosted
            .space
            .snapshot()
            .await
            .map_err(crate::KvRangeTransportError::Internal)
    }
}

impl MetadataRpc {
    fn registry(&self) -> anyhow::Result<Arc<dyn MetadataRegistry>> {
        self.inner
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("metadata registry unavailable"))
    }

    pub(crate) async fn upsert_member_record(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.registry()?.upsert_member(member).await
    }

    pub(crate) async fn lookup_member_record(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.registry()?.resolve_member(node_id).await
    }

    pub(crate) async fn remove_member_record(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        self.registry()?.remove_member(node_id).await
    }

    pub(crate) async fn list_member_records(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        self.registry()?.list_members().await
    }

    pub(crate) async fn upsert_balancer_state_record(
        &self,
        name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.registry()?.upsert_balancer_state(name, state).await
    }

    pub(crate) async fn lookup_balancer_state_record(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.registry()?.resolve_balancer_state(name).await
    }

    pub(crate) async fn remove_balancer_state_record(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        self.registry()?.remove_balancer_state(name).await
    }

    pub(crate) async fn list_balancer_state_records(
        &self,
    ) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        self.registry()?.list_balancer_states().await
    }

    pub(crate) async fn upsert_range_record(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.registry()?.upsert_range(descriptor).await
    }

    pub(crate) async fn lookup_range_record(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.registry()?.resolve_range(range_id).await
    }

    pub(crate) async fn remove_range_record(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.registry()?.remove_range(range_id).await
    }

    pub(crate) async fn list_range_records(
        &self,
        shard_kind: Option<ServiceShardKind>,
        tenant_id: Option<&str>,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        let mut descriptors = self.registry()?.list_ranges(shard_kind).await?;
        if let Some(tenant_id) = tenant_id {
            descriptors.retain(|descriptor| descriptor.shard.tenant_id == tenant_id);
        }
        if let Some(scope) = scope {
            descriptors.retain(|descriptor| descriptor.shard.scope == scope);
        }
        Ok(descriptors)
    }

    pub(crate) async fn route_range_record(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        self.registry()?.route_range_for_key(shard, key).await
    }
}

#[async_trait]
impl crate::SessionDictTransportService for SessionDictRpc {
    async fn register_session_record(
        &self,
        record: SessionRecord,
    ) -> anyhow::Result<Option<SessionRecord>> {
        SessionDictRpc::register_session_record(self, record).await
    }

    async fn unregister_session_record(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        SessionDictRpc::unregister_session_record(self, session_id).await
    }

    async fn lookup_session_record_by_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        SessionDictRpc::lookup_session_record_by_identity(self, identity).await
    }

    async fn lookup_session_record_by_id(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Option<SessionRecord>> {
        SessionDictRpc::lookup_session_record_by_id(self, session_id).await
    }

    async fn list_session_records(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<SessionRecord>> {
        SessionDictRpc::list_session_records(self, tenant_id).await
    }
}

#[async_trait]
impl crate::DistTransportService for DistRpc {
    async fn add_route_record(&self, route: RouteRecord) -> anyhow::Result<()> {
        DistRpc::add_route_record(self, route).await
    }

    async fn remove_route_record(&self, route: &RouteRecord) -> anyhow::Result<()> {
        DistRpc::remove_route_record(self, route).await
    }

    async fn list_session_route_records(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        DistRpc::list_session_route_records(self, session_id).await
    }

    async fn match_route_records(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        DistRpc::match_route_records(self, tenant_id, topic).await
    }

    async fn list_route_records(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<RouteRecord>> {
        DistRpc::list_route_records(self, tenant_id).await
    }
}

#[async_trait]
impl crate::InboxTransportService for InboxRpc {
    async fn attach_session(&self, session_id: &str) -> anyhow::Result<()> {
        InboxRpc::attach_session(self, session_id).await
    }

    async fn detach_session(&self, session_id: &str) -> anyhow::Result<()> {
        InboxRpc::detach_session(self, session_id).await
    }

    async fn purge_session_state(&self, session_id: &str) -> anyhow::Result<()> {
        InboxRpc::purge_session_state(self, session_id).await
    }

    async fn ack_inflight_packet(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        InboxRpc::ack_inflight_packet(self, session_id, packet_id).await
    }
}

#[async_trait]
impl crate::KvRangeTransportService for KvRangeRpc {
    async fn get_value(
        &self,
        range_id: &str,
        key: &[u8],
        expected_epoch: u64,
    ) -> Result<Option<Bytes>, crate::KvRangeTransportError> {
        KvRangeRpc::get_value(self, range_id, key, expected_epoch).await
    }

    async fn scan_entries(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
        expected_epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>, crate::KvRangeTransportError> {
        KvRangeRpc::scan_entries(self, range_id, boundary, limit, expected_epoch).await
    }

    async fn apply_mutations(
        &self,
        range_id: &str,
        mutations: Vec<KvMutation>,
        expected_epoch: u64,
    ) -> Result<(), crate::KvRangeTransportError> {
        KvRangeRpc::apply_mutations(self, range_id, mutations, expected_epoch).await
    }

    async fn checkpoint_range(
        &self,
        range_id: &str,
        checkpoint_id: &str,
        expected_epoch: u64,
    ) -> Result<KvRangeCheckpoint, crate::KvRangeTransportError> {
        KvRangeRpc::checkpoint_range(self, range_id, checkpoint_id, expected_epoch).await
    }

    async fn snapshot_metadata(
        &self,
        range_id: &str,
        expected_epoch: u64,
    ) -> Result<KvRangeSnapshot, crate::KvRangeTransportError> {
        KvRangeRpc::snapshot_metadata(self, range_id, expected_epoch).await
    }
}

#[async_trait]
impl crate::RetainTransportService for RetainRpc {
    async fn retain_message(&self, message: RetainedMessage) -> anyhow::Result<()> {
        self.inner.retain(message).await
    }

    async fn match_retained(
        &self,
        tenant_id: &str,
        topic_filter: &str,
    ) -> anyhow::Result<Vec<RetainedMessage>> {
        self.inner.match_topic(tenant_id, topic_filter).await
    }
}

#[async_trait]
impl crate::RangeAdminTransportService for RangeAdminRpc {
    async fn list_range_health_snapshots(&self) -> anyhow::Result<Vec<RangeHealthSnapshot>> {
        RangeAdminRpc::list_range_health_snapshots(self).await
    }

    async fn get_range_health_snapshot(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<RangeHealthSnapshot>> {
        RangeAdminRpc::get_range_health_snapshot(self, range_id).await
    }

    async fn debug_dump_text(&self) -> anyhow::Result<String> {
        RangeAdminRpc::debug_dump_text(self).await
    }
}

#[async_trait]
impl crate::MetadataTransportService for MetadataRpc {
    async fn upsert_member_record(
        &self,
        member: ClusterNodeMembership,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        MetadataRpc::upsert_member_record(self, member).await
    }

    async fn lookup_member_record(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        MetadataRpc::lookup_member_record(self, node_id).await
    }

    async fn remove_member_record(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<ClusterNodeMembership>> {
        MetadataRpc::remove_member_record(self, node_id).await
    }

    async fn list_member_records(&self) -> anyhow::Result<Vec<ClusterNodeMembership>> {
        MetadataRpc::list_member_records(self).await
    }

    async fn upsert_balancer_state_record(
        &self,
        name: &str,
        state: BalancerState,
    ) -> anyhow::Result<Option<BalancerState>> {
        MetadataRpc::upsert_balancer_state_record(self, name, state).await
    }

    async fn lookup_balancer_state_record(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        MetadataRpc::lookup_balancer_state_record(self, name).await
    }

    async fn remove_balancer_state_record(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<BalancerState>> {
        MetadataRpc::remove_balancer_state_record(self, name).await
    }

    async fn list_balancer_state_records(&self) -> anyhow::Result<BTreeMap<String, BalancerState>> {
        MetadataRpc::list_balancer_state_records(self).await
    }

    async fn upsert_range_record(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        MetadataRpc::upsert_range_record(self, descriptor).await
    }

    async fn lookup_range_record(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        MetadataRpc::lookup_range_record(self, range_id).await
    }

    async fn remove_range_record(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        MetadataRpc::remove_range_record(self, range_id).await
    }

    async fn list_range_records(
        &self,
        shard_kind: Option<ServiceShardKind>,
        tenant_id: Option<&str>,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        MetadataRpc::list_range_records(self, shard_kind, tenant_id, scope).await
    }

    async fn route_range_record(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        MetadataRpc::route_range_record(self, shard, key).await
    }
}

#[async_trait]
impl crate::RangeControlTransportService for RangeControlRpc {
    async fn bootstrap_range_action(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<String> {
        RangeControlRpc::bootstrap_range_action(self, descriptor).await
    }

    async fn change_replicas_action(
        &self,
        range_id: &str,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> anyhow::Result<()> {
        RangeControlRpc::change_replicas_action(self, range_id, voters, learners).await
    }

    async fn transfer_leadership_action(
        &self,
        range_id: &str,
        target_node_id: NodeId,
    ) -> anyhow::Result<()> {
        RangeControlRpc::transfer_leadership_action(self, range_id, target_node_id).await
    }

    async fn recover_range_action(
        &self,
        range_id: &str,
        new_leader_node_id: NodeId,
    ) -> anyhow::Result<()> {
        RangeControlRpc::recover_range_action(self, range_id, new_leader_node_id).await
    }

    async fn split_range_action(
        &self,
        range_id: &str,
        split_key: Vec<u8>,
    ) -> anyhow::Result<(String, String)> {
        RangeControlRpc::split_range_action(self, range_id, split_key).await
    }

    async fn merge_ranges_action(
        &self,
        left_range_id: &str,
        right_range_id: &str,
    ) -> anyhow::Result<String> {
        RangeControlRpc::merge_ranges_action(self, left_range_id, right_range_id).await
    }

    async fn drain_range_action(&self, range_id: &str) -> anyhow::Result<()> {
        RangeControlRpc::drain_range_action(self, range_id).await
    }

    async fn retire_range_action(&self, range_id: &str) -> anyhow::Result<()> {
        RangeControlRpc::retire_range_action(self, range_id).await
    }

    async fn list_zombie_range_snapshots(&self) -> anyhow::Result<Vec<ZombieRangeSnapshot>> {
        RangeControlRpc::list_zombie_range_snapshots(self).await
    }
}
