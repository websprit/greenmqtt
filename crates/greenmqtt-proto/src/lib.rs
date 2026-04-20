pub mod internal {
    tonic::include_proto!("greenmqtt.internal.v1");
}

use bytes::Bytes;
use greenmqtt_core::{
    BalancerState, ClientIdentity, ClusterNodeLifecycle, ClusterNodeMembership, Delivery,
    InflightMessage, InflightPhase, OfflineMessage, PublishProperties, RangeBoundary, RangeReplica,
    ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, RetainedMessage, RouteRecord,
    ServiceEndpoint, ServiceKind, ServiceShardKey, ServiceShardKind, ServiceShardLifecycle,
    SessionKind, SessionRecord, SharedPayload, Subscription, UserProperty,
};
use greenmqtt_kv_engine::{KvMutation, KvRangeCheckpoint, KvRangeSnapshot};
use greenmqtt_kv_raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    OutboundRaftMessage, RaftConfigLogEntry, RaftLogEntry, RaftMessage, RaftSnapshot,
    RequestVoteRequest, RequestVoteResponse,
};
use internal as proto;

const CONFIG_CHANGE_PREFIX: &[u8] = b"__greenmqtt_cfg__";

pub fn to_proto_client_identity(identity: &ClientIdentity) -> proto::ClientIdentity {
    proto::ClientIdentity {
        tenant_id: identity.tenant_id.clone(),
        user_id: identity.user_id.clone(),
        client_id: identity.client_id.clone(),
    }
}

pub fn from_proto_client_identity(identity: proto::ClientIdentity) -> ClientIdentity {
    ClientIdentity {
        tenant_id: identity.tenant_id,
        user_id: identity.user_id,
        client_id: identity.client_id,
    }
}

pub fn to_proto_session_kind(kind: &SessionKind) -> String {
    match kind {
        SessionKind::Transient => "transient".to_string(),
        SessionKind::Persistent => "persistent".to_string(),
    }
}

pub fn from_proto_session_kind(kind: &str) -> SessionKind {
    match kind {
        "transient" => SessionKind::Transient,
        _ => SessionKind::Persistent,
    }
}

pub fn to_proto_session(record: &SessionRecord) -> proto::SessionRecord {
    proto::SessionRecord {
        session_id: record.session_id.clone(),
        node_id: record.node_id,
        kind: to_proto_session_kind(&record.kind),
        identity: Some(to_proto_client_identity(&record.identity)),
        session_expiry_interval_secs: record.session_expiry_interval_secs.unwrap_or_default(),
        expires_at_ms: record.expires_at_ms.unwrap_or_default(),
    }
}

pub fn from_proto_session(record: proto::SessionRecord) -> SessionRecord {
    SessionRecord {
        session_id: record.session_id,
        node_id: record.node_id,
        kind: from_proto_session_kind(&record.kind),
        identity: from_proto_client_identity(record.identity.unwrap_or_default()),
        session_expiry_interval_secs: if record.session_expiry_interval_secs == 0 {
            None
        } else {
            Some(record.session_expiry_interval_secs)
        },
        expires_at_ms: if record.expires_at_ms == 0 {
            None
        } else {
            Some(record.expires_at_ms)
        },
    }
}

pub fn to_proto_route(route: &RouteRecord) -> proto::RouteRecord {
    proto::RouteRecord {
        tenant_id: route.tenant_id.clone(),
        topic_filter: route.topic_filter.clone(),
        session_id: route.session_id.clone(),
        node_id: route.node_id,
        subscription_identifier: route.subscription_identifier.unwrap_or_default(),
        no_local: route.no_local,
        retain_as_published: route.retain_as_published,
        shared_group: route.shared_group.clone().unwrap_or_default(),
        kind: to_proto_session_kind(&route.kind),
    }
}

pub fn from_proto_route(route: proto::RouteRecord) -> RouteRecord {
    RouteRecord {
        tenant_id: route.tenant_id,
        topic_filter: route.topic_filter,
        session_id: route.session_id,
        node_id: route.node_id,
        subscription_identifier: if route.subscription_identifier == 0 {
            None
        } else {
            Some(route.subscription_identifier)
        },
        no_local: route.no_local,
        retain_as_published: route.retain_as_published,
        shared_group: if route.shared_group.is_empty() {
            None
        } else {
            Some(route.shared_group)
        },
        kind: from_proto_session_kind(&route.kind),
    }
}

pub fn to_proto_shard_kind(kind: &ServiceShardKind) -> String {
    match kind {
        ServiceShardKind::SessionDict => "sessiondict".to_string(),
        ServiceShardKind::Inbox => "inbox".to_string(),
        ServiceShardKind::Inflight => "inflight".to_string(),
        ServiceShardKind::Dist => "dist".to_string(),
        ServiceShardKind::Retain => "retain".to_string(),
    }
}

pub fn from_proto_shard_kind(kind: &str) -> ServiceShardKind {
    match kind {
        "sessiondict" => ServiceShardKind::SessionDict,
        "inbox" => ServiceShardKind::Inbox,
        "inflight" => ServiceShardKind::Inflight,
        "retain" => ServiceShardKind::Retain,
        _ => ServiceShardKind::Dist,
    }
}

pub fn to_proto_service_kind(kind: &ServiceKind) -> String {
    match kind {
        ServiceKind::Broker => "broker".to_string(),
        ServiceKind::SessionDict => "sessiondict".to_string(),
        ServiceKind::Dist => "dist".to_string(),
        ServiceKind::Inbox => "inbox".to_string(),
        ServiceKind::Retain => "retain".to_string(),
        ServiceKind::HttpApi => "http-api".to_string(),
    }
}

pub fn from_proto_service_kind(kind: &str) -> ServiceKind {
    match kind {
        "broker" => ServiceKind::Broker,
        "sessiondict" => ServiceKind::SessionDict,
        "inbox" => ServiceKind::Inbox,
        "retain" => ServiceKind::Retain,
        "http-api" => ServiceKind::HttpApi,
        _ => ServiceKind::Dist,
    }
}

pub fn to_proto_shard_lifecycle(lifecycle: &ServiceShardLifecycle) -> String {
    match lifecycle {
        ServiceShardLifecycle::Bootstrapping => "bootstrapping".to_string(),
        ServiceShardLifecycle::Serving => "serving".to_string(),
        ServiceShardLifecycle::Draining => "draining".to_string(),
        ServiceShardLifecycle::Recovering => "recovering".to_string(),
        ServiceShardLifecycle::Offline => "offline".to_string(),
    }
}

pub fn from_proto_shard_lifecycle(lifecycle: &str) -> ServiceShardLifecycle {
    match lifecycle {
        "bootstrapping" => ServiceShardLifecycle::Bootstrapping,
        "draining" => ServiceShardLifecycle::Draining,
        "recovering" => ServiceShardLifecycle::Recovering,
        "offline" => ServiceShardLifecycle::Offline,
        _ => ServiceShardLifecycle::Serving,
    }
}

pub fn to_proto_cluster_node_lifecycle(lifecycle: &ClusterNodeLifecycle) -> String {
    match lifecycle {
        ClusterNodeLifecycle::Joining => "joining".to_string(),
        ClusterNodeLifecycle::Serving => "serving".to_string(),
        ClusterNodeLifecycle::Suspect => "suspect".to_string(),
        ClusterNodeLifecycle::Leaving => "leaving".to_string(),
        ClusterNodeLifecycle::Offline => "offline".to_string(),
    }
}

pub fn from_proto_cluster_node_lifecycle(lifecycle: &str) -> ClusterNodeLifecycle {
    match lifecycle {
        "joining" => ClusterNodeLifecycle::Joining,
        "suspect" => ClusterNodeLifecycle::Suspect,
        "leaving" => ClusterNodeLifecycle::Leaving,
        "offline" => ClusterNodeLifecycle::Offline,
        _ => ClusterNodeLifecycle::Serving,
    }
}

pub fn to_proto_service_endpoint(endpoint: &ServiceEndpoint) -> proto::ServiceEndpointRecord {
    proto::ServiceEndpointRecord {
        kind: to_proto_service_kind(&endpoint.kind),
        node_id: endpoint.node_id,
        endpoint: endpoint.endpoint.clone(),
    }
}

pub fn from_proto_service_endpoint(endpoint: proto::ServiceEndpointRecord) -> ServiceEndpoint {
    ServiceEndpoint {
        kind: from_proto_service_kind(&endpoint.kind),
        node_id: endpoint.node_id,
        endpoint: endpoint.endpoint,
    }
}

pub fn to_proto_cluster_node_membership(
    member: &ClusterNodeMembership,
) -> proto::ClusterNodeMembershipRecord {
    proto::ClusterNodeMembershipRecord {
        node_id: member.node_id,
        epoch: member.epoch,
        lifecycle: to_proto_cluster_node_lifecycle(&member.lifecycle),
        endpoints: member
            .endpoints
            .iter()
            .map(to_proto_service_endpoint)
            .collect(),
    }
}

pub fn from_proto_cluster_node_membership(
    member: proto::ClusterNodeMembershipRecord,
) -> ClusterNodeMembership {
    ClusterNodeMembership {
        node_id: member.node_id,
        epoch: member.epoch,
        lifecycle: from_proto_cluster_node_lifecycle(&member.lifecycle),
        endpoints: member
            .endpoints
            .into_iter()
            .map(from_proto_service_endpoint)
            .collect(),
    }
}

pub fn to_proto_replica_role(role: &ReplicaRole) -> String {
    match role {
        ReplicaRole::Voter => "voter".to_string(),
        ReplicaRole::Learner => "learner".to_string(),
    }
}

pub fn from_proto_replica_role(role: &str) -> ReplicaRole {
    match role {
        "learner" => ReplicaRole::Learner,
        _ => ReplicaRole::Voter,
    }
}

pub fn to_proto_replica_sync_state(state: &ReplicaSyncState) -> String {
    match state {
        ReplicaSyncState::Probing => "probing".to_string(),
        ReplicaSyncState::Snapshotting => "snapshotting".to_string(),
        ReplicaSyncState::Replicating => "replicating".to_string(),
        ReplicaSyncState::Offline => "offline".to_string(),
    }
}

pub fn from_proto_replica_sync_state(state: &str) -> ReplicaSyncState {
    match state {
        "snapshotting" => ReplicaSyncState::Snapshotting,
        "replicating" => ReplicaSyncState::Replicating,
        "offline" => ReplicaSyncState::Offline,
        _ => ReplicaSyncState::Probing,
    }
}

pub fn to_proto_range_boundary(boundary: &RangeBoundary) -> proto::RangeBoundaryRecord {
    proto::RangeBoundaryRecord {
        start_key: boundary.start_key.clone().unwrap_or_default(),
        end_key: boundary.end_key.clone().unwrap_or_default(),
        has_start_key: boundary.start_key.is_some(),
        has_end_key: boundary.end_key.is_some(),
    }
}

pub fn from_proto_range_boundary(boundary: proto::RangeBoundaryRecord) -> RangeBoundary {
    RangeBoundary::new(
        boundary.has_start_key.then_some(boundary.start_key),
        boundary.has_end_key.then_some(boundary.end_key),
    )
}

pub fn to_proto_range_replica(replica: &RangeReplica) -> proto::RangeReplicaRecord {
    proto::RangeReplicaRecord {
        node_id: replica.node_id,
        role: to_proto_replica_role(&replica.role),
        sync_state: to_proto_replica_sync_state(&replica.sync_state),
    }
}

pub fn from_proto_range_replica(replica: proto::RangeReplicaRecord) -> RangeReplica {
    RangeReplica {
        node_id: replica.node_id,
        role: from_proto_replica_role(&replica.role),
        sync_state: from_proto_replica_sync_state(&replica.sync_state),
    }
}

pub fn to_proto_replicated_range(
    descriptor: &ReplicatedRangeDescriptor,
) -> proto::ReplicatedRangeRecord {
    proto::ReplicatedRangeRecord {
        id: descriptor.id.clone(),
        shard_kind: to_proto_shard_kind(&descriptor.shard.kind),
        tenant_id: descriptor.shard.tenant_id.clone(),
        scope: descriptor.shard.scope.clone(),
        boundary: Some(to_proto_range_boundary(&descriptor.boundary)),
        epoch: descriptor.epoch,
        config_version: descriptor.config_version,
        leader_node_id: descriptor.leader_node_id.unwrap_or_default(),
        replicas: descriptor
            .replicas
            .iter()
            .map(to_proto_range_replica)
            .collect(),
        commit_index: descriptor.commit_index,
        applied_index: descriptor.applied_index,
        lifecycle: to_proto_shard_lifecycle(&descriptor.lifecycle),
    }
}

pub fn from_proto_replicated_range(
    descriptor: proto::ReplicatedRangeRecord,
) -> ReplicatedRangeDescriptor {
    ReplicatedRangeDescriptor {
        id: descriptor.id,
        shard: ServiceShardKey {
            kind: from_proto_shard_kind(&descriptor.shard_kind),
            tenant_id: descriptor.tenant_id,
            scope: descriptor.scope,
        },
        boundary: descriptor
            .boundary
            .map(from_proto_range_boundary)
            .unwrap_or_else(RangeBoundary::full),
        epoch: descriptor.epoch,
        config_version: descriptor.config_version,
        leader_node_id: if descriptor.leader_node_id == 0 {
            None
        } else {
            Some(descriptor.leader_node_id)
        },
        replicas: descriptor
            .replicas
            .into_iter()
            .map(from_proto_range_replica)
            .collect(),
        commit_index: descriptor.commit_index,
        applied_index: descriptor.applied_index,
        lifecycle: from_proto_shard_lifecycle(&descriptor.lifecycle),
    }
}

pub fn to_proto_balancer_state(state: &BalancerState) -> proto::BalancerStateRecord {
    proto::BalancerStateRecord {
        disabled: state.disabled,
        load_rules: state.load_rules.clone().into_iter().collect(),
    }
}

pub fn from_proto_balancer_state(state: proto::BalancerStateRecord) -> BalancerState {
    BalancerState {
        disabled: state.disabled,
        load_rules: state.load_rules.into_iter().collect(),
    }
}

pub fn to_proto_kv_entry(entry: &(Bytes, Bytes)) -> proto::KvEntryRecord {
    proto::KvEntryRecord {
        key: entry.0.to_vec(),
        value: entry.1.to_vec(),
    }
}

pub fn from_proto_kv_entry(entry: proto::KvEntryRecord) -> (Bytes, Bytes) {
    (Bytes::from(entry.key), Bytes::from(entry.value))
}

pub fn to_proto_kv_mutation(mutation: &KvMutation) -> proto::KvMutationRecord {
    proto::KvMutationRecord {
        key: mutation.key.to_vec(),
        value: mutation.value.clone().unwrap_or_default().to_vec(),
        has_value: mutation.value.is_some(),
    }
}

pub fn from_proto_kv_mutation(mutation: proto::KvMutationRecord) -> KvMutation {
    KvMutation {
        key: Bytes::from(mutation.key),
        value: mutation.has_value.then_some(Bytes::from(mutation.value)),
    }
}

pub fn to_proto_kv_range_checkpoint(
    checkpoint: &KvRangeCheckpoint,
) -> proto::KvRangeCheckpointReply {
    proto::KvRangeCheckpointReply {
        range_id: checkpoint.range_id.clone(),
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        path: checkpoint.path.clone(),
    }
}

pub fn from_proto_kv_range_checkpoint(
    checkpoint: proto::KvRangeCheckpointReply,
) -> KvRangeCheckpoint {
    KvRangeCheckpoint {
        range_id: checkpoint.range_id,
        checkpoint_id: checkpoint.checkpoint_id,
        path: checkpoint.path,
    }
}

pub fn to_proto_kv_range_snapshot(snapshot: &KvRangeSnapshot) -> proto::KvRangeSnapshotReply {
    proto::KvRangeSnapshotReply {
        range_id: snapshot.range_id.clone(),
        boundary: Some(to_proto_range_boundary(&snapshot.boundary)),
        term: snapshot.term,
        index: snapshot.index,
        checksum: snapshot.checksum,
        layout_version: snapshot.layout_version,
        data_path: snapshot.data_path.clone(),
    }
}

pub fn from_proto_kv_range_snapshot(snapshot: proto::KvRangeSnapshotReply) -> KvRangeSnapshot {
    KvRangeSnapshot {
        range_id: snapshot.range_id,
        boundary: snapshot
            .boundary
            .map(from_proto_range_boundary)
            .unwrap_or_else(RangeBoundary::full),
        term: snapshot.term,
        index: snapshot.index,
        checksum: snapshot.checksum,
        layout_version: snapshot.layout_version,
        data_path: snapshot.data_path,
    }
}

pub fn to_proto_raft_log_entry(entry: &RaftLogEntry) -> proto::RaftLogEntryRecord {
    proto::RaftLogEntryRecord {
        term: entry.term,
        index: entry.index,
        command: if let Some(config_change) = &entry.config_change {
            let mut encoded = CONFIG_CHANGE_PREFIX.to_vec();
            encoded.extend(
                bincode::serialize(config_change)
                    .expect("config-change log entry should serialize"),
            );
            encoded
        } else {
            entry.command.to_vec()
        },
    }
}

pub fn from_proto_raft_log_entry(entry: proto::RaftLogEntryRecord) -> RaftLogEntry {
    let (config_change, command) = if entry.command.starts_with(CONFIG_CHANGE_PREFIX) {
        let payload = &entry.command[CONFIG_CHANGE_PREFIX.len()..];
        let decoded = bincode::deserialize::<RaftConfigLogEntry>(payload)
            .expect("config-change log entry should deserialize");
        (Some(decoded), Bytes::new())
    } else {
        (None, Bytes::from(entry.command))
    };
    RaftLogEntry {
        term: entry.term,
        index: entry.index,
        config_change,
        command,
    }
}

pub fn to_proto_raft_snapshot(snapshot: &RaftSnapshot) -> proto::RaftSnapshotRecord {
    proto::RaftSnapshotRecord {
        range_id: snapshot.range_id.clone(),
        term: snapshot.term,
        index: snapshot.index,
        payload: snapshot.payload.to_vec(),
    }
}

pub fn from_proto_raft_snapshot(snapshot: proto::RaftSnapshotRecord) -> RaftSnapshot {
    RaftSnapshot {
        range_id: snapshot.range_id,
        term: snapshot.term,
        index: snapshot.index,
        payload: Bytes::from(snapshot.payload),
    }
}

pub fn to_proto_raft_transport_request(
    range_id: &str,
    from_node_id: u64,
    message: &RaftMessage,
) -> proto::RaftTransportRequest {
    let payload = match message {
        RaftMessage::AppendEntries(append) => {
            proto::raft_transport_request::Payload::AppendEntries(
                proto::RaftAppendEntriesRequestRecord {
                    term: append.term,
                    leader_id: append.leader_id,
                    prev_log_index: append.prev_log_index,
                    prev_log_term: append.prev_log_term,
                    entries: append.entries.iter().map(to_proto_raft_log_entry).collect(),
                    leader_commit: append.leader_commit,
                },
            )
        }
        RaftMessage::AppendEntriesResponse(response) => {
            proto::raft_transport_request::Payload::AppendEntriesResponse(
                proto::RaftAppendEntriesResponseRecord {
                    term: response.term,
                    success: response.success,
                    match_index: response.match_index,
                },
            )
        }
        RaftMessage::RequestVote(vote) => proto::raft_transport_request::Payload::RequestVote(
            proto::RaftRequestVoteRequestRecord {
                term: vote.term,
                candidate_id: vote.candidate_id,
                last_log_index: vote.last_log_index,
                last_log_term: vote.last_log_term,
            },
        ),
        RaftMessage::RequestVoteResponse(response) => {
            proto::raft_transport_request::Payload::RequestVoteResponse(
                proto::RaftRequestVoteResponseRecord {
                    term: response.term,
                    vote_granted: response.vote_granted,
                },
            )
        }
        RaftMessage::InstallSnapshot(install) => {
            proto::raft_transport_request::Payload::InstallSnapshot(
                proto::RaftInstallSnapshotRequestRecord {
                    term: install.term,
                    leader_id: install.leader_id,
                    snapshot: Some(to_proto_raft_snapshot(&install.snapshot)),
                },
            )
        }
        RaftMessage::InstallSnapshotResponse(response) => {
            proto::raft_transport_request::Payload::InstallSnapshotResponse(
                proto::RaftInstallSnapshotResponseRecord {
                    term: response.term,
                    accepted: response.accepted,
                },
            )
        }
    };
    proto::RaftTransportRequest {
        range_id: range_id.to_string(),
        from_node_id,
        payload: Some(payload),
    }
}

pub fn from_proto_raft_transport_request(
    request: proto::RaftTransportRequest,
) -> anyhow::Result<(String, u64, RaftMessage)> {
    let range_id = request.range_id;
    let from_node_id = request.from_node_id;
    let message = match request.payload {
        Some(proto::raft_transport_request::Payload::AppendEntries(append)) => {
            RaftMessage::AppendEntries(AppendEntriesRequest {
                term: append.term,
                leader_id: append.leader_id,
                prev_log_index: append.prev_log_index,
                prev_log_term: append.prev_log_term,
                entries: append
                    .entries
                    .into_iter()
                    .map(from_proto_raft_log_entry)
                    .collect(),
                leader_commit: append.leader_commit,
            })
        }
        Some(proto::raft_transport_request::Payload::AppendEntriesResponse(response)) => {
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: response.term,
                success: response.success,
                match_index: response.match_index,
            })
        }
        Some(proto::raft_transport_request::Payload::RequestVote(vote)) => {
            RaftMessage::RequestVote(RequestVoteRequest {
                term: vote.term,
                candidate_id: vote.candidate_id,
                last_log_index: vote.last_log_index,
                last_log_term: vote.last_log_term,
            })
        }
        Some(proto::raft_transport_request::Payload::RequestVoteResponse(response)) => {
            RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: response.term,
                vote_granted: response.vote_granted,
            })
        }
        Some(proto::raft_transport_request::Payload::InstallSnapshot(install)) => {
            RaftMessage::InstallSnapshot(InstallSnapshotRequest {
                term: install.term,
                leader_id: install.leader_id,
                snapshot: install
                    .snapshot
                    .map(from_proto_raft_snapshot)
                    .ok_or_else(|| anyhow::anyhow!("missing raft snapshot"))?,
            })
        }
        Some(proto::raft_transport_request::Payload::InstallSnapshotResponse(response)) => {
            RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                term: response.term,
                accepted: response.accepted,
            })
        }
        None => anyhow::bail!("missing raft transport payload"),
    };
    Ok((range_id, from_node_id, message))
}

pub fn to_proto_outbound_raft_message(
    range_id: &str,
    from_node_id: u64,
    outbound: &OutboundRaftMessage,
) -> proto::RaftTransportRequest {
    to_proto_raft_transport_request(range_id, from_node_id, &outbound.message)
}

pub fn to_proto_subscription(subscription: &Subscription) -> proto::SubscriptionRecord {
    proto::SubscriptionRecord {
        session_id: subscription.session_id.clone(),
        tenant_id: subscription.tenant_id.clone(),
        topic_filter: subscription.topic_filter.clone(),
        qos: subscription.qos as u32,
        subscription_identifier: subscription.subscription_identifier.unwrap_or_default(),
        no_local: subscription.no_local,
        retain_as_published: subscription.retain_as_published,
        retain_handling: subscription.retain_handling as u32,
        shared_group: subscription.shared_group.clone().unwrap_or_default(),
        kind: to_proto_session_kind(&subscription.kind),
    }
}

pub fn from_proto_subscription(subscription: proto::SubscriptionRecord) -> Subscription {
    Subscription {
        session_id: subscription.session_id,
        tenant_id: subscription.tenant_id,
        topic_filter: subscription.topic_filter,
        qos: subscription.qos as u8,
        subscription_identifier: if subscription.subscription_identifier == 0 {
            None
        } else {
            Some(subscription.subscription_identifier)
        },
        no_local: subscription.no_local,
        retain_as_published: subscription.retain_as_published,
        retain_handling: subscription.retain_handling as u8,
        shared_group: if subscription.shared_group.is_empty() {
            None
        } else {
            Some(subscription.shared_group)
        },
        kind: from_proto_session_kind(&subscription.kind),
    }
}

pub fn to_proto_offline(message: &OfflineMessage) -> proto::OfflineMessage {
    proto::OfflineMessage {
        tenant_id: message.tenant_id.clone(),
        session_id: message.session_id.clone(),
        topic: message.topic.clone(),
        payload: to_proto_payload(&message.payload),
        qos: message.qos as u32,
        retain: message.retain,
        from_session_id: message.from_session_id.clone(),
        properties: Some(to_proto_publish_properties(&message.properties)),
    }
}

pub fn from_proto_offline(message: proto::OfflineMessage) -> OfflineMessage {
    OfflineMessage {
        tenant_id: message.tenant_id,
        session_id: message.session_id,
        topic: message.topic,
        payload: Bytes::from(message.payload),
        qos: message.qos as u8,
        retain: message.retain,
        from_session_id: message.from_session_id,
        properties: message
            .properties
            .map(from_proto_publish_properties)
            .unwrap_or_default(),
    }
}

pub fn to_proto_inflight(message: &InflightMessage) -> proto::InflightMessage {
    proto::InflightMessage {
        tenant_id: message.tenant_id.clone(),
        session_id: message.session_id.clone(),
        packet_id: message.packet_id as u32,
        topic: message.topic.clone(),
        payload: to_proto_payload(&message.payload),
        qos: message.qos as u32,
        retain: message.retain,
        from_session_id: message.from_session_id.clone(),
        properties: Some(to_proto_publish_properties(&message.properties)),
        phase: to_proto_inflight_phase(&message.phase),
    }
}

pub fn from_proto_inflight(message: proto::InflightMessage) -> InflightMessage {
    InflightMessage {
        tenant_id: message.tenant_id,
        session_id: message.session_id,
        packet_id: message.packet_id as u16,
        topic: message.topic,
        payload: Bytes::from(message.payload),
        qos: message.qos as u8,
        retain: message.retain,
        from_session_id: message.from_session_id,
        properties: message
            .properties
            .map(from_proto_publish_properties)
            .unwrap_or_default(),
        phase: from_proto_inflight_phase(&message.phase),
    }
}

pub fn to_proto_delivery(message: &Delivery) -> proto::Delivery {
    proto::Delivery {
        tenant_id: message.tenant_id.clone(),
        session_id: message.session_id.clone(),
        topic: message.topic.clone(),
        payload: to_proto_payload(&message.payload),
        qos: message.qos as u32,
        retain: message.retain,
        from_session_id: message.from_session_id.clone(),
        properties: Some(to_proto_publish_properties(&message.properties)),
    }
}

pub fn from_proto_delivery(message: proto::Delivery) -> Delivery {
    Delivery {
        tenant_id: message.tenant_id,
        session_id: message.session_id,
        topic: message.topic,
        payload: Bytes::from(message.payload),
        qos: message.qos as u8,
        retain: message.retain,
        from_session_id: message.from_session_id,
        properties: message
            .properties
            .map(from_proto_publish_properties)
            .unwrap_or_default(),
    }
}

pub fn to_proto_inflight_phase(phase: &InflightPhase) -> String {
    match phase {
        InflightPhase::Publish => "publish".to_string(),
        InflightPhase::Release => "release".to_string(),
    }
}

pub fn from_proto_inflight_phase(phase: &str) -> InflightPhase {
    match phase {
        "release" => InflightPhase::Release,
        _ => InflightPhase::Publish,
    }
}

pub fn to_proto_publish_properties(properties: &PublishProperties) -> proto::PublishProperties {
    proto::PublishProperties {
        payload_format_indicator: properties
            .payload_format_indicator
            .map(u32::from)
            .unwrap_or_default(),
        content_type: properties.content_type.clone().unwrap_or_default(),
        message_expiry_interval_secs: properties.message_expiry_interval_secs.unwrap_or_default(),
        stored_at_ms: properties.stored_at_ms.unwrap_or_default(),
        response_topic: properties.response_topic.clone().unwrap_or_default(),
        correlation_data: properties.correlation_data.clone().unwrap_or_default(),
        subscription_identifiers: properties.subscription_identifiers.clone(),
        user_properties: properties
            .user_properties
            .iter()
            .map(|property| proto::UserProperty {
                key: property.key.clone(),
                value: property.value.clone(),
            })
            .collect(),
    }
}

pub fn from_proto_publish_properties(properties: proto::PublishProperties) -> PublishProperties {
    PublishProperties {
        payload_format_indicator: if properties.payload_format_indicator == 0 {
            None
        } else {
            Some(properties.payload_format_indicator as u8)
        },
        content_type: if properties.content_type.is_empty() {
            None
        } else {
            Some(properties.content_type)
        },
        message_expiry_interval_secs: if properties.message_expiry_interval_secs == 0 {
            None
        } else {
            Some(properties.message_expiry_interval_secs)
        },
        stored_at_ms: if properties.stored_at_ms == 0 {
            None
        } else {
            Some(properties.stored_at_ms)
        },
        response_topic: if properties.response_topic.is_empty() {
            None
        } else {
            Some(properties.response_topic)
        },
        correlation_data: if properties.correlation_data.is_empty() {
            None
        } else {
            Some(properties.correlation_data)
        },
        subscription_identifiers: properties.subscription_identifiers,
        user_properties: properties
            .user_properties
            .into_iter()
            .map(|property| UserProperty {
                key: property.key,
                value: property.value,
            })
            .collect(),
    }
}

pub fn to_proto_retain(message: &RetainedMessage) -> proto::RetainedMessage {
    proto::RetainedMessage {
        tenant_id: message.tenant_id.clone(),
        topic: message.topic.clone(),
        payload: to_proto_payload(&message.payload),
        qos: message.qos as u32,
    }
}

fn to_proto_payload(payload: &SharedPayload) -> Vec<u8> {
    payload.to_vec()
}

pub fn from_proto_retain(message: proto::RetainedMessage) -> RetainedMessage {
    RetainedMessage {
        tenant_id: message.tenant_id,
        topic: message.topic,
        payload: Bytes::from(message.payload),
        qos: message.qos as u8,
    }
}
