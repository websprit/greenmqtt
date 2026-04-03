pub mod internal {
    tonic::include_proto!("greenmqtt.internal.v1");
}

use bytes::Bytes;
use greenmqtt_core::{
    ClientIdentity, Delivery, InflightMessage, InflightPhase, OfflineMessage, PublishProperties,
    RetainedMessage, RouteRecord, SessionKind, SessionRecord, SharedPayload, Subscription,
    UserProperty,
};
use internal as proto;

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
