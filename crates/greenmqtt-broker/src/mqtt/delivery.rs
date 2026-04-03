use super::codec::build_packet;
use super::parser::write_publish_properties;
use super::{write_utf8, PACKET_TYPE_PUBLISH};
use greenmqtt_core::{Delivery, InflightMessage, InflightPhase, OfflineMessage, PublishProperties};

pub(crate) fn build_delivery_packet(
    protocol_level: u8,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    properties: &PublishProperties,
    packet_id: Option<u16>,
) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::new();
    write_utf8(&mut body, topic)?;
    if qos > 0 {
        body.extend_from_slice(
            &packet_id
                .ok_or_else(|| anyhow::anyhow!("missing packet id for qos delivery"))?
                .to_be_bytes(),
        );
    }
    if protocol_level == 5 {
        write_publish_properties(&mut body, properties)?;
    }
    body.extend_from_slice(payload);
    let flags = (qos << 1) | u8::from(retain);
    Ok(build_packet((PACKET_TYPE_PUBLISH << 4) | flags, &body))
}

pub(crate) fn delivery_from_offline(message: &OfflineMessage) -> Delivery {
    Delivery {
        tenant_id: message.tenant_id.clone(),
        session_id: message.session_id.clone(),
        topic: message.topic.clone(),
        payload: message.payload.clone(),
        qos: message.qos,
        retain: message.retain,
        from_session_id: message.from_session_id.clone(),
        properties: message.properties.clone(),
    }
}

pub(crate) fn delivery_from_inflight(message: &InflightMessage) -> Delivery {
    Delivery {
        tenant_id: message.tenant_id.clone(),
        session_id: message.session_id.clone(),
        topic: message.topic.clone(),
        payload: message.payload.clone(),
        qos: message.qos,
        retain: message.retain,
        from_session_id: message.from_session_id.clone(),
        properties: message.properties.clone(),
    }
}

pub(crate) fn inflight_from_offline(
    message: &OfflineMessage,
    packet_id: u16,
    phase: InflightPhase,
) -> InflightMessage {
    InflightMessage {
        tenant_id: message.tenant_id.clone(),
        session_id: message.session_id.clone(),
        packet_id,
        topic: message.topic.clone(),
        payload: message.payload.clone(),
        qos: message.qos,
        retain: message.retain,
        from_session_id: message.from_session_id.clone(),
        properties: message.properties.clone(),
        phase,
    }
}

pub(crate) fn inflight_from_delivery(
    delivery: &Delivery,
    packet_id: u16,
    phase: InflightPhase,
) -> InflightMessage {
    InflightMessage {
        tenant_id: delivery.tenant_id.clone(),
        session_id: delivery.session_id.clone(),
        packet_id,
        topic: delivery.topic.clone(),
        payload: delivery.payload.clone(),
        qos: delivery.qos,
        retain: delivery.retain,
        from_session_id: delivery.from_session_id.clone(),
        properties: delivery.properties.clone(),
        phase,
    }
}
