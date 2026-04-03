use super::response::{parse_mqtt5_response_properties, Mqtt5ResponseProperties};
use super::{
    build_packet, encode_remaining_length, property_section_end, read_binary, read_u16, read_u32,
    read_u8, read_utf8, read_varint_from_slice, skip_property_value, PACKET_TYPE_PUBACK,
    PACKET_TYPE_PUBCOMP, PACKET_TYPE_PUBREC, PACKET_TYPE_PUBREL,
};
use greenmqtt_core::{PublishProperties, UserProperty};

#[derive(Debug)]
pub(crate) struct PublishPacket {
    pub(super) packet_id: Option<u16>,
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
    pub(super) topic_alias: Option<u16>,
    pub(super) properties: PublishProperties,
}

struct ParsedPublishProperties {
    topic_alias: Option<u16>,
    properties: PublishProperties,
}

pub(super) fn parse_publish(
    flags: u8,
    body: &[u8],
    protocol_level: u8,
) -> anyhow::Result<PublishPacket> {
    let retain = (flags & 0b0001) != 0;
    let qos = (flags >> 1) & 0b11;
    let dup = (flags & 0b1000) != 0;
    if qos > 2 {
        anyhow::bail!("invalid publish qos");
    }
    if qos == 0 && dup {
        anyhow::bail!("invalid publish flags");
    }
    let mut cursor = 0usize;
    let topic = read_utf8(body, &mut cursor)?;
    let packet_id = if qos > 0 {
        Some(read_u16(body, &mut cursor)?)
    } else {
        None
    };
    let mut topic_alias = None;
    let mut properties = PublishProperties::default();
    if protocol_level == 5 {
        let parsed = parse_publish_properties(body, &mut cursor)?;
        topic_alias = parsed.topic_alias;
        properties = parsed.properties;
    }
    let payload = body[cursor..].to_vec();
    Ok(PublishPacket {
        packet_id,
        topic,
        payload,
        qos,
        retain,
        topic_alias,
        properties,
    })
}

pub(super) fn parse_pubrel(flags: u8, body: &[u8], protocol_level: u8) -> anyhow::Result<u16> {
    if flags != 0b0010 {
        anyhow::bail!("invalid pubrel flags");
    }
    let mut cursor = 0usize;
    let packet_id = read_u16(body, &mut cursor)?;
    if protocol_level == 5 && cursor < body.len() {
        let reason_code = read_u8(body, &mut cursor)?;
        validate_pubrel_reason_code(reason_code)?;
        parse_pub_response_properties(body, &mut cursor, "pubrel property")?;
    }
    Ok(packet_id)
}

pub(super) fn parse_puback(
    packet_type: u8,
    flags: u8,
    body: &[u8],
    protocol_level: u8,
) -> anyhow::Result<u16> {
    if flags != 0 {
        anyhow::bail!("invalid puback/pubrec/pubcomp flags");
    }
    let mut cursor = 0usize;
    let packet_id = read_u16(body, &mut cursor)?;
    if protocol_level == 5 && cursor < body.len() {
        let reason_code = read_u8(body, &mut cursor)?;
        validate_pub_response_reason_code(packet_type, reason_code)?;
        parse_pub_response_properties(body, &mut cursor, "puback/pubrec/pubcomp property")?;
    }
    Ok(packet_id)
}

pub(super) fn build_puback_packet(
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    reason_string: Option<&str>,
) -> Vec<u8> {
    build_pub_response_packet(
        PACKET_TYPE_PUBACK << 4,
        protocol_level,
        packet_id,
        reason_code,
        Mqtt5ResponseProperties::new(reason_string, &[]),
    )
}

pub(super) fn build_pubrec_packet(
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    reason_string: Option<&str>,
) -> Vec<u8> {
    build_pub_response_packet(
        PACKET_TYPE_PUBREC << 4,
        protocol_level,
        packet_id,
        reason_code,
        Mqtt5ResponseProperties::new(reason_string, &[]),
    )
}

pub(super) fn build_pubcomp_packet(protocol_level: u8, packet_id: u16) -> Vec<u8> {
    build_pubcomp_packet_with_reason(
        protocol_level,
        packet_id,
        None,
        Mqtt5ResponseProperties::new(None, &[]),
    )
}

pub(super) fn build_pubcomp_packet_with_reason(
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    properties: Mqtt5ResponseProperties<'_>,
) -> Vec<u8> {
    build_pub_response_packet(
        PACKET_TYPE_PUBCOMP << 4,
        protocol_level,
        packet_id,
        reason_code,
        properties,
    )
}

pub(super) fn build_pubrel_packet(protocol_level: u8, packet_id: u16) -> Vec<u8> {
    build_pubrel_packet_with_reason(
        protocol_level,
        packet_id,
        None,
        Mqtt5ResponseProperties::new(None, &[]),
    )
}

pub(super) fn build_pubrel_packet_with_reason(
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    properties: Mqtt5ResponseProperties<'_>,
) -> Vec<u8> {
    build_pub_response_packet(
        (PACKET_TYPE_PUBREL << 4) | 0b0010,
        protocol_level,
        packet_id,
        reason_code,
        properties,
    )
}

pub(super) fn build_pub_response_packet(
    packet_type: u8,
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    properties: Mqtt5ResponseProperties<'_>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    if protocol_level == 5 {
        body.push(reason_code.unwrap_or(0));
        let encoded = properties.encode();
        encode_remaining_length(&mut body, encoded.len());
        body.extend_from_slice(&encoded);
    }
    build_packet(packet_type, &body)
}

fn parse_pub_response_properties(
    body: &[u8],
    cursor: &mut usize,
    property_context: &str,
) -> anyhow::Result<()> {
    if *cursor >= body.len() {
        return Ok(());
    }
    let parsed = parse_mqtt5_response_properties(body, cursor, property_context)?;
    let _ = parsed.reason_string;
    let _ = parsed.user_properties;
    Ok(())
}

fn validate_pub_response_reason_code(packet_type: u8, reason_code: u8) -> anyhow::Result<()> {
    match packet_type {
        PACKET_TYPE_PUBACK | PACKET_TYPE_PUBREC => anyhow::ensure!(
            matches!(
                reason_code,
                0x00 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99
            ),
            "invalid puback/pubrec reason code"
        ),
        PACKET_TYPE_PUBCOMP => anyhow::ensure!(
            matches!(reason_code, 0x00 | 0x92),
            "invalid pubcomp reason code"
        ),
        _ => anyhow::bail!("unsupported publish response packet type"),
    }
    Ok(())
}

fn validate_pubrel_reason_code(reason_code: u8) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(reason_code, 0x00 | 0x92),
        "invalid pubrel reason code"
    );
    Ok(())
}

fn parse_publish_properties(
    body: &[u8],
    cursor: &mut usize,
) -> anyhow::Result<ParsedPublishProperties> {
    let end = property_section_end(body, cursor)?;
    let mut topic_alias = None;
    let mut payload_format_indicator = None;
    let mut content_type = None;
    let mut message_expiry_interval_secs = None;
    let mut response_topic = None;
    let mut correlation_data = None;
    let subscription_identifiers = Vec::new();
    let mut user_properties = Vec::new();
    while *cursor < end {
        let property_id = read_u8(body, cursor)?;
        match property_id {
            0x01 => {
                let indicator = read_u8(body, cursor)?;
                if payload_format_indicator.is_some() {
                    anyhow::bail!("duplicate payload format indicator");
                }
                if indicator > 1 {
                    anyhow::bail!("invalid payload format indicator");
                }
                payload_format_indicator = Some(indicator);
            }
            0x03 => {
                if content_type.is_some() {
                    anyhow::bail!("duplicate content type");
                }
                content_type = Some(read_utf8(body, cursor)?);
            }
            0x02 => {
                if message_expiry_interval_secs.is_some() {
                    anyhow::bail!("duplicate message expiry interval");
                }
                message_expiry_interval_secs = Some(read_u32(body, cursor)?);
            }
            0x23 => {
                if topic_alias.is_some() {
                    anyhow::bail!("duplicate topic alias");
                }
                topic_alias = Some(read_u16(body, cursor)?);
            }
            0x08 => {
                if response_topic.is_some() {
                    anyhow::bail!("duplicate response topic");
                }
                response_topic = Some(read_utf8(body, cursor)?);
            }
            0x09 => {
                if correlation_data.is_some() {
                    anyhow::bail!("duplicate correlation data");
                }
                correlation_data = Some(read_binary(body, cursor)?);
            }
            0x0B => {
                let _ = read_varint_from_slice(body, cursor)?;
                anyhow::bail!("subscription identifier not allowed on publish");
            }
            0x26 => {
                user_properties.push(UserProperty {
                    key: read_utf8(body, cursor)?,
                    value: read_utf8(body, cursor)?,
                });
            }
            _ => skip_property_value(body, cursor, property_id)?,
        }
    }
    Ok(ParsedPublishProperties {
        topic_alias,
        properties: PublishProperties {
            payload_format_indicator,
            content_type,
            message_expiry_interval_secs,
            stored_at_ms: None,
            response_topic,
            correlation_data,
            subscription_identifiers,
            user_properties,
        },
    })
}
