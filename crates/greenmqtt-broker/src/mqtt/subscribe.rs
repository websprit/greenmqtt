use super::{
    codec::{
        build_packet, encode_remaining_length, read_varint_from_slice, PACKET_TYPE_SUBACK,
        PACKET_TYPE_UNSUBACK,
    },
    property_section_end, read_u16, read_u8, read_utf8,
    response::Mqtt5ResponseProperties,
};

#[derive(Debug)]
pub(crate) struct SubscribePacket {
    pub(super) packet_id: u16,
    pub(super) subscriptions: Vec<SubscriptionRequest>,
}

struct ParsedSubscribeProperties {
    subscription_identifier: Option<u32>,
}

#[derive(Debug)]
pub(super) struct SubscriptionRequest {
    pub(super) topic_filter: String,
    pub(super) qos: u8,
    pub(super) subscription_identifier: Option<u32>,
    pub(super) no_local: bool,
    pub(super) retain_as_published: bool,
    pub(super) retain_handling: u8,
}

#[derive(Debug)]
pub(crate) struct UnsubscribePacket {
    pub(super) packet_id: u16,
    pub(super) topic_filters: Vec<String>,
}

pub(super) fn parse_subscribe(
    flags: u8,
    body: &[u8],
    protocol_level: u8,
) -> anyhow::Result<SubscribePacket> {
    if flags != 0b0010 {
        anyhow::bail!("invalid subscribe flags");
    }
    let mut cursor = 0usize;
    let packet_id = read_u16(body, &mut cursor)?;
    let subscription_identifier = if protocol_level == 5 {
        parse_subscribe_properties(body, &mut cursor)?.subscription_identifier
    } else {
        None
    };
    let mut subscriptions = Vec::new();
    while cursor < body.len() {
        let topic_filter = read_utf8(body, &mut cursor)?;
        validate_subscription_filter(&topic_filter)?;
        let options = read_u8(body, &mut cursor)?;
        if protocol_level == 5 && (options & 0b1100_0000) != 0 {
            anyhow::bail!("invalid subscribe options");
        }
        let qos = options & 0b11;
        if qos > 2 {
            anyhow::bail!("invalid subscription qos");
        }
        subscriptions.push(SubscriptionRequest {
            topic_filter,
            qos,
            subscription_identifier,
            no_local: protocol_level == 5 && (options & 0b0100) != 0,
            retain_as_published: protocol_level == 5 && (options & 0b1000) != 0,
            retain_handling: if protocol_level == 5 {
                (options >> 4) & 0b11
            } else {
                0
            },
        });
        if protocol_level == 5 && subscriptions.last().expect("inserted").retain_handling > 2 {
            anyhow::bail!("invalid retain handling");
        }
    }
    if subscriptions.is_empty() {
        anyhow::bail!("empty subscribe payload");
    }
    Ok(SubscribePacket {
        packet_id,
        subscriptions,
    })
}

fn parse_subscribe_properties(
    body: &[u8],
    cursor: &mut usize,
) -> anyhow::Result<ParsedSubscribeProperties> {
    let end = property_section_end(body, cursor)?;
    let mut subscription_identifier = None;
    while *cursor < end {
        let property_id = read_u8(body, cursor)?;
        match property_id {
            0x0B => {
                let identifier = read_varint_from_slice(body, cursor)?;
                if identifier == 0 {
                    anyhow::bail!("invalid subscription identifier");
                }
                if subscription_identifier.is_some() {
                    anyhow::bail!("duplicate subscription identifier");
                }
                subscription_identifier = Some(identifier);
            }
            0x26 => {
                let _ = read_utf8(body, cursor)?;
                let _ = read_utf8(body, cursor)?;
            }
            _ => anyhow::bail!("invalid subscribe property"),
        }
    }
    Ok(ParsedSubscribeProperties {
        subscription_identifier,
    })
}

pub(super) fn parse_unsubscribe(
    flags: u8,
    body: &[u8],
    protocol_level: u8,
) -> anyhow::Result<UnsubscribePacket> {
    if flags != 0b0010 {
        anyhow::bail!("invalid unsubscribe flags");
    }
    let mut cursor = 0usize;
    let packet_id = read_u16(body, &mut cursor)?;
    if protocol_level == 5 {
        let properties_end = property_section_end(body, &mut cursor)?;
        while cursor < properties_end {
            match body[cursor] {
                0x26 => {
                    cursor += 1;
                    let _ = read_utf8(body, &mut cursor)?;
                    let _ = read_utf8(body, &mut cursor)?;
                }
                _ => anyhow::bail!("invalid unsubscribe property"),
            }
        }
    }
    let mut topic_filters = Vec::new();
    while cursor < body.len() {
        let topic_filter = read_utf8(body, &mut cursor)?;
        validate_subscription_filter(&topic_filter)?;
        topic_filters.push(topic_filter);
    }
    if topic_filters.is_empty() {
        anyhow::bail!("empty unsubscribe payload");
    }
    Ok(UnsubscribePacket {
        packet_id,
        topic_filters,
    })
}

fn validate_subscription_filter(topic_filter: &str) -> anyhow::Result<()> {
    if let Some(rest) = topic_filter.strip_prefix("$share/") {
        let mut parts = rest.splitn(2, '/');
        let group = parts.next().unwrap_or_default();
        let shared_filter = parts.next().unwrap_or_default();
        if group.is_empty()
            || group.contains('+')
            || group.contains('#')
            || shared_filter.is_empty()
        {
            anyhow::bail!("invalid topic filter");
        }
        return validate_standard_topic_filter(shared_filter);
    }
    validate_standard_topic_filter(topic_filter)
}

fn validate_standard_topic_filter(topic_filter: &str) -> anyhow::Result<()> {
    if topic_filter.is_empty() || topic_filter.contains('\0') {
        anyhow::bail!("invalid topic filter");
    }
    let mut levels = topic_filter.split('/').peekable();
    while let Some(level) = levels.next() {
        if level.contains('#') && level != "#" {
            anyhow::bail!("invalid topic filter");
        }
        if level == "#" && levels.peek().is_some() {
            anyhow::bail!("invalid topic filter");
        }
        if level.contains('+') && level != "+" {
            anyhow::bail!("invalid topic filter");
        }
    }
    Ok(())
}

pub(super) fn build_suback_packet(
    protocol_level: u8,
    packet_id: u16,
    return_codes: &[u8],
    reason_string: Option<&str>,
) -> Vec<u8> {
    build_suback_packet_with_properties(
        protocol_level,
        packet_id,
        return_codes,
        Mqtt5ResponseProperties::new(reason_string, &[]),
    )
}

pub(super) fn build_suback_packet_with_properties(
    protocol_level: u8,
    packet_id: u16,
    return_codes: &[u8],
    properties: Mqtt5ResponseProperties<'_>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    if protocol_level == 5 {
        let encoded = properties.encode();
        encode_remaining_length(&mut body, encoded.len());
        body.extend_from_slice(&encoded);
    }
    body.extend_from_slice(return_codes);
    build_packet(PACKET_TYPE_SUBACK << 4, &body)
}

pub(super) fn build_unsuback_packet(
    protocol_level: u8,
    packet_id: u16,
    return_codes: &[u8],
) -> Vec<u8> {
    build_unsuback_packet_with_properties(
        protocol_level,
        packet_id,
        return_codes,
        Mqtt5ResponseProperties::new(None, &[]),
    )
}

pub(super) fn build_unsuback_packet_with_properties(
    protocol_level: u8,
    packet_id: u16,
    return_codes: &[u8],
    properties: Mqtt5ResponseProperties<'_>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    if protocol_level == 5 {
        let encoded = properties.encode();
        encode_remaining_length(&mut body, encoded.len());
        body.extend_from_slice(&encoded);
        body.extend_from_slice(return_codes);
    }
    build_packet(PACKET_TYPE_UNSUBACK << 4, &body)
}
