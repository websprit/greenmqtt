use super::codec::{encode_remaining_length, read_varint_from_slice};
use super::util::current_millis;
use greenmqtt_core::{PublishProperties, UserProperty};

pub(crate) struct ParsedWillProperties {
    pub(crate) properties: PublishProperties,
    pub(crate) will_delay_interval_secs: Option<u32>,
}

pub(crate) fn parse_will_properties(
    body: &[u8],
    cursor: &mut usize,
) -> anyhow::Result<ParsedWillProperties> {
    let end = property_section_end(body, cursor)?;
    let mut will_delay_interval_secs = None;
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
            0x02 => {
                if message_expiry_interval_secs.is_some() {
                    anyhow::bail!("duplicate message expiry interval");
                }
                message_expiry_interval_secs = Some(read_u32(body, cursor)?);
            }
            0x03 => {
                if content_type.is_some() {
                    anyhow::bail!("duplicate content type");
                }
                content_type = Some(read_utf8(body, cursor)?);
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
            0x18 => {
                if will_delay_interval_secs.is_some() {
                    anyhow::bail!("duplicate will delay interval");
                }
                will_delay_interval_secs = Some(read_u32(body, cursor)?);
            }
            0x23 => {
                if topic_alias.is_some() {
                    anyhow::bail!("duplicate topic alias");
                }
                topic_alias = Some(read_u16(body, cursor)?);
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
    let _ = topic_alias;
    Ok(ParsedWillProperties {
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
        will_delay_interval_secs,
    })
}

pub(crate) fn property_section_end(body: &[u8], cursor: &mut usize) -> anyhow::Result<usize> {
    let properties_len = read_varint_from_slice(body, cursor)? as usize;
    if *cursor + properties_len > body.len() {
        anyhow::bail!("buffer underflow");
    }
    Ok(*cursor + properties_len)
}

pub(crate) fn skip_property_value(
    body: &[u8],
    cursor: &mut usize,
    property_id: u8,
) -> anyhow::Result<()> {
    match property_id {
        0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => {
            let _ = read_u8(body, cursor)?;
        }
        0x13 | 0x21 | 0x22 | 0x23 => {
            let _ = read_u16(body, cursor)?;
        }
        0x02 | 0x11 | 0x18 | 0x27 => {
            let _ = read_u32(body, cursor)?;
        }
        0x0B => {
            let _ = read_varint_from_slice(body, cursor)?;
        }
        0x09 | 0x16 => {
            let _ = read_binary(body, cursor)?;
        }
        0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F => {
            let _ = read_utf8(body, cursor)?;
        }
        0x26 => {
            let _ = read_utf8(body, cursor)?;
            let _ = read_utf8(body, cursor)?;
        }
        other => anyhow::bail!("unsupported mqtt5 property id {other:#x}"),
    }
    Ok(())
}

pub(crate) fn read_utf8(body: &[u8], cursor: &mut usize) -> anyhow::Result<String> {
    let data = read_binary(body, cursor)?;
    Ok(String::from_utf8(data)?)
}

pub(crate) fn write_utf8(out: &mut Vec<u8>, value: &str) -> anyhow::Result<()> {
    let len = u16::try_from(value.len())?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(crate) fn write_binary(out: &mut Vec<u8>, value: &[u8]) -> anyhow::Result<()> {
    let len = u16::try_from(value.len())?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

pub(crate) fn read_binary(body: &[u8], cursor: &mut usize) -> anyhow::Result<Vec<u8>> {
    let len = read_u16(body, cursor)? as usize;
    if *cursor + len > body.len() {
        anyhow::bail!("buffer underflow");
    }
    let data = body[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(data)
}

pub(crate) fn read_u8(body: &[u8], cursor: &mut usize) -> anyhow::Result<u8> {
    if *cursor >= body.len() {
        anyhow::bail!("buffer underflow");
    }
    let value = body[*cursor];
    *cursor += 1;
    Ok(value)
}

pub(crate) fn read_u16(body: &[u8], cursor: &mut usize) -> anyhow::Result<u16> {
    if *cursor + 2 > body.len() {
        anyhow::bail!("buffer underflow");
    }
    let value = u16::from_be_bytes([body[*cursor], body[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

pub(crate) fn read_u32(body: &[u8], cursor: &mut usize) -> anyhow::Result<u32> {
    if *cursor + 4 > body.len() {
        anyhow::bail!("buffer underflow");
    }
    let value = u32::from_be_bytes([
        body[*cursor],
        body[*cursor + 1],
        body[*cursor + 2],
        body[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

pub(crate) fn write_publish_properties(
    out: &mut Vec<u8>,
    properties: &PublishProperties,
) -> anyhow::Result<()> {
    let mut encoded = Vec::new();
    if let Some(payload_format_indicator) = properties.payload_format_indicator {
        encoded.push(0x01);
        encoded.push(payload_format_indicator);
    }
    if let Some(content_type) = &properties.content_type {
        encoded.push(0x03);
        write_utf8(&mut encoded, content_type)?;
    }
    if let Some(expiry_secs) = remaining_message_expiry_secs(properties) {
        encoded.push(0x02);
        encoded.extend_from_slice(&expiry_secs.to_be_bytes());
    }
    if let Some(response_topic) = &properties.response_topic {
        encoded.push(0x08);
        write_utf8(&mut encoded, response_topic)?;
    }
    if let Some(correlation_data) = &properties.correlation_data {
        encoded.push(0x09);
        write_binary(&mut encoded, correlation_data)?;
    }
    for subscription_identifier in &properties.subscription_identifiers {
        encoded.push(0x0B);
        encode_remaining_length(&mut encoded, *subscription_identifier as usize);
    }
    for property in &properties.user_properties {
        encoded.push(0x26);
        write_utf8(&mut encoded, &property.key)?;
        write_utf8(&mut encoded, &property.value)?;
    }
    encode_remaining_length(out, encoded.len());
    out.extend_from_slice(&encoded);
    Ok(())
}

pub(crate) fn remaining_message_expiry_secs(properties: &PublishProperties) -> Option<u32> {
    let expiry_secs = properties.message_expiry_interval_secs?;
    let stored_at_ms = properties.stored_at_ms?;
    let elapsed_ms = current_millis().saturating_sub(stored_at_ms);
    let total_ms = u64::from(expiry_secs) * 1000;
    if elapsed_ms >= total_ms {
        return Some(0);
    }
    let remaining_ms = total_ms - elapsed_ms;
    let remaining_secs = remaining_ms.div_ceil(1000) as u32;
    Some(remaining_secs)
}

pub(crate) fn delivery_properties_with_subscription_identifier(
    subscription_identifier: Option<u32>,
) -> PublishProperties {
    PublishProperties {
        subscription_identifiers: subscription_identifier.into_iter().collect(),
        ..PublishProperties::default()
    }
}
