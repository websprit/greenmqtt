use super::{property_section_end, read_utf8};
use greenmqtt_core::UserProperty;

#[derive(Debug, Default, Clone)]
pub(super) struct ParsedMqtt5ResponseProperties {
    pub(super) reason_string: Option<String>,
    pub(super) user_properties: Vec<UserProperty>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Mqtt5ResponseProperties<'a> {
    pub(super) reason_string: Option<&'a str>,
    pub(super) user_properties: &'a [UserProperty],
}

impl<'a> Mqtt5ResponseProperties<'a> {
    pub(super) fn new(reason_string: Option<&'a str>, user_properties: &'a [UserProperty]) -> Self {
        Self {
            reason_string,
            user_properties,
        }
    }

    pub(super) fn encode(self) -> Vec<u8> {
        append_mqtt5_property_section(|encoded| {
            if let Some(reason_string) = self.reason_string {
                append_mqtt5_utf8_property(encoded, 0x1F, reason_string);
            }
            append_mqtt5_user_properties(encoded, self.user_properties);
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Mqtt5ConnackErrorProperties<'a> {
    pub(super) response: Mqtt5ResponseProperties<'a>,
    pub(super) server_reference: Option<&'a str>,
}

impl<'a> Mqtt5ConnackErrorProperties<'a> {
    pub(super) fn encode(self) -> Vec<u8> {
        append_mqtt5_property_section(|encoded| {
            if let Some(reason_string) = self.response.reason_string {
                append_mqtt5_utf8_property(encoded, 0x1F, reason_string);
            }
            if let Some(server_reference) = self.server_reference {
                append_mqtt5_utf8_property(encoded, 0x1C, server_reference);
            }
            append_mqtt5_user_properties(encoded, self.response.user_properties);
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Mqtt5ConnackProperties<'a> {
    pub(super) server_keep_alive_secs: Option<u16>,
    pub(super) maximum_packet_size: Option<u32>,
    pub(super) topic_alias_maximum: Option<u16>,
    pub(super) retain_available: bool,
    pub(super) wildcard_subscription_available: bool,
    pub(super) shared_subscription_available: bool,
    pub(super) subscription_identifiers_available: bool,
    pub(super) assigned_client_identifier: Option<&'a str>,
    pub(super) response_information: Option<&'a str>,
    pub(super) server_reference: Option<&'a str>,
    pub(super) user_properties: &'a [UserProperty],
}

impl<'a> Mqtt5ConnackProperties<'a> {
    pub(super) fn encode(self) -> Vec<u8> {
        append_mqtt5_property_section(|encoded| {
            if let Some(server_keep_alive_secs) = self.server_keep_alive_secs {
                encoded.push(0x13);
                encoded.extend_from_slice(&server_keep_alive_secs.to_be_bytes());
            }
            if let Some(maximum_packet_size) = self.maximum_packet_size {
                encoded.push(0x27);
                encoded.extend_from_slice(&maximum_packet_size.to_be_bytes());
            }
            if let Some(topic_alias_maximum) = self.topic_alias_maximum {
                encoded.push(0x22);
                encoded.extend_from_slice(&topic_alias_maximum.to_be_bytes());
            }
            if self.retain_available {
                encoded.push(0x25);
                encoded.push(1);
            }
            if self.wildcard_subscription_available {
                encoded.push(0x28);
                encoded.push(1);
            }
            if self.subscription_identifiers_available {
                encoded.push(0x29);
                encoded.push(1);
            }
            if self.shared_subscription_available {
                encoded.push(0x2A);
                encoded.push(1);
            }
            if let Some(assigned_client_identifier) = self.assigned_client_identifier {
                append_mqtt5_utf8_property(encoded, 0x12, assigned_client_identifier);
            }
            if let Some(response_information) = self.response_information {
                append_mqtt5_utf8_property(encoded, 0x1A, response_information);
            }
            if let Some(server_reference) = self.server_reference {
                append_mqtt5_utf8_property(encoded, 0x1C, server_reference);
            }
            append_mqtt5_user_properties(encoded, self.user_properties);
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Mqtt5DisconnectProperties<'a> {
    pub(super) response: Mqtt5ResponseProperties<'a>,
    pub(super) session_expiry_interval: Option<u32>,
    pub(super) server_reference: Option<&'a str>,
}

impl<'a> Mqtt5DisconnectProperties<'a> {
    pub(super) fn encode(self) -> Vec<u8> {
        append_mqtt5_property_section(|encoded| {
            if let Some(session_expiry_interval) = self.session_expiry_interval {
                encoded.push(0x11);
                encoded.extend_from_slice(&session_expiry_interval.to_be_bytes());
            }
            if let Some(reason_string) = self.response.reason_string {
                append_mqtt5_utf8_property(encoded, 0x1F, reason_string);
            }
            if let Some(server_reference) = self.server_reference {
                append_mqtt5_utf8_property(encoded, 0x1C, server_reference);
            }
            append_mqtt5_user_properties(encoded, self.response.user_properties);
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Mqtt5AuthProperties<'a> {
    pub(super) response: Mqtt5ResponseProperties<'a>,
    pub(super) auth_method: &'a str,
    pub(super) auth_data: Option<&'a [u8]>,
}

impl<'a> Mqtt5AuthProperties<'a> {
    pub(super) fn encode(self) -> Vec<u8> {
        append_mqtt5_property_section(|encoded| {
            append_mqtt5_utf8_property(encoded, 0x15, self.auth_method);
            if let Some(auth_data) = self.auth_data {
                append_mqtt5_binary_property(encoded, 0x16, auth_data);
            }
            if let Some(reason_string) = self.response.reason_string {
                append_mqtt5_utf8_property(encoded, 0x1F, reason_string);
            }
            append_mqtt5_user_properties(encoded, self.response.user_properties);
        })
    }
}

pub(super) fn append_mqtt5_property_section<F>(build: F) -> Vec<u8>
where
    F: FnOnce(&mut Vec<u8>),
{
    let mut encoded = Vec::new();
    build(&mut encoded);
    encoded
}

pub(super) fn append_mqtt5_utf8_property(encoded: &mut Vec<u8>, property_id: u8, value: &str) {
    encoded.push(property_id);
    encoded.extend_from_slice(&(value.len() as u16).to_be_bytes());
    encoded.extend_from_slice(value.as_bytes());
}

pub(super) fn append_mqtt5_binary_property(encoded: &mut Vec<u8>, property_id: u8, value: &[u8]) {
    encoded.push(property_id);
    encoded.extend_from_slice(&(value.len() as u16).to_be_bytes());
    encoded.extend_from_slice(value);
}

pub(super) fn append_mqtt5_user_properties(
    encoded: &mut Vec<u8>,
    user_properties: &[UserProperty],
) {
    for user_property in user_properties {
        encoded.push(0x26);
        encoded.extend_from_slice(&(user_property.key.len() as u16).to_be_bytes());
        encoded.extend_from_slice(user_property.key.as_bytes());
        encoded.extend_from_slice(&(user_property.value.len() as u16).to_be_bytes());
        encoded.extend_from_slice(user_property.value.as_bytes());
    }
}

pub(super) fn parse_mqtt5_response_properties(
    body: &[u8],
    cursor: &mut usize,
    property_context: &str,
) -> anyhow::Result<ParsedMqtt5ResponseProperties> {
    let properties_end = property_section_end(body, cursor)?;
    let mut reason_string = None;
    let mut user_properties = Vec::new();
    while *cursor < properties_end {
        match body[*cursor] {
            0x1F => {
                *cursor += 1;
                anyhow::ensure!(reason_string.is_none(), "duplicate {property_context}");
                reason_string = Some(read_utf8(body, cursor)?);
            }
            0x26 => {
                *cursor += 1;
                user_properties.push(UserProperty {
                    key: read_utf8(body, cursor)?,
                    value: read_utf8(body, cursor)?,
                });
            }
            _ => anyhow::bail!("invalid {property_context}"),
        }
    }
    Ok(ParsedMqtt5ResponseProperties {
        reason_string,
        user_properties,
    })
}
