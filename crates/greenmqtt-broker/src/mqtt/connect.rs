use super::response::{
    Mqtt5AuthProperties, Mqtt5ConnackErrorProperties, Mqtt5ConnackProperties,
    Mqtt5DisconnectProperties, Mqtt5ResponseProperties,
};
use super::{
    build_packet, encode_remaining_length, property_section_end, read_binary, read_u16, read_u32,
    read_u8, read_utf8, skip_property_value, PACKET_TYPE_AUTH, PACKET_TYPE_CONNACK,
    PACKET_TYPE_DISCONNECT,
};
use crate::mqtt::util::validate_utf8_payload_format;
use crate::BrokerRuntime;
use greenmqtt_core::{
    ClientIdentity, ConnectRequest, PublishProperties, PublishRequest, SessionKind, UserProperty,
};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};

#[derive(Debug)]
pub(crate) struct ConnectPacket {
    pub(super) protocol_level: u8,
    pub(super) clean_start: bool,
    pub(super) client_id: String,
    pub(super) username: Option<String>,
    pub(super) password: Option<Vec<u8>>,
    pub(super) keep_alive_secs: u16,
    pub(super) session_expiry_interval: Option<u32>,
    pub(super) receive_maximum: Option<u16>,
    pub(super) request_response_information: Option<bool>,
    pub(super) request_problem_information: Option<bool>,
    pub(super) invalid_connect_property_reason: Option<&'static str>,
    pub(super) _maximum_packet_size: Option<u32>,
    pub(super) auth_method: Option<String>,
    pub(super) auth_data: Option<Vec<u8>>,
    pub(super) will: Option<PublishRequest>,
    pub(super) will_delay_interval_secs: Option<u32>,
}

pub(super) struct ResolvedConnect {
    pub(super) client_id: String,
    pub(super) assigned_client_id: Option<String>,
}

#[derive(Debug)]
pub(super) struct PreparedConnect {
    pub(super) protocol_level: u8,
    pub(super) connect_request: ConnectRequest,
    pub(super) connack_properties: ConnackProperties,
    pub(super) receive_maximum: usize,
    pub(super) include_problem_information: bool,
    pub(super) client_max_packet_size: Option<u32>,
    pub(super) keep_alive_secs: u16,
    pub(super) current_session_expiry_interval: Option<u32>,
    pub(super) will_publish: Option<PublishRequest>,
    pub(super) will_delay_interval_secs: Option<u32>,
    pub(super) auth_method: Option<String>,
    pub(super) auth_data: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
pub(crate) struct DisconnectPacket {
    pub(super) session_expiry_interval: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct AuthPacket {
    pub(super) reason_code: u8,
    pub(super) auth_method: Option<String>,
    pub(super) auth_data: Option<Vec<u8>>,
    pub(super) reason_string: Option<String>,
}

struct ParsedConnectProperties {
    session_expiry_interval: Option<u32>,
    receive_maximum: Option<u16>,
    request_response_information: Option<bool>,
    request_problem_information: Option<bool>,
    invalid_connect_property_reason: Option<&'static str>,
    maximum_packet_size: Option<u32>,
    auth_method: Option<String>,
    auth_data: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
pub(super) struct ConnackProperties {
    pub(super) server_keep_alive_secs: Option<u16>,
    pub(super) maximum_packet_size: Option<u32>,
    pub(super) topic_alias_maximum: Option<u16>,
    pub(super) retain_available: bool,
    pub(super) wildcard_subscription_available: bool,
    pub(super) shared_subscription_available: bool,
    pub(super) subscription_identifiers_available: bool,
    pub(super) assigned_client_identifier: Option<String>,
    pub(super) response_information: Option<String>,
    pub(super) server_reference: Option<String>,
    pub(super) user_properties: Vec<UserProperty>,
}

pub(super) fn parse_connect(body: &[u8]) -> anyhow::Result<ConnectPacket> {
    let mut cursor = 0usize;
    let protocol_name = read_utf8(body, &mut cursor)?;
    let protocol_level = read_u8(body, &mut cursor)?;
    if protocol_name != "MQTT" {
        anyhow::bail!("unsupported protocol name");
    }
    let connect_flags = read_u8(body, &mut cursor)?;
    let mut invalid_connect_property_reason = None;
    if (connect_flags & 0b0000_0001) != 0 {
        invalid_connect_property_reason = Some("invalid connect flags");
    }
    let clean_start = (connect_flags & 0b10) != 0;
    let keep_alive_secs = read_u16(body, &mut cursor)?;
    let mut session_expiry_interval = None;
    let mut receive_maximum = None;
    let mut request_response_information = None;
    let mut request_problem_information = None;
    let mut maximum_packet_size = None;
    let mut auth_method = None;
    let mut auth_data = None;
    if protocol_level == 5 {
        let parsed = parse_connect_properties(body, &mut cursor)?;
        session_expiry_interval = parsed.session_expiry_interval;
        receive_maximum = parsed.receive_maximum;
        request_response_information = parsed.request_response_information;
        request_problem_information = parsed.request_problem_information;
        if invalid_connect_property_reason.is_none() {
            invalid_connect_property_reason = parsed.invalid_connect_property_reason;
        }
        maximum_packet_size = parsed.maximum_packet_size;
        auth_method = parsed.auth_method;
        auth_data = parsed.auth_data;
    }
    let client_id = read_utf8(body, &mut cursor)?;

    let will_flag = (connect_flags & 0b100) != 0;
    let will_qos = (connect_flags >> 3) & 0b11;
    let will_retain = (connect_flags & 0b0010_0000) != 0;
    if will_qos > 2 || (!will_flag && (will_qos != 0 || will_retain)) {
        invalid_connect_property_reason = Some("invalid connect flags");
    }
    let mut will = None;
    let mut will_delay_interval_secs = None;
    if will_flag {
        let mut will_properties = PublishProperties::default();
        if protocol_level == 5 {
            let parsed = super::parse_will_properties(body, &mut cursor)?;
            will_properties = parsed.properties;
            will_delay_interval_secs = parsed.will_delay_interval_secs;
        }
        let will_topic = read_utf8(body, &mut cursor)?;
        let will_payload = read_binary(body, &mut cursor)?;
        will = Some(PublishRequest {
            topic: will_topic,
            payload: will_payload.into(),
            qos: will_qos,
            retain: will_retain,
            properties: will_properties,
        });
    }

    let username = if (connect_flags & 0b1000_0000) != 0 {
        Some(read_utf8(body, &mut cursor)?)
    } else {
        None
    };
    let password = if (connect_flags & 0b0100_0000) != 0 {
        Some(read_binary(body, &mut cursor)?)
    } else {
        None
    };

    Ok(ConnectPacket {
        protocol_level,
        clean_start,
        client_id,
        username,
        password,
        keep_alive_secs,
        session_expiry_interval,
        receive_maximum,
        request_response_information,
        request_problem_information,
        invalid_connect_property_reason,
        _maximum_packet_size: maximum_packet_size,
        auth_method,
        auth_data,
        will,
        will_delay_interval_secs,
    })
}

pub(super) fn parse_disconnect(
    body: &[u8],
    protocol_level: u8,
) -> anyhow::Result<DisconnectPacket> {
    if protocol_level != 5 {
        if !body.is_empty() {
            anyhow::bail!("disconnect packet must be empty for mqtt 3.1.1");
        }
        return Ok(DisconnectPacket::default());
    }
    if body.is_empty() {
        return Ok(DisconnectPacket::default());
    }
    let mut cursor = 0usize;
    let reason_code = read_u8(body, &mut cursor)?;
    anyhow::ensure!(
        is_valid_disconnect_reason_code(reason_code),
        "invalid disconnect reason code"
    );
    let properties_end = property_section_end(body, &mut cursor)?;
    let mut session_expiry_interval = None;
    let mut reason_string = None;
    while cursor < properties_end {
        match body[cursor] {
            0x11 => {
                cursor += 1;
                if session_expiry_interval.is_some() {
                    anyhow::bail!("duplicate disconnect session expiry interval");
                }
                session_expiry_interval = Some(read_u32(body, &mut cursor)?);
            }
            0x1F => {
                cursor += 1;
                anyhow::ensure!(
                    reason_string.is_none(),
                    "duplicate disconnect reason string"
                );
                reason_string = Some(read_utf8(body, &mut cursor)?);
            }
            0x26 => {
                cursor += 1;
                let _ = read_utf8(body, &mut cursor)?;
                let _ = read_utf8(body, &mut cursor)?;
            }
            _ => anyhow::bail!("invalid disconnect property"),
        }
    }
    Ok(DisconnectPacket {
        session_expiry_interval,
    })
}

fn is_valid_disconnect_reason_code(reason_code: u8) -> bool {
    matches!(
        reason_code,
        0x00 | 0x04
            | 0x80
            | 0x81
            | 0x82
            | 0x83
            | 0x87
            | 0x89
            | 0x8B
            | 0x8D
            | 0x8E
            | 0x8F
            | 0x90
            | 0x93
            | 0x94
            | 0x95
            | 0x96
            | 0x97
            | 0x98
            | 0x99
            | 0x9A
            | 0x9B
            | 0x9C
            | 0x9D
            | 0x9E
            | 0xA0
            | 0xA1
            | 0xA2
    )
}

pub(super) fn parse_auth(flags: u8, body: &[u8], protocol_level: u8) -> anyhow::Result<AuthPacket> {
    anyhow::ensure!(protocol_level == 5, "auth packet requires mqtt 5");
    anyhow::ensure!(flags == 0, "invalid auth flags");
    if body.is_empty() {
        return Ok(AuthPacket {
            reason_code: 0,
            auth_method: None,
            auth_data: None,
            reason_string: None,
        });
    }
    let mut cursor = 0usize;
    let reason_code = read_u8(body, &mut cursor)?;
    anyhow::ensure!(
        matches!(reason_code, 0x00 | 0x18 | 0x19),
        "invalid auth reason code"
    );
    let properties_end = property_section_end(body, &mut cursor)?;
    let mut auth_method = None;
    let mut auth_data = None;
    let mut reason_string = None;
    while cursor < properties_end {
        match body[cursor] {
            0x15 => {
                cursor += 1;
                anyhow::ensure!(auth_method.is_none(), "duplicate auth method");
                auth_method = Some(read_utf8(body, &mut cursor)?);
            }
            0x16 => {
                cursor += 1;
                anyhow::ensure!(auth_data.is_none(), "duplicate auth data");
                auth_data = Some(read_binary(body, &mut cursor)?);
            }
            0x1F => {
                cursor += 1;
                anyhow::ensure!(reason_string.is_none(), "duplicate auth reason string");
                reason_string = Some(read_utf8(body, &mut cursor)?);
            }
            0x26 => {
                cursor += 1;
                let _ = read_utf8(body, &mut cursor)?;
                let _ = read_utf8(body, &mut cursor)?;
            }
            _ => anyhow::bail!("invalid auth property"),
        }
    }
    if auth_data.is_some() && auth_method.is_none() {
        anyhow::bail!("auth data without auth method");
    }
    Ok(AuthPacket {
        reason_code,
        auth_method,
        auth_data,
        reason_string,
    })
}

pub(super) fn build_connack_packet_with_properties(
    protocol_level: u8,
    session_present: bool,
    properties: &ConnackProperties,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(if session_present { 1 } else { 0 });
    body.push(0);
    if protocol_level == 5 {
        let encoded = Mqtt5ConnackProperties {
            server_keep_alive_secs: properties.server_keep_alive_secs,
            maximum_packet_size: properties.maximum_packet_size,
            topic_alias_maximum: properties.topic_alias_maximum,
            retain_available: properties.retain_available,
            wildcard_subscription_available: properties.wildcard_subscription_available,
            shared_subscription_available: properties.shared_subscription_available,
            subscription_identifiers_available: properties.subscription_identifiers_available,
            assigned_client_identifier: properties.assigned_client_identifier.as_deref(),
            response_information: properties.response_information.as_deref(),
            server_reference: properties.server_reference.as_deref(),
            user_properties: &properties.user_properties,
        }
        .encode();
        encode_remaining_length(&mut body, encoded.len());
        body.extend_from_slice(&encoded);
    }
    build_packet(PACKET_TYPE_CONNACK << 4, &body)
}

pub(super) fn build_connack_error_packet(
    protocol_level: u8,
    reason_code: u8,
    reason_string: Option<&str>,
    server_reference: Option<&str>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0);
    body.push(reason_code);
    if protocol_level == 5 {
        let encoded = Mqtt5ConnackErrorProperties {
            response: Mqtt5ResponseProperties::new(reason_string, &[]),
            server_reference,
        }
        .encode();
        encode_remaining_length(&mut body, encoded.len());
        body.extend_from_slice(&encoded);
    }
    build_packet(PACKET_TYPE_CONNACK << 4, &body)
}

pub(super) fn build_disconnect_packet_with_server_reference(
    protocol_level: u8,
    reason_code: Option<u8>,
    reason_string: Option<&str>,
    server_reference: Option<&str>,
) -> Vec<u8> {
    if protocol_level == 5 {
        let mut body = Vec::new();
        body.push(reason_code.unwrap_or(0));
        let properties = Mqtt5DisconnectProperties {
            response: Mqtt5ResponseProperties::new(reason_string, &[]),
            session_expiry_interval: None,
            server_reference,
        }
        .encode();
        encode_remaining_length(&mut body, properties.len());
        body.extend_from_slice(&properties);
        build_packet(PACKET_TYPE_DISCONNECT << 4, &body)
    } else {
        build_packet(PACKET_TYPE_DISCONNECT << 4, &[])
    }
}

pub(super) fn build_auth_packet_with_reason(
    reason_code: u8,
    method: &str,
    auth_data: Option<&[u8]>,
    reason_string: Option<&str>,
) -> anyhow::Result<Vec<u8>> {
    let mut body = vec![reason_code];
    let properties = Mqtt5AuthProperties {
        response: Mqtt5ResponseProperties::new(reason_string, &[]),
        auth_method: method,
        auth_data,
    }
    .encode();
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    Ok(build_packet(PACKET_TYPE_AUTH << 4, &body))
}

pub(super) fn build_auth_packet(
    method: &str,
    auth_data: Option<&[u8]>,
    reason_string: Option<&str>,
) -> anyhow::Result<Vec<u8>> {
    build_auth_packet_with_reason(0x18, method, auth_data, reason_string)
}

fn parse_connect_properties(
    body: &[u8],
    cursor: &mut usize,
) -> anyhow::Result<ParsedConnectProperties> {
    let end = property_section_end(body, cursor)?;
    let mut session_expiry_interval = None;
    let mut receive_maximum = None;
    let mut request_response_information = None;
    let mut request_problem_information = None;
    let mut invalid_connect_property_reason = None;
    let mut maximum_packet_size = None;
    let mut auth_method = None;
    let mut auth_data = None;
    while *cursor < end {
        let property_id = read_u8(body, cursor)?;
        match property_id {
            0x11 => {
                if session_expiry_interval.is_some() {
                    invalid_connect_property_reason = Some("duplicate session expiry interval");
                }
                session_expiry_interval = Some(read_u32(body, cursor)?);
            }
            0x17 => {
                if request_problem_information.is_some() {
                    invalid_connect_property_reason = Some("duplicate request problem information");
                }
                let value = read_u8(body, cursor)?;
                match value {
                    0 => request_problem_information = Some(false),
                    1 => request_problem_information = Some(true),
                    _ => {
                        invalid_connect_property_reason =
                            Some("invalid request problem information")
                    }
                }
            }
            0x19 => {
                if request_response_information.is_some() {
                    invalid_connect_property_reason =
                        Some("duplicate request response information");
                }
                let value = read_u8(body, cursor)?;
                match value {
                    0 => request_response_information = Some(false),
                    1 => request_response_information = Some(true),
                    _ => {
                        invalid_connect_property_reason =
                            Some("invalid request response information")
                    }
                }
            }
            0x21 => {
                if receive_maximum.is_some() {
                    invalid_connect_property_reason = Some("duplicate receive maximum");
                }
                receive_maximum = Some(read_u16(body, cursor)?);
            }
            0x27 => {
                if maximum_packet_size.is_some() {
                    invalid_connect_property_reason = Some("duplicate maximum packet size");
                }
                maximum_packet_size = Some(read_u32(body, cursor)?);
            }
            0x15 => {
                if auth_method.is_some() {
                    invalid_connect_property_reason = Some("duplicate auth method");
                }
                let method = read_utf8(body, cursor)?;
                if method.is_empty() {
                    invalid_connect_property_reason = Some("invalid auth method");
                }
                auth_method = Some(method);
            }
            0x16 => {
                if auth_data.is_some() {
                    invalid_connect_property_reason = Some("duplicate auth data");
                }
                auth_data = Some(read_binary(body, cursor)?);
            }
            _ => skip_property_value(body, cursor, property_id)?,
        }
    }
    if auth_data.is_some() && auth_method.is_none() {
        invalid_connect_property_reason = Some("auth data without auth method");
    }
    Ok(ParsedConnectProperties {
        session_expiry_interval,
        receive_maximum,
        request_response_information,
        request_problem_information,
        invalid_connect_property_reason,
        maximum_packet_size,
        auth_method,
        auth_data,
    })
}

pub(super) fn resolve_connect<A, C, H>(
    broker: &BrokerRuntime<A, C, H>,
    connect: &ConnectPacket,
) -> ResolvedConnect
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    if connect.client_id.is_empty() {
        let assigned_client_id = broker.next_assigned_client_id();
        ResolvedConnect {
            client_id: assigned_client_id.clone(),
            assigned_client_id: Some(assigned_client_id),
        }
    } else {
        ResolvedConnect {
            client_id: connect.client_id.clone(),
            assigned_client_id: None,
        }
    }
}

pub(super) fn prepare_connect<A, C, H>(
    broker: &BrokerRuntime<A, C, H>,
    connect: ConnectPacket,
) -> anyhow::Result<PreparedConnect>
where
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    let protocol_level = connect.protocol_level;
    if protocol_level == 5 {
        if let Some(reason) = connect.invalid_connect_property_reason {
            anyhow::bail!(reason);
        }
        if connect.receive_maximum == Some(0) {
            anyhow::bail!("invalid receive maximum");
        }
        if connect._maximum_packet_size == Some(0) {
            anyhow::bail!("invalid maximum packet size");
        }
    }

    let (tenant_id, user_id) = parse_username_parts(connect.username.as_deref());
    let kind = parse_session_kind(connect.password.as_deref(), connect.session_expiry_interval);
    let resolved_connect = resolve_connect(broker, &connect);
    let include_problem_information = connect.request_problem_information.unwrap_or(true);
    if let Some(will_publish) = &connect.will {
        validate_utf8_payload_format(&will_publish.properties, &will_publish.payload)?;
    }
    Ok(PreparedConnect {
        protocol_level,
        connect_request: ConnectRequest {
            identity: ClientIdentity {
                tenant_id,
                user_id,
                client_id: resolved_connect.client_id,
            },
            node_id: broker.config.node_id,
            kind,
            clean_start: connect.clean_start,
            session_expiry_interval_secs: connect.session_expiry_interval,
        },
        connack_properties: ConnackProperties {
            server_keep_alive_secs: (protocol_level == 5)
                .then_some(broker.config.server_keep_alive_secs)
                .flatten(),
            maximum_packet_size: (protocol_level == 5)
                .then_some(broker.config.max_packet_size)
                .flatten(),
            topic_alias_maximum: (protocol_level == 5).then_some(u16::MAX),
            retain_available: protocol_level == 5,
            wildcard_subscription_available: protocol_level == 5,
            shared_subscription_available: protocol_level == 5,
            subscription_identifiers_available: protocol_level == 5,
            assigned_client_identifier: resolved_connect.assigned_client_id,
            response_information: if protocol_level == 5
                && connect.request_response_information == Some(true)
            {
                broker.config.response_information.clone()
            } else {
                None
            },
            server_reference: None,
            user_properties: Vec::new(),
        },
        receive_maximum: connect.receive_maximum.unwrap_or(u16::MAX) as usize,
        include_problem_information,
        client_max_packet_size: connect._maximum_packet_size,
        keep_alive_secs: connect.keep_alive_secs,
        current_session_expiry_interval: connect.session_expiry_interval,
        will_publish: connect.will,
        will_delay_interval_secs: connect.will_delay_interval_secs,
        auth_method: connect.auth_method,
        auth_data: connect.auth_data,
    })
}

pub(super) fn parse_username_parts(username: Option<&str>) -> (String, String) {
    match username.and_then(|value| value.split_once(':')) {
        Some((tenant_id, user_id)) if !tenant_id.is_empty() && !user_id.is_empty() => {
            (tenant_id.to_string(), user_id.to_string())
        }
        _ => (username.unwrap_or("public").to_string(), "mqtt".to_string()),
    }
}

pub(super) fn parse_session_kind(
    password: Option<&[u8]>,
    session_expiry_interval: Option<u32>,
) -> SessionKind {
    match password.and_then(|value| std::str::from_utf8(value).ok()) {
        Some("transient") => SessionKind::Transient,
        _ if session_expiry_interval == Some(0) => SessionKind::Transient,
        _ => SessionKind::Persistent,
    }
}

pub(super) fn connect_error_reason_code(protocol_level: u8, error: &anyhow::Error) -> Option<u8> {
    let message = format!("{error:#}");
    if message.contains("authentication failed") {
        return Some(if protocol_level == 5 { 0x87 } else { 0x05 });
    }
    if protocol_level == 5 && message.contains("unsupported authentication method") {
        return Some(0x8C);
    }
    if protocol_level == 5 && message.contains("invalid receive maximum") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("invalid maximum packet size") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("invalid request problem information") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("invalid request response information") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate session expiry interval") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate request problem information") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate request response information") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate receive maximum") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate maximum packet size") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate auth method") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("duplicate auth data") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("invalid auth method") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("auth data without auth method") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("invalid auth reason code") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("invalid connect flags") {
        return Some(0x82);
    }
    if protocol_level == 5 && message.contains("tenant quota exceeded") {
        return Some(0x97);
    }
    if protocol_level == 5 && message.contains("payload format invalid") {
        return Some(0x99);
    }
    None
}

pub(super) fn connect_error_reason_string(error: &anyhow::Error) -> Option<&'static str> {
    let message = format!("{error:#}");
    if message.contains("authentication failed") {
        return Some("authentication failed");
    }
    if message.contains("unsupported authentication method") {
        return Some("unsupported authentication method");
    }
    if message.contains("invalid receive maximum") {
        return Some("invalid receive maximum");
    }
    if message.contains("invalid maximum packet size") {
        return Some("invalid maximum packet size");
    }
    if message.contains("invalid request problem information") {
        return Some("invalid request problem information");
    }
    if message.contains("invalid request response information") {
        return Some("invalid request response information");
    }
    if message.contains("duplicate session expiry interval") {
        return Some("duplicate session expiry interval");
    }
    if message.contains("duplicate request problem information") {
        return Some("duplicate request problem information");
    }
    if message.contains("duplicate request response information") {
        return Some("duplicate request response information");
    }
    if message.contains("duplicate receive maximum") {
        return Some("duplicate receive maximum");
    }
    if message.contains("duplicate maximum packet size") {
        return Some("duplicate maximum packet size");
    }
    if message.contains("duplicate auth method") {
        return Some("duplicate auth method");
    }
    if message.contains("duplicate auth data") {
        return Some("duplicate auth data");
    }
    if message.contains("invalid auth method") {
        return Some("invalid auth method");
    }
    if message.contains("auth data without auth method") {
        return Some("auth data without auth method");
    }
    if message.contains("invalid auth reason code") {
        return Some("invalid auth reason code");
    }
    if message.contains("duplicate disconnect reason string") {
        return Some("duplicate disconnect reason string");
    }
    if message.contains("invalid connect flags") {
        return Some("invalid connect flags");
    }
    if message.contains("tenant quota exceeded") {
        return Some("quota exceeded");
    }
    if message.contains("payload format invalid") {
        return Some("payload format invalid");
    }
    None
}
