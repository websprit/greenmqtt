use super::*;
use crate::mqtt::connect::{
    build_auth_packet_with_reason, build_connack_packet_with_properties, parse_auth,
    parse_disconnect, prepare_connect, AuthPacket, ConnackProperties, ConnectPacket,
};
use crate::mqtt::publish::{
    build_pub_response_packet, build_puback_packet, build_pubcomp_packet_with_reason,
    build_pubrel_packet_with_reason, parse_puback, parse_publish, parse_pubrel,
};
use crate::mqtt::response::Mqtt5ResponseProperties;
use crate::mqtt::state::{ProtocolPacketKind, ProtocolSessionState, ProtocolStateError};
use crate::mqtt::subscribe::{
    build_suback_packet, build_suback_packet_with_properties, build_unsuback_packet,
    build_unsuback_packet_with_properties,
};

fn base_config() -> BrokerConfig {
    BrokerConfig {
        node_id: 7,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: true,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: Some(30),
        max_packet_size: Some(4096),
        response_information: Some("reply/topic".to_string()),
        server_reference: Some("node-a".to_string()),
        audit_log_path: None,
    }
}

fn base_connect_packet() -> ConnectPacket {
    ConnectPacket {
        protocol_level: 5,
        clean_start: true,
        client_id: String::new(),
        username: Some("tenant-a:user-a".to_string()),
        password: None,
        keep_alive_secs: 12,
        session_expiry_interval: Some(60),
        receive_maximum: Some(25),
        request_response_information: Some(true),
        request_problem_information: Some(false),
        invalid_connect_property_reason: None,
        _maximum_packet_size: Some(1024),
        auth_method: None,
        auth_data: None,
        will: None,
        will_delay_interval_secs: None,
    }
}

fn read_varint(packet: &[u8], cursor: &mut usize) -> usize {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    loop {
        let byte = packet[*cursor];
        *cursor += 1;
        value += ((byte & 0x7F) as usize) * multiplier;
        if (byte & 0x80) == 0 {
            return value;
        }
        multiplier *= 128;
    }
}

#[test]
fn mqtt_v5_duplicate_subscription_identifier_is_rejected() {
    let mut properties = subscription_identifier_property(7);
    properties.extend_from_slice(&subscription_identifier_property(9));
    let packet = subscribe_packet_v5_with_properties(1, "devices/+/state", &properties);
    let error = parse_packet_frame(&packet, 5).unwrap_err();
    assert!(error
        .root_cause()
        .to_string()
        .contains("duplicate subscription identifier"));
}

#[test]
fn mqtt_v5_malformed_remaining_length_is_rejected() {
    let error = parse_packet_frame(&malformed_remaining_length_packet(), 5).unwrap_err();
    let message = error.root_cause().to_string();
    assert!(
        message.contains("malformed varint") || message.contains("malformed frame length"),
        "unexpected error: {message}"
    );
}

fn identity() -> ClientIdentity {
    ClientIdentity {
        tenant_id: "tenant".into(),
        user_id: "user".into(),
        client_id: "client".into(),
    }
}

fn auth_packet(reason_code: u8, auth_method: Option<&str>) -> AuthPacket {
    AuthPacket {
        reason_code,
        auth_method: auth_method.map(str::to_string),
        auth_data: None,
        reason_string: None,
    }
}

#[test]
fn prepare_connect_assigns_client_id_and_response_information() {
    let broker = test_broker_with_config(base_config());
    let prepared = prepare_connect(broker.as_ref(), base_connect_packet()).unwrap();

    assert_eq!(prepared.protocol_level, 5);
    assert_eq!(prepared.connect_request.identity.tenant_id, "tenant-a");
    assert_eq!(prepared.connect_request.identity.user_id, "user-a");
    assert!(!prepared.connect_request.identity.client_id.is_empty());
    assert_eq!(
        prepared
            .connack_properties
            .assigned_client_identifier
            .as_deref(),
        Some(prepared.connect_request.identity.client_id.as_str())
    );
    assert_eq!(
        prepared.connack_properties.response_information.as_deref(),
        Some("reply/topic")
    );
    assert_eq!(prepared.receive_maximum, 25);
    assert!(!prepared.include_problem_information);
    assert_eq!(prepared.client_max_packet_size, Some(1024));
    assert!(matches!(
        prepared.connect_request.kind,
        SessionKind::Persistent
    ));
}

#[test]
fn prepare_connect_rejects_zero_receive_maximum() {
    let broker = test_broker_with_config(base_config());
    let mut connect = base_connect_packet();
    connect.receive_maximum = Some(0);

    let error = prepare_connect(broker.as_ref(), connect).unwrap_err();

    assert!(format!("{error:#}").contains("invalid receive maximum"));
}

#[test]
fn prepare_connect_marks_transient_session_when_expiry_zero() {
    let broker = test_broker_with_config(base_config());
    let mut connect = base_connect_packet();
    connect.clean_start = false;
    connect.client_id = "existing-client".to_string();
    connect.session_expiry_interval = Some(0);

    let prepared = prepare_connect(broker.as_ref(), connect).unwrap();

    assert_eq!(
        prepared.connect_request.identity.client_id,
        "existing-client"
    );
    assert!(matches!(
        prepared.connect_request.kind,
        SessionKind::Transient
    ));
}

#[test]
fn build_disconnect_packet_encodes_reason_string_and_server_reference_for_mqtt5() {
    let packet = build_disconnect_packet_with_server_reference(
        5,
        Some(0x82),
        Some("invalid connect flags"),
        Some("greenmqtt://node-a"),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_DISCONNECT);

    let mut cursor = 1usize;
    let remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(remaining as usize, packet.len() - cursor);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x82);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(packet[cursor], 0x1F);
    cursor += 1;
    assert_eq!(
        read_utf8(&packet, &mut cursor).unwrap(),
        "invalid connect flags"
    );
    assert_eq!(packet[cursor], 0x1C);
    cursor += 1;
    assert_eq!(
        read_utf8(&packet, &mut cursor).unwrap(),
        "greenmqtt://node-a"
    );
    assert_eq!(cursor, properties_end);
}

#[test]
fn build_connack_packet_encodes_user_properties_for_mqtt5() {
    let packet = build_connack_packet_with_properties(
        5,
        false,
        &ConnackProperties {
            assigned_client_identifier: Some("client-a".to_string()),
            response_information: Some("reply/topic".to_string()),
            user_properties: vec![UserProperty {
                key: "tenant".to_string(),
                value: "demo".to_string(),
            }],
            ..Default::default()
        },
    );

    let mut cursor = 1usize;
    let remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(remaining as usize, packet.len() - cursor);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(packet[cursor], 0x12);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "client-a");
    assert_eq!(packet[cursor], 0x1A);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "reply/topic");
    assert_eq!(packet[cursor], 0x26);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "tenant");
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "demo");
    assert_eq!(cursor, properties_end);
}

#[test]
fn build_auth_packet_encodes_method_data_and_reason_string_for_mqtt5() {
    let packet = build_auth_packet_with_reason(
        0x18,
        "custom",
        Some(b"client-response"),
        Some("server-challenge"),
    )
    .unwrap();
    assert_eq!(packet[0] >> 4, PACKET_TYPE_AUTH);

    let mut cursor = 1usize;
    let remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(remaining as usize, packet.len() - cursor);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x18);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(packet[cursor], 0x15);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "custom");
    assert_eq!(packet[cursor], 0x16);
    cursor += 1;
    assert_eq!(
        read_binary(&packet, &mut cursor).unwrap(),
        b"client-response"
    );
    assert_eq!(packet[cursor], 0x1F);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "server-challenge");
    assert_eq!(cursor, properties_end);
}

#[test]
fn build_connack_packet_encodes_all_mqtt5_properties_in_order() {
    let packet = build_connack_packet_with_properties(
        5,
        true,
        &ConnackProperties {
            server_keep_alive_secs: Some(30),
            maximum_packet_size: Some(4096),
            topic_alias_maximum: Some(7),
            retain_available: true,
            wildcard_subscription_available: true,
            shared_subscription_available: true,
            subscription_identifiers_available: true,
            assigned_client_identifier: Some("client-123".to_string()),
            response_information: Some("reply/topic".to_string()),
            server_reference: Some("greenmqtt://node-a".to_string()),
            user_properties: vec![UserProperty {
                key: "tenant".to_string(),
                value: "demo".to_string(),
            }],
        },
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_CONNACK);

    let mut cursor = 1usize;
    let remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(remaining as usize, packet.len() - cursor);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 1);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(packet[cursor], 0x13);
    cursor += 1;
    assert_eq!(read_u16(&packet, &mut cursor).unwrap(), 30);
    assert_eq!(packet[cursor], 0x27);
    cursor += 1;
    assert_eq!(read_u32(&packet, &mut cursor).unwrap(), 4096);
    assert_eq!(packet[cursor], 0x22);
    cursor += 1;
    assert_eq!(read_u16(&packet, &mut cursor).unwrap(), 7);
    assert_eq!(packet[cursor], 0x25);
    cursor += 1;
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 1);
    assert_eq!(packet[cursor], 0x28);
    cursor += 1;
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 1);
    assert_eq!(packet[cursor], 0x29);
    cursor += 1;
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 1);
    assert_eq!(packet[cursor], 0x2A);
    cursor += 1;
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 1);
    assert_eq!(packet[cursor], 0x12);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "client-123");
    assert_eq!(packet[cursor], 0x1A);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "reply/topic");
    assert_eq!(packet[cursor], 0x1C);
    cursor += 1;
    assert_eq!(
        read_utf8(&packet, &mut cursor).unwrap(),
        "greenmqtt://node-a"
    );
    assert_eq!(packet[cursor], 0x26);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "tenant");
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "demo");
    assert_eq!(cursor, properties_end);
}

#[test]
fn parse_disconnect_rejects_duplicate_reason_string() {
    let body = [0x00, 0x08, 0x1F, 0x00, 0x01, b'a', 0x1F, 0x00, 0x01, b'b'];
    let error = parse_disconnect(&body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("duplicate disconnect reason string"));
}

#[test]
fn parse_disconnect_rejects_duplicate_session_expiry_interval() {
    let body = [
        0x00, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x01, 0x11, 0x00, 0x00, 0x00, 0x02,
    ];
    let error = parse_disconnect(&body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("duplicate disconnect session expiry interval"));
}

#[test]
fn parse_disconnect_rejects_invalid_reason_code() {
    let body = [0x03, 0x00];
    let error = parse_disconnect(&body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("invalid disconnect reason code"));
}

#[test]
fn parse_auth_rejects_duplicate_reason_string() {
    let body = [
        0x18, 0x16, 0x15, 0x00, 0x06, b'c', b'u', b's', b't', b'o', b'm', 0x1F, 0x00, 0x05, b'f',
        b'i', b'r', b's', b't', 0x1F, 0x00, 0x06, b's', b'e', b'c', b'o', b'n', b'd',
    ];
    let error = parse_auth(0, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("duplicate auth reason string"));
}

#[test]
fn parse_auth_rejects_invalid_reason_code() {
    let body = [0x02];
    let error = parse_auth(0, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("invalid auth reason code"));
}

#[test]
fn parse_publish_rejects_qos0_dup_flag() {
    let error = parse_publish(0b1000, &[0, 1, b'a'], 4).unwrap_err();
    assert!(format!("{error:#}").contains("invalid publish flags"));
}

#[test]
fn build_puback_packet_encodes_reason_string_for_mqtt5() {
    let packet = build_puback_packet(5, 7, Some(0x87), Some("publish denied"));
    assert_eq!(packet[0] >> 4, PACKET_TYPE_PUBACK);

    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(read_u16(&packet, &mut cursor).unwrap(), 7);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x87);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert!(cursor < properties_end);
    assert_eq!(packet[cursor], 0x1F);
    cursor += 1;
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "publish denied");
}

#[test]
fn build_puback_packet_encodes_reason_string_and_user_properties_for_mqtt5() {
    let packet = build_pub_response_packet(
        PACKET_TYPE_PUBACK << 4,
        5,
        7,
        Some(0x87),
        Mqtt5ResponseProperties::new(
            Some("publish denied"),
            &[UserProperty {
                key: "tenant".to_string(),
                value: "demo".to_string(),
            }],
        ),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_PUBACK);

    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(read_u16(&packet, &mut cursor).unwrap(), 7);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x87);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x1F);
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "publish denied");
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x26);
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "tenant");
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "demo");
    assert_eq!(cursor, properties_end);
}

#[test]
fn build_pubrel_packet_encodes_reason_string_for_mqtt5() {
    let packet = build_pubrel_packet_with_reason(
        5,
        11,
        Some(0x92),
        Mqtt5ResponseProperties::new(Some("release denied"), &[]),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_PUBREL);

    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(read_u16(&packet, &mut cursor).unwrap(), 11);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x92);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x1F);
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "release denied");
    assert_eq!(cursor, properties_end);
}

#[test]
fn build_pubcomp_packet_encodes_user_properties_for_mqtt5() {
    let packet = build_pubcomp_packet_with_reason(
        5,
        13,
        Some(0x00),
        Mqtt5ResponseProperties::new(
            None,
            &[UserProperty {
                key: "tenant".to_string(),
                value: "demo".to_string(),
            }],
        ),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_PUBCOMP);

    let mut cursor = 1usize;
    let _remaining = read_varint_from_frame(&packet, &mut cursor).unwrap();
    assert_eq!(read_u16(&packet, &mut cursor).unwrap(), 13);
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x00);
    let properties_end = property_section_end(&packet, &mut cursor).unwrap();
    assert_eq!(read_u8(&packet, &mut cursor).unwrap(), 0x26);
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "tenant");
    assert_eq!(read_utf8(&packet, &mut cursor).unwrap(), "demo");
    assert_eq!(cursor, properties_end);
}

#[test]
fn parse_puback_rejects_duplicate_reason_string() {
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    body.push(0x00);
    let properties = vec![
        0x1F, 0x00, 0x05, b'f', b'i', b'r', b's', b't', 0x1F, 0x00, 0x06, b's', b'e', b'c', b'o',
        b'n', b'd',
    ];
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let error = parse_puback(PACKET_TYPE_PUBACK, 0, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("duplicate puback/pubrec/pubcomp property"));
}

#[test]
fn parse_pubrel_rejects_duplicate_reason_string() {
    let mut body = Vec::new();
    body.extend_from_slice(&9u16.to_be_bytes());
    body.push(0x92);
    let properties = vec![
        0x1F, 0x00, 0x05, b'f', b'i', b'r', b's', b't', 0x1F, 0x00, 0x06, b's', b'e', b'c', b'o',
        b'n', b'd',
    ];
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let error = parse_pubrel(0b0010, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("duplicate pubrel property"));
}

#[test]
fn parse_puback_allows_user_properties_for_mqtt5() {
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    body.push(0x00);
    let properties = vec![
        0x1F, 0x00, 0x05, b'f', b'i', b'r', b's', b't', 0x26, 0x00, 0x06, b't', b'e', b'n', b'a',
        b'n', b't', 0x00, 0x04, b'd', b'e', b'm', b'o',
    ];
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);

    let packet_id = parse_puback(PACKET_TYPE_PUBACK, 0, &body, 5).unwrap();
    assert_eq!(packet_id, 7);
}

#[test]
fn parse_puback_rejects_invalid_reason_code() {
    let mut body = Vec::new();
    body.extend_from_slice(&7u16.to_be_bytes());
    body.push(0x92);
    encode_remaining_length(&mut body, 0);
    let error = parse_puback(PACKET_TYPE_PUBACK, 0, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("invalid puback/pubrec reason code"));
}

#[test]
fn parse_pubcomp_rejects_invalid_reason_code() {
    let mut body = Vec::new();
    body.extend_from_slice(&11u16.to_be_bytes());
    body.push(0x10);
    encode_remaining_length(&mut body, 0);
    let error = parse_puback(PACKET_TYPE_PUBCOMP, 0, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("invalid pubcomp reason code"));
}

#[test]
fn parse_pubrel_rejects_invalid_reason_code() {
    let mut body = Vec::new();
    body.extend_from_slice(&9u16.to_be_bytes());
    body.push(0x10);
    encode_remaining_length(&mut body, 0);
    let error = parse_pubrel(0b0010, &body, 5).unwrap_err();
    assert!(format!("{error:#}").contains("invalid pubrel reason code"));
}

#[test]
fn build_suback_packet_encodes_reason_string_for_mqtt5() {
    let packet = build_suback_packet(5, 7, &[0x00], Some("subscribe denied"));
    assert_eq!(packet[0] >> 4, PACKET_TYPE_SUBACK);
    let mut cursor = 1usize;
    let remaining = read_varint(&packet, &mut cursor);
    assert_eq!(remaining, packet.len() - cursor);
    let packet_id = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]);
    assert_eq!(packet_id, 7);
    cursor += 2;
    let properties_len = read_varint(&packet, &mut cursor);
    let properties_end = cursor + properties_len;
    assert_eq!(packet[cursor], 0x1F);
    cursor += 1;
    let reason_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    assert_eq!(&packet[cursor..cursor + reason_len], b"subscribe denied");
    cursor += reason_len;
    assert_eq!(cursor, properties_end);
    assert_eq!(packet[cursor], 0x00);
}

#[test]
fn build_unsuback_packet_encodes_empty_property_section_for_mqtt5() {
    let packet = build_unsuback_packet(5, 9, &[0x00, 0x11]);
    assert_eq!(packet[0] >> 4, PACKET_TYPE_UNSUBACK);
    let mut cursor = 1usize;
    let remaining = read_varint(&packet, &mut cursor);
    assert_eq!(remaining, packet.len() - cursor);
    let packet_id = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]);
    assert_eq!(packet_id, 9);
    cursor += 2;
    let properties_len = read_varint(&packet, &mut cursor);
    assert_eq!(properties_len, 0);
    assert_eq!(packet[cursor], 0x00);
    assert_eq!(packet[cursor + 1], 0x11);
}

#[test]
fn build_suback_packet_encodes_user_properties_for_mqtt5() {
    let packet = build_suback_packet_with_properties(
        5,
        11,
        &[0x00],
        Mqtt5ResponseProperties::new(
            Some("subscribe denied"),
            &[UserProperty {
                key: "tenant".to_string(),
                value: "demo".to_string(),
            }],
        ),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_SUBACK);
    let mut cursor = 1usize;
    let remaining = read_varint(&packet, &mut cursor);
    assert_eq!(remaining, packet.len() - cursor);
    let packet_id = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]);
    assert_eq!(packet_id, 11);
    cursor += 2;
    let properties_len = read_varint(&packet, &mut cursor);
    let properties_end = cursor + properties_len;
    assert_eq!(packet[cursor], 0x1F);
    cursor += 1;
    let reason_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    assert_eq!(&packet[cursor..cursor + reason_len], b"subscribe denied");
    cursor += reason_len;
    assert_eq!(packet[cursor], 0x26);
    cursor += 1;
    let key_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    assert_eq!(&packet[cursor..cursor + key_len], b"tenant");
    cursor += key_len;
    let value_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    assert_eq!(&packet[cursor..cursor + value_len], b"demo");
    cursor += value_len;
    assert_eq!(cursor, properties_end);
    assert_eq!(packet[cursor], 0x00);
}

#[test]
fn build_unsuback_packet_encodes_user_properties_for_mqtt5() {
    let packet = build_unsuback_packet_with_properties(
        5,
        13,
        &[0x00],
        Mqtt5ResponseProperties::new(
            None,
            &[UserProperty {
                key: "tenant".to_string(),
                value: "demo".to_string(),
            }],
        ),
    );
    assert_eq!(packet[0] >> 4, PACKET_TYPE_UNSUBACK);
    let mut cursor = 1usize;
    let remaining = read_varint(&packet, &mut cursor);
    assert_eq!(remaining, packet.len() - cursor);
    let packet_id = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]);
    assert_eq!(packet_id, 13);
    cursor += 2;
    let properties_len = read_varint(&packet, &mut cursor);
    let properties_end = cursor + properties_len;
    assert_eq!(packet[cursor], 0x26);
    cursor += 1;
    let key_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    assert_eq!(&packet[cursor..cursor + key_len], b"tenant");
    cursor += key_len;
    let value_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    assert_eq!(&packet[cursor..cursor + value_len], b"demo");
    cursor += value_len;
    assert_eq!(cursor, properties_end);
    assert_eq!(packet[cursor], 0x00);
}

#[test]
fn protocol_session_state_rejects_auth_before_connect() {
    let state = ProtocolSessionState::default();
    let error = state
        .prepare_session_reauth(&auth_packet(0x18, Some("static")))
        .expect_err("expected auth before connect to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "unexpected auth packet",
        }
    );
}

#[test]
fn protocol_session_state_rejects_success_reason_code_without_existing_method() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session".into(), identity(), None, 5, 7, true, None);
    let error = state
        .prepare_session_reauth(&auth_packet(0x00, Some("static")))
        .expect_err("expected success reason code to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "invalid auth reason code",
        }
    );
}

#[test]
fn protocol_session_state_uses_existing_method_when_packet_omits_method() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session".into(),
        identity(),
        Some("static".into()),
        5,
        7,
        true,
        None,
    );
    let request = state
        .prepare_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth request to reuse active method");
    assert_eq!(request.auth_method, "static");
    assert_eq!(request.identity.client_id, "client");
}

#[test]
fn protocol_session_state_rejects_mismatched_reauth_method() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session".into(),
        identity(),
        Some("static".into()),
        5,
        7,
        true,
        None,
    );
    let error = state
        .prepare_session_reauth(&auth_packet(0x18, Some("other")))
        .expect_err("expected mismatched method to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x8C,
            reason_string: "unsupported authentication method",
        }
    );
}

#[test]
fn protocol_session_state_updates_method_after_successful_reauth() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session".into(), identity(), None, 5, 7, true, None);
    let request = state
        .prepare_session_reauth(&auth_packet(0x19, Some("static")))
        .expect("expected re-auth request to accept method");
    assert_eq!(request.auth_method, "static");
    state.complete_session_reauth(request.auth_method);
    let follow_up = state
        .prepare_session_reauth(&auth_packet(0x18, None))
        .expect("expected follow-up auth to reuse method");
    assert_eq!(follow_up.auth_method, "static");
}

#[test]
fn protocol_session_state_rejects_second_connect_after_established() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session".into(), identity(), None, 5, 7, true, None);
    let error = state
        .prepare_packet(ProtocolPacketKind::Connect)
        .expect_err("expected duplicate connect to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "unexpected connect packet",
        }
    );
}

#[test]
fn protocol_session_state_rejects_business_packets_before_connect() {
    let state = ProtocolSessionState::default();
    let error = state
        .prepare_packet(ProtocolPacketKind::PingReq)
        .expect_err("expected packet before connect to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "pingreq before connect",
        }
    );
}

#[test]
fn protocol_session_state_exposes_established_session_id() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session-1".into(), identity(), None, 5, 7, true, None);
    assert_eq!(
        state.active_session_id().map(String::as_str),
        Some("session-1")
    );
    assert_eq!(
        state
            .require_established_session("subscribe before connect")
            .expect("expected session id"),
        "session-1"
    );
    assert_eq!(state.take_session_id(), Some("session-1".into()));
    assert_eq!(state.active_session_id(), None);
}

#[test]
fn protocol_session_state_maps_packet_kind_to_session_requirement() {
    let state = ProtocolSessionState::default();
    let error = state
        .session_id_for_packet(ProtocolPacketKind::Unsubscribe)
        .expect_err("expected unsubscribe before connect to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "unsubscribe before connect",
        }
    );
}

#[test]
fn protocol_session_state_clones_active_session_for_packet() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session-9".into(), identity(), None, 5, 7, true, None);
    let active = state
        .session_id_for_packet(ProtocolPacketKind::Publish)
        .expect("expected active session id");
    assert_eq!(active, "session-9");
}

#[test]
fn protocol_session_state_clones_active_session_id() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session-10".into(), identity(), None, 5, 7, true, None);
    assert_eq!(
        state.active_session_id_cloned().as_deref(),
        Some("session-10")
    );
}

#[test]
fn protocol_session_state_reports_active_session_presence() {
    let mut state = ProtocolSessionState::default();
    assert!(!state.has_active_session());
    state.on_connected(
        "session-14".into(),
        identity(),
        Some("static".into()),
        5,
        7,
        true,
        None,
    );
    assert!(state.has_active_session());
    state
        .start_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth to start");
    assert!(state.has_active_session());
}

#[test]
fn protocol_session_state_blocks_business_packets_during_reauth() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session-11".into(),
        identity(),
        Some("static".into()),
        5,
        7,
        true,
        None,
    );
    state
        .start_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth to start");
    let error = state
        .prepare_packet(ProtocolPacketKind::Publish)
        .expect_err("expected publish during reauth to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "packet during re-authentication",
        }
    );
    assert_eq!(
        state.active_session_id_cloned().as_deref(),
        Some("session-11")
    );
}

#[test]
fn protocol_session_state_starts_reauth_into_reauthenticating_phase() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session-13".into(), identity(), None, 5, 7, true, None);
    let request = state
        .start_session_reauth(&auth_packet(0x19, Some("static")))
        .expect("expected re-auth to start");
    assert_eq!(request.auth_method, "static");
    let error = state
        .prepare_packet(ProtocolPacketKind::Publish)
        .expect_err("expected publish during reauth to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "packet during re-authentication",
        }
    );
}

#[test]
fn protocol_session_state_completes_reauth_back_to_established() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session-12".into(),
        identity(),
        Some("static".into()),
        5,
        7,
        true,
        None,
    );
    state
        .start_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth to start");
    state.complete_session_reauth("static".into());
    assert_eq!(
        state
            .active_session_id_cloned()
            .expect("expected active session after reauth"),
        "session-12"
    );
    let active = state
        .session_id_for_packet(ProtocolPacketKind::Subscribe)
        .expect("expected session to be established again");
    assert_eq!(active, "session-12");
}

#[test]
fn protocol_session_state_exposes_protocol_context() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session-15".into(),
        identity(),
        Some("static".into()),
        5,
        19,
        false,
        Some(2048),
    );
    assert_eq!(state.protocol_level(), Some(5));
    assert_eq!(state.receive_maximum(), Some(19));
    assert_eq!(state.include_problem_information(), Some(false));
    assert_eq!(state.client_max_packet_size(), Some(2048));
}

#[test]
fn protocol_session_state_carries_session_lifecycle_context() {
    let mut state = ProtocolSessionState::default();
    state.on_connected_with_lifecycle(
        "session-21".into(),
        identity(),
        Some("static".into()),
        5,
        31,
        false,
        Some(8192),
        Some(120),
        Some(45),
        true,
    );
    assert_eq!(state.current_session_expiry_interval(), Some(120));
    assert_eq!(state.will_delay_interval_secs(), Some(45));
    assert_eq!(state.publish_will(), Some(true));
    assert_eq!(state.effective_session_expiry_interval(), Some(120));
    assert_eq!(state.effective_will_delay_interval_secs(), Some(45));
}

#[test]
fn protocol_session_state_disconnect_context_suppresses_will_and_preserves_expiry() {
    let mut state = ProtocolSessionState::default();
    state.on_connected_with_lifecycle(
        "session-22".into(),
        identity(),
        Some("static".into()),
        5,
        31,
        false,
        Some(8192),
        Some(120),
        Some(45),
        true,
    );
    state.begin_disconnect(Some(60));
    let context = state
        .disconnect_context()
        .expect("expected disconnect context");
    assert_eq!(context.session_id, "session-22");
    assert_eq!(context.current_session_expiry_interval, Some(120));
    assert_eq!(context.disconnect_session_expiry_interval, Some(60));
    assert_eq!(context.will_delay_interval_secs, Some(45));
    assert!(!context.publish_will);
    assert_eq!(state.effective_session_expiry_interval(), Some(60));
    assert_eq!(state.effective_will_delay_interval_secs(), None);
}

#[test]
fn protocol_session_state_disconnect_context_can_retain_will_when_requested() {
    let mut state = ProtocolSessionState::default();
    state.on_connected_with_lifecycle(
        "session-24".into(),
        identity(),
        Some("static".into()),
        5,
        31,
        false,
        Some(8192),
        Some(120),
        Some(45),
        true,
    );
    state.begin_disconnect_with_publish_will(Some(60), true);
    let context = state
        .disconnect_context()
        .expect("expected disconnect context");
    assert!(context.publish_will);
    assert_eq!(state.effective_session_expiry_interval(), Some(60));
    assert_eq!(state.effective_will_delay_interval_secs(), Some(45));
}

#[test]
fn protocol_session_state_reauth_preserves_lifecycle_context() {
    let mut state = ProtocolSessionState::default();
    state.on_connected_with_lifecycle(
        "session-23".into(),
        identity(),
        Some("static".into()),
        5,
        31,
        false,
        Some(8192),
        Some(120),
        Some(45),
        false,
    );
    state
        .start_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth to start");
    assert_eq!(state.current_session_expiry_interval(), Some(120));
    assert_eq!(state.will_delay_interval_secs(), Some(45));
    assert_eq!(state.publish_will(), Some(false));
    state.complete_session_reauth("static".into());
    assert_eq!(state.current_session_expiry_interval(), Some(120));
    assert_eq!(state.will_delay_interval_secs(), Some(45));
    assert_eq!(state.publish_will(), Some(false));
}

#[test]
fn protocol_session_state_enters_disconnecting_phase() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session-19".into(),
        identity(),
        Some("static".into()),
        5,
        21,
        false,
        Some(1024),
    );
    state.begin_disconnect(Some(900));
    assert!(state.is_disconnecting());
    assert_eq!(
        state.active_session_id().map(String::as_str),
        Some("session-19")
    );
    assert_eq!(state.protocol_level(), Some(5));
    assert_eq!(state.receive_maximum(), Some(21));
    assert_eq!(state.include_problem_information(), Some(false));
    assert_eq!(state.client_max_packet_size(), Some(1024));
    assert_eq!(state.disconnect_session_expiry_interval(), Some(900));
}

#[test]
fn protocol_session_state_blocks_business_packets_during_disconnecting() {
    let mut state = ProtocolSessionState::default();
    state.on_connected("session-20".into(), identity(), None, 5, 9, true, None);
    state.begin_disconnect(None);
    let error = state
        .prepare_packet(ProtocolPacketKind::Publish)
        .expect_err("expected publish during disconnecting to be rejected");
    assert_eq!(
        error,
        ProtocolStateError {
            reason_code: 0x82,
            reason_string: "packet during disconnecting",
        }
    );
}

#[test]
fn protocol_session_state_tracks_topic_aliases_across_reauth() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session-17".into(),
        identity(),
        Some("static".into()),
        5,
        8,
        true,
        None,
    );
    assert_eq!(
        state
            .resolve_publish_topic(Some(11), "sensors/temp")
            .expect("expected alias to be stored"),
        "sensors/temp"
    );
    state
        .start_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth to start");
    assert_eq!(
        state
            .resolve_publish_topic(Some(11), "")
            .expect("expected alias to survive reauth"),
        "sensors/temp"
    );
    state.complete_session_reauth("static".into());
    assert_eq!(
        state
            .resolve_publish_topic(Some(11), "")
            .expect("expected alias to survive completed reauth"),
        "sensors/temp"
    );
    state.on_connected("session-18".into(), identity(), None, 5, 8, true, None);
    let error = state
        .resolve_publish_topic(Some(11), "")
        .expect_err("expected new connect to reset aliases");
    assert_eq!(error.to_string(), "unknown topic alias 11");
}

#[test]
fn protocol_session_state_preserves_protocol_context_during_reauth() {
    let mut state = ProtocolSessionState::default();
    state.on_connected(
        "session-16".into(),
        identity(),
        Some("static".into()),
        5,
        23,
        false,
        Some(4096),
    );
    state
        .start_session_reauth(&auth_packet(0x18, None))
        .expect("expected re-auth to start");
    assert_eq!(state.protocol_level(), Some(5));
    assert_eq!(state.receive_maximum(), Some(23));
    assert_eq!(state.include_problem_information(), Some(false));
    assert_eq!(state.client_max_packet_size(), Some(4096));
    state.complete_session_reauth("static".into());
    assert_eq!(state.protocol_level(), Some(5));
    assert_eq!(state.receive_maximum(), Some(23));
    assert_eq!(state.include_problem_information(), Some(false));
    assert_eq!(state.client_max_packet_size(), Some(4096));
}
