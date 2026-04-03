mod auth;
mod codec;
mod connect;
mod delivery;
mod error;
mod parser;
mod publish;
mod response;
pub mod session;
mod state;
mod subscribe;
pub mod transport;
mod util;
mod writer;

use crate::BrokerRuntime;
use codec::{
    build_packet, encode_remaining_length, read_varint_from_slice, Packet, PACKET_TYPE_AUTH,
    PACKET_TYPE_CONNACK, PACKET_TYPE_CONNECT, PACKET_TYPE_DISCONNECT, PACKET_TYPE_PINGREQ,
    PACKET_TYPE_PINGRESP, PACKET_TYPE_PUBACK, PACKET_TYPE_PUBCOMP, PACKET_TYPE_PUBLISH,
    PACKET_TYPE_PUBREC, PACKET_TYPE_PUBREL, PACKET_TYPE_SUBSCRIBE, PACKET_TYPE_UNSUBSCRIBE,
};
use connect::{parse_auth, parse_connect, parse_disconnect};
use greenmqtt_core::{InflightMessage, PublishRequest};
use parser::{
    delivery_properties_with_subscription_identifier, parse_will_properties, property_section_end,
    read_binary, read_u16, read_u32, read_u8, read_utf8, skip_property_value, write_utf8,
};
use publish::{build_pubrel_packet, parse_puback, parse_publish, parse_pubrel};
use subscribe::{parse_subscribe, parse_unsubscribe};
pub use transport::{serve_quic, serve_tcp, serve_tls, serve_ws, serve_wss};
use util::packet_exceeds_limit;

fn parse_packet_parts(
    packet_type: u8,
    flags: u8,
    body: &[u8],
    protocol_level: u8,
) -> anyhow::Result<Packet> {
    match packet_type {
        PACKET_TYPE_CONNECT => {
            anyhow::ensure!(flags == 0, "invalid connect fixed header flags");
            parse_connect(body).map(Packet::Connect)
        }
        PACKET_TYPE_SUBSCRIBE => {
            parse_subscribe(flags, body, protocol_level).map(Packet::Subscribe)
        }
        PACKET_TYPE_UNSUBSCRIBE => {
            parse_unsubscribe(flags, body, protocol_level).map(Packet::Unsubscribe)
        }
        PACKET_TYPE_PUBLISH => parse_publish(flags, body, protocol_level).map(Packet::Publish),
        PACKET_TYPE_PUBACK => {
            parse_puback(PACKET_TYPE_PUBACK, flags, body, protocol_level).map(Packet::PubAck)
        }
        PACKET_TYPE_PUBREC => {
            parse_puback(PACKET_TYPE_PUBREC, flags, body, protocol_level).map(Packet::PubRec)
        }
        PACKET_TYPE_PUBREL => parse_pubrel(flags, body, protocol_level).map(Packet::PubRel),
        PACKET_TYPE_PUBCOMP => {
            parse_puback(PACKET_TYPE_PUBCOMP, flags, body, protocol_level).map(Packet::PubComp)
        }
        PACKET_TYPE_PINGREQ => {
            anyhow::ensure!(flags == 0, "invalid pingreq flags");
            Ok(Packet::PingReq)
        }
        PACKET_TYPE_DISCONNECT => {
            anyhow::ensure!(flags == 0, "invalid disconnect flags");
            parse_disconnect(body, protocol_level).map(Packet::Disconnect)
        }
        PACKET_TYPE_AUTH => parse_auth(flags, body, protocol_level).map(Packet::Auth),
        other => anyhow::bail!("unsupported packet type {other}"),
    }
}

fn protocol_error_disconnect_reason_string(error: &anyhow::Error) -> Option<&'static str> {
    let message = error.root_cause().to_string();
    if message.contains("malformed varint") || message.contains("malformed frame length") {
        Some("malformed remaining length")
    } else if message.contains("invalid connect fixed header flags") {
        Some("invalid connect fixed header flags")
    } else if message.contains("invalid pingreq flags") {
        Some("invalid pingreq flags")
    } else if message.contains("invalid disconnect flags") {
        Some("invalid disconnect flags")
    } else if message.contains("unexpected pubrec packet id") {
        Some("unexpected pubrec packet id")
    } else if message.contains("unexpected pubrel packet id") {
        Some("unexpected pubrel packet id")
    } else if message.contains("unexpected pubcomp packet id") {
        Some("unexpected pubcomp packet id")
    } else if message.contains("invalid subscribe flags") {
        Some("invalid subscribe flags")
    } else if message.contains("empty subscribe payload") {
        Some("empty subscribe payload")
    } else if message.contains("invalid subscribe property") {
        Some("invalid subscribe property")
    } else if message.contains("invalid retain handling")
        || message.contains("invalid subscribe options")
    {
        Some("invalid subscribe options")
    } else if message.contains("duplicate subscription identifier") {
        Some("duplicate subscription identifier")
    } else if message.contains("invalid topic filter") {
        Some("invalid topic filter")
    } else if message.contains("invalid unsubscribe flags") {
        Some("invalid unsubscribe flags")
    } else if message.contains("empty unsubscribe payload") {
        Some("empty unsubscribe payload")
    } else if message.contains("invalid unsubscribe property") {
        Some("invalid unsubscribe property")
    } else if message.contains("invalid subscription identifier") {
        Some("invalid subscription identifier")
    } else if message.contains("invalid subscription qos") {
        Some("invalid subscription qos")
    } else if message.contains("invalid pubrel flags") {
        Some("invalid pubrel flags")
    } else if message.contains("duplicate pubrel property") {
        Some("duplicate pubrel property")
    } else if message.contains("invalid pubrel reason code") {
        Some("invalid pubrel reason code")
    } else if message.contains("invalid pubrel property") {
        Some("invalid pubrel property")
    } else if message.contains("invalid puback/pubrec/pubcomp flags") {
        Some("invalid puback/pubrec/pubcomp flags")
    } else if message.contains("invalid puback/pubrec reason code") {
        Some("invalid puback/pubrec reason code")
    } else if message.contains("invalid pubcomp reason code") {
        Some("invalid pubcomp reason code")
    } else if message.contains("duplicate puback/pubrec/pubcomp property") {
        Some("duplicate puback/pubrec/pubcomp property")
    } else if message.contains("invalid puback/pubrec/pubcomp property") {
        Some("invalid puback/pubrec/pubcomp property")
    } else if message.contains("invalid publish flags") {
        Some("invalid publish flags")
    } else if message.contains("invalid publish qos") {
        Some("invalid publish qos")
    } else if message.contains("duplicate disconnect session expiry interval") {
        Some("duplicate disconnect session expiry interval")
    } else if message.contains("duplicate disconnect reason string") {
        Some("duplicate disconnect reason string")
    } else if message.contains("invalid disconnect reason code") {
        Some("invalid disconnect reason code")
    } else if message.contains("invalid disconnect property") {
        Some("invalid disconnect property")
    } else if message.contains("invalid payload format indicator") {
        Some("invalid payload format indicator")
    } else if message.contains("duplicate payload format indicator") {
        Some("duplicate payload format indicator")
    } else if message.contains("duplicate content type") {
        Some("duplicate content type")
    } else if message.contains("duplicate message expiry interval") {
        Some("duplicate message expiry interval")
    } else if message.contains("duplicate topic alias") {
        Some("duplicate topic alias")
    } else if message.contains("duplicate response topic") {
        Some("duplicate response topic")
    } else if message.contains("duplicate correlation data") {
        Some("duplicate correlation data")
    } else if message.contains("duplicate auth method") {
        Some("duplicate auth method")
    } else if message.contains("duplicate auth data") {
        Some("duplicate auth data")
    } else if message.contains("duplicate auth reason string") {
        Some("duplicate auth reason string")
    } else if message.contains("invalid auth property") {
        Some("invalid auth property")
    } else if message.contains("auth data without auth method") {
        Some("auth data without auth method")
    } else if message.contains("invalid auth flags") {
        Some("invalid auth flags")
    } else if message.contains("invalid auth reason code") {
        Some("invalid auth reason code")
    } else if message.contains("subscription identifier not allowed on publish") {
        Some("subscription identifier not allowed on publish")
    } else {
        None
    }
}

#[cfg(feature = "fuzzing")]
pub fn fuzz_parse_packet_frame(frame: &[u8]) {
    let _ = codec::parse_packet_frame(frame, 4);
    let _ = codec::parse_packet_frame(frame, 5);
}

#[cfg(test)]
mod tests;
