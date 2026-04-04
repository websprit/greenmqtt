use super::{
    connect::{AuthPacket, ConnectPacket, DisconnectPacket},
    parse_packet_parts,
    publish::PublishPacket,
    subscribe::{SubscribePacket, UnsubscribePacket},
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(super) const PACKET_TYPE_CONNECT: u8 = 1;
pub(super) const PACKET_TYPE_CONNACK: u8 = 2;
pub(super) const PACKET_TYPE_PUBLISH: u8 = 3;
pub(super) const PACKET_TYPE_PUBACK: u8 = 4;
pub(super) const PACKET_TYPE_PUBREC: u8 = 5;
pub(super) const PACKET_TYPE_PUBREL: u8 = 6;
pub(super) const PACKET_TYPE_PUBCOMP: u8 = 7;
pub(super) const PACKET_TYPE_SUBSCRIBE: u8 = 8;
pub(super) const PACKET_TYPE_SUBACK: u8 = 9;
pub(super) const PACKET_TYPE_UNSUBSCRIBE: u8 = 10;
pub(super) const PACKET_TYPE_UNSUBACK: u8 = 11;
pub(super) const PACKET_TYPE_PINGREQ: u8 = 12;
pub(super) const PACKET_TYPE_PINGRESP: u8 = 13;
pub(super) const PACKET_TYPE_DISCONNECT: u8 = 14;
pub(super) const PACKET_TYPE_AUTH: u8 = 15;

#[derive(Debug)]
pub(crate) enum Packet {
    Connect(ConnectPacket),
    Subscribe(SubscribePacket),
    Unsubscribe(UnsubscribePacket),
    Publish(PublishPacket),
    PubAck(u16),
    PubRec(u16),
    PubRel(u16),
    PingReq,
    PubComp(u16),
    Disconnect(DisconnectPacket),
    Auth(AuthPacket),
}

#[allow(dead_code)]
pub(super) async fn read_packet<S>(
    stream: &mut S,
    protocol_level: u8,
    max_packet_size: Option<u32>,
) -> anyhow::Result<Packet>
where
    S: AsyncRead + Unpin,
{
    let header = stream.read_u8().await?;
    let packet_type = header >> 4;
    let flags = header & 0x0f;
    let (remaining_len, len_bytes) = read_remaining_length(stream).await?;
    if super::packet_exceeds_limit(1 + len_bytes + remaining_len, max_packet_size) {
        anyhow::bail!("packet exceeds configured maximum packet size");
    }
    let mut body = vec![0u8; remaining_len];
    stream.read_exact(&mut body).await?;
    parse_packet_parts(packet_type, flags, &body, protocol_level)
}

pub(super) async fn read_packet_with_size<S>(
    stream: &mut S,
    protocol_level: u8,
    max_packet_size: Option<u32>,
) -> anyhow::Result<(Packet, usize)>
where
    S: AsyncRead + Unpin,
{
    let header = stream.read_u8().await?;
    let packet_type = header >> 4;
    let flags = header & 0x0f;
    let (remaining_len, len_bytes) = read_remaining_length(stream).await?;
    let packet_size = 1 + len_bytes + remaining_len;
    if super::packet_exceeds_limit(packet_size, max_packet_size) {
        anyhow::bail!("packet exceeds configured maximum packet size");
    }
    let mut body = vec![0u8; remaining_len];
    stream.read_exact(&mut body).await?;
    Ok((
        parse_packet_parts(packet_type, flags, &body, protocol_level)?,
        packet_size,
    ))
}

pub(super) fn parse_packet_frame(frame: &[u8], protocol_level: u8) -> anyhow::Result<Packet> {
    if frame.is_empty() {
        anyhow::bail!("empty frame");
    }
    let header = frame[0];
    let packet_type = header >> 4;
    let flags = header & 0x0f;
    let mut cursor = 1usize;
    let remaining_len = read_varint_from_frame(frame, &mut cursor)? as usize;
    if cursor + remaining_len != frame.len() {
        anyhow::bail!("malformed frame length");
    }
    parse_packet_parts(packet_type, flags, &frame[cursor..], protocol_level)
}

pub(super) fn build_packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![header];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(body);
    packet
}

pub(super) fn encode_remaining_length(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut encoded = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            encoded |= 0x80;
        }
        out.push(encoded);
        if len == 0 {
            break;
        }
    }
}

async fn read_remaining_length<S>(stream: &mut S) -> anyhow::Result<(usize, usize)>
where
    S: AsyncRead + Unpin,
{
    let mut multiplier = 1usize;
    let mut value = 0usize;
    let mut bytes = 0usize;
    loop {
        let encoded = stream.read_u8().await?;
        bytes += 1;
        value += ((encoded & 127) as usize) * multiplier;
        if encoded & 128 == 0 {
            return Ok((value, bytes));
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            anyhow::bail!("malformed varint");
        }
    }
}

pub(super) fn read_varint_from_slice(body: &[u8], cursor: &mut usize) -> anyhow::Result<u32> {
    let mut multiplier = 1u32;
    let mut value = 0u32;
    loop {
        if *cursor >= body.len() {
            anyhow::bail!("buffer underflow");
        }
        let encoded = body[*cursor];
        *cursor += 1;
        value += ((encoded & 127) as u32) * multiplier;
        if encoded & 128 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            anyhow::bail!("malformed varint");
        }
    }
}

pub(super) fn read_varint_from_frame(frame: &[u8], cursor: &mut usize) -> anyhow::Result<u32> {
    let mut multiplier = 1u32;
    let mut value = 0u32;
    loop {
        if *cursor >= frame.len() {
            anyhow::bail!("buffer underflow");
        }
        let encoded = frame[*cursor];
        *cursor += 1;
        value += ((encoded & 127) as u32) * multiplier;
        if encoded & 128 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            anyhow::bail!("malformed varint");
        }
    }
}
