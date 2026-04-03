use super::codec::build_packet;
use super::publish::{
    build_puback_packet, build_pubcomp_packet, build_pubrec_packet, build_pubrel_packet,
};
use super::session::SessionTransport;
use super::subscribe::{build_suback_packet, build_unsuback_packet};
use super::PACKET_TYPE_PINGRESP;

pub(crate) async fn write_suback<T>(
    stream: &mut T,
    protocol_level: u8,
    packet_id: u16,
    return_codes: &[u8],
    reason_string: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream
        .write_bytes(&build_suback_packet(
            protocol_level,
            packet_id,
            return_codes,
            reason_string,
        ))
        .await
}

pub(crate) async fn write_unsuback<T>(
    stream: &mut T,
    protocol_level: u8,
    packet_id: u16,
    return_codes: &[u8],
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream
        .write_bytes(&build_unsuback_packet(
            protocol_level,
            packet_id,
            return_codes,
        ))
        .await
}

pub(crate) async fn write_puback<T>(
    stream: &mut T,
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    reason_string: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream
        .write_bytes(&build_puback_packet(
            protocol_level,
            packet_id,
            reason_code,
            reason_string,
        ))
        .await
}

pub(crate) async fn write_pubrec<T>(
    stream: &mut T,
    protocol_level: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    reason_string: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream
        .write_bytes(&build_pubrec_packet(
            protocol_level,
            packet_id,
            reason_code,
            reason_string,
        ))
        .await
}

pub(crate) async fn write_pubcomp<T>(
    stream: &mut T,
    protocol_level: u8,
    packet_id: u16,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream
        .write_bytes(&build_pubcomp_packet(protocol_level, packet_id))
        .await
}

pub(crate) async fn write_pubrel<T>(
    stream: &mut T,
    protocol_level: u8,
    packet_id: u16,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream
        .write_bytes(&build_pubrel_packet(protocol_level, packet_id))
        .await
}

pub(crate) async fn write_pingresp<T>(stream: &mut T) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    stream.write_bytes(&build_pingresp_packet()).await
}

pub(crate) fn build_pingresp_packet() -> Vec<u8> {
    build_packet(PACKET_TYPE_PINGRESP << 4, &[])
}
