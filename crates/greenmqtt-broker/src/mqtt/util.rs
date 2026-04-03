use std::time::Duration;
use std::time::Instant;

use greenmqtt_core::PublishProperties;

pub(crate) fn next_packet_id(current: u16) -> u16 {
    if current == u16::MAX {
        1
    } else {
        current + 1
    }
}

pub(crate) fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock drift before unix epoch")
        .as_millis() as u64
}

pub(crate) fn packet_exceeds_limit(packet_size: usize, max_packet_size: Option<u32>) -> bool {
    max_packet_size
        .and_then(|limit| usize::try_from(limit).ok())
        .is_some_and(|limit| packet_size > limit)
}

pub(crate) fn negotiated_keep_alive(
    client_keep_alive_secs: u16,
    server_keep_alive_secs: Option<u16>,
) -> Option<Duration> {
    match server_keep_alive_secs {
        Some(server_keep_alive_secs) => {
            Some(Duration::from_secs(u64::from(server_keep_alive_secs)))
        }
        None if client_keep_alive_secs == 0 => None,
        None => Some(Duration::from_secs(u64::from(client_keep_alive_secs))),
    }
}

pub(crate) fn keep_alive_timed_out(last_activity: Instant, keep_alive: Option<Duration>) -> bool {
    let Some(keep_alive) = keep_alive else {
        return false;
    };
    let grace = keep_alive
        .checked_add(keep_alive / 2)
        .unwrap_or(Duration::from_secs(u64::MAX));
    last_activity.elapsed() > grace
}

pub(crate) fn split_shared_subscription(topic_filter: &str) -> (Option<String>, String) {
    let mut parts = topic_filter.splitn(4, '/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("$share"), Some(group), Some(first), Some(rest)) if !group.is_empty() => {
            (Some(group.to_string()), format!("{first}/{rest}"))
        }
        _ => (None, topic_filter.to_string()),
    }
}

pub(crate) fn subscribe_error_return_code(protocol_level: u8, error: &anyhow::Error) -> Option<u8> {
    let message = format!("{error:#}");
    if message.contains("subscribe denied") {
        return Some(if protocol_level == 5 { 0x87 } else { 0x80 });
    }
    None
}

pub(crate) fn publish_error_reason_code(protocol_level: u8, error: &anyhow::Error) -> Option<u8> {
    let message = format!("{error:#}");
    if protocol_level == 5 && message.contains("tenant quota exceeded") {
        return Some(0x97);
    }
    if protocol_level == 5 && message.contains("payload format invalid") {
        return Some(0x99);
    }
    if protocol_level == 5 && message.contains("publish rate exceeded") {
        return Some(0x97);
    }
    if protocol_level == 5 && message.contains("publish denied") {
        return Some(0x87);
    }
    None
}

pub(crate) fn publish_error_reason_string(error: &anyhow::Error) -> Option<&'static str> {
    let message = format!("{error:#}");
    if message.contains("tenant quota exceeded") {
        return Some("quota exceeded");
    }
    if message.contains("payload format invalid") {
        return Some("payload format invalid");
    }
    if message.contains("publish rate exceeded") {
        return Some("quota exceeded");
    }
    if message.contains("publish denied") {
        return Some("publish denied");
    }
    None
}

pub(crate) fn validate_utf8_payload_format(
    properties: &PublishProperties,
    payload: &[u8],
) -> anyhow::Result<()> {
    if properties.payload_format_indicator == Some(1) {
        std::str::from_utf8(payload).map_err(|_| anyhow::anyhow!("payload format invalid"))?;
    }
    Ok(())
}
