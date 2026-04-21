use greenmqtt_core::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetryDirective {
    RetryWithLeader(NodeId),
    RefreshRouteCache,
    FailFast,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvRangeErrorKind {
    NotLeader(Option<NodeId>),
    EpochMismatch,
    RangeNotFound,
    ConfigChanging,
    SnapshotInProgress,
    TransportUnavailable,
    RetryExhausted,
    Other,
}

pub fn classify_kv_error(error: &anyhow::Error) -> KvRangeErrorKind {
    let message = error.to_string();
    if let Some(rest) = message.strip_prefix("kv/not-leader") {
        let leader = rest
            .split("leader=")
            .nth(1)
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse().ok());
        return KvRangeErrorKind::NotLeader(leader);
    }
    if message.starts_with("kv/epoch-mismatch") {
        return KvRangeErrorKind::EpochMismatch;
    }
    if message.starts_with("kv/range-not-found") {
        return KvRangeErrorKind::RangeNotFound;
    }
    if message.starts_with("kv/config-changing") {
        return KvRangeErrorKind::ConfigChanging;
    }
    if message.starts_with("kv/snapshot-in-progress") {
        return KvRangeErrorKind::SnapshotInProgress;
    }
    if message.contains("transport error")
        || message.contains("Connection refused")
        || message.contains("error trying to connect")
        || message.contains("dns error")
        || message.contains("connection closed")
    {
        return KvRangeErrorKind::TransportUnavailable;
    }
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        return match status.code() {
            tonic::Code::Unavailable
            | tonic::Code::ResourceExhausted
            | tonic::Code::DeadlineExceeded => KvRangeErrorKind::TransportUnavailable,
            tonic::Code::NotFound => KvRangeErrorKind::RangeNotFound,
            tonic::Code::FailedPrecondition => KvRangeErrorKind::ConfigChanging,
            _ => KvRangeErrorKind::Other,
        };
    }
    KvRangeErrorKind::Other
}

pub fn should_refresh_route(kind: &KvRangeErrorKind) -> bool {
    matches!(
        kind,
        KvRangeErrorKind::NotLeader(None)
            | KvRangeErrorKind::EpochMismatch
            | KvRangeErrorKind::RangeNotFound
    )
}
