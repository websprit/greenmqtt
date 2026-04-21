use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadConsistency {
    LocalMaybeStale,
    #[default]
    LeaderReadIndex,
    LeaderWriteAck,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeReadOptions {
    pub consistency: ReadConsistency,
    pub expected_epoch: Option<u64>,
}
