use async_trait::async_trait;
use greenmqtt_core::RangeId;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::{LogIndex, RaftLogEntry, RaftSnapshot, StoredRaftHardState, StoredRaftProgressState, StoredRaftState};

pub(crate) const HARD_STATE_PREFIX: &[u8] = b"hard\0";
pub(crate) const PROGRESS_STATE_PREFIX: &[u8] = b"progress\0";
pub(crate) const SNAPSHOT_META_PREFIX: &[u8] = b"snapshot\0";
pub(crate) const LOG_ENTRY_PREFIX: &[u8] = b"log\0";

#[async_trait]
pub trait RaftStateStore: Send + Sync {
    async fn load(&self, range_id: &str) -> anyhow::Result<Option<StoredRaftState>>;
    async fn save(&self, range_id: &str, state: StoredRaftState) -> anyhow::Result<()>;
    async fn delete(&self, range_id: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct MemoryRaftStateStore {
    inner: Arc<Mutex<HashMap<RangeId, StoredRaftState>>>,
}

#[derive(Clone)]
pub struct RocksDbRaftStateStore {
    pub(crate) db: Arc<DB>,
}

const STORED_STATE_PREFIX: &[u8] = b"state\0";

pub(crate) fn stored_state_key(range_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(STORED_STATE_PREFIX.len() + range_id.len());
    key.extend_from_slice(STORED_STATE_PREFIX);
    key.extend_from_slice(range_id.as_bytes());
    key
}

pub(crate) fn scoped_state_key(prefix: &[u8], range_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + range_id.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(range_id.as_bytes());
    key
}

pub(crate) fn scoped_log_prefix(range_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(LOG_ENTRY_PREFIX.len() + range_id.len() + 1);
    key.extend_from_slice(LOG_ENTRY_PREFIX);
    key.extend_from_slice(range_id.as_bytes());
    key.push(0);
    key
}

pub(crate) fn scoped_log_entry_key(range_id: &str, index: LogIndex) -> Vec<u8> {
    let mut key = scoped_log_prefix(range_id);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

#[async_trait]
impl RaftStateStore for MemoryRaftStateStore {
    async fn load(&self, range_id: &str) -> anyhow::Result<Option<StoredRaftState>> {
        Ok(self
            .inner
            .lock()
            .expect("raft state store poisoned")
            .get(range_id)
            .cloned())
    }

    async fn save(&self, range_id: &str, state: StoredRaftState) -> anyhow::Result<()> {
        self.inner
            .lock()
            .expect("raft state store poisoned")
            .insert(range_id.to_string(), state);
        Ok(())
    }

    async fn delete(&self, range_id: &str) -> anyhow::Result<()> {
        self.inner
            .lock()
            .expect("raft state store poisoned")
            .remove(range_id);
        Ok(())
    }
}

impl RocksDbRaftStateStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        Ok(Self {
            db: Arc::new(DB::open(&options, path)?),
        })
    }
}

#[async_trait]
impl RaftStateStore for RocksDbRaftStateStore {
    async fn load(&self, range_id: &str) -> anyhow::Result<Option<StoredRaftState>> {
        if let Some(bytes) = self.db.get(stored_state_key(range_id))? {
            return Ok(Some(bincode::deserialize(&bytes)?));
        }

        let Some(hard_state_bytes) = self.db.get(scoped_state_key(HARD_STATE_PREFIX, range_id))?
        else {
            return Ok(None);
        };
        let hard_state: StoredRaftHardState = bincode::deserialize(&hard_state_bytes)?;
        let progress_state = self
            .db
            .get(scoped_state_key(PROGRESS_STATE_PREFIX, range_id))?
            .map(|bytes| bincode::deserialize::<StoredRaftProgressState>(&bytes))
            .transpose()?
            .unwrap_or_default();
        let latest_snapshot = self
            .db
            .get(scoped_state_key(SNAPSHOT_META_PREFIX, range_id))?
            .map(|bytes| bincode::deserialize::<RaftSnapshot>(&bytes))
            .transpose()?;
        let log_prefix = scoped_log_prefix(range_id);
        let mut log = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&log_prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&log_prefix) {
                break;
            }
            log.push(bincode::deserialize::<RaftLogEntry>(&value)?);
        }
        Ok(Some(StoredRaftState {
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for,
            commit_index: progress_state.commit_index,
            applied_index: progress_state.applied_index,
            leader_node_id: progress_state.leader_node_id,
            cluster_config: hard_state.cluster_config,
            config_transition: hard_state.config_transition,
            log,
            latest_snapshot,
        }))
    }

    async fn save(&self, range_id: &str, state: StoredRaftState) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(
            scoped_state_key(HARD_STATE_PREFIX, range_id),
            bincode::serialize(&StoredRaftHardState {
                current_term: state.current_term,
                voted_for: state.voted_for,
                cluster_config: state.cluster_config.clone(),
                config_transition: state.config_transition.clone(),
            })?,
        );
        batch.put(
            scoped_state_key(PROGRESS_STATE_PREFIX, range_id),
            bincode::serialize(&StoredRaftProgressState {
                commit_index: state.commit_index,
                applied_index: state.applied_index,
                leader_node_id: state.leader_node_id,
            })?,
        );
        let snapshot_key = scoped_state_key(SNAPSHOT_META_PREFIX, range_id);
        if let Some(snapshot) = &state.latest_snapshot {
            batch.put(snapshot_key, bincode::serialize(snapshot)?);
        } else {
            batch.delete(snapshot_key);
        }
        let log_prefix = scoped_log_prefix(range_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&log_prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&log_prefix) {
                break;
            }
            batch.delete(key);
        }
        for entry in &state.log {
            batch.put(
                scoped_log_entry_key(range_id, entry.index),
                bincode::serialize(entry)?,
            );
        }
        batch.delete(stored_state_key(range_id));
        self.db.write(batch)?;
        Ok(())
    }

    async fn delete(&self, range_id: &str) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete(stored_state_key(range_id));
        batch.delete(scoped_state_key(HARD_STATE_PREFIX, range_id));
        batch.delete(scoped_state_key(PROGRESS_STATE_PREFIX, range_id));
        batch.delete(scoped_state_key(SNAPSHOT_META_PREFIX, range_id));
        let log_prefix = scoped_log_prefix(range_id);
        for item in self
            .db
            .iterator(IteratorMode::From(&log_prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&log_prefix) {
                break;
            }
            batch.delete(key);
        }
        self.db.write(batch)?;
        Ok(())
    }
}
