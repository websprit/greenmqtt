use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{RangeBoundary, RangeId};
use rocksdb::{checkpoint::Checkpoint, Direction, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvMutation {
    pub key: Bytes,
    #[serde(default)]
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvRangeBootstrap {
    pub range_id: RangeId,
    pub boundary: RangeBoundary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvRangeCheckpoint {
    pub range_id: RangeId,
    pub checkpoint_id: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvRangeSnapshot {
    pub range_id: RangeId,
    pub boundary: RangeBoundary,
    pub term: u64,
    pub index: u64,
    pub checksum: u64,
    pub layout_version: u32,
    pub data_path: String,
}

#[async_trait]
pub trait KvRangeReader: Send + Sync {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>>;
    async fn scan(
        &self,
        boundary: &RangeBoundary,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>>;
}

#[async_trait]
pub trait KvRangeWriter: Send + Sync {
    async fn apply(&self, mutations: Vec<KvMutation>) -> anyhow::Result<()>;
    async fn compact(&self, boundary: Option<RangeBoundary>) -> anyhow::Result<()>;
}

#[async_trait]
pub trait KvRangeSpace: Send + Sync {
    fn range_id(&self) -> &RangeId;
    fn boundary(&self) -> &RangeBoundary;
    fn reader(&self) -> &dyn KvRangeReader;
    fn writer(&self) -> &dyn KvRangeWriter;

    async fn checkpoint(&self, checkpoint_id: &str) -> anyhow::Result<KvRangeCheckpoint>;
    async fn snapshot(&self) -> anyhow::Result<KvRangeSnapshot>;
    async fn destroy(&self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait KvWalStore: Send + Sync {
    async fn append(&self, entry: Bytes) -> anyhow::Result<u64>;
    async fn read_from(&self, offset: u64, limit: usize) -> anyhow::Result<Vec<(u64, Bytes)>>;
    async fn truncate_prefix(&self, offset: u64) -> anyhow::Result<()>;
}

#[async_trait]
pub trait KvEngine: Send + Sync {
    async fn bootstrap(&self, bootstrap: KvRangeBootstrap) -> anyhow::Result<()>;
    async fn open_range(&self, range_id: &str) -> anyhow::Result<Box<dyn KvRangeSpace>>;
    async fn destroy_range(&self, range_id: &str) -> anyhow::Result<()>;
    async fn list_ranges(&self) -> anyhow::Result<Vec<RangeId>>;
    async fn restore_snapshot(
        &self,
        bootstrap: KvRangeBootstrap,
        snapshot: &KvRangeSnapshot,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedRangeMeta {
    range_id: RangeId,
    boundary: RangeBoundary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SnapshotManifest {
    range_id: RangeId,
    term: u64,
    index: u64,
    boundary: RangeBoundary,
    checksum: u64,
    layout_version: u32,
}

#[derive(Debug, Clone)]
pub struct RocksDbKvEngine {
    root: Arc<PathBuf>,
}

#[derive(Debug)]
pub struct RocksDbKvRangeSpace {
    range_id: RangeId,
    boundary: RangeBoundary,
    range_dir: PathBuf,
    db: DB,
}

#[derive(Debug, Clone)]
pub struct RocksDbWalStore {
    db: Arc<DB>,
}

const WAL_NEXT_OFFSET_KEY: &[u8] = b"__wal_next_offset";
const WAL_ENTRY_PREFIX: &[u8] = b"entry:";

fn open_rocksdb(path: impl AsRef<Path>) -> anyhow::Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.increase_parallelism(
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(2)
            .min(i32::MAX as usize) as i32,
    );
    options.set_max_background_jobs(4);
    options.set_bytes_per_sync(1 << 20);
    Ok(DB::open(&options, path)?)
}

fn sanitized_range_dir(range_id: &str) -> String {
    range_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

fn range_root(root: &Path, range_id: &str) -> PathBuf {
    root.join("ranges").join(sanitized_range_dir(range_id))
}

fn range_data_dir(root: &Path, range_id: &str) -> PathBuf {
    range_root(root, range_id).join("data")
}

fn range_checkpoint_root(root: &Path, range_id: &str) -> PathBuf {
    range_root(root, range_id).join("checkpoints")
}

fn range_snapshot_root(root: &Path, range_id: &str) -> PathBuf {
    range_root(root, range_id).join("snapshots")
}

fn range_meta_path(root: &Path, range_id: &str) -> PathBuf {
    range_root(root, range_id).join("meta.bin")
}

fn wal_db_dir(root: &Path, range_id: &str) -> PathBuf {
    range_root(root, range_id).join("wal")
}

fn write_range_meta(
    root: &Path,
    bootstrap: &KvRangeBootstrap,
) -> anyhow::Result<PersistedRangeMeta> {
    let meta = PersistedRangeMeta {
        range_id: bootstrap.range_id.clone(),
        boundary: bootstrap.boundary.clone(),
    };
    let meta_path = range_meta_path(root, &bootstrap.range_id);
    fs::create_dir_all(meta_path.parent().expect("meta has parent"))?;
    fs::write(meta_path, bincode::serialize(&meta)?)?;
    Ok(meta)
}

fn read_range_meta(root: &Path, range_id: &str) -> anyhow::Result<PersistedRangeMeta> {
    Ok(bincode::deserialize(&fs::read(range_meta_path(
        root, range_id,
    ))?)?)
}

fn copy_dir_recursive(source: &Path, target: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&source_path, &target_path)?;
        } else {
            fs::copy(source_path, target_path)?;
        }
    }
    Ok(())
}

fn snapshot_manifest_path(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join("manifest.bin")
}

fn kv_checksum(entries: &[(Bytes, Bytes)]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (key, value) in entries {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    hasher.finish()
}

fn decode_wal_offset(value: &[u8]) -> anyhow::Result<u64> {
    anyhow::ensure!(value.len() == 8, "invalid wal offset encoding");
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(value);
    Ok(u64::from_be_bytes(bytes))
}

fn encode_wal_offset(offset: u64) -> [u8; 8] {
    offset.to_be_bytes()
}

fn wal_entry_key(offset: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(WAL_ENTRY_PREFIX.len() + 8);
    key.extend_from_slice(WAL_ENTRY_PREFIX);
    key.extend_from_slice(&encode_wal_offset(offset));
    key
}

impl RocksDbKvEngine {
    pub fn open(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("ranges"))?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub fn open_wal(&self, range_id: &str) -> anyhow::Result<RocksDbWalStore> {
        let path = wal_db_dir(&self.root, range_id);
        fs::create_dir_all(&path)?;
        let db = Arc::new(open_rocksdb(path)?);
        if db.get(WAL_NEXT_OFFSET_KEY)?.is_none() {
            db.put(WAL_NEXT_OFFSET_KEY, encode_wal_offset(1))?;
        }
        Ok(RocksDbWalStore { db })
    }
}

impl RocksDbKvRangeSpace {
    fn open(root: &Path, range_id: &str) -> anyhow::Result<Self> {
        let meta = read_range_meta(root, range_id)?;
        let range_dir = range_root(root, range_id);
        let data_dir = range_data_dir(root, range_id);
        fs::create_dir_all(range_checkpoint_root(root, range_id))?;
        fs::create_dir_all(range_snapshot_root(root, range_id))?;
        Ok(Self {
            range_id: meta.range_id,
            boundary: meta.boundary,
            range_dir,
            db: open_rocksdb(data_dir)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryWalStore {
    inner: Arc<Mutex<Vec<(u64, Bytes)>>>,
}

#[async_trait]
impl KvWalStore for MemoryWalStore {
    async fn append(&self, entry: Bytes) -> anyhow::Result<u64> {
        let mut inner = self.inner.lock().expect("wal poisoned");
        let offset = inner.last().map(|(offset, _)| offset + 1).unwrap_or(1);
        inner.push((offset, entry));
        Ok(offset)
    }

    async fn read_from(&self, offset: u64, limit: usize) -> anyhow::Result<Vec<(u64, Bytes)>> {
        let inner = self.inner.lock().expect("wal poisoned");
        Ok(inner
            .iter()
            .filter(|(current_offset, _)| *current_offset >= offset)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn truncate_prefix(&self, offset: u64) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().expect("wal poisoned");
        inner.retain(|(current_offset, _)| *current_offset >= offset);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryKvEngine {
    ranges: Arc<RwLock<HashMap<RangeId, MemoryKvRangeSpace>>>,
}

#[derive(Debug, Clone)]
pub struct MemoryKvRangeSpace {
    range_id: RangeId,
    boundary: RangeBoundary,
    io: MemoryKvRangeIo,
}

#[derive(Debug, Clone)]
struct MemoryKvRangeIo {
    boundary: RangeBoundary,
    data: Arc<RwLock<BTreeMap<Vec<u8>, Bytes>>>,
    checkpoints: Arc<RwLock<HashMap<String, BTreeMap<Vec<u8>, Bytes>>>>,
    snapshots: Arc<RwLock<HashMap<String, BTreeMap<Vec<u8>, Bytes>>>>,
}

impl MemoryKvRangeSpace {
    fn new(range_id: RangeId, boundary: RangeBoundary) -> Self {
        Self {
            range_id,
            boundary: boundary.clone(),
            io: MemoryKvRangeIo {
                boundary,
                data: Arc::new(RwLock::new(BTreeMap::new())),
                checkpoints: Arc::new(RwLock::new(HashMap::new())),
                snapshots: Arc::new(RwLock::new(HashMap::new())),
            },
        }
    }
}

#[async_trait]
impl KvRangeReader for MemoryKvRangeIo {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        Ok(self
            .data
            .read()
            .expect("range data poisoned")
            .get(key)
            .cloned())
    }

    async fn scan(
        &self,
        boundary: &RangeBoundary,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        Ok(self
            .data
            .read()
            .expect("range data poisoned")
            .iter()
            .filter(|(key, _)| self.boundary.contains(key) && boundary.contains(key))
            .take(limit)
            .map(|(key, value)| (Bytes::copy_from_slice(key), value.clone()))
            .collect())
    }
}

#[async_trait]
impl KvRangeWriter for MemoryKvRangeIo {
    async fn apply(&self, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let mut data = self.data.write().expect("range data poisoned");
        for mutation in mutations {
            anyhow::ensure!(
                self.boundary.contains(&mutation.key),
                "mutation outside range boundary"
            );
            match mutation.value {
                Some(value) => {
                    data.insert(mutation.key.to_vec(), value);
                }
                None => {
                    data.remove(mutation.key.as_ref());
                }
            }
        }
        Ok(())
    }

    async fn compact(&self, _boundary: Option<RangeBoundary>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl KvRangeSpace for MemoryKvRangeSpace {
    fn range_id(&self) -> &RangeId {
        &self.range_id
    }

    fn boundary(&self) -> &RangeBoundary {
        &self.boundary
    }

    fn reader(&self) -> &dyn KvRangeReader {
        &self.io
    }

    fn writer(&self) -> &dyn KvRangeWriter {
        &self.io
    }

    async fn checkpoint(&self, checkpoint_id: &str) -> anyhow::Result<KvRangeCheckpoint> {
        let snapshot = self.io.data.read().expect("range data poisoned").clone();
        self.io
            .checkpoints
            .write()
            .expect("range checkpoints poisoned")
            .insert(checkpoint_id.to_string(), snapshot);
        Ok(KvRangeCheckpoint {
            range_id: self.range_id.clone(),
            checkpoint_id: checkpoint_id.to_string(),
            path: format!("memory://{}/checkpoints/{}", self.range_id, checkpoint_id),
        })
    }

    async fn snapshot(&self) -> anyhow::Result<KvRangeSnapshot> {
        let snapshot_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift before unix epoch")
            .as_nanos();
        let snapshot_key = format!("snapshot-{snapshot_id}");
        let snapshot = self.io.data.read().expect("range data poisoned").clone();
        self.io
            .snapshots
            .write()
            .expect("range snapshots poisoned")
            .insert(snapshot_key.clone(), snapshot);
        let checksum = kv_checksum(
            &self
                .io
                .data
                .read()
                .expect("range data poisoned")
                .iter()
                .map(|(key, value)| (Bytes::copy_from_slice(key), value.clone()))
                .collect::<Vec<_>>(),
        );
        Ok(KvRangeSnapshot {
            range_id: self.range_id.clone(),
            boundary: self.boundary.clone(),
            term: 0,
            index: 0,
            checksum,
            layout_version: 1,
            data_path: format!("memory://{}/snapshots/{}", self.range_id, snapshot_key),
        })
    }

    async fn destroy(&self) -> anyhow::Result<()> {
        self.io.data.write().expect("range data poisoned").clear();
        self.io
            .checkpoints
            .write()
            .expect("range checkpoints poisoned")
            .clear();
        self.io
            .snapshots
            .write()
            .expect("range snapshots poisoned")
            .clear();
        Ok(())
    }
}

#[async_trait]
impl KvEngine for MemoryKvEngine {
    async fn bootstrap(&self, bootstrap: KvRangeBootstrap) -> anyhow::Result<()> {
        let mut ranges = self.ranges.write().expect("engine ranges poisoned");
        ranges
            .entry(bootstrap.range_id.clone())
            .or_insert_with(|| MemoryKvRangeSpace::new(bootstrap.range_id, bootstrap.boundary));
        Ok(())
    }

    async fn open_range(&self, range_id: &str) -> anyhow::Result<Box<dyn KvRangeSpace>> {
        self.ranges
            .read()
            .expect("engine ranges poisoned")
            .get(range_id)
            .cloned()
            .map(|range| Box::new(range) as Box<dyn KvRangeSpace>)
            .ok_or_else(|| anyhow::anyhow!("range `{range_id}` not found"))
    }

    async fn destroy_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.ranges
            .write()
            .expect("engine ranges poisoned")
            .remove(range_id);
        Ok(())
    }

    async fn list_ranges(&self) -> anyhow::Result<Vec<RangeId>> {
        Ok(self
            .ranges
            .read()
            .expect("engine ranges poisoned")
            .keys()
            .cloned()
            .collect())
    }

    async fn restore_snapshot(
        &self,
        bootstrap: KvRangeBootstrap,
        snapshot: &KvRangeSnapshot,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            snapshot.data_path.starts_with("memory://"),
            "memory engine can only restore memory snapshots"
        );
        let snapshot_id = snapshot
            .data_path
            .rsplit('/')
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid memory snapshot path"))?;
        let source = self
            .ranges
            .read()
            .expect("engine ranges poisoned")
            .get(&snapshot.range_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source range `{}` not found", snapshot.range_id))?;
        let restored = source
            .io
            .snapshots
            .read()
            .expect("range snapshots poisoned")
            .get(snapshot_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("memory snapshot `{snapshot_id}` not found"))?;
        let range_id = bootstrap.range_id.clone();
        let range = MemoryKvRangeSpace::new(bootstrap.range_id, bootstrap.boundary);
        *range.io.data.write().expect("range data poisoned") = restored;
        self.ranges
            .write()
            .expect("engine ranges poisoned")
            .insert(range_id, range);
        Ok(())
    }
}

#[async_trait]
impl KvRangeReader for RocksDbKvRangeSpace {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        anyhow::ensure!(self.boundary.contains(key), "key outside range boundary");
        Ok(self.db.get(key)?.map(Bytes::from))
    }

    async fn scan(
        &self,
        boundary: &RangeBoundary,
        limit: usize,
    ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let start = boundary
            .start_key
            .as_deref()
            .or(self.boundary.start_key.as_deref())
            .unwrap_or_default();
        let upper_bound = boundary
            .end_key
            .as_deref()
            .or(self.boundary.end_key.as_deref());
        let mut entries = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(start, Direction::Forward))
        {
            let (key, value) = item?;
            if !self.boundary.contains(&key) || !boundary.contains(&key) {
                if upper_bound.is_some_and(|end| key.as_ref() >= end) {
                    break;
                }
                continue;
            }
            entries.push((Bytes::copy_from_slice(&key), Bytes::from(value.to_vec())));
            if entries.len() >= limit {
                break;
            }
        }
        Ok(entries)
    }
}

#[async_trait]
impl KvRangeWriter for RocksDbKvRangeSpace {
    async fn apply(&self, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        for mutation in mutations {
            anyhow::ensure!(
                self.boundary.contains(&mutation.key),
                "mutation outside range boundary"
            );
            match mutation.value {
                Some(value) => batch.put(mutation.key, value),
                None => batch.delete(mutation.key),
            }
        }
        self.db.write(batch)?;
        Ok(())
    }

    async fn compact(&self, boundary: Option<RangeBoundary>) -> anyhow::Result<()> {
        match boundary {
            Some(boundary) => {
                self.db
                    .compact_range(boundary.start_key.as_deref(), boundary.end_key.as_deref());
            }
            None => {
                self.db.compact_range(
                    self.boundary.start_key.as_deref(),
                    self.boundary.end_key.as_deref(),
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl KvRangeSpace for RocksDbKvRangeSpace {
    fn range_id(&self) -> &RangeId {
        &self.range_id
    }

    fn boundary(&self) -> &RangeBoundary {
        &self.boundary
    }

    fn reader(&self) -> &dyn KvRangeReader {
        self
    }

    fn writer(&self) -> &dyn KvRangeWriter {
        self
    }

    async fn checkpoint(&self, checkpoint_id: &str) -> anyhow::Result<KvRangeCheckpoint> {
        let checkpoint_dir = self.range_dir.join("checkpoints").join(checkpoint_id);
        if checkpoint_dir.exists() {
            fs::remove_dir_all(&checkpoint_dir)?;
        }
        fs::create_dir_all(checkpoint_dir.parent().expect("checkpoint has parent"))?;
        Checkpoint::new(&self.db)?.create_checkpoint(&checkpoint_dir)?;
        Ok(KvRangeCheckpoint {
            range_id: self.range_id.clone(),
            checkpoint_id: checkpoint_id.to_string(),
            path: checkpoint_dir.display().to_string(),
        })
    }

    async fn snapshot(&self) -> anyhow::Result<KvRangeSnapshot> {
        let snapshot_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift before unix epoch")
            .as_nanos();
        let snapshot_dir = self
            .range_dir
            .join("snapshots")
            .join(format!("snapshot-{snapshot_id}"));
        fs::create_dir_all(snapshot_dir.parent().expect("snapshot has parent"))?;
        Checkpoint::new(&self.db)?.create_checkpoint(&snapshot_dir)?;
        let entries = self
            .reader()
            .scan(&RangeBoundary::full(), usize::MAX)
            .await?;
        let checksum = kv_checksum(&entries);
        let manifest = SnapshotManifest {
            range_id: self.range_id.clone(),
            term: 0,
            index: 0,
            boundary: self.boundary.clone(),
            checksum,
            layout_version: 1,
        };
        fs::write(
            snapshot_manifest_path(&snapshot_dir),
            bincode::serialize(&manifest)?,
        )?;
        Ok(KvRangeSnapshot {
            range_id: self.range_id.clone(),
            boundary: self.boundary.clone(),
            term: 0,
            index: 0,
            checksum,
            layout_version: 1,
            data_path: snapshot_dir.display().to_string(),
        })
    }

    async fn destroy(&self) -> anyhow::Result<()> {
        self.db.flush()?;
        let keys: Vec<_> = self
            .db
            .iterator(IteratorMode::Start)
            .map(|item| item.map(|(key, _)| key.to_vec()))
            .collect::<Result<_, _>>()?;
        let mut batch = WriteBatch::default();
        for key in keys {
            batch.delete(key);
        }
        self.db.write(batch)?;
        let checkpoints_dir = self.range_dir.join("checkpoints");
        if checkpoints_dir.exists() {
            fs::remove_dir_all(checkpoints_dir)?;
        }
        let snapshots_dir = self.range_dir.join("snapshots");
        if snapshots_dir.exists() {
            fs::remove_dir_all(snapshots_dir)?;
        }
        Ok(())
    }
}

#[async_trait]
impl KvWalStore for RocksDbWalStore {
    async fn append(&self, entry: Bytes) -> anyhow::Result<u64> {
        let next_offset = self
            .db
            .get(WAL_NEXT_OFFSET_KEY)?
            .map(|value| decode_wal_offset(&value))
            .transpose()?
            .unwrap_or(1);
        let mut batch = WriteBatch::default();
        batch.put(wal_entry_key(next_offset), entry);
        batch.put(WAL_NEXT_OFFSET_KEY, encode_wal_offset(next_offset + 1));
        self.db.write(batch)?;
        Ok(next_offset)
    }

    async fn read_from(&self, offset: u64, limit: usize) -> anyhow::Result<Vec<(u64, Bytes)>> {
        let mut entries = Vec::new();
        let start_key = wal_entry_key(offset);
        for item in self
            .db
            .iterator(IteratorMode::From(&start_key, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(WAL_ENTRY_PREFIX) {
                break;
            }
            let entry_offset = decode_wal_offset(&key[WAL_ENTRY_PREFIX.len()..])?;
            entries.push((entry_offset, Bytes::from(value.to_vec())));
            if entries.len() >= limit {
                break;
            }
        }
        Ok(entries)
    }

    async fn truncate_prefix(&self, offset: u64) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        for item in self.db.iterator(IteratorMode::Start) {
            let (key, _) = item?;
            if !key.starts_with(WAL_ENTRY_PREFIX) {
                continue;
            }
            let entry_offset = decode_wal_offset(&key[WAL_ENTRY_PREFIX.len()..])?;
            if entry_offset < offset {
                batch.delete(key);
            }
        }
        self.db.write(batch)?;
        Ok(())
    }
}

#[async_trait]
impl KvEngine for RocksDbKvEngine {
    async fn bootstrap(&self, bootstrap: KvRangeBootstrap) -> anyhow::Result<()> {
        write_range_meta(&self.root, &bootstrap)?;
        fs::create_dir_all(range_data_dir(&self.root, &bootstrap.range_id))?;
        fs::create_dir_all(range_checkpoint_root(&self.root, &bootstrap.range_id))?;
        fs::create_dir_all(range_snapshot_root(&self.root, &bootstrap.range_id))?;
        fs::create_dir_all(wal_db_dir(&self.root, &bootstrap.range_id))?;
        let _ = open_rocksdb(range_data_dir(&self.root, &bootstrap.range_id))?;
        let wal = self.open_wal(&bootstrap.range_id)?;
        if wal.db.get(WAL_NEXT_OFFSET_KEY)?.is_none() {
            wal.db.put(WAL_NEXT_OFFSET_KEY, encode_wal_offset(1))?;
        }
        Ok(())
    }

    async fn open_range(&self, range_id: &str) -> anyhow::Result<Box<dyn KvRangeSpace>> {
        Ok(Box::new(RocksDbKvRangeSpace::open(&self.root, range_id)?))
    }

    async fn destroy_range(&self, range_id: &str) -> anyhow::Result<()> {
        let range_dir = range_root(&self.root, range_id);
        if range_dir.exists() {
            fs::remove_dir_all(range_dir)?;
        }
        Ok(())
    }

    async fn list_ranges(&self) -> anyhow::Result<Vec<RangeId>> {
        let ranges_root = self.root.join("ranges");
        if !ranges_root.exists() {
            return Ok(Vec::new());
        }
        let mut range_ids = Vec::new();
        for entry in fs::read_dir(ranges_root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let meta_path = entry.path().join("meta.bin");
                if meta_path.exists() {
                    let meta: PersistedRangeMeta = bincode::deserialize(&fs::read(meta_path)?)?;
                    range_ids.push(meta.range_id);
                }
            }
        }
        range_ids.sort();
        Ok(range_ids)
    }

    async fn restore_snapshot(
        &self,
        bootstrap: KvRangeBootstrap,
        snapshot: &KvRangeSnapshot,
    ) -> anyhow::Result<()> {
        let target_root = range_root(&self.root, &bootstrap.range_id);
        if target_root.exists() {
            fs::remove_dir_all(&target_root)?;
        }
        let data_dir = range_data_dir(&self.root, &bootstrap.range_id);
        fs::create_dir_all(data_dir.parent().expect("range data has parent"))?;
        copy_dir_recursive(Path::new(&snapshot.data_path), &data_dir)?;
        anyhow::ensure!(
            snapshot_manifest_path(Path::new(&snapshot.data_path)).exists(),
            "snapshot manifest missing for {}",
            snapshot.range_id
        );
        fs::create_dir_all(range_checkpoint_root(&self.root, &bootstrap.range_id))?;
        fs::create_dir_all(range_snapshot_root(&self.root, &bootstrap.range_id))?;
        fs::create_dir_all(wal_db_dir(&self.root, &bootstrap.range_id))?;
        write_range_meta(&self.root, &bootstrap)?;
        let wal = self.open_wal(&bootstrap.range_id)?;
        if wal.db.get(WAL_NEXT_OFFSET_KEY)?.is_none() {
            wal.db.put(WAL_NEXT_OFFSET_KEY, encode_wal_offset(1))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        snapshot_manifest_path, KvEngine, KvMutation, KvRangeBootstrap, KvWalStore, MemoryKvEngine,
        MemoryWalStore, RocksDbKvEngine,
    };
    use greenmqtt_core::RangeBoundary;

    #[test]
    fn kv_mutation_distinguishes_put_from_delete() {
        let put = KvMutation {
            key: Bytes::from_static(b"tenant/a"),
            value: Some(Bytes::from_static(b"value")),
        };
        let delete = KvMutation {
            key: Bytes::from_static(b"tenant/a"),
            value: None,
        };
        assert!(put.value.is_some());
        assert!(delete.value.is_none());
    }

    use bytes::Bytes;

    #[test]
    fn kv_range_bootstrap_carries_range_identity_and_boundary() {
        let bootstrap = KvRangeBootstrap {
            range_id: "range-1".into(),
            boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
        };
        assert_eq!(bootstrap.range_id, "range-1");
        assert!(bootstrap.boundary.contains(b"m"));
        assert!(!bootstrap.boundary.contains(b"z"));
    }

    #[tokio::test]
    async fn memory_kv_engine_bootstraps_ranges_and_applies_mutations() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-a".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let range = engine.open_range("range-a").await.unwrap();
        range
            .writer()
            .apply(vec![KvMutation {
                key: Bytes::from_static(b"m"),
                value: Some(Bytes::from_static(b"value")),
            }])
            .await
            .unwrap();
        assert_eq!(
            range.reader().get(b"m").await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(
            engine.list_ranges().await.unwrap(),
            vec!["range-a".to_string()]
        );
    }

    #[tokio::test]
    async fn memory_kv_engine_restores_range_from_snapshot() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-src".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let range = engine.open_range("range-src").await.unwrap();
        range
            .writer()
            .apply(vec![KvMutation {
                key: Bytes::from_static(b"alpha"),
                value: Some(Bytes::from_static(b"one")),
            }])
            .await
            .unwrap();
        let snapshot = range.snapshot().await.unwrap();

        engine
            .restore_snapshot(
                KvRangeBootstrap {
                    range_id: "range-restored".into(),
                    boundary: RangeBoundary::full(),
                },
                &snapshot,
            )
            .await
            .unwrap();
        let restored = engine.open_range("range-restored").await.unwrap();
        assert_eq!(
            restored.reader().get(b"alpha").await.unwrap(),
            Some(Bytes::from_static(b"one"))
        );
    }

    #[tokio::test]
    async fn memory_wal_store_appends_reads_and_truncates_entries() {
        let wal = MemoryWalStore::default();
        assert_eq!(wal.append(Bytes::from_static(b"e1")).await.unwrap(), 1);
        assert_eq!(wal.append(Bytes::from_static(b"e2")).await.unwrap(), 2);
        assert_eq!(wal.read_from(2, 10).await.unwrap().len(), 1);
        wal.truncate_prefix(2).await.unwrap();
        let remaining = wal.read_from(1, 10).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 2);
    }

    #[tokio::test]
    async fn rocksdb_kv_engine_bootstraps_applies_and_snapshots_range() {
        let tempdir = tempfile::tempdir().unwrap();
        let engine = RocksDbKvEngine::open(tempdir.path()).unwrap();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-r".into(),
                boundary: RangeBoundary::new(Some(b"a".to_vec()), Some(b"z".to_vec())),
            })
            .await
            .unwrap();
        let range = engine.open_range("range-r").await.unwrap();
        range
            .writer()
            .apply(vec![
                KvMutation {
                    key: Bytes::from_static(b"mango"),
                    value: Some(Bytes::from_static(b"yellow")),
                },
                KvMutation {
                    key: Bytes::from_static(b"pear"),
                    value: Some(Bytes::from_static(b"green")),
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            range.reader().get(b"mango").await.unwrap(),
            Some(Bytes::from_static(b"yellow"))
        );
        assert_eq!(
            range
                .reader()
                .scan(
                    &RangeBoundary::new(Some(b"m".to_vec()), Some(b"z".to_vec())),
                    10
                )
                .await
                .unwrap()
                .len(),
            2
        );
        range.writer().compact(None).await.unwrap();
        let checkpoint = range.checkpoint("cp-1").await.unwrap();
        assert!(std::path::Path::new(&checkpoint.path).exists());
        let snapshot = range.snapshot().await.unwrap();
        assert!(std::path::Path::new(&snapshot.data_path).exists());
        assert!(snapshot_manifest_path(std::path::Path::new(&snapshot.data_path)).exists());
        assert_eq!(snapshot.layout_version, 1);
        assert!(snapshot.checksum > 0);
        assert_eq!(
            engine.list_ranges().await.unwrap(),
            vec!["range-r".to_string()]
        );
    }

    #[tokio::test]
    async fn rocksdb_kv_engine_restores_range_from_snapshot() {
        let tempdir = tempfile::tempdir().unwrap();
        let engine = RocksDbKvEngine::open(tempdir.path()).unwrap();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "range-src".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let source = engine.open_range("range-src").await.unwrap();
        source
            .writer()
            .apply(vec![KvMutation {
                key: Bytes::from_static(b"beta"),
                value: Some(Bytes::from_static(b"two")),
            }])
            .await
            .unwrap();
        let snapshot = source.snapshot().await.unwrap();
        drop(source);

        engine
            .restore_snapshot(
                KvRangeBootstrap {
                    range_id: "range-restored".into(),
                    boundary: RangeBoundary::full(),
                },
                &snapshot,
            )
            .await
            .unwrap();
        let restored = engine.open_range("range-restored").await.unwrap();
        assert_eq!(
            restored.reader().get(b"beta").await.unwrap(),
            Some(Bytes::from_static(b"two"))
        );
    }

    #[tokio::test]
    async fn rocksdb_wal_store_appends_reads_and_truncates_entries() {
        let tempdir = tempfile::tempdir().unwrap();
        let engine = RocksDbKvEngine::open(tempdir.path()).unwrap();
        let wal = engine.open_wal("range-w").unwrap();
        assert_eq!(wal.append(Bytes::from_static(b"e1")).await.unwrap(), 1);
        assert_eq!(wal.append(Bytes::from_static(b"e2")).await.unwrap(), 2);
        let entries = wal.read_from(1, 10).await.unwrap();
        assert_eq!(entries.len(), 2);
        wal.truncate_prefix(2).await.unwrap();
        let entries = wal.read_from(1, 10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 2);
        assert_eq!(entries[0].1, Bytes::from_static(b"e2"));
    }
}
