use async_trait::async_trait;
use bytes::Bytes;
use greenmqtt_core::{ClientIdentity, Lifecycle, ServiceShardKey, ServiceShardKind, SessionRecord};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter};
use greenmqtt_kv_engine::KvMutation;
use greenmqtt_storage::SessionStore;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[async_trait]
pub trait SessionDirectory: Send + Sync {
    async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>>;
    async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>>;
    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>>;
    async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>>;
    async fn session_exists(&self, session_id: &str) -> anyhow::Result<bool> {
        Ok(self.lookup_session(session_id).await?.is_some())
    }
    async fn identity_exists(&self, identity: &ClientIdentity) -> anyhow::Result<bool> {
        Ok(self.lookup_identity(identity).await?.is_some())
    }
    async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>>;
    async fn session_count(&self) -> anyhow::Result<usize>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRegistrationState {
    Registered,
    Kicked,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKillNotification {
    pub requested: SessionRecord,
    pub current_owner: Option<SessionRecord>,
    pub server_reference: Option<String>,
}

#[async_trait]
pub trait SessionKillListener: Send + Sync {
    async fn on_kill(&self, notification: SessionKillNotification) -> anyhow::Result<()>;
}

pub trait SessionOwnerResolver: Send + Sync {
    fn server_reference(&self, node_id: u64) -> Option<String>;
}

#[derive(Clone)]
pub struct SessionRegistrationManager {
    directory: Arc<dyn SessionDirectory>,
    poll_interval: Duration,
    owner_resolver: Option<Arc<dyn SessionOwnerResolver>>,
}

impl SessionRegistrationManager {
    pub fn new(directory: Arc<dyn SessionDirectory>) -> Self {
        Self {
            directory,
            poll_interval: Duration::from_secs(5),
            owner_resolver: None,
        }
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_owner_resolver(mut self, owner_resolver: Arc<dyn SessionOwnerResolver>) -> Self {
        self.owner_resolver = Some(owner_resolver);
        self
    }

    pub async fn register(
        &self,
        record: SessionRecord,
        kill_listener: Arc<dyn SessionKillListener>,
    ) -> anyhow::Result<SessionRegistrationHandle> {
        let (_, handle) = self.register_with_previous(record, kill_listener).await?;
        Ok(handle)
    }

    pub async fn register_with_previous(
        &self,
        record: SessionRecord,
        kill_listener: Arc<dyn SessionKillListener>,
    ) -> anyhow::Result<(Option<SessionRecord>, SessionRegistrationHandle)> {
        let previous = self.directory.register(record.clone()).await?;
        let state = Arc::new(RwLock::new(SessionRegistrationState::Registered));
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let directory = self.directory.clone();
        let owner_resolver = self.owner_resolver.clone();
        let watched_record = record.clone();
        let task_state = state.clone();
        let poll_interval = self.poll_interval;
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let current = directory.lookup_identity(&watched_record.identity).await?;
                        let still_owned = current.as_ref().is_some_and(|current| {
                            current.session_id == watched_record.session_id
                                && current.node_id == watched_record.node_id
                        });
                        if still_owned {
                            continue;
                        }
                        *task_state.write().expect("session registration poisoned") =
                            SessionRegistrationState::Kicked;
                        let server_reference = current.as_ref().and_then(|current| {
                            owner_resolver
                                .as_ref()
                                .and_then(|resolver| resolver.server_reference(current.node_id))
                        });
                        kill_listener
                            .on_kill(SessionKillNotification {
                                requested: watched_record.clone(),
                                current_owner: current,
                                server_reference,
                            })
                            .await?;
                        break;
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            Ok(())
        });

        Ok((
            previous,
            SessionRegistrationHandle {
                record,
                directory: self.directory.clone(),
                state,
                stop_tx,
                task: Mutex::new(Some(task)),
            },
        ))
    }
}

pub struct SessionRegistrationHandle {
    record: SessionRecord,
    directory: Arc<dyn SessionDirectory>,
    state: Arc<RwLock<SessionRegistrationState>>,
    stop_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<anyhow::Result<()>>>>,
}

impl SessionRegistrationHandle {
    pub fn record(&self) -> &SessionRecord {
        &self.record
    }

    pub fn state(&self) -> SessionRegistrationState {
        *self.state.read().expect("session registration poisoned")
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let _ = self.stop_tx.send(true);
        let task = self
            .task
            .lock()
            .expect("session registration poisoned")
            .take();
        if self.state() == SessionRegistrationState::Registered {
            let current = self.directory.lookup_identity(&self.record.identity).await?;
            if current.as_ref().is_some_and(|current| {
                current.session_id == self.record.session_id
                    && current.node_id == self.record.node_id
            }) {
                let _ = self.directory.unregister(&self.record.session_id).await?;
            }
            *self.state.write().expect("session registration poisoned") =
                SessionRegistrationState::Stopped;
        }

        if let Some(task) = task {
            match task.await {
                Ok(result) => result?,
                Err(error) if error.is_cancelled() => {}
                Err(error) => anyhow::bail!("session registration task failed: {error}"),
            }
        }
        Ok(())
    }
}

impl Drop for SessionRegistrationHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.lock().expect("session registration poisoned").take() {
            task.abort();
        }
    }
}

#[derive(Clone, Default)]
pub struct SessionDictHandle {
    inner: Arc<RwLock<SessionDirectoryState>>,
}

#[derive(Default)]
struct SessionDirectoryState {
    by_identity: HashMap<(String, String, String), SessionRecord>,
    by_session: HashMap<String, SessionRecord>,
    by_tenant: HashMap<String, HashMap<String, SessionRecord>>,
    loaded_identity_keys: HashSet<(String, String, String)>,
    loaded_session_ids: HashSet<String>,
    warmed_tenants: HashSet<String>,
    all_sessions_warmed: bool,
}

impl SessionDirectoryState {
    fn remove_tenant_record(&mut self, record: &SessionRecord) {
        if let Some(records) = self.by_tenant.get_mut(&record.identity.tenant_id) {
            records.remove(&record.session_id);
            if records.is_empty() {
                self.by_tenant.remove(&record.identity.tenant_id);
            }
        }
    }

    fn insert_record(&mut self, record: SessionRecord) -> Option<SessionRecord> {
        let session_id = record.session_id.clone();
        let identity_key = SessionDictHandle::identity_key(&record.identity);
        let replaced = self
            .by_identity
            .insert(identity_key.clone(), record.clone());
        if let Some(previous) = &replaced {
            self.by_session.remove(&previous.session_id);
            self.remove_tenant_record(previous);
            self.loaded_session_ids.insert(previous.session_id.clone());
        }
        if let Some(previous) = self
            .by_session
            .insert(record.session_id.clone(), record.clone())
        {
            if replaced
                .as_ref()
                .map(|existing| existing.session_id.as_str())
                != Some(previous.session_id.as_str())
            {
                let previous_identity_key = SessionDictHandle::identity_key(&previous.identity);
                self.by_identity.remove(&previous_identity_key);
                self.loaded_identity_keys.insert(previous_identity_key);
                self.remove_tenant_record(&previous);
                self.loaded_session_ids.insert(previous.session_id.clone());
            }
        }
        self.loaded_identity_keys.insert(identity_key);
        self.by_tenant
            .entry(record.identity.tenant_id.clone())
            .or_default()
            .insert(record.session_id.clone(), record);
        self.loaded_session_ids.insert(session_id);
        replaced
    }

    fn remove_session(&mut self, session_id: &str) -> Option<SessionRecord> {
        let removed = self.by_session.remove(session_id);
        if let Some(record) = &removed {
            let identity_key = SessionDictHandle::identity_key(&record.identity);
            self.by_identity.remove(&identity_key);
            self.loaded_identity_keys.insert(identity_key);
            self.remove_tenant_record(record);
        }
        self.loaded_session_ids.insert(session_id.to_string());
        removed
    }

    fn replace_tenant_records(&mut self, tenant_id: &str, records: Vec<SessionRecord>) {
        if let Some(existing) = self.by_tenant.remove(tenant_id) {
            for record in existing.into_values() {
                self.by_session.remove(&record.session_id);
                self.by_identity
                    .remove(&SessionDictHandle::identity_key(&record.identity));
                self.loaded_session_ids.insert(record.session_id);
            }
        }
        for record in records {
            self.insert_record(record);
        }
        self.warmed_tenants.insert(tenant_id.to_string());
    }

    fn replace_all_records(&mut self, records: Vec<SessionRecord>) {
        self.by_identity.clear();
        self.by_session.clear();
        self.by_tenant.clear();
        self.loaded_identity_keys.clear();
        self.loaded_session_ids.clear();
        for record in records {
            self.insert_record(record);
        }
        self.warmed_tenants.clear();
        self.all_sessions_warmed = true;
    }
}

#[derive(Clone)]
pub struct PersistentSessionDictHandle {
    store: Arc<dyn SessionStore>,
    inner: Arc<RwLock<SessionDirectoryState>>,
}

#[derive(Clone)]
pub struct ReplicatedSessionDictHandle {
    router: Arc<dyn KvRangeRouter>,
    executor: Arc<dyn KvRangeExecutor>,
    identity_cache: Arc<RwLock<HashMap<(String, String, String), SessionRecord>>>,
    session_cache: Arc<RwLock<HashMap<String, SessionRecord>>>,
    session_range_cache: Arc<RwLock<HashMap<String, String>>>,
}

#[derive(Clone)]
pub struct SessionOnlineCheckScheduler {
    directory: Arc<dyn SessionDirectory>,
}

impl SessionOnlineCheckScheduler {
    pub fn new(directory: Arc<dyn SessionDirectory>) -> Self {
        Self { directory }
    }

    pub async fn check_sessions(
        &self,
        session_ids: &[String],
    ) -> anyhow::Result<BTreeMap<String, bool>> {
        let known = self
            .directory
            .list_sessions(None)
            .await?
            .into_iter()
            .map(|record| record.session_id)
            .collect::<HashSet<_>>();
        Ok(session_ids
            .iter()
            .map(|session_id| (session_id.clone(), known.contains(session_id)))
            .collect())
    }

    pub async fn check_identities(
        &self,
        identities: &[ClientIdentity],
    ) -> anyhow::Result<BTreeMap<(String, String, String), bool>> {
        let mut grouped: BTreeMap<String, Vec<&ClientIdentity>> = BTreeMap::new();
        for identity in identities {
            grouped
                .entry(identity.tenant_id.clone())
                .or_default()
                .push(identity);
        }
        let mut results = BTreeMap::new();
        for (tenant_id, identities) in grouped {
            let known = self
                .directory
                .list_sessions(Some(&tenant_id))
                .await?
                .into_iter()
                .map(|record| SessionDictHandle::identity_key(&record.identity))
                .collect::<HashSet<_>>();
            for identity in identities {
                let key = SessionDictHandle::identity_key(identity);
                results.insert(key.clone(), known.contains(&key));
            }
        }
        Ok(results)
    }
}

pub fn session_record_shard(record: &SessionRecord) -> ServiceShardKey {
    ServiceShardKey::sessiondict(record.identity.tenant_id.clone(), record.session_id.clone())
}

pub fn session_identity_shard(identity: &ClientIdentity) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::SessionDict,
        tenant_id: identity.tenant_id.clone(),
        scope: format!("identity:{}:{}", identity.user_id, identity.client_id),
    }
}

pub fn session_scan_shard(tenant_id: &str) -> ServiceShardKey {
    ServiceShardKey {
        kind: ServiceShardKind::SessionDict,
        tenant_id: tenant_id.to_string(),
        scope: "*".into(),
    }
}

impl SessionDictHandle {
    fn identity_key(identity: &ClientIdentity) -> (String, String, String) {
        (
            identity.tenant_id.clone(),
            identity.user_id.clone(),
            identity.client_id.clone(),
        )
    }
}

impl PersistentSessionDictHandle {
    pub async fn open(store: Arc<dyn SessionStore>) -> anyhow::Result<Self> {
        Ok(Self {
            store,
            inner: Arc::new(RwLock::new(SessionDirectoryState::default())),
        })
    }

    fn refresh_record(
        &self,
        identity: &ClientIdentity,
        record: Option<SessionRecord>,
    ) -> Option<SessionRecord> {
        let mut guard = self.inner.write().expect("sessiondict poisoned");
        let identity_key = SessionDictHandle::identity_key(identity);
        if let Some(existing) = guard.by_identity.remove(&identity_key) {
            guard.remove_session(&existing.session_id);
        }
        if let Some(record) = record.clone() {
            guard.insert_record(record.clone());
            Some(record)
        } else {
            guard.loaded_identity_keys.insert(identity_key);
            None
        }
    }
}

impl ReplicatedSessionDictHandle {
    pub fn new(router: Arc<dyn KvRangeRouter>, executor: Arc<dyn KvRangeExecutor>) -> Self {
        Self {
            router,
            executor,
            identity_cache: Arc::new(RwLock::new(HashMap::new())),
            session_cache: Arc::new(RwLock::new(HashMap::new())),
            session_range_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn tenant_routes(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<greenmqtt_kv_client::RangeRoute>> {
        self.router
            .route_shard(&session_scan_shard(tenant_id))
            .await
    }

    async fn apply_grouped(
        &self,
        tenant_id: &str,
        mutations: Vec<KvMutation>,
    ) -> anyhow::Result<()> {
        let shard = session_scan_shard(tenant_id);
        let mut grouped: HashMap<String, Vec<KvMutation>> = HashMap::new();
        for mutation in mutations {
            let route = self
                .router
                .route_key(&shard, &mutation.key)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("no sessiondict range available for tenant {tenant_id}")
                })?;
            grouped
                .entry(route.descriptor.id.clone())
                .or_default()
                .push(mutation);
        }
        for (range_id, mutations) in grouped {
            self.executor.apply(&range_id, mutations).await?;
        }
        Ok(())
    }

    fn cache_record(&self, record: SessionRecord, range_id: Option<String>) {
        let identity_key = SessionDictHandle::identity_key(&record.identity);
        self.identity_cache
            .write()
            .expect("sessiondict cache poisoned")
            .insert(identity_key, record.clone());
        self.session_cache
            .write()
            .expect("sessiondict cache poisoned")
            .insert(record.session_id.clone(), record.clone());
        if let Some(range_id) = range_id {
            self.session_range_cache
                .write()
                .expect("sessiondict cache poisoned")
                .insert(record.session_id.clone(), range_id);
        }
    }

    fn evict_record(&self, record: &SessionRecord) {
        self.identity_cache
            .write()
            .expect("sessiondict cache poisoned")
            .remove(&SessionDictHandle::identity_key(&record.identity));
        self.session_cache
            .write()
            .expect("sessiondict cache poisoned")
            .remove(&record.session_id);
        self.session_range_cache
            .write()
            .expect("sessiondict cache poisoned")
            .remove(&record.session_id);
    }

    async fn scan_session_records_for_tenant(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<SessionRecord>> {
        let mut records = Vec::new();
        for route in self.tenant_routes(tenant_id).await? {
            for (key, value) in self
                .executor
                .scan(&route.descriptor.id, None, usize::MAX)
                .await?
            {
                if key.starts_with(SESSION_KEY_PREFIX) {
                    let record = decode_session_record(&value)?;
                    if record.identity.tenant_id == tenant_id {
                        records.push(record);
                    }
                }
            }
        }
        records.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(records)
    }

    async fn scan_all_session_records(&self) -> anyhow::Result<Vec<SessionRecord>> {
        let mut records = Vec::new();
        for descriptor in self.router.list().await? {
            if descriptor.shard.kind != ServiceShardKind::SessionDict {
                continue;
            }
            for (key, value) in self.executor.scan(&descriptor.id, None, usize::MAX).await? {
                if key.starts_with(SESSION_KEY_PREFIX) {
                    records.push(decode_session_record(&value)?);
                }
            }
        }
        records.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(records)
    }
}

const IDENTITY_KEY_PREFIX: &[u8] = b"identity\0";
const SESSION_KEY_PREFIX: &[u8] = b"session\0";

fn encoded_identity_key(identity: &ClientIdentity) -> Bytes {
    let mut key = Vec::with_capacity(
        IDENTITY_KEY_PREFIX.len() + identity.user_id.len() + identity.client_id.len() + 2,
    );
    key.extend_from_slice(IDENTITY_KEY_PREFIX);
    key.extend_from_slice(identity.user_id.as_bytes());
    key.push(0);
    key.extend_from_slice(identity.client_id.as_bytes());
    Bytes::from(key)
}

fn encoded_session_key(session_id: &str) -> Bytes {
    let mut key = Vec::with_capacity(SESSION_KEY_PREFIX.len() + session_id.len());
    key.extend_from_slice(SESSION_KEY_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    Bytes::from(key)
}

fn encode_session_record(record: &SessionRecord) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(record)?))
}

fn decode_session_record(value: &[u8]) -> anyhow::Result<SessionRecord> {
    Ok(bincode::deserialize(value)?)
}

#[async_trait]
impl SessionDirectory for SessionDictHandle {
    async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>> {
        let mut guard = self.inner.write().expect("sessiondict poisoned");
        Ok(guard.insert_record(record))
    }

    async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        let mut guard = self.inner.write().expect("sessiondict poisoned");
        Ok(guard.remove_session(session_id))
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let guard = self.inner.read().expect("sessiondict poisoned");
        Ok(guard
            .by_identity
            .get(&Self::identity_key(identity))
            .cloned())
    }

    async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        let guard = self.inner.read().expect("sessiondict poisoned");
        Ok(guard.by_session.get(session_id).cloned())
    }

    async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>> {
        let guard = self.inner.read().expect("sessiondict poisoned");
        let mut records: Vec<_> = match tenant_id {
            Some(tenant_id) => guard
                .by_tenant
                .get(tenant_id)
                .into_iter()
                .flat_map(|records| records.values())
                .cloned()
                .collect(),
            None => guard.by_session.values().cloned().collect(),
        };
        records.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(records)
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        Ok(self
            .inner
            .read()
            .expect("sessiondict poisoned")
            .by_session
            .len())
    }
}

#[async_trait]
impl SessionDirectory for PersistentSessionDictHandle {
    async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>> {
        let key = SessionDictHandle::identity_key(&record.identity);
        let (replaced, identity_loaded) = {
            let guard = self.inner.read().expect("sessiondict poisoned");
            (
                guard.by_identity.get(&key).cloned(),
                guard.all_sessions_warmed
                    || guard.warmed_tenants.contains(&record.identity.tenant_id)
                    || guard.loaded_identity_keys.contains(&key),
            )
        };
        let replaced = match replaced {
            Some(record) => Some(record),
            None if !identity_loaded => {
                self.store
                    .load_session(&record.identity.tenant_id, &record.identity.client_id)
                    .await?
            }
            None => None,
        };
        self.store.save_session(&record).await?;
        let mut guard = self.inner.write().expect("sessiondict poisoned");
        if let Some(previous) = &replaced {
            guard.by_identity.remove(&key);
            guard.remove_session(&previous.session_id);
        }
        guard.insert_record(record);
        Ok(replaced)
    }

    async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        let (removed, session_loaded) = {
            let mut guard = self.inner.write().expect("sessiondict poisoned");
            let session_loaded =
                guard.all_sessions_warmed || guard.loaded_session_ids.contains(session_id);
            let removed = guard.remove_session(session_id);
            (removed, session_loaded)
        };
        let removed = match removed {
            Some(record) => Some(record),
            None if !session_loaded => self.store.load_session_by_session_id(session_id).await?,
            None => None,
        };
        if let Some(record) = &removed {
            self.refresh_record(&record.identity, None);
            self.store
                .delete_session(&record.identity.tenant_id, &record.identity.client_id)
                .await?;
        }
        Ok(removed)
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        let key = SessionDictHandle::identity_key(identity);
        if let Some(record) = {
            let guard = self.inner.read().expect("sessiondict poisoned");
            if guard.all_sessions_warmed
                || guard.warmed_tenants.contains(&identity.tenant_id)
                || guard.loaded_identity_keys.contains(&key)
            {
                Some(guard.by_identity.get(&key).cloned())
            } else {
                None
            }
        } {
            return Ok(record);
        }
        let loaded = self
            .store
            .load_session(&identity.tenant_id, &identity.client_id)
            .await?;
        Ok(self.refresh_record(identity, loaded))
    }

    async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        if let Some(record) = self
            .inner
            .read()
            .expect("sessiondict poisoned")
            .by_session
            .get(session_id)
            .cloned()
        {
            return Ok(Some(record));
        }
        let loaded = {
            let guard = self.inner.read().expect("sessiondict poisoned");
            guard.all_sessions_warmed || guard.loaded_session_ids.contains(session_id)
        };
        if loaded {
            return Ok(None);
        }
        let record = self.store.load_session_by_session_id(session_id).await?;
        if let Some(record) = &record {
            self.refresh_record(&record.identity, Some(record.clone()));
        } else {
            self.inner
                .write()
                .expect("sessiondict poisoned")
                .loaded_session_ids
                .insert(session_id.to_string());
        }
        Ok(record)
    }

    async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>> {
        let mut records = match tenant_id {
            Some(tenant_id) => {
                let cached = {
                    let guard = self.inner.read().expect("sessiondict poisoned");
                    if guard.all_sessions_warmed || guard.warmed_tenants.contains(tenant_id) {
                        Some(
                            guard
                                .by_tenant
                                .get(tenant_id)
                                .into_iter()
                                .flat_map(|records| records.values())
                                .cloned()
                                .collect::<Vec<_>>(),
                        )
                    } else {
                        None
                    }
                };
                match cached {
                    Some(records) => records,
                    None => {
                        let records = self.store.list_tenant_sessions(tenant_id).await?;
                        self.inner
                            .write()
                            .expect("sessiondict poisoned")
                            .replace_tenant_records(tenant_id, records.clone());
                        records
                    }
                }
            }
            None => {
                let cached = {
                    let guard = self.inner.read().expect("sessiondict poisoned");
                    if guard.all_sessions_warmed {
                        Some(guard.by_session.values().cloned().collect::<Vec<_>>())
                    } else {
                        None
                    }
                };
                match cached {
                    Some(records) => records,
                    None => {
                        let records = self.store.list_sessions().await?;
                        self.inner
                            .write()
                            .expect("sessiondict poisoned")
                            .replace_all_records(records.clone());
                        records
                    }
                }
            }
        };
        records.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(records)
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        if let Some(count) = {
            let guard = self.inner.read().expect("sessiondict poisoned");
            if guard.all_sessions_warmed {
                Some(guard.by_session.len())
            } else {
                None
            }
        } {
            return Ok(count);
        }
        self.store.count_sessions().await
    }
}

#[async_trait]
impl SessionDirectory for ReplicatedSessionDictHandle {
    async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>> {
        let replaced_by_identity = self.lookup_identity(&record.identity).await?;
        let replaced_by_session = self.lookup_session(&record.session_id).await?;
        let replaced = replaced_by_identity.clone().or(replaced_by_session.clone());
        let mut mutations = Vec::new();

        if let Some(previous) = replaced_by_identity {
            mutations.push(KvMutation {
                key: encoded_identity_key(&previous.identity),
                value: None,
            });
            mutations.push(KvMutation {
                key: encoded_session_key(&previous.session_id),
                value: None,
            });
        }

        if let Some(previous) = replaced_by_session {
            if replaced
                .as_ref()
                .map(|candidate| candidate.session_id.as_str())
                != Some(previous.session_id.as_str())
            {
                mutations.push(KvMutation {
                    key: encoded_identity_key(&previous.identity),
                    value: None,
                });
                mutations.push(KvMutation {
                    key: encoded_session_key(&previous.session_id),
                    value: None,
                });
            }
        }

        let encoded = encode_session_record(&record)?;
        mutations.push(KvMutation {
            key: encoded_identity_key(&record.identity),
            value: Some(encoded.clone()),
        });
        mutations.push(KvMutation {
            key: encoded_session_key(&record.session_id),
            value: Some(encoded),
        });
        self.apply_grouped(&record.identity.tenant_id, mutations)
            .await?;
        if let Some(previous) = replaced.as_ref() {
            self.evict_record(previous);
        }
        let range_id = self
            .router
            .route_key(&session_scan_shard(&record.identity.tenant_id), &encoded_session_key(&record.session_id))
            .await?
            .map(|route| route.descriptor.id);
        self.cache_record(record.clone(), range_id);
        Ok(replaced)
    }

    async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        let Some(record) = self.lookup_session(session_id).await? else {
            return Ok(None);
        };
        self.apply_grouped(
            &record.identity.tenant_id,
            vec![
                KvMutation {
                    key: encoded_identity_key(&record.identity),
                    value: None,
                },
                KvMutation {
                    key: encoded_session_key(&record.session_id),
                    value: None,
                },
            ],
        )
        .await?;
        self.evict_record(&record);
        Ok(Some(record))
    }

    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>> {
        if let Some(cached) = self
            .identity_cache
            .read()
            .expect("sessiondict cache poisoned")
            .get(&SessionDictHandle::identity_key(identity))
            .cloned()
        {
            return Ok(Some(cached));
        }
        let shard = session_scan_shard(&identity.tenant_id);
        let Some(route) = self
            .router
            .route_key(&shard, &encoded_identity_key(identity))
            .await?
        else {
            return Ok(None);
        };
        let record = self
            .executor
            .get(&route.descriptor.id, &encoded_identity_key(identity))
            .await?
            .map(|value| decode_session_record(&value))
            .transpose()?;
        if let Some(record) = &record {
            self.cache_record(record.clone(), Some(route.descriptor.id.clone()));
        }
        Ok(record)
    }

    async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
        if let Some(cached) = self
            .session_cache
            .read()
            .expect("sessiondict cache poisoned")
            .get(session_id)
            .cloned()
        {
            return Ok(Some(cached));
        }
        let target_key = encoded_session_key(session_id);
        let cached_range_id = self
            .session_range_cache
            .read()
            .expect("sessiondict cache poisoned")
            .get(session_id)
            .cloned();
        if let Some(range_id) = cached_range_id {
            if let Some(value) = self.executor.get(&range_id, &target_key).await? {
                let record = decode_session_record(&value)?;
                self.cache_record(record.clone(), Some(range_id));
                return Ok(Some(record));
            }
            self.session_range_cache
                .write()
                .expect("sessiondict cache poisoned")
                .remove(session_id);
        }
        for descriptor in self.router.list().await? {
            if descriptor.shard.kind != ServiceShardKind::SessionDict {
                continue;
            }
            if let Some(value) = self.executor.get(&descriptor.id, &target_key).await? {
                let record = decode_session_record(&value)?;
                self.cache_record(record.clone(), Some(descriptor.id.clone()));
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>> {
        match tenant_id {
            Some(tenant_id) => self.scan_session_records_for_tenant(tenant_id).await,
            None => self.scan_all_session_records().await,
        }
    }

    async fn session_count(&self) -> anyhow::Result<usize> {
        Ok(self.scan_all_session_records().await?.len())
    }
}

#[async_trait]
impl Lifecycle for SessionDictHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Lifecycle for PersistentSessionDictHandle {
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        session_identity_shard, session_record_shard, session_scan_shard,
        PersistentSessionDictHandle, ReplicatedSessionDictHandle, SessionDictHandle,
        SessionDirectory, SessionDirectoryState, SessionKillListener, SessionOnlineCheckScheduler,
        SessionOwnerResolver, SessionRegistrationManager, SessionRegistrationState,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use greenmqtt_core::{
        ClientIdentity, RangeBoundary, RangeReplica, ReplicaRole, ReplicaSyncState,
        ReplicatedRangeDescriptor, ServiceShardKey, ServiceShardLifecycle, SessionKind,
        SessionRecord,
    };
    use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, MemoryKvRangeRouter};
    use greenmqtt_kv_engine::{
        KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, MemoryKvEngine,
    };
    use greenmqtt_storage::{MemorySessionStore, SessionStore};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    #[derive(Clone)]
    struct LocalKvRangeExecutor {
        engine: MemoryKvEngine,
    }

    #[async_trait]
    impl KvRangeExecutor for LocalKvRangeExecutor {
        async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            let range = self.engine.open_range(range_id).await?;
            range.reader().get(key).await
        }

        async fn scan(
            &self,
            range_id: &str,
            boundary: Option<RangeBoundary>,
            limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            let range = self.engine.open_range(range_id).await?;
            range
                .reader()
                .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
                .await
        }

        async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
            let range = self.engine.open_range(range_id).await?;
            range.writer().apply(mutations).await
        }

        async fn checkpoint(
            &self,
            range_id: &str,
            checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            let range = self.engine.open_range(range_id).await?;
            range.checkpoint(checkpoint_id).await
        }

        async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            let range = self.engine.open_range(range_id).await?;
            range.snapshot().await
        }
    }

    #[derive(Clone, Default)]
    struct RecordingKvRangeExecutor {
        engine: MemoryKvEngine,
        get_calls: Arc<AtomicUsize>,
        scan_calls: Arc<AtomicUsize>,
        scanned_boundaries: Arc<Mutex<Vec<Option<RangeBoundary>>>>,
    }

    #[async_trait]
    impl KvRangeExecutor for RecordingKvRangeExecutor {
        async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            let range = self.engine.open_range(range_id).await?;
            range.reader().get(key).await
        }

        async fn scan(
            &self,
            range_id: &str,
            boundary: Option<RangeBoundary>,
            limit: usize,
        ) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            self.scanned_boundaries
                .lock()
                .expect("executor poisoned")
                .push(boundary.clone());
            let range = self.engine.open_range(range_id).await?;
            range
                .reader()
                .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
                .await
        }

        async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
            let range = self.engine.open_range(range_id).await?;
            range.writer().apply(mutations).await
        }

        async fn checkpoint(
            &self,
            range_id: &str,
            checkpoint_id: &str,
        ) -> anyhow::Result<KvRangeCheckpoint> {
            let range = self.engine.open_range(range_id).await?;
            range.checkpoint(checkpoint_id).await
        }

        async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
            let range = self.engine.open_range(range_id).await?;
            range.snapshot().await
        }
    }

    #[derive(Default)]
    struct RecordingKillListener {
        notifications: Mutex<Vec<super::SessionKillNotification>>,
    }

    #[async_trait]
    impl SessionKillListener for RecordingKillListener {
        async fn on_kill(
            &self,
            notification: super::SessionKillNotification,
        ) -> anyhow::Result<()> {
            self.notifications
                .lock()
                .expect("kill listener poisoned")
                .push(notification);
            Ok(())
        }
    }

    #[derive(Default)]
    struct StaticOwnerResolver {
        endpoints: BTreeMap<u64, String>,
    }

    impl SessionOwnerResolver for StaticOwnerResolver {
        fn server_reference(&self, node_id: u64) -> Option<String> {
            self.endpoints.get(&node_id).cloned()
        }
    }

    #[tokio::test]
    async fn replacing_same_client_returns_previous_session() {
        let service = SessionDictHandle::default();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        let first = SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: identity.clone(),
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        };
        let second = SessionRecord {
            session_id: "s2".into(),
            node_id: 2,
            kind: SessionKind::Persistent,
            identity,
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        };

        assert!(service.register(first).await.unwrap().is_none());
        let replaced = service.register(second.clone()).await.unwrap();
        assert_eq!(replaced.unwrap().session_id, "s1");
        assert_eq!(
            service
                .lookup_identity(&second.identity)
                .await
                .unwrap()
                .unwrap()
                .session_id,
            "s2"
        );
    }

    #[tokio::test]
    async fn replicated_sessiondict_registers_looks_up_and_unregisters_sessions() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "session-range-t1".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "session-range-t1",
                session_scan_shard("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let service = ReplicatedSessionDictHandle::new(
            router,
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );

        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let record = SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: identity.clone(),
            session_expiry_interval_secs: Some(60),
            expires_at_ms: Some(1234),
        };
        assert!(service.register(record.clone()).await.unwrap().is_none());
        assert_eq!(
            service
                .lookup_identity(&identity)
                .await
                .unwrap()
                .unwrap()
                .session_id,
            "s1"
        );
        assert_eq!(
            service.lookup_session("s1").await.unwrap().unwrap().node_id,
            1
        );
        assert_eq!(service.list_sessions(Some("t1")).await.unwrap().len(), 1);
        assert_eq!(service.session_count().await.unwrap(), 1);

        let removed = service.unregister("s1").await.unwrap().unwrap();
        assert_eq!(removed.session_id, "s1");
        assert!(service.lookup_identity(&identity).await.unwrap().is_none());
        assert!(service.lookup_session("s1").await.unwrap().is_none());
        assert_eq!(service.session_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn replicated_sessiondict_reapplying_same_record_keeps_single_session() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "session-range-idem".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "session-range-idem",
                session_scan_shard("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let service = ReplicatedSessionDictHandle::new(
            router,
            Arc::new(LocalKvRangeExecutor {
                engine: engine.clone(),
            }),
        );
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let record = SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity,
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        };

        service.register(record.clone()).await.unwrap();
        service.register(record).await.unwrap();

        assert_eq!(service.list_sessions(Some("t1")).await.unwrap().len(), 1);
        assert_eq!(service.session_count().await.unwrap(), 1);
    }

    #[test]
    fn sessiondict_helpers_map_point_and_scan_apis_to_shards() {
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let record = SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: identity.clone(),
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        };

        assert_eq!(
            session_record_shard(&record),
            ServiceShardKey::sessiondict("t1", "s1")
        );
        assert_eq!(
            session_identity_shard(&identity),
            ServiceShardKey {
                kind: greenmqtt_core::ServiceShardKind::SessionDict,
                tenant_id: "t1".into(),
                scope: "identity:u1:c1".into(),
            }
        );
        assert_eq!(
            session_scan_shard("t1"),
            ServiceShardKey {
                kind: greenmqtt_core::ServiceShardKind::SessionDict,
                tenant_id: "t1".into(),
                scope: "*".into(),
            }
        );
    }

    #[tokio::test]
    async fn persistent_sessiondict_restores_from_store() {
        let store = Arc::new(MemorySessionStore::default());
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let reopened = PersistentSessionDictHandle::open(store).await.unwrap();
        assert_eq!(
            reopened
                .lookup_identity(&identity)
                .await
                .unwrap()
                .unwrap()
                .session_id,
            "s1"
        );
    }

    #[tokio::test]
    async fn persistent_unregister_removes_store_record_without_cached_session() {
        let store = Arc::new(MemorySessionStore::default());
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let writer = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        writer
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let cold = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let removed = cold.unregister("s1").await.unwrap().unwrap();
        assert_eq!(removed.session_id, "s1");
        assert!(store
            .load_session_by_session_id("s1")
            .await
            .unwrap()
            .is_none());
        let reopened = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        assert!(reopened.lookup_identity(&identity).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn persistent_register_returns_replaced_session_without_cached_identity() {
        let store = Arc::new(MemorySessionStore::default());
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let writer = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        writer
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let cold = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let replaced = cold
            .register(SessionRecord {
                session_id: "s2".into(),
                node_id: 2,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replaced.session_id, "s1");
        assert_eq!(
            store
                .load_session("t1", "c1")
                .await
                .unwrap()
                .unwrap()
                .session_id,
            "s2"
        );
    }

    #[tokio::test]
    async fn persistent_sessiondict_refreshes_identity_from_shared_store() {
        let store = Arc::new(MemorySessionStore::default());
        let first = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let second = PersistentSessionDictHandle::open(store).await.unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        first
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        assert_eq!(
            second
                .lookup_identity(&identity)
                .await
                .unwrap()
                .unwrap()
                .session_id,
            "s1"
        );
    }

    #[tokio::test]
    async fn session_registration_unregisters_on_stop() {
        let directory: Arc<dyn SessionDirectory> = Arc::new(SessionDictHandle::default());
        let listener = Arc::new(RecordingKillListener::default());
        let registration = SessionRegistrationManager::new(directory.clone())
            .with_poll_interval(Duration::from_millis(5))
            .register(
                SessionRecord {
                    session_id: "s1".into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: "t1".into(),
                        user_id: "u1".into(),
                        client_id: "c1".into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                },
                listener.clone(),
            )
            .await
            .unwrap();

        assert_eq!(registration.state(), SessionRegistrationState::Registered);
        registration.stop().await.unwrap();
        assert_eq!(registration.state(), SessionRegistrationState::Stopped);
        assert!(directory.lookup_session("s1").await.unwrap().is_none());
        assert!(listener
            .notifications
            .lock()
            .expect("kill listener poisoned")
            .is_empty());
    }

    #[tokio::test]
    async fn session_registration_notifies_when_replaced_by_new_owner() {
        let directory: Arc<dyn SessionDirectory> = Arc::new(SessionDictHandle::default());
        let listener = Arc::new(RecordingKillListener::default());
        let registration = SessionRegistrationManager::new(directory.clone())
            .with_poll_interval(Duration::from_millis(5))
            .register(
                SessionRecord {
                    session_id: "s1".into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: "t1".into(),
                        user_id: "u1".into(),
                        client_id: "c1".into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                },
                listener.clone(),
            )
            .await
            .unwrap();

        directory
            .register(SessionRecord {
                session_id: "s2".into(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let notifications = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notifications = listener
                    .notifications
                    .lock()
                    .expect("kill listener poisoned")
                    .clone();
                if !notifications.is_empty() {
                    break notifications;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(registration.state(), SessionRegistrationState::Kicked);
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].requested.session_id, "s1");
        assert_eq!(
            notifications[0]
                .current_owner
                .as_ref()
                .expect("replacement owner")
                .session_id,
            "s2"
        );
        assert!(notifications[0].server_reference.is_none());
        registration.stop().await.unwrap();
    }

    #[tokio::test]
    async fn session_registration_resolves_server_reference_for_remote_owner() {
        let directory: Arc<dyn SessionDirectory> = Arc::new(SessionDictHandle::default());
        let listener = Arc::new(RecordingKillListener::default());
        let resolver = Arc::new(StaticOwnerResolver {
            endpoints: BTreeMap::from([(7, "http://127.0.0.1:50077".to_string())]),
        });
        let registration = SessionRegistrationManager::new(directory.clone())
            .with_poll_interval(Duration::from_millis(5))
            .with_owner_resolver(resolver)
            .register(
                SessionRecord {
                    session_id: "s1".into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: "t1".into(),
                        user_id: "u1".into(),
                        client_id: "c1".into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                },
                listener.clone(),
            )
            .await
            .unwrap();

        directory
            .register(SessionRecord {
                session_id: "s2".into(),
                node_id: 7,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let notifications = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notifications = listener
                    .notifications
                    .lock()
                    .expect("kill listener poisoned")
                    .clone();
                if !notifications.is_empty() {
                    break notifications;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(notifications[0].server_reference.as_deref(), Some("http://127.0.0.1:50077"));
        registration.stop().await.unwrap();
    }

    #[tokio::test]
    async fn list_sessions_can_filter_by_tenant() {
        let service = SessionDictHandle::default();
        for (tenant_id, session_id) in [("t1", "s1"), ("t2", "s2"), ("t1", "s3")] {
            service
                .register(SessionRecord {
                    session_id: session_id.into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: tenant_id.into(),
                        user_id: "u".into(),
                        client_id: session_id.into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                })
                .await
                .unwrap();
        }

        let filtered = service.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(filtered.len(), 2);
        assert!(filtered
            .iter()
            .all(|record| record.identity.tenant_id == "t1"));

        let all = service.list_sessions(None).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn tenant_index_tracks_replace_and_unregister() {
        let service = SessionDictHandle::default();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s2".into(),
                node_id: 2,
                kind: SessionKind::Persistent,
                identity,
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let tenant_records = service.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(tenant_records.len(), 1);
        assert_eq!(tenant_records[0].session_id, "s2");

        let removed = service.unregister("s2").await.unwrap().unwrap();
        assert_eq!(removed.session_id, "s2");
        assert!(service.list_sessions(Some("t1")).await.unwrap().is_empty());
    }

    #[derive(Default)]
    struct CountingDirectory {
        records: Arc<RwLock<Vec<SessionRecord>>>,
        list_all_calls: AtomicUsize,
        list_tenant_calls: AtomicUsize,
    }

    #[async_trait]
    impl SessionDirectory for CountingDirectory {
        async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>> {
            self.records.write().expect("directory poisoned").push(record);
            Ok(None)
        }

        async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
            let mut guard = self.records.write().expect("directory poisoned");
            let index = guard.iter().position(|record| record.session_id == session_id);
            Ok(index.map(|index| guard.remove(index)))
        }

        async fn lookup_identity(
            &self,
            identity: &ClientIdentity,
        ) -> anyhow::Result<Option<SessionRecord>> {
            Ok(self
                .records
                .read()
                .expect("directory poisoned")
                .iter()
                .find(|record| &record.identity == identity)
                .cloned())
        }

        async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>> {
            Ok(self
                .records
                .read()
                .expect("directory poisoned")
                .iter()
                .find(|record| record.session_id == session_id)
                .cloned())
        }

        async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>> {
            match tenant_id {
                Some(tenant_id) => {
                    self.list_tenant_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(self
                        .records
                        .read()
                        .expect("directory poisoned")
                        .iter()
                        .filter(|record| record.identity.tenant_id == tenant_id)
                        .cloned()
                        .collect())
                }
                None => {
                    self.list_all_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(self.records.read().expect("directory poisoned").clone())
                }
            }
        }

        async fn session_count(&self) -> anyhow::Result<usize> {
            Ok(self.records.read().expect("directory poisoned").len())
        }
    }

    #[tokio::test]
    async fn session_online_check_scheduler_batches_session_checks_via_single_global_list() {
        let directory = Arc::new(CountingDirectory::default());
        for (tenant_id, session_id) in [("t1", "s1"), ("t2", "s2")] {
            directory
                .register(SessionRecord {
                    session_id: session_id.into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: tenant_id.into(),
                        user_id: format!("u-{session_id}"),
                        client_id: format!("c-{session_id}"),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                })
                .await
                .unwrap();
        }
        let scheduler = SessionOnlineCheckScheduler::new(directory.clone());
        let checks = scheduler
            .check_sessions(&["s1".into(), "missing".into(), "s2".into()])
            .await
            .unwrap();
        assert_eq!(checks["s1"], true);
        assert_eq!(checks["s2"], true);
        assert_eq!(checks["missing"], false);
        assert_eq!(directory.list_all_calls.load(Ordering::SeqCst), 1);
        assert_eq!(directory.list_tenant_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn session_online_check_scheduler_batches_identity_checks_per_tenant() {
        let directory = Arc::new(CountingDirectory::default());
        for (tenant_id, user_id, client_id, session_id) in [
            ("t1", "u1", "c1", "s1"),
            ("t1", "u2", "c2", "s2"),
            ("t2", "u3", "c3", "s3"),
        ] {
            directory
                .register(SessionRecord {
                    session_id: session_id.into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: tenant_id.into(),
                        user_id: user_id.into(),
                        client_id: client_id.into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                })
                .await
                .unwrap();
        }
        let scheduler = SessionOnlineCheckScheduler::new(directory.clone());
        let checks = scheduler
            .check_identities(&[
                ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "missing".into(),
                    client_id: "c9".into(),
                },
                ClientIdentity {
                    tenant_id: "t2".into(),
                    user_id: "u3".into(),
                    client_id: "c3".into(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(checks[&("t1".into(), "u1".into(), "c1".into())], true);
        assert_eq!(checks[&("t1".into(), "missing".into(), "c9".into())], false);
        assert_eq!(checks[&("t2".into(), "u3".into(), "c3".into())], true);
        assert_eq!(directory.list_all_calls.load(Ordering::SeqCst), 0);
        assert_eq!(directory.list_tenant_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn persistent_sessiondict_recovers_large_tenant_population() {
        let store = Arc::new(MemorySessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        for index in 0..256u32 {
            service
                .register(SessionRecord {
                    session_id: format!("s-{index}"),
                    node_id: u64::from(index % 3),
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: "t-large".into(),
                        user_id: format!("u-{index}"),
                        client_id: format!("c-{index}"),
                    },
                    session_expiry_interval_secs: Some(60),
                    expires_at_ms: Some(1_000 + u64::from(index)),
                })
                .await
                .unwrap();
        }

        let reopened = PersistentSessionDictHandle::open(store).await.unwrap();
        let records = reopened.list_sessions(Some("t-large")).await.unwrap();
        assert_eq!(records.len(), 256);
        assert_eq!(reopened.session_count().await.unwrap(), 256);
        assert!(reopened.lookup_session("s-42").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn replicated_sessiondict_lookup_session_uses_exact_get_not_full_scan() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "session-range-get".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "session-range-get",
                session_scan_shard("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let executor = Arc::new(RecordingKvRangeExecutor {
            engine: engine.clone(),
            ..RecordingKvRangeExecutor::default()
        });
        let service = ReplicatedSessionDictHandle::new(router.clone(), executor.clone());
        let record = SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        };
        service.register(record).await.unwrap();
        executor.get_calls.store(0, Ordering::SeqCst);
        executor.scan_calls.store(0, Ordering::SeqCst);
        let cold = ReplicatedSessionDictHandle::new(router, executor.clone());

        let loaded = cold.lookup_session("s1").await.unwrap().unwrap();
        assert_eq!(loaded.session_id, "s1");
        assert_eq!(executor.scan_calls.load(Ordering::SeqCst), 0);
        assert!(executor.get_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn replicated_sessiondict_lookup_session_reuses_cached_range_after_first_hit() {
        let engine = MemoryKvEngine::default();
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: "session-range-cache".into(),
                boundary: RangeBoundary::full(),
            })
            .await
            .unwrap();
        let router = Arc::new(MemoryKvRangeRouter::default());
        router
            .upsert(ReplicatedRangeDescriptor::new(
                "session-range-cache",
                session_scan_shard("t1"),
                RangeBoundary::full(),
                1,
                1,
                Some(1),
                vec![RangeReplica::new(
                    1,
                    ReplicaRole::Voter,
                    ReplicaSyncState::Replicating,
                )],
                0,
                0,
                ServiceShardLifecycle::Serving,
            ))
            .await
            .unwrap();
        let executor = Arc::new(RecordingKvRangeExecutor {
            engine: engine.clone(),
            ..RecordingKvRangeExecutor::default()
        });
        let service = ReplicatedSessionDictHandle::new(router, executor.clone());
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let _ = service.lookup_session("s1").await.unwrap().unwrap();
        executor.get_calls.store(0, Ordering::SeqCst);
        let _ = service.lookup_session("s1").await.unwrap().unwrap();
        assert_eq!(executor.get_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn tenant_refresh_marks_removed_session_ids_as_loaded_missing() {
        let mut state = SessionDirectoryState::default();
        state.insert_record(SessionRecord {
            session_id: "s1".into(),
            node_id: 1,
            kind: SessionKind::Persistent,
            identity: ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u1".into(),
                client_id: "c1".into(),
            },
            session_expiry_interval_secs: None,
            expires_at_ms: None,
        });

        state.replace_tenant_records("t1", Vec::new());

        assert!(!state.by_session.contains_key("s1"));
        assert!(state.loaded_session_ids.contains("s1"));
    }

    #[derive(Default)]
    struct CountingSessionStore {
        inner: MemorySessionStore,
        load_session_calls: AtomicUsize,
        load_session_by_session_id_calls: AtomicUsize,
        list_sessions_calls: AtomicUsize,
        list_tenant_sessions_calls: AtomicUsize,
        count_sessions_calls: AtomicUsize,
    }

    impl CountingSessionStore {
        fn load_session_calls(&self) -> usize {
            self.load_session_calls.load(Ordering::SeqCst)
        }

        fn load_session_by_session_id_calls(&self) -> usize {
            self.load_session_by_session_id_calls.load(Ordering::SeqCst)
        }

        fn list_sessions_calls(&self) -> usize {
            self.list_sessions_calls.load(Ordering::SeqCst)
        }

        fn list_tenant_sessions_calls(&self) -> usize {
            self.list_tenant_sessions_calls.load(Ordering::SeqCst)
        }

        fn count_sessions_calls(&self) -> usize {
            self.count_sessions_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SessionStore for CountingSessionStore {
        async fn save_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
            self.inner.save_session(session).await
        }

        async fn load_session(
            &self,
            tenant_id: &str,
            client_id: &str,
        ) -> anyhow::Result<Option<SessionRecord>> {
            self.load_session_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.load_session(tenant_id, client_id).await
        }

        async fn load_session_by_session_id(
            &self,
            session_id: &str,
        ) -> anyhow::Result<Option<SessionRecord>> {
            self.load_session_by_session_id_calls
                .fetch_add(1, Ordering::SeqCst);
            self.inner.load_session_by_session_id(session_id).await
        }

        async fn delete_session(&self, tenant_id: &str, client_id: &str) -> anyhow::Result<()> {
            self.inner.delete_session(tenant_id, client_id).await
        }

        async fn list_sessions(&self) -> anyhow::Result<Vec<SessionRecord>> {
            self.list_sessions_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_sessions().await
        }

        async fn list_tenant_sessions(
            &self,
            tenant_id: &str,
        ) -> anyhow::Result<Vec<SessionRecord>> {
            self.list_tenant_sessions_calls
                .fetch_add(1, Ordering::SeqCst);
            self.inner.list_tenant_sessions(tenant_id).await
        }

        async fn count_sessions(&self) -> anyhow::Result<usize> {
            self.count_sessions_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.count_sessions().await
        }
    }

    #[tokio::test]
    async fn persistent_lookup_session_uses_cached_index_before_listing_store() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity,
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let before = store.list_sessions_calls();
        let record = service.lookup_session("s1").await.unwrap().unwrap();
        assert_eq!(record.session_id, "s1");
        assert_eq!(store.list_sessions_calls(), before);
    }

    #[tokio::test]
    async fn persistent_sessiondict_open_is_lazy() {
        let store = Arc::new(CountingSessionStore::default());
        let _service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        assert_eq!(store.list_sessions_calls(), 0);
        assert_eq!(store.count_sessions_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_sessiondict_session_count_uses_store_count() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        assert_eq!(service.session_count().await.unwrap(), 1);
        assert_eq!(store.count_sessions_calls(), 1);
        assert_eq!(store.list_sessions_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_sessiondict_session_count_reuses_warm_global_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let warmed = service.list_sessions(None).await.unwrap();
        assert_eq!(warmed.len(), 1);
        assert_eq!(store.list_sessions_calls(), 1);

        let count_calls_before = store.count_sessions_calls();
        assert_eq!(service.session_count().await.unwrap(), 1);
        assert_eq!(store.count_sessions_calls(), count_calls_before);
    }

    #[tokio::test]
    async fn persistent_list_sessions_with_tenant_uses_store_tenant_lookup() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        for (tenant_id, client_id, session_id) in [("t1", "c1", "s1"), ("t2", "c2", "s2")] {
            service
                .register(SessionRecord {
                    session_id: session_id.into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: tenant_id.into(),
                        user_id: "u1".into(),
                        client_id: client_id.into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                })
                .await
                .unwrap();
        }

        let records = service.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].session_id, "s1");
        assert_eq!(store.list_tenant_sessions_calls(), 1);
        assert_eq!(store.list_sessions_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_list_sessions_with_tenant_reuses_warm_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        for (tenant_id, client_id, session_id) in [("t1", "c1", "s1"), ("t1", "c2", "s2")] {
            service
                .register(SessionRecord {
                    session_id: session_id.into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: tenant_id.into(),
                        user_id: "u1".into(),
                        client_id: client_id.into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                })
                .await
                .unwrap();
        }

        let first = service.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(store.list_tenant_sessions_calls(), 1);

        let second = service.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(store.list_tenant_sessions_calls(), 1);
        assert_eq!(store.list_sessions_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_list_sessions_reuses_warm_global_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        for (tenant_id, client_id, session_id) in [("t1", "c1", "s1"), ("t2", "c2", "s2")] {
            service
                .register(SessionRecord {
                    session_id: session_id.into(),
                    node_id: 1,
                    kind: SessionKind::Persistent,
                    identity: ClientIdentity {
                        tenant_id: tenant_id.into(),
                        user_id: "u1".into(),
                        client_id: client_id.into(),
                    },
                    session_expiry_interval_secs: None,
                    expires_at_ms: None,
                })
                .await
                .unwrap();
        }

        let first = service.list_sessions(None).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(store.list_sessions_calls(), 1);

        let second = service.list_sessions(None).await.unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(store.list_sessions_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_lookup_identity_reuses_warm_tenant_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let warmed = service.list_sessions(Some("t1")).await.unwrap();
        assert_eq!(warmed.len(), 1);
        assert_eq!(store.list_tenant_sessions_calls(), 1);

        let load_calls_before = store.load_session_calls();
        let record = service.lookup_identity(&identity).await.unwrap().unwrap();
        assert_eq!(record.session_id, "s1");
        assert_eq!(store.load_session_calls(), load_calls_before);

        let missing = service
            .lookup_identity(&ClientIdentity {
                tenant_id: "t1".into(),
                user_id: "u2".into(),
                client_id: "missing".into(),
            })
            .await
            .unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_calls(), load_calls_before);
    }

    #[tokio::test]
    async fn persistent_lookup_identity_reuses_loaded_identity_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let load_calls_before = store.load_session_calls();
        let first = service.lookup_identity(&identity).await.unwrap();
        let second = service.lookup_identity(&identity).await.unwrap();
        assert!(first.is_some());
        assert!(second.is_some());
        assert_eq!(store.load_session_calls(), load_calls_before);
    }

    #[tokio::test]
    async fn persistent_missing_lookup_identity_reuses_loaded_identity_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "missing".into(),
        };

        let first = service.lookup_identity(&identity).await.unwrap();
        let second = service.lookup_identity(&identity).await.unwrap();
        assert!(first.is_none());
        assert!(second.is_none());
        assert_eq!(store.load_session_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_register_skips_store_lookup_for_loaded_missing_identity() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        let missing = service.lookup_identity(&identity).await.unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_calls(), 1);

        let replaced = service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();
        assert!(replaced.is_none());
        assert_eq!(store.load_session_calls(), 1);
        assert_eq!(
            service
                .lookup_identity(&identity)
                .await
                .unwrap()
                .unwrap()
                .session_id,
            "s1"
        );
    }

    #[tokio::test]
    async fn persistent_identity_replace_marks_old_session_id_as_loaded_missing() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s2".into(),
                node_id: 2,
                kind: SessionKind::Persistent,
                identity,
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let missing = service.lookup_session("s1").await.unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_by_session_id_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_lookup_session_missing_reuses_warm_global_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let warmed = service.list_sessions(None).await.unwrap();
        assert_eq!(warmed.len(), 1);
        assert_eq!(store.list_sessions_calls(), 1);

        let load_calls_before = store.load_session_by_session_id_calls();
        let missing = service.lookup_session("missing").await.unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_by_session_id_calls(), load_calls_before);
    }

    #[tokio::test]
    async fn persistent_lookup_session_reuses_loaded_session_id_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: ClientIdentity {
                    tenant_id: "t1".into(),
                    user_id: "u1".into(),
                    client_id: "c1".into(),
                },
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let first = service.lookup_session("s1").await.unwrap();
        let second = service.lookup_session("s1").await.unwrap();
        assert!(first.is_some());
        assert!(second.is_some());
        assert_eq!(store.load_session_by_session_id_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_missing_lookup_session_reuses_loaded_session_id_cache() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();

        let first = service.lookup_session("missing").await.unwrap();
        let second = service.lookup_session("missing").await.unwrap();
        assert!(first.is_none());
        assert!(second.is_none());
        assert_eq!(store.load_session_by_session_id_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_unregister_skips_store_lookup_for_loaded_missing_session() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();

        let missing = service.lookup_session("missing").await.unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_by_session_id_calls(), 1);

        let removed = service.unregister("missing").await.unwrap();
        assert!(removed.is_none());
        assert_eq!(store.load_session_by_session_id_calls(), 1);
    }

    #[tokio::test]
    async fn persistent_unregister_marks_removed_session_id_as_loaded_missing() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };

        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity,
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let removed = service.unregister("s1").await.unwrap();
        assert!(removed.is_some());
        assert_eq!(store.load_session_by_session_id_calls(), 0);

        let missing = service.lookup_session("s1").await.unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_by_session_id_calls(), 0);
    }

    #[tokio::test]
    async fn persistent_session_id_reuse_marks_old_identity_as_loaded_missing() {
        let store = Arc::new(CountingSessionStore::default());
        let service = PersistentSessionDictHandle::open(store.clone())
            .await
            .unwrap();
        let first_identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            client_id: "c1".into(),
        };
        let second_identity = ClientIdentity {
            tenant_id: "t1".into(),
            user_id: "u2".into(),
            client_id: "c2".into(),
        };

        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 1,
                kind: SessionKind::Persistent,
                identity: first_identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();
        service
            .register(SessionRecord {
                session_id: "s1".into(),
                node_id: 2,
                kind: SessionKind::Persistent,
                identity: second_identity.clone(),
                session_expiry_interval_secs: None,
                expires_at_ms: None,
            })
            .await
            .unwrap();

        let load_calls_before = store.load_session_calls();
        let missing = service.lookup_identity(&first_identity).await.unwrap();
        assert!(missing.is_none());
        assert_eq!(store.load_session_calls(), load_calls_before);
        let current = service.lookup_identity(&second_identity).await.unwrap();
        assert_eq!(current.unwrap().session_id, "s1");
    }
}
