use async_trait::async_trait;
use greenmqtt_core::{ClientIdentity, Lifecycle, ServiceShardKey, ServiceShardKind, SessionRecord};
use greenmqtt_storage::SessionStore;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

#[async_trait]
pub trait SessionDirectory: Send + Sync {
    async fn register(&self, record: SessionRecord) -> anyhow::Result<Option<SessionRecord>>;
    async fn unregister(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>>;
    async fn lookup_identity(
        &self,
        identity: &ClientIdentity,
    ) -> anyhow::Result<Option<SessionRecord>>;
    async fn lookup_session(&self, session_id: &str) -> anyhow::Result<Option<SessionRecord>>;
    async fn list_sessions(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<SessionRecord>>;
    async fn session_count(&self) -> anyhow::Result<usize>;
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
        PersistentSessionDictHandle, SessionDictHandle, SessionDirectory, SessionDirectoryState,
    };
    use async_trait::async_trait;
    use greenmqtt_core::{ClientIdentity, ServiceShardKey, SessionKind, SessionRecord};
    use greenmqtt_storage::{MemorySessionStore, SessionStore};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
