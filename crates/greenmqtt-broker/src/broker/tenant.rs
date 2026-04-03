use dashmap::DashMap;
use greenmqtt_core::{TenantQuota, TenantUsage, TenantUsageSnapshot};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::session_mgr::now_millis;

#[derive(Debug)]
struct TenantState {
    quota: TenantQuota,
    usage: TenantUsage,
    rate_window_started_ms: AtomicU64,
}

impl TenantState {
    fn new(quota: TenantQuota) -> Self {
        Self {
            quota,
            usage: TenantUsage::default(),
            rate_window_started_ms: AtomicU64::new(0),
        }
    }
}

#[derive(Default)]
pub struct TenantResourceManager {
    tenants: DashMap<String, Arc<TenantState>>,
}

pub struct TenantConnectionReservation {
    manager: Arc<TenantResourceManager>,
    tenant_id: String,
    committed: bool,
}

impl TenantConnectionReservation {
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for TenantConnectionReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.manager.release_connection(&self.tenant_id);
        }
    }
}

impl TenantResourceManager {
    pub fn set_quota(&self, tenant_id: impl Into<String>, quota: TenantQuota) {
        self.tenants
            .insert(tenant_id.into(), Arc::new(TenantState::new(quota)));
    }

    pub fn quota(&self, tenant_id: &str) -> Option<TenantQuota> {
        self.tenant(tenant_id).map(|state| state.quota.clone())
    }

    fn tenant(&self, tenant_id: &str) -> Option<Arc<TenantState>> {
        self.tenants
            .get(tenant_id)
            .map(|entry| Arc::clone(entry.value()))
    }

    pub fn check_connection(&self, tenant_id: &str) -> anyhow::Result<()> {
        let Some(state) = self.tenant(tenant_id) else {
            return Ok(());
        };
        let previous = state.usage.connections.fetch_add(1, Ordering::SeqCst);
        if previous + 1 > state.quota.max_connections {
            state.usage.connections.fetch_sub(1, Ordering::SeqCst);
            anyhow::bail!("tenant quota exceeded");
        }
        Ok(())
    }

    pub fn reserve_connection(
        self: &Arc<Self>,
        tenant_id: &str,
    ) -> anyhow::Result<TenantConnectionReservation> {
        self.check_connection(tenant_id)?;
        Ok(TenantConnectionReservation {
            manager: Arc::clone(self),
            tenant_id: tenant_id.to_string(),
            committed: false,
        })
    }

    pub fn release_connection(&self, tenant_id: &str) {
        if let Some(state) = self.tenant(tenant_id) {
            state.usage.connections.fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn check_publish(&self, tenant_id: &str) -> anyhow::Result<()> {
        let Some(state) = self.tenant(tenant_id) else {
            return Ok(());
        };
        let now = now_millis();
        let window_started = state.rate_window_started_ms.load(Ordering::SeqCst);
        if window_started == 0 || now.saturating_sub(window_started) >= 1_000 {
            state.rate_window_started_ms.store(now, Ordering::SeqCst);
            state.usage.msg_rate.store(0, Ordering::SeqCst);
        }
        let previous = state.usage.msg_rate.fetch_add(1, Ordering::SeqCst);
        if previous + 1 > state.quota.max_msg_per_sec {
            state.usage.msg_rate.fetch_sub(1, Ordering::SeqCst);
            anyhow::bail!("tenant quota exceeded");
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn track_memory(&self, tenant_id: &str, delta: i64) {
        let Some(state) = self.tenant(tenant_id) else {
            return;
        };
        if delta >= 0 {
            state
                .usage
                .memory_bytes
                .fetch_add(delta as u64, Ordering::SeqCst);
        } else {
            let amount = (-delta) as u64;
            let _ = state.usage.memory_bytes.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |current| Some(current.saturating_sub(amount)),
            );
        }
    }

    #[allow(dead_code)]
    pub fn usage(&self, tenant_id: &str) -> Option<TenantUsageSnapshot> {
        self.tenant(tenant_id).map(|state| state.usage.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::TenantResourceManager;
    use greenmqtt_core::TenantQuota;

    fn quota() -> TenantQuota {
        TenantQuota {
            max_connections: 1,
            max_subscriptions: 10,
            max_msg_per_sec: 2,
            max_memory_bytes: 1024,
        }
    }

    #[test]
    fn connection_quota_rejects_second_connection() {
        let manager = TenantResourceManager::default();
        manager.set_quota("t1", quota());
        assert!(manager.check_connection("t1").is_ok());
        assert!(manager.check_connection("t1").is_err());
        manager.release_connection("t1");
        assert!(manager.check_connection("t1").is_ok());
    }

    #[test]
    fn publish_quota_rejects_when_window_is_exhausted() {
        let manager = TenantResourceManager::default();
        manager.set_quota("t1", quota());
        assert!(manager.check_publish("t1").is_ok());
        assert!(manager.check_publish("t1").is_ok());
        assert!(manager.check_publish("t1").is_err());
    }

    #[test]
    fn memory_tracking_is_saturating() {
        let manager = TenantResourceManager::default();
        manager.set_quota("t1", quota());
        manager.track_memory("t1", 512);
        manager.track_memory("t1", -1024);
        let usage = manager.usage("t1").unwrap();
        assert_eq!(usage.memory_bytes, 0);
    }
}
