use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use sysinfo::System;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum PressureLevel {
    Normal = 0,
    Warning = 1,
    Critical = 2,
    Emergency = 3,
}

impl PressureLevel {
    fn from_raw(value: u8) -> Self {
        match value {
            1 => Self::Warning,
            2 => Self::Critical,
            3 => Self::Emergency,
            _ => Self::Normal,
        }
    }

    fn as_raw(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy)]
struct PressureThresholds {
    warning_percent: u8,
    critical_percent: u8,
    emergency_percent: u8,
}

impl Default for PressureThresholds {
    fn default() -> Self {
        Self {
            warning_percent: 70,
            critical_percent: 80,
            emergency_percent: 90,
        }
    }
}

impl PressureThresholds {
    fn classify(self, used_percent: u64) -> PressureLevel {
        if used_percent >= u64::from(self.emergency_percent) {
            PressureLevel::Emergency
        } else if used_percent >= u64::from(self.critical_percent) {
            PressureLevel::Critical
        } else if used_percent >= u64::from(self.warning_percent) {
            PressureLevel::Warning
        } else {
            PressureLevel::Normal
        }
    }
}

pub struct MemoryPressureGuard {
    sample_interval: Duration,
    thresholds: PressureThresholds,
    current_level: Arc<AtomicU8>,
    stop_requested: Arc<AtomicBool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Default for MemoryPressureGuard {
    fn default() -> Self {
        Self::new(Duration::from_secs(2), PressureThresholds::default())
    }
}

impl MemoryPressureGuard {
    fn new(sample_interval: Duration, thresholds: PressureThresholds) -> Self {
        Self {
            sample_interval,
            thresholds,
            current_level: Arc::new(AtomicU8::new(PressureLevel::Normal.as_raw())),
            stop_requested: Arc::new(AtomicBool::new(false)),
            task: Mutex::new(None),
        }
    }

    pub fn current_level(&self) -> PressureLevel {
        PressureLevel::from_raw(self.current_level.load(Ordering::Relaxed))
    }

    pub fn rejects_new_connections(&self) -> bool {
        matches!(
            self.current_level(),
            PressureLevel::Critical | PressureLevel::Emergency
        )
    }

    pub async fn start(&self) {
        let mut task = self.task.lock().await;
        if task.is_some() {
            return;
        }

        self.stop_requested.store(false, Ordering::Relaxed);
        let current_level = Arc::clone(&self.current_level);
        let stop_requested = Arc::clone(&self.stop_requested);
        let sample_interval = self.sample_interval;
        let thresholds = self.thresholds;

        *task = Some(tokio::spawn(async move {
            let mut system = System::new();
            loop {
                let level = sample_pressure_level(&mut system, thresholds);
                current_level.store(level.as_raw(), Ordering::Relaxed);

                if stop_requested.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(sample_interval).await;
            }
        }));
    }

    pub async fn stop(&self) {
        self.stop_requested.store(true, Ordering::Relaxed);
        let mut task = self.task.lock().await;
        if let Some(handle) = task.take() {
            let _ = handle.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn force_level(&self, level: PressureLevel) {
        self.current_level.store(level.as_raw(), Ordering::Relaxed);
    }
}

fn sample_pressure_level(system: &mut System, thresholds: PressureThresholds) -> PressureLevel {
    system.refresh_memory();
    let total = system.total_memory();
    if total == 0 {
        return PressureLevel::Normal;
    }
    let used_percent = system.used_memory().saturating_mul(100) / total;
    thresholds.classify(used_percent)
}

#[cfg(test)]
mod tests {
    use super::{MemoryPressureGuard, PressureLevel, PressureThresholds};
    use tokio::time::Duration;

    #[test]
    fn thresholds_classify_expected_levels() {
        let thresholds = PressureThresholds::default();
        assert_eq!(thresholds.classify(69), PressureLevel::Normal);
        assert_eq!(thresholds.classify(70), PressureLevel::Warning);
        assert_eq!(thresholds.classify(79), PressureLevel::Warning);
        assert_eq!(thresholds.classify(80), PressureLevel::Critical);
        assert_eq!(thresholds.classify(89), PressureLevel::Critical);
        assert_eq!(thresholds.classify(90), PressureLevel::Emergency);
    }

    #[test]
    fn guard_rejects_only_critical_and_emergency() {
        let guard = MemoryPressureGuard::new(Duration::from_secs(2), PressureThresholds::default());
        guard.force_level(PressureLevel::Normal);
        assert!(!guard.rejects_new_connections());
        guard.force_level(PressureLevel::Warning);
        assert!(!guard.rejects_new_connections());
        guard.force_level(PressureLevel::Critical);
        assert!(guard.rejects_new_connections());
        guard.force_level(PressureLevel::Emergency);
        assert!(guard.rejects_new_connections());
    }
}
