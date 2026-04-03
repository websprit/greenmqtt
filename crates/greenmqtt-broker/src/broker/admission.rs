use super::pressure::{MemoryPressureGuard, PressureLevel};
use std::sync::Arc;
use tokio::time::Duration;

pub(crate) struct AdmissionController {
    memory_pressure: Arc<MemoryPressureGuard>,
    max_online_sessions: Option<usize>,
    connect_slowdown_threshold: Option<usize>,
    connect_slowdown_delay: Option<Duration>,
    connect_shaping_delay: Option<Duration>,
}

impl Default for AdmissionController {
    fn default() -> Self {
        Self {
            memory_pressure: Arc::new(MemoryPressureGuard::default()),
            max_online_sessions: None,
            connect_slowdown_threshold: None,
            connect_slowdown_delay: None,
            connect_shaping_delay: None,
        }
    }
}

impl AdmissionController {
    pub(crate) fn set_max_online_sessions(&mut self, limit: usize) {
        self.max_online_sessions = Some(limit);
    }

    pub(crate) fn connect_pressure_exceeded(&self, local_online_sessions: usize) -> bool {
        self.max_online_sessions
            .is_some_and(|limit| local_online_sessions >= limit)
            || self.memory_pressure.rejects_new_connections()
    }

    pub(crate) fn set_connection_slowdown(&mut self, threshold: usize, delay: Duration) {
        self.connect_slowdown_threshold = Some(threshold);
        self.connect_slowdown_delay = Some(delay);
    }

    pub(crate) fn connect_slowdown_delay(&self, local_online_sessions: usize) -> Option<Duration> {
        if self.connect_pressure_exceeded(local_online_sessions) {
            return None;
        }
        let threshold = self.connect_slowdown_threshold?;
        let delay = self.connect_slowdown_delay?;
        (local_online_sessions >= threshold).then_some(delay)
    }

    pub(crate) fn set_connection_shaping(&mut self, delay: Duration) {
        self.connect_shaping_delay = Some(delay);
    }

    pub(crate) fn connect_effective_delay(&self, local_online_sessions: usize) -> Option<Duration> {
        if self.connect_pressure_exceeded(local_online_sessions) {
            return None;
        }
        match (
            self.connect_shaping_delay,
            self.connect_slowdown_delay(local_online_sessions),
        ) {
            (Some(shape), Some(slow)) => Some(shape.max(slow)),
            (Some(shape), None) => Some(shape),
            (None, Some(slow)) => Some(slow),
            (None, None) => None,
        }
    }

    pub(crate) async fn start(&self) {
        self.memory_pressure.start().await;
    }

    pub(crate) async fn stop(&self) {
        self.memory_pressure.stop().await;
    }

    pub(crate) fn current_pressure_level(&self) -> PressureLevel {
        self.memory_pressure.current_level()
    }

    #[cfg(test)]
    pub(crate) fn force_pressure_level(&self, level: PressureLevel) {
        self.memory_pressure.force_level(level);
    }
}
