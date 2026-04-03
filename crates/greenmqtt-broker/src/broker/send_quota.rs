use std::time::Instant;

const DEFAULT_ALPHA: f64 = 0.15;
const TARGET_WINDOW_SECS: f64 = 1.0;

pub(crate) struct SendQuotaManager {
    alpha: f64,
    max_quota: usize,
    current_quota: usize,
    ema_acks_per_sec: Option<f64>,
    last_ack_at: Option<Instant>,
}

impl SendQuotaManager {
    pub(crate) fn new(max_quota: usize) -> Self {
        let max_quota = max_quota.max(1);
        Self {
            alpha: DEFAULT_ALPHA,
            max_quota,
            current_quota: max_quota,
            ema_acks_per_sec: None,
            last_ack_at: None,
        }
    }

    pub(crate) fn set_max_quota(&mut self, max_quota: usize) {
        self.max_quota = max_quota.max(1);
        self.current_quota = self.current_quota.min(self.max_quota).max(1);
    }

    pub(crate) fn current_quota(&self) -> usize {
        self.current_quota.max(1)
    }

    pub(crate) fn note_ack(&mut self) {
        let now = Instant::now();
        if let Some(last_ack_at) = self.last_ack_at {
            let elapsed = now.duration_since(last_ack_at).as_secs_f64();
            if elapsed > 0.0 {
                let observed_acks_per_sec = 1.0 / elapsed;
                let ema = match self.ema_acks_per_sec {
                    Some(previous) => {
                        (self.alpha * observed_acks_per_sec) + ((1.0 - self.alpha) * previous)
                    }
                    None => observed_acks_per_sec,
                };
                self.ema_acks_per_sec = Some(ema);
                self.current_quota = quota_from_rate(ema, self.max_quota);
            }
        }
        self.last_ack_at = Some(now);
    }
}

fn quota_from_rate(acks_per_sec: f64, max_quota: usize) -> usize {
    let estimated = (acks_per_sec * TARGET_WINDOW_SECS).round() as usize;
    estimated.clamp(1, max_quota.max(1))
}

#[cfg(test)]
mod tests {
    use super::{quota_from_rate, SendQuotaManager};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn quota_from_rate_stays_within_bounds() {
        assert_eq!(quota_from_rate(0.1, 16), 1);
        assert_eq!(quota_from_rate(3.6, 16), 4);
        assert_eq!(quota_from_rate(128.0, 16), 16);
    }

    #[test]
    fn slow_acks_reduce_quota() {
        let mut quota = SendQuotaManager::new(16);
        assert_eq!(quota.current_quota(), 16);
        quota.note_ack();
        thread::sleep(Duration::from_millis(600));
        quota.note_ack();
        assert!(quota.current_quota() < 16);
    }

    #[test]
    fn quota_respects_updated_receive_maximum() {
        let mut quota = SendQuotaManager::new(32);
        quota.set_max_quota(4);
        assert_eq!(quota.current_quota(), 4);
    }
}
