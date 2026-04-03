use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Copy)]
struct RateLimitConfig {
    rate_per_sec: f64,
    burst: f64,
}

#[derive(Clone, Copy)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Default)]
struct BucketMap {
    buckets: Mutex<HashMap<String, BucketState>>,
}

impl BucketMap {
    fn allow(&self, key: &str, config: RateLimitConfig) -> bool {
        let now = Instant::now();
        let mut guard = self.buckets.lock().expect("throttle poisoned");
        let bucket = guard.entry(key.to_string()).or_insert(BucketState {
            tokens: config.burst,
            last_refill: now,
        });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * config.rate_per_sec).min(config.burst);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Default)]
pub(crate) struct MessageRateLimiter {
    per_connection: Option<RateLimitConfig>,
    per_tenant: Option<RateLimitConfig>,
    connection_buckets: BucketMap,
    tenant_buckets: BucketMap,
}

impl MessageRateLimiter {
    pub(crate) fn set_per_connection(&mut self, rate_per_sec: u64, burst: u64) {
        self.per_connection = Some(RateLimitConfig {
            rate_per_sec: rate_per_sec as f64,
            burst: burst as f64,
        });
    }

    pub(crate) fn set_per_tenant(&mut self, rate_per_sec: u64, burst: u64) {
        self.per_tenant = Some(RateLimitConfig {
            rate_per_sec: rate_per_sec as f64,
            burst: burst as f64,
        });
    }

    pub(crate) fn allow(&self, session_id: &str, tenant_id: &str) -> bool {
        if let Some(config) = self.per_connection {
            if !self.connection_buckets.allow(session_id, config) {
                return false;
            }
        }
        if let Some(config) = self.per_tenant {
            if !self.tenant_buckets.allow(tenant_id, config) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::MessageRateLimiter;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn connection_limit_blocks_after_burst_and_recovers_after_refill() {
        let mut limiter = MessageRateLimiter::default();
        limiter.set_per_connection(2, 2);
        assert!(limiter.allow("s1", "t1"));
        assert!(limiter.allow("s1", "t1"));
        assert!(!limiter.allow("s1", "t1"));
        thread::sleep(Duration::from_millis(600));
        assert!(limiter.allow("s1", "t1"));
    }

    #[test]
    fn tenant_limit_is_shared_across_sessions() {
        let mut limiter = MessageRateLimiter::default();
        limiter.set_per_tenant(1, 1);
        assert!(limiter.allow("s1", "t1"));
        assert!(!limiter.allow("s2", "t1"));
        assert!(limiter.allow("s2", "t2"));
    }
}
