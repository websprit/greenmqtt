use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use tokio::time::{sleep, Duration};

#[allow(dead_code)]
pub struct TokenBucket {
    rate: u64,
    burst: u64,
    tokens: AtomicU64,
    last_refill: Mutex<Instant>,
}

#[allow(dead_code)]
impl TokenBucket {
    pub fn new(rate: u64, burst: u64) -> Self {
        Self {
            rate,
            burst,
            tokens: AtomicU64::new(burst),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    pub async fn acquire(&self, bytes: u64) -> bool {
        loop {
            self.refill();
            if self
                .tokens
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |available| {
                    (available >= bytes).then(|| available - bytes)
                })
                .is_ok()
            {
                return true;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn burst(&self) -> u64 {
        self.burst
    }

    fn refill(&self) {
        let mut last_refill = self.last_refill.lock().expect("token bucket poisoned");
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();
        if elapsed <= 0.0 || self.rate == 0 {
            return;
        }
        let refill = (elapsed * self.rate as f64) as u64;
        if refill == 0 {
            return;
        }
        let _ = self
            .tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |available| {
                Some((available + refill).min(self.burst))
            });
        *last_refill = now;
    }
}

#[cfg(test)]
mod tests {
    use super::TokenBucket;
    use std::time::Instant;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn acquire_consumes_and_refills_tokens() {
        let bucket = TokenBucket::new(10, 10);
        assert!(bucket.acquire(8).await);
        let started = Instant::now();
        assert!(bucket.acquire(4).await);
        assert!(started.elapsed() >= Duration::from_millis(180));
        assert!(bucket.acquire(2).await);
    }

    #[tokio::test]
    async fn burst_caps_available_tokens() {
        let bucket = TokenBucket::new(100, 5);
        sleep(Duration::from_millis(100)).await;
        assert!(bucket.acquire(5).await);
        let started = Instant::now();
        assert!(bucket.acquire(1).await);
        assert!(started.elapsed() >= Duration::from_millis(10));
    }
}
