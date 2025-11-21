use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

/// Configuration for command rate limiting.
#[derive(Clone, Debug)]
pub struct RateLimiterConfig {
    /// Steady-state commands allowed per second for each client.
    pub commands_per_second: u32,
    /// Maximum burst capacity (in commands) allowed to accumulate.
    pub burst_capacity: u32,
}

impl RateLimiterConfig {
    pub fn is_enabled(&self) -> bool {
        self.commands_per_second > 0 && self.burst_capacity > 0
    }
}

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

struct RateLimiterInner {
    buckets: Mutex<HashMap<IpAddr, TokenBucket>>,
    config: RateLimiterConfig,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(config: &RateLimiterConfig) -> Self {
        Self {
            tokens: config.burst_capacity as f64,
            last_refill: Instant::now(),
        }
    }

    fn allow(&mut self, config: &RateLimiterConfig) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        self.last_refill = now;

        let refill = elapsed.as_secs_f64() * config.commands_per_second as f64;
        self.tokens = (self.tokens + refill).min(config.burst_capacity as f64);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

impl RateLimiter {
    pub fn new(config: RateLimiterConfig) -> Option<Self> {
        if !config.is_enabled() {
            return None;
        }

        Some(Self {
            inner: Arc::new(RateLimiterInner {
                buckets: Mutex::new(HashMap::new()),
                config,
            }),
        })
    }

    pub async fn allow(&self, ip: IpAddr) -> bool {
        let mut buckets = self.inner.buckets.lock().await;
        let bucket = buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(&self.inner.config));
        bucket.allow(&self.inner.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration as TokioDuration};

    #[tokio::test]
    async fn rate_limiter_allows_within_capacity() {
        let limiter = RateLimiter::new(RateLimiterConfig {
            commands_per_second: 10,
            burst_capacity: 10,
        })
        .expect("limiter");

        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        for _ in 0..10 {
            assert!(limiter.allow(ip).await);
        }
        assert!(!limiter.allow(ip).await);
    }

    #[tokio::test]
    async fn rate_limiter_refills_over_time() {
        let limiter = RateLimiter::new(RateLimiterConfig {
            commands_per_second: 2,
            burst_capacity: 2,
        })
        .expect("limiter");
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        assert!(limiter.allow(ip).await);
        assert!(limiter.allow(ip).await);
        assert!(!limiter.allow(ip).await);

        sleep(TokioDuration::from_millis(600)).await;
        assert!(limiter.allow(ip).await);
    }
}
