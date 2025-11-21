use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Resource limits to prevent abuse and OOM
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
pub struct ResourceLimits {
    /// Maximum concurrent connections (optional)
    max_connections: Option<Arc<Semaphore>>,

    /// Current active connections
    active_connections: Arc<AtomicUsize>,

    /// Maximum memory in bytes (optional)
    max_memory_bytes: Option<usize>,

    /// Maximum request size in bytes
    max_request_size: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ResourceLimits {
    pub fn new(
        max_connections: Option<usize>,
        max_memory_mb: Option<usize>,
        max_request_mb: usize,
    ) -> Self {
        Self {
            max_connections: max_connections.map(|n| Arc::new(Semaphore::new(n))),
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_memory_bytes: max_memory_mb.map(|mb| mb * 1024 * 1024),
            max_request_size: max_request_mb * 1024 * 1024,
        }
    }

    /// Try to acquire a connection slot
    /// Returns a guard that releases the slot when dropped
    pub async fn acquire_connection(&self) -> Option<ConnectionGuard> {
        self.active_connections.fetch_add(1, Ordering::Relaxed);

        if let Some(semaphore) = &self.max_connections {
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(ConnectionGuard {
                    _permit: Some(permit),
                    counter: Arc::clone(&self.active_connections),
                }),
                Err(_) => {
                    // Connection limit reached, decrement counter
                    self.active_connections.fetch_sub(1, Ordering::Relaxed);
                    None
                }
            }
        } else {
            // No connection limit
            Some(ConnectionGuard {
                _permit: None,
                counter: Arc::clone(&self.active_connections),
            })
        }
    }

    /// Check if memory usage is within limits
    pub fn check_memory(&self, current_memory_bytes: usize) -> bool {
        if let Some(max) = self.max_memory_bytes {
            current_memory_bytes <= max
        } else {
            true
        }
    }

    /// Get maximum request size in bytes
    pub fn max_request_size(&self) -> usize {
        self.max_request_size
    }

    /// Get current active connections
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get maximum memory bytes
    pub fn max_memory_bytes(&self) -> Option<usize> {
        self.max_memory_bytes
    }
}

/// Guard that decrements connection counter when dropped
#[cfg_attr(not(test), allow(dead_code))]
pub struct ConnectionGuard {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    counter: Arc<AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_limit() {
        let limits = ResourceLimits::new(Some(2), None, 10);

        let _guard1 = limits.acquire_connection().await.unwrap();
        let _guard2 = limits.acquire_connection().await.unwrap();

        // Third connection should fail
        assert!(limits.acquire_connection().await.is_none());
        assert_eq!(limits.active_connections(), 2);

        // Drop guard1, should allow new connection
        drop(_guard1);
        let _guard3 = limits.acquire_connection().await.unwrap();
        assert_eq!(limits.active_connections(), 2);
    }

    #[test]
    fn test_memory_limit() {
        let limits = ResourceLimits::new(None, Some(100), 10); // 100 MB max

        assert!(limits.check_memory(50 * 1024 * 1024)); // 50 MB OK
        assert!(limits.check_memory(100 * 1024 * 1024)); // 100 MB OK
        assert!(!limits.check_memory(101 * 1024 * 1024)); // 101 MB exceeds
    }

    #[test]
    fn test_no_limits() {
        let limits = ResourceLimits::new(None, None, 10);

        assert!(limits.check_memory(usize::MAX));
        assert_eq!(limits.max_memory_bytes(), None);
    }
}
