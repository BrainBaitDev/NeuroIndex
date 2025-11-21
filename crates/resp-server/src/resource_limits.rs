use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Resource limits for RESP server
#[derive(Clone)]
pub struct RespResourceLimits {
    /// Maximum concurrent connections
    max_connections: Option<Arc<Semaphore>>,

    /// Current active connections
    active_connections: Arc<AtomicUsize>,

    /// Maximum memory in bytes
    max_memory_bytes: Option<usize>,

    /// Maximum command size in bytes
    max_command_size: usize,
}

impl RespResourceLimits {
    pub fn new(
        max_connections: Option<usize>,
        max_memory_mb: Option<usize>,
        max_command_kb: usize,
    ) -> Self {
        Self {
            max_connections: max_connections.map(|n| Arc::new(Semaphore::new(n))),
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_memory_bytes: max_memory_mb.map(|mb| mb * 1024 * 1024),
            max_command_size: max_command_kb * 1024,
        }
    }

    /// Try to acquire a connection slot
    pub async fn acquire_connection(&self) -> Option<ConnectionGuard> {
        self.active_connections.fetch_add(1, Ordering::Relaxed);

        if let Some(semaphore) = &self.max_connections {
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(ConnectionGuard {
                    _permit: Some(permit),
                    counter: Arc::clone(&self.active_connections),
                }),
                Err(_) => {
                    self.active_connections.fetch_sub(1, Ordering::Relaxed);
                    None
                }
            }
        } else {
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

    /// Get maximum command size
    pub fn max_command_size(&self) -> usize {
        self.max_command_size
    }

    /// Get current active connections
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}

pub struct ConnectionGuard {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    counter: Arc<AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}
