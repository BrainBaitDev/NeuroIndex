use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Policy di eviction quando si raggiunge max_memory
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// No eviction - PUT fallisce quando memoria piena
    NoEviction,
    /// Least Recently Used - rimuove chiavi meno usate di recente
    LRU,
    /// Least Frequently Used - rimuove chiavi meno usate
    LFU,
    /// Random eviction - rimuove chiavi casuali
    Random,
    /// Volatile LRU - rimuove solo chiavi con expiration (TODO)
    VolatileLRU,
}

/// Configurazione dei limiti di risorse
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Memoria massima (bytes). 0 = illimitato
    pub max_memory: usize,

    /// Numero massimo di client connessi. 0 = illimitato
    pub max_clients: usize,

    /// Dimensione massima di una singola richiesta (bytes). 0 = illimitato
    pub max_request_size: usize,

    /// Numero massimo di chiavi. 0 = illimitato
    pub max_keys: usize,

    /// Policy di eviction quando max_memory raggiunto
    pub eviction_policy: EvictionPolicy,

    /// Soglia per iniziare eviction (% di max_memory)
    /// Default: 0.95 (inizia a 95% di memoria)
    pub eviction_threshold: f64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 0,           // Illimitato
            max_clients: 10_000,     // 10K clients max (ragionevole)
            max_request_size: 512 * 1024 * 1024, // 512 MB max per richiesta
            max_keys: 0,             // Illimitato
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_threshold: 0.95,
        }
    }
}

impl ResourceLimits {
    /// Preset: Small server (2GB RAM, 1000 clients)
    pub fn small() -> Self {
        Self {
            max_memory: 2 * 1024 * 1024 * 1024, // 2 GB
            max_clients: 1_000,
            max_request_size: 10 * 1024 * 1024, // 10 MB
            max_keys: 10_000_000, // 10M keys
            eviction_policy: EvictionPolicy::LRU,
            eviction_threshold: 0.90,
        }
    }

    /// Preset: Medium server (8GB RAM, 5000 clients)
    pub fn medium() -> Self {
        Self {
            max_memory: 8 * 1024 * 1024 * 1024, // 8 GB
            max_clients: 5_000,
            max_request_size: 50 * 1024 * 1024, // 50 MB
            max_keys: 50_000_000, // 50M keys
            eviction_policy: EvictionPolicy::LRU,
            eviction_threshold: 0.95,
        }
    }

    /// Preset: Large server (32GB RAM, 10000 clients)
    pub fn large() -> Self {
        Self {
            max_memory: 32 * 1024 * 1024 * 1024, // 32 GB
            max_clients: 10_000,
            max_request_size: 512 * 1024 * 1024, // 512 MB
            max_keys: 200_000_000, // 200M keys
            eviction_policy: EvictionPolicy::LRU,
            eviction_threshold: 0.95,
        }
    }

    /// Valida la configurazione
    pub fn validate(&self) -> Result<(), String> {
        if self.eviction_threshold < 0.0 || self.eviction_threshold > 1.0 {
            return Err("eviction_threshold must be between 0.0 and 1.0".to_string());
        }

        if self.max_memory > 0 && self.eviction_policy == EvictionPolicy::NoEviction {
            eprintln!(
                "WARNING: max_memory set but eviction_policy is NoEviction. \
                 PUT will fail when memory is full."
            );
        }

        Ok(())
    }

    /// Verifica se la memoria attuale supera la soglia di eviction
    pub fn should_evict(&self, current_memory: usize) -> bool {
        if self.max_memory == 0 {
            return false; // Nessun limite
        }

        let threshold_bytes = (self.max_memory as f64 * self.eviction_threshold) as usize;
        current_memory >= threshold_bytes
    }

    /// Calcola quanta memoria liberare
    pub fn bytes_to_evict(&self, current_memory: usize) -> usize {
        if self.max_memory == 0 {
            return 0;
        }

        if current_memory <= self.max_memory {
            return 0;
        }

        // Libera fino a tornare sotto la soglia
        let target = (self.max_memory as f64 * (self.eviction_threshold - 0.05)) as usize;
        current_memory.saturating_sub(target)
    }
}

/// Tracker delle risorse usate in real-time
#[derive(Debug)]
pub struct ResourceTracker {
    /// Memoria attualmente usata (bytes)
    memory_used: AtomicUsize,

    /// Numero di chiavi attualmente nel database
    keys_count: AtomicUsize,

    /// Numero di client attualmente connessi
    clients_count: AtomicUsize,

    /// Configurazione limiti
    limits: ResourceLimits,

    /// Statistiche eviction
    evictions_count: AtomicU64,
    evictions_failed: AtomicU64,
}

impl ResourceTracker {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            memory_used: AtomicUsize::new(0),
            keys_count: AtomicUsize::new(0),
            clients_count: AtomicUsize::new(0),
            limits,
            evictions_count: AtomicU64::new(0),
            evictions_failed: AtomicU64::new(0),
        }
    }

    /// Aggiorna memoria usata
    pub fn add_memory(&self, bytes: usize) {
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn sub_memory(&self, bytes: usize) {
        self.memory_used.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Incrementa/decrementa contatore chiavi
    pub fn increment_keys(&self) {
        self.keys_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_keys(&self) {
        self.keys_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Gestione client connessi
    pub fn add_client(&self) -> Result<(), ResourceLimitError> {
        let current = self.clients_count.fetch_add(1, Ordering::Relaxed);

        if self.limits.max_clients > 0 && current >= self.limits.max_clients {
            self.clients_count.fetch_sub(1, Ordering::Relaxed);
            return Err(ResourceLimitError::MaxClientsReached {
                max: self.limits.max_clients,
            });
        }

        Ok(())
    }

    pub fn remove_client(&self) {
        self.clients_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Verifica se una richiesta è troppo grande
    pub fn check_request_size(&self, size: usize) -> Result<(), ResourceLimitError> {
        if self.limits.max_request_size > 0 && size > self.limits.max_request_size {
            return Err(ResourceLimitError::RequestTooLarge {
                size,
                max: self.limits.max_request_size,
            });
        }
        Ok(())
    }

    /// Verifica se si può inserire una nuova chiave
    pub fn check_can_insert(&self, key_size: usize, value_size: usize) -> Result<(), ResourceLimitError> {
        // Check max keys (only if NoEviction - otherwise eviction will handle it)
        if self.limits.eviction_policy == EvictionPolicy::NoEviction {
            let current_keys = self.keys_count.load(Ordering::Relaxed);
            if self.limits.max_keys > 0 && current_keys >= self.limits.max_keys {
                return Err(ResourceLimitError::MaxKeysReached {
                    max: self.limits.max_keys,
                });
            }

            // Check max memory
            let current_memory = self.memory_used.load(Ordering::Relaxed);
            let new_memory = current_memory + key_size + value_size;

            if self.limits.max_memory > 0 && new_memory > self.limits.max_memory {
                return Err(ResourceLimitError::OutOfMemory {
                    used: current_memory,
                    max: self.limits.max_memory,
                });
            }
        }

        Ok(())
    }

    /// Verifica se serve eviction
    pub fn needs_eviction(&self) -> bool {
        let current = self.memory_used.load(Ordering::Relaxed);
        self.limits.should_evict(current)
    }

    /// Calcola quanti bytes evictare
    pub fn bytes_to_evict(&self) -> usize {
        let current = self.memory_used.load(Ordering::Relaxed);
        self.limits.bytes_to_evict(current)
    }

    /// Incrementa statistiche eviction
    pub fn record_eviction(&self, success: bool) {
        if success {
            self.evictions_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.evictions_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Statistiche correnti
    pub fn stats(&self) -> ResourceStats {
        ResourceStats {
            memory_used: self.memory_used.load(Ordering::Relaxed),
            memory_max: self.limits.max_memory,
            memory_utilization: if self.limits.max_memory > 0 {
                (self.memory_used.load(Ordering::Relaxed) as f64 / self.limits.max_memory as f64) * 100.0
            } else {
                0.0
            },
            keys_count: self.keys_count.load(Ordering::Relaxed),
            keys_max: self.limits.max_keys,
            clients_count: self.clients_count.load(Ordering::Relaxed),
            clients_max: self.limits.max_clients,
            evictions_total: self.evictions_count.load(Ordering::Relaxed),
            evictions_failed: self.evictions_failed.load(Ordering::Relaxed),
            eviction_policy: self.limits.eviction_policy,
        }
    }
}

/// Statistiche risorse
#[derive(Debug, Clone)]
pub struct ResourceStats {
    pub memory_used: usize,
    pub memory_max: usize,
    pub memory_utilization: f64, // Percentuale 0-100
    pub keys_count: usize,
    pub keys_max: usize,
    pub clients_count: usize,
    pub clients_max: usize,
    pub evictions_total: u64,
    pub evictions_failed: u64,
    pub eviction_policy: EvictionPolicy,
}

impl std::fmt::Display for ResourceStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Resource Statistics:")?;
        writeln!(
            f,
            "  Memory: {:.2} MB / {} MB ({:.1}%)",
            self.memory_used as f64 / 1_048_576.0,
            if self.memory_max > 0 {
                format!("{:.2}", self.memory_max as f64 / 1_048_576.0)
            } else {
                "unlimited".to_string()
            },
            self.memory_utilization
        )?;
        writeln!(
            f,
            "  Keys: {} / {}",
            self.keys_count,
            if self.keys_max > 0 {
                self.keys_max.to_string()
            } else {
                "unlimited".to_string()
            }
        )?;
        writeln!(
            f,
            "  Clients: {} / {}",
            self.clients_count,
            if self.clients_max > 0 {
                self.clients_max.to_string()
            } else {
                "unlimited".to_string()
            }
        )?;
        writeln!(f, "  Eviction Policy: {:?}", self.eviction_policy)?;
        writeln!(
            f,
            "  Evictions: {} total, {} failed",
            self.evictions_total, self.evictions_failed
        )?;
        Ok(())
    }
}

/// Errori legati ai limiti di risorse
#[derive(Debug, Clone)]
pub enum ResourceLimitError {
    OutOfMemory { used: usize, max: usize },
    MaxKeysReached { max: usize },
    MaxClientsReached { max: usize },
    RequestTooLarge { size: usize, max: usize },
}

impl std::fmt::Display for ResourceLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceLimitError::OutOfMemory { used, max } => {
                write!(
                    f,
                    "OOM: memory limit exceeded ({:.2} MB / {:.2} MB)",
                    *used as f64 / 1_048_576.0,
                    *max as f64 / 1_048_576.0
                )
            }
            ResourceLimitError::MaxKeysReached { max } => {
                write!(f, "Max keys limit reached ({})", max)
            }
            ResourceLimitError::MaxClientsReached { max } => {
                write!(f, "Max clients limit reached ({})", max)
            }
            ResourceLimitError::RequestTooLarge { size, max } => {
                write!(
                    f,
                    "Request too large ({:.2} MB > {:.2} MB max)",
                    *size as f64 / 1_048_576.0,
                    *max as f64 / 1_048_576.0
                )
            }
        }
    }
}

impl std::error::Error for ResourceLimitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_limits_validation() {
        let mut limits = ResourceLimits::default();
        assert!(limits.validate().is_ok());

        limits.eviction_threshold = 1.5;
        assert!(limits.validate().is_err());

        limits.eviction_threshold = 0.95;
        assert!(limits.validate().is_ok());
    }

    #[test]
    fn test_should_evict() {
        let limits = ResourceLimits {
            max_memory: 1_000_000,
            eviction_threshold: 0.90,
            ..Default::default()
        };

        assert!(!limits.should_evict(800_000)); // 80% - NO
        assert!(limits.should_evict(900_000));  // 90% - YES
        assert!(limits.should_evict(950_000));  // 95% - YES
    }

    #[test]
    fn test_bytes_to_evict() {
        let limits = ResourceLimits {
            max_memory: 1_000_000,
            eviction_threshold: 0.90,
            ..Default::default()
        };

        // Non serve eviction (sotto max_memory)
        assert_eq!(limits.bytes_to_evict(800_000), 0);
        assert_eq!(limits.bytes_to_evict(950_000), 0);

        // Serve eviction: 1.1M → target 850K (90% - 5% = 85%)
        let to_evict = limits.bytes_to_evict(1_100_000);
        assert!(to_evict > 0);
        assert!(to_evict >= 100_000); // Almeno 100KB
    }

    #[test]
    fn test_resource_tracker() {
        let limits = ResourceLimits {
            max_memory: 1_000_000,
            max_keys: 100,
            max_clients: 10,
            ..Default::default()
        };

        let tracker = ResourceTracker::new(limits);

        // Test memory tracking
        tracker.add_memory(500_000);
        assert_eq!(tracker.memory_used.load(Ordering::Relaxed), 500_000);

        tracker.sub_memory(200_000);
        assert_eq!(tracker.memory_used.load(Ordering::Relaxed), 300_000);

        // Test keys tracking
        tracker.increment_keys();
        tracker.increment_keys();
        assert_eq!(tracker.keys_count.load(Ordering::Relaxed), 2);

        // Test client limits
        for _ in 0..10 {
            assert!(tracker.add_client().is_ok());
        }
        assert!(tracker.add_client().is_err()); // 11th client should fail

        // Test request size
        assert!(tracker.check_request_size(100).is_ok());
        assert!(tracker.check_request_size(1_000_000_000).is_err());
    }

    #[test]
    fn test_presets() {
        let small = ResourceLimits::small();
        assert_eq!(small.max_memory, 2 * 1024 * 1024 * 1024);
        assert_eq!(small.max_clients, 1_000);

        let medium = ResourceLimits::medium();
        assert_eq!(medium.max_memory, 8 * 1024 * 1024 * 1024);

        let large = ResourceLimits::large();
        assert_eq!(large.max_memory, 32 * 1024 * 1024 * 1024);
    }
}
