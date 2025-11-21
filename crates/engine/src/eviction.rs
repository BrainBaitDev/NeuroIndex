use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// LRU eviction tracker
///
/// Tracks access times for keys to implement Least Recently Used eviction.
/// Uses a global atomic counter for timestamps to avoid system calls.
#[derive(Debug)]
pub struct LruTracker<K> {
    /// Access timestamps for each key
    access_times: RwLock<HashMap<K, u64>>,
    /// Global monotonic counter for timestamps
    clock: Arc<AtomicU64>,
}

impl<K> LruTracker<K>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            access_times: RwLock::new(HashMap::new()),
            clock: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record access to a key
    pub fn touch(&self, key: &K) {
        let timestamp = self.clock.fetch_add(1, Ordering::Relaxed);
        self.access_times.write().insert(key.clone(), timestamp);
    }

    /// Remove key from tracking
    pub fn remove(&self, key: &K) {
        self.access_times.write().remove(key);
    }

    /// Get least recently used keys
    ///
    /// Returns up to `count` keys sorted by access time (oldest first)
    pub fn get_lru_keys(&self, count: usize) -> Vec<K> {
        let access_times = self.access_times.read();

        if access_times.is_empty() {
            return Vec::new();
        }

        // Convert to vec and sort by timestamp
        let mut entries: Vec<_> = access_times.iter().collect();
        entries.sort_by_key(|(_, &timestamp)| timestamp);

        // Take the oldest `count` keys
        entries
            .into_iter()
            .take(count)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Get number of tracked keys
    pub fn len(&self) -> usize {
        self.access_times.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.access_times.read().is_empty()
    }

    /// Clear all tracking data
    pub fn clear(&self) {
        self.access_times.write().clear();
    }
}

impl<K> Default for LruTracker<K>
where
    K: Eq + std::hash::Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// LFU (Least Frequently Used) eviction tracker
#[derive(Debug)]
pub struct LfuTracker<K> {
    /// Frequency counter for each key
    frequencies: RwLock<HashMap<K, u64>>,
}

impl<K> LfuTracker<K>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            frequencies: RwLock::new(HashMap::new()),
        }
    }

    /// Record access to a key (increment frequency)
    pub fn touch(&self, key: &K) {
        self.frequencies
            .write()
            .entry(key.clone())
            .and_modify(|freq| *freq += 1)
            .or_insert(1);
    }

    /// Remove key from tracking
    pub fn remove(&self, key: &K) {
        self.frequencies.write().remove(key);
    }

    /// Get least frequently used keys
    pub fn get_lfu_keys(&self, count: usize) -> Vec<K> {
        let frequencies = self.frequencies.read();

        if frequencies.is_empty() {
            return Vec::new();
        }

        // Convert to vec and sort by frequency
        let mut entries: Vec<_> = frequencies.iter().collect();
        entries.sort_by_key(|(_, &freq)| freq);

        // Take the least frequent `count` keys
        entries
            .into_iter()
            .take(count)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Get number of tracked keys
    pub fn len(&self) -> usize {
        self.frequencies.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.frequencies.read().is_empty()
    }

    /// Clear all tracking data
    pub fn clear(&self) {
        self.frequencies.write().clear();
    }
}

impl<K> Default for LfuTracker<K>
where
    K: Eq + std::hash::Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Eviction policy for when resource limits are reached
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// No eviction, return errors on resource limit violation
    NoEviction,
    /// Evict least recently used items
    LRU,
    /// Evict least frequently used items
    LFU,
    /// Evict random items
    Random,
    /// Evict least recently used items among those with `expire` set
    VolatileLRU,
}

/// Resource limits for the engine
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum number of keys
    pub max_keys: u64,
    /// Maximum memory usage in bytes
    pub max_memory: u64,
    /// Eviction policy to use when limits are reached
    pub eviction_policy: EvictionPolicy,
    /// Percentage of memory to free when eviction is triggered (0-100)
    pub eviction_target_percentage: u8,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_keys: u64::MAX,
            max_memory: u64::MAX,
            eviction_policy: EvictionPolicy::NoEviction,
            eviction_target_percentage: 20,
        }
    }
}

/// Tracks resource usage against limits
#[derive(Debug)]
pub struct ResourceTracker {
    limits: ResourceLimits,
    memory_used: AtomicU64,
    keys_count: AtomicU64,
    evictions: AtomicU64,
    failed_evictions: AtomicU64,
}

impl ResourceTracker {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            limits,
            memory_used: AtomicU64::new(0),
            keys_count: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            failed_evictions: AtomicU64::new(0),
        }
    }

    pub fn add_memory(&self, bytes: usize) {
        self.memory_used.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn sub_memory(&self, bytes: usize) {
        self.memory_used.fetch_sub(bytes as u64, Ordering::Relaxed);
    }

    pub fn increment_keys(&self) {
        self.keys_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_keys(&self) {
        self.keys_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self, success: bool) {
        if success {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn stats(&self) -> ResourceStats {
        ResourceStats {
            memory_used: self.memory_used.load(Ordering::Relaxed),
            keys_count: self.keys_count.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            failed_evictions: self.failed_evictions.load(Ordering::Relaxed),
            limits: self.limits,
            eviction_policy: self.limits.eviction_policy,
        }
    }

    /// Check if resource limits allow for a new insertion
    pub fn check_can_insert(&self, key_size: usize, value_size: usize) -> Result<(), &'static str> {
        if self.limits.eviction_policy == EvictionPolicy::NoEviction {
            let current_keys = self.keys_count.load(Ordering::Relaxed);
            if current_keys >= self.limits.max_keys {
                return Err("max keys limit reached");
            }

            let current_memory = self.memory_used.load(Ordering::Relaxed);
            if current_memory + (key_size + value_size) as u64 > self.limits.max_memory {
                return Err("max memory limit reached");
            }
        }
        Ok(())
    }

    /// Check if eviction is needed
    pub fn needs_eviction(&self) -> bool {
        self.keys_count.load(Ordering::Relaxed) >= self.limits.max_keys
            || self.memory_used.load(Ordering::Relaxed) >= self.limits.max_memory
    }

    /// Calculate how many bytes need to be freed to meet eviction target
    pub fn bytes_to_evict(&self) -> u64 {
        let memory_used = self.memory_used.load(Ordering::Relaxed);
        if memory_used < self.limits.max_memory {
            return 0;
        }

        let target_memory =
            self.limits.max_memory * (100 - self.limits.eviction_target_percentage as u64) / 100;

        memory_used.saturating_sub(target_memory)
    }
}

/// A snapshot of resource usage statistics
#[derive(Debug, Clone, Copy)]
pub struct ResourceStats {
    pub memory_used: u64,
    pub keys_count: u64,
    pub evictions: u64,
    pub failed_evictions: u64,
    pub limits: ResourceLimits,
    pub eviction_policy: EvictionPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_tracker_basic() {
        let tracker = LruTracker::<String>::new();

        // Touch keys in order
        tracker.touch(&"key1".to_string());
        tracker.touch(&"key2".to_string());
        tracker.touch(&"key3".to_string());

        assert_eq!(tracker.len(), 3);

        // Get oldest key
        let lru = tracker.get_lru_keys(1);
        assert_eq!(lru.len(), 1);
        assert_eq!(lru[0], "key1");

        // Get 2 oldest keys
        let lru = tracker.get_lru_keys(2);
        assert_eq!(lru.len(), 2);
        assert_eq!(lru[0], "key1");
        assert_eq!(lru[1], "key2");
    }

    #[test]
    fn test_lru_tracker_retouch() {
        let tracker = LruTracker::<String>::new();

        tracker.touch(&"key1".to_string());
        tracker.touch(&"key2".to_string());
        tracker.touch(&"key3".to_string());

        // Re-touch key1 - should move it to the end
        tracker.touch(&"key1".to_string());

        // Now key2 should be oldest
        let lru = tracker.get_lru_keys(1);
        assert_eq!(lru[0], "key2");
    }

    #[test]
    fn test_lru_tracker_remove() {
        let tracker = LruTracker::<String>::new();

        tracker.touch(&"key1".to_string());
        tracker.touch(&"key2".to_string());

        tracker.remove(&"key1".to_string());

        assert_eq!(tracker.len(), 1);
        let lru = tracker.get_lru_keys(10);
        assert_eq!(lru.len(), 1);
        assert_eq!(lru[0], "key2");
    }

    #[test]
    fn test_lfu_tracker_basic() {
        let tracker = LfuTracker::<String>::new();

        // Touch keys with different frequencies
        tracker.touch(&"key1".to_string());
        tracker.touch(&"key2".to_string());
        tracker.touch(&"key2".to_string());
        tracker.touch(&"key3".to_string());
        tracker.touch(&"key3".to_string());
        tracker.touch(&"key3".to_string());

        // key1 has freq=1, key2 has freq=2, key3 has freq=3
        let lfu = tracker.get_lfu_keys(1);
        assert_eq!(lfu[0], "key1");

        let lfu = tracker.get_lfu_keys(2);
        assert_eq!(lfu[0], "key1");
        assert_eq!(lfu[1], "key2");
    }

    #[test]
    fn test_lfu_tracker_remove() {
        let tracker = LfuTracker::<String>::new();

        tracker.touch(&"key1".to_string());
        tracker.touch(&"key2".to_string());

        tracker.remove(&"key1".to_string());

        assert_eq!(tracker.len(), 1);
        let lfu = tracker.get_lfu_keys(10);
        assert_eq!(lfu.len(), 1);
        assert_eq!(lfu[0], "key2");
    }
}
