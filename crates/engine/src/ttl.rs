use crate::Engine;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// TTL (Time-To-Live) manager for automatic key expiration
pub struct TtlManager<K> {
    /// Map from expiration time to set of keys expiring at that time
    expirations: Arc<Mutex<BTreeMap<Instant, HashSet<K>>>>,

    /// Map from key to its expiration time
    key_expirations: Arc<Mutex<HashMap<K, Instant>>>,

    /// Background cleanup worker handle
    cleanup_handle: Option<thread::JoinHandle<()>>,

    /// Signal to stop the background worker
    stop_signal: Arc<AtomicBool>,

    /// Phantom data to hold type parameters
    _phantom: PhantomData<K>,
}

/// TTL statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlStats {
    pub keys_with_ttl: usize,
    pub keys_expired: usize,
    pub next_expiration_ms: Option<u64>,
}

impl<K> TtlManager<K>
where
    K: Clone + Eq + Hash + Send + Sync + art::AsBytes + 'static,
{
    /// Create a new TTL manager
    pub fn new() -> Self {
        Self {
            expirations: Arc::new(Mutex::new(BTreeMap::new())),
            key_expirations: Arc::new(Mutex::new(HashMap::new())),
            cleanup_handle: None,
            stop_signal: Arc::new(AtomicBool::new(false)),
            _phantom: PhantomData,
        }
    }

    /// Set TTL for a key
    pub fn set_ttl(&self, key: K, ttl: Duration) {
        let expiration_time = Instant::now() + ttl;

        // Remove old expiration if exists
        self.remove_ttl(&key);

        // Add new expiration
        let mut expirations = self.expirations.lock().unwrap();
        let mut key_expirations = self.key_expirations.lock().unwrap();

        expirations
            .entry(expiration_time)
            .or_insert_with(HashSet::new)
            .insert(key.clone());

        key_expirations.insert(key, expiration_time);
    }

    /// Remove TTL for a key (make it persistent)
    pub fn remove_ttl(&self, key: &K) -> bool {
        let mut key_expirations = self.key_expirations.lock().unwrap();

        if let Some(old_expiration) = key_expirations.remove(key) {
            let mut expirations = self.expirations.lock().unwrap();

            if let Some(keys_at_time) = expirations.get_mut(&old_expiration) {
                keys_at_time.remove(key);

                // Clean up empty expiration bucket
                if keys_at_time.is_empty() {
                    expirations.remove(&old_expiration);
                }
            }

            true
        } else {
            false
        }
    }

    /// Get remaining TTL for a key
    pub fn ttl(&self, key: &K) -> Option<Duration> {
        let key_expirations = self.key_expirations.lock().unwrap();

        if let Some(&expiration_time) = key_expirations.get(key) {
            let now = Instant::now();
            if expiration_time > now {
                Some(expiration_time - now)
            } else {
                Some(Duration::from_secs(0)) // Expired
            }
        } else {
            None // No TTL set
        }
    }

    /// Check if a key has expired
    pub fn is_expired(&self, key: &K) -> bool {
        let key_expirations = self.key_expirations.lock().unwrap();

        if let Some(&expiration_time) = key_expirations.get(key) {
            Instant::now() >= expiration_time
        } else {
            false
        }
    }

    /// Get all expired keys
    pub fn get_expired_keys(&self) -> Vec<K> {
        let now = Instant::now();
        let mut expired = Vec::new();

        let expirations = self.expirations.lock().unwrap();

        // Collect all keys with expiration <= now
        for (&expiration_time, keys) in expirations.iter() {
            if expiration_time <= now {
                expired.extend(keys.iter().cloned());
            } else {
                break; // BTreeMap is sorted, no need to check further
            }
        }

        expired
    }

    /// Remove expired entries from tracking (called after deletion from engine)
    pub fn cleanup_expired_keys(&self, keys: &[K]) {
        let mut expirations = self.expirations.lock().unwrap();
        let mut key_expirations = self.key_expirations.lock().unwrap();

        for key in keys {
            if let Some(expiration_time) = key_expirations.remove(key) {
                if let Some(keys_at_time) = expirations.get_mut(&expiration_time) {
                    keys_at_time.remove(key);

                    if keys_at_time.is_empty() {
                        expirations.remove(&expiration_time);
                    }
                }
            }
        }
    }

    /// Get TTL statistics
    pub fn stats(&self) -> TtlStats {
        let expirations = self.expirations.lock().unwrap();
        let key_expirations = self.key_expirations.lock().unwrap();

        let next_expiration_ms = expirations.keys().next().map(|&t| {
            let now = Instant::now();
            if t > now {
                (t - now).as_millis() as u64
            } else {
                0
            }
        });

        TtlStats {
            keys_with_ttl: key_expirations.len(),
            keys_expired: 0, // This will be tracked by the engine
            next_expiration_ms,
        }
    }

    /// Start background cleanup worker
    pub fn start_cleanup_worker<V>(&mut self, engine: Arc<Engine<K, V>>, check_interval: Duration)
    where
        V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        K: Ord + Send + Sync + Serialize + DeserializeOwned,
    {
        if self.cleanup_handle.is_some() {
            return; // Already running
        }

        let expirations = Arc::clone(&self.expirations);
        let key_expirations = Arc::clone(&self.key_expirations);
        let stop_signal = Arc::clone(&self.stop_signal);

        let handle = thread::spawn(move || {
            while !stop_signal.load(Ordering::Relaxed) {
                thread::sleep(check_interval);

                // Get expired keys
                let now = Instant::now();
                let mut expired = Vec::new();

                {
                    let exps = expirations.lock().unwrap();
                    for (&expiration_time, keys) in exps.iter() {
                        if expiration_time <= now {
                            expired.extend(keys.iter().cloned());
                        } else {
                            break;
                        }
                    }
                }

                if expired.is_empty() {
                    continue;
                }

                // Delete expired keys from engine
                let mut deleted_count = 0;
                for key in &expired {
                    if let Ok(Some(_)) = engine.delete(key) {
                        deleted_count += 1;
                    }
                }

                // Clean up tracking structures
                {
                    let mut exps = expirations.lock().unwrap();
                    let mut key_exps = key_expirations.lock().unwrap();

                    for key in &expired {
                        if let Some(expiration_time) = key_exps.remove(key) {
                            if let Some(keys_at_time) = exps.get_mut(&expiration_time) {
                                keys_at_time.remove(key);
                                if keys_at_time.is_empty() {
                                    exps.remove(&expiration_time);
                                }
                            }
                        }
                    }
                }

                if deleted_count > 0 {
                    println!(
                        "[TTL] Expired {} keys (checked {} candidates)",
                        deleted_count,
                        expired.len()
                    );
                }
            }
        });

        self.cleanup_handle = Some(handle);
    }
}

// Implementation without bounds for methods that don't need them
impl<K> TtlManager<K> {
    /// Stop background cleanup worker
    pub fn stop_cleanup_worker(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            self.stop_signal.store(true, Ordering::Relaxed);
            let _ = handle.join();
            self.stop_signal.store(false, Ordering::Relaxed);
        }
    }
}

impl<K> Drop for TtlManager<K> {
    fn drop(&mut self) {
        self.stop_cleanup_worker();
    }
}

impl<K> Default for TtlManager<K>
where
    K: Clone + Eq + Hash + Send + Sync + art::AsBytes + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_ttl() {
        let manager = TtlManager::<String>::new();

        manager.set_ttl("key1".to_string(), Duration::from_secs(10));

        let ttl = manager.ttl(&"key1".to_string());
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() <= 10);
        assert!(ttl.unwrap().as_secs() >= 9); // Account for small time difference
    }

    #[test]
    fn test_remove_ttl() {
        let manager = TtlManager::<String>::new();

        manager.set_ttl("key1".to_string(), Duration::from_secs(10));
        assert!(manager.ttl(&"key1".to_string()).is_some());

        manager.remove_ttl(&"key1".to_string());
        assert!(manager.ttl(&"key1".to_string()).is_none());
    }

    #[test]
    fn test_is_expired() {
        let manager = TtlManager::<String>::new();

        // Set very short TTL
        manager.set_ttl("key1".to_string(), Duration::from_millis(1));

        // Wait for expiration
        thread::sleep(Duration::from_millis(10));

        assert!(manager.is_expired(&"key1".to_string()));
    }

    #[test]
    fn test_get_expired_keys() {
        let manager = TtlManager::<String>::new();

        // Set short TTLs
        manager.set_ttl("key1".to_string(), Duration::from_millis(1));
        manager.set_ttl("key2".to_string(), Duration::from_millis(1));
        manager.set_ttl("key3".to_string(), Duration::from_secs(100));

        // Wait for some to expire
        thread::sleep(Duration::from_millis(10));

        let expired = manager.get_expired_keys();
        assert!(expired.contains(&"key1".to_string()));
        assert!(expired.contains(&"key2".to_string()));
        assert!(!expired.contains(&"key3".to_string()));
    }

    #[test]
    fn test_cleanup_expired_keys() {
        let manager = TtlManager::<String>::new();

        manager.set_ttl("key1".to_string(), Duration::from_millis(1));
        manager.set_ttl("key2".to_string(), Duration::from_millis(1));

        thread::sleep(Duration::from_millis(10));

        let expired = manager.get_expired_keys();
        manager.cleanup_expired_keys(&expired);

        assert!(manager.ttl(&"key1".to_string()).is_none());
        assert!(manager.ttl(&"key2".to_string()).is_none());
    }

    #[test]
    fn test_stats() {
        let manager = TtlManager::<String>::new();

        manager.set_ttl("key1".to_string(), Duration::from_secs(10));
        manager.set_ttl("key2".to_string(), Duration::from_secs(20));

        let stats = manager.stats();
        assert_eq!(stats.keys_with_ttl, 2);
        assert!(stats.next_expiration_ms.is_some());
    }
}
