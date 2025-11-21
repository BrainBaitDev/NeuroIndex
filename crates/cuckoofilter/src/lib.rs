//! Optimized Cuckoo Filter for NeuroIndex
//!
//! A space-efficient probabilistic data structure for membership testing.
//! Similar to Bloom filters but supports deletion.
//!
//! ## Properties
//! - False positive rate: ~3% at 95% capacity
//! - No false negatives
//! - Supports deletion
//! - Cache-friendly (2 buckets per lookup)
//!
//! ## Use Cases
//! - Negative lookup optimization (avoid expensive searches for non-existent keys)
//! - Reduce cache pollution from misses
//! - Early rejection in distributed systems

use ahash::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Fingerprint size in bits (12 bits = 4096 possible values)
const FINGERPRINT_BITS: usize = 12;
const FINGERPRINT_MASK: u16 = (1 << FINGERPRINT_BITS) - 1;

/// Bucket size (4 entries per bucket for good space efficiency)
const BUCKET_SIZE: usize = 4;

/// Maximum kicks before giving up on insertion
const MAX_KICKS: usize = 500;

/// A single fingerprint entry
type Fingerprint = u16;

/// Bucket containing BUCKET_SIZE fingerprints
#[derive(Clone, Copy)]
struct Bucket {
    entries: [Fingerprint; BUCKET_SIZE],
}

impl Bucket {
    fn new() -> Self {
        Self {
            entries: [0; BUCKET_SIZE],
        }
    }

    fn insert(&mut self, fp: Fingerprint) -> bool {
        for entry in &mut self.entries {
            if *entry == 0 {
                *entry = fp;
                return true;
            }
        }
        false
    }

    fn delete(&mut self, fp: Fingerprint) -> bool {
        for entry in &mut self.entries {
            if *entry == fp {
                *entry = 0;
                return true;
            }
        }
        false
    }

    fn contains(&self, fp: Fingerprint) -> bool {
        self.entries.iter().any(|&e| e == fp)
    }

    #[allow(dead_code)]
    fn is_full(&self) -> bool {
        self.entries.iter().all(|&e| e != 0)
    }
}

/// Cuckoo Filter for fast membership testing
pub struct CuckooFilter {
    buckets: Vec<Bucket>,
    num_buckets: usize,
    hasher: RandomState,
    count: AtomicUsize,
}

impl CuckooFilter {
    /// Create a new CuckooFilter with given capacity
    ///
    /// Actual capacity will be capacity * BUCKET_SIZE
    pub fn new(capacity: usize) -> Self {
        let num_buckets = capacity.next_power_of_two();
        let mut buckets = Vec::with_capacity(num_buckets);
        buckets.resize(num_buckets, Bucket::new());

        Self {
            buckets,
            num_buckets,
            hasher: RandomState::new(),
            count: AtomicUsize::new(0),
        }
    }

    /// Generate fingerprint from hash value
    #[inline]
    fn fingerprint(&self, hash: u64) -> Fingerprint {
        let fp = ((hash >> 32) & FINGERPRINT_MASK as u64) as u16;
        if fp == 0 {
            1
        } else {
            fp
        } // Avoid 0 (used as empty marker)
    }

    /// Calculate primary bucket index
    #[inline]
    fn index1(&self, hash: u64) -> usize {
        (hash as usize) & (self.num_buckets - 1)
    }

    /// Calculate alternate bucket index using fingerprint
    #[inline]
    fn index2(&self, index: usize, fp: Fingerprint) -> usize {
        // XOR-based alternate index calculation
        let hash = self.hash_fp(fp);
        (index ^ hash) & (self.num_buckets - 1)
    }

    /// Hash a fingerprint for alternate index calculation
    #[inline]
    fn hash_fp(&self, fp: Fingerprint) -> usize {
        let mut hasher = self.hasher.build_hasher();
        fp.hash(&mut hasher);
        hasher.finish() as usize
    }

    /// Hash a key
    #[inline]
    fn hash_key<K: Hash>(&self, key: &K) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Insert a key into the filter
    ///
    /// Returns true if inserted successfully, false if filter is full
    pub fn insert<K: Hash>(&mut self, key: &K) -> bool {
        let hash = self.hash_key(key);
        let fp = self.fingerprint(hash);
        let i1 = self.index1(hash);
        let i2 = self.index2(i1, fp);

        // Try to insert in primary bucket
        if self.buckets[i1].insert(fp) {
            self.count.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        // Try to insert in alternate bucket
        if self.buckets[i2].insert(fp) {
            self.count.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        // Both buckets full - start cuckoo kicking
        let mut index = if fastrand::bool() { i1 } else { i2 };
        let mut fingerprint = fp;

        for _ in 0..MAX_KICKS {
            // Pick random entry in bucket to evict
            let entry_idx = fastrand::usize(..BUCKET_SIZE);
            let evicted = self.buckets[index].entries[entry_idx];
            self.buckets[index].entries[entry_idx] = fingerprint;

            // Calculate alternate location for evicted fingerprint
            fingerprint = evicted;
            index = self.index2(index, fingerprint);

            // Try to insert evicted fingerprint
            if self.buckets[index].insert(fingerprint) {
                self.count.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }

        // Failed to insert after MAX_KICKS attempts
        false
    }

    /// Check if a key might be in the filter
    ///
    /// - Returns true: key **might** be present (with ~3% false positive rate)
    /// - Returns false: key is **definitely not** present
    pub fn contains<K: Hash>(&self, key: &K) -> bool {
        let hash = self.hash_key(key);
        let fp = self.fingerprint(hash);
        let i1 = self.index1(hash);
        let i2 = self.index2(i1, fp);

        self.buckets[i1].contains(fp) || self.buckets[i2].contains(fp)
    }

    /// Delete a key from the filter
    ///
    /// Returns true if key was found and deleted
    pub fn delete<K: Hash>(&mut self, key: &K) -> bool {
        let hash = self.hash_key(key);
        let fp = self.fingerprint(hash);
        let i1 = self.index1(hash);
        let i2 = self.index2(i1, fp);

        if self.buckets[i1].delete(fp) {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return true;
        }

        if self.buckets[i2].delete(fp) {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Get current number of items (approximate)
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if filter is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get load factor (0.0 to 1.0)
    pub fn load_factor(&self) -> f64 {
        self.len() as f64 / (self.num_buckets * BUCKET_SIZE) as f64
    }

    /// Get total capacity
    pub fn capacity(&self) -> usize {
        self.num_buckets * BUCKET_SIZE
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            *bucket = Bucket::new();
        }
        self.count.store(0, Ordering::Relaxed);
    }
}

impl Default for CuckooFilter {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_contains() {
        let mut filter = CuckooFilter::new(100);

        filter.insert(&"hello");
        filter.insert(&"world");
        filter.insert(&42u64);

        assert!(filter.contains(&"hello"));
        assert!(filter.contains(&"world"));
        assert!(filter.contains(&42u64));
        assert!(!filter.contains(&"nonexistent"));
    }

    #[test]
    fn test_delete() {
        let mut filter = CuckooFilter::new(100);

        filter.insert(&"test");
        assert!(filter.contains(&"test"));

        assert!(filter.delete(&"test"));
        assert!(!filter.contains(&"test"));

        // Delete non-existent
        assert!(!filter.delete(&"nonexistent"));
    }

    #[test]
    fn test_capacity() {
        let mut filter = CuckooFilter::new(100);
        let capacity = filter.capacity();

        let mut inserted = 0;
        for i in 0..capacity {
            if filter.insert(&i) {
                inserted += 1;
            }
        }

        // Should be able to insert most items
        assert!(inserted as f64 > capacity as f64 * 0.9);
    }

    #[test]
    fn test_false_positives() {
        let mut filter = CuckooFilter::new(1000);

        // Insert 1000 items
        for i in 0..1000 {
            filter.insert(&i);
        }

        // Check false positive rate
        let mut false_positives = 0;
        for i in 10000..20000 {
            if filter.contains(&i) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        println!("False positive rate: {:.2}%", fp_rate * 100.0);

        // Should be less than 5%
        assert!(fp_rate < 0.05);
    }

    #[test]
    fn test_load_factor() {
        let mut filter = CuckooFilter::new(100);
        assert_eq!(filter.load_factor(), 0.0);

        filter.insert(&1);
        assert!(filter.load_factor() > 0.0);

        filter.clear();
        assert_eq!(filter.load_factor(), 0.0);
    }
}
