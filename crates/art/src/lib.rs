//! Adaptive Radix Tree (ART) implementation for NeuroIndex
//!
//! This module provides an ordered index structure optimized for:
//! - Range queries (O(log n + k))
//! - Prefix searches
//! - Ordered iteration
//! - Memory efficiency
//!
//! ## Current Implementation
//! The `ArtTree` currently uses a highly-optimized `BTreeMap` as its backend.
//! Rust's BTreeMap provides excellent performance with:
//! - O(log n) lookups, inserts, and deletes
//! - Excellent cache locality due to node packing
//! - Native support for range queries
//! - Low memory overhead
//!
//! ## Future Optimization
//! A full Adaptive Radix Tree implementation with adaptive node types
//! (Node4/16/48/256) and SIMD-accelerated search is available in the
//! `full_art` module. This can be enabled for even better performance on:
//! - Very large datasets (>10M keys)
//! - String keys or UUIDs
//! - Workloads with heavy prefix searches

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::ops::Bound;

pub mod full_art;

/// Convert key to byte slice
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for String {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for &str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }
}

impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> AsBytes for &'a [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

// Implement AsBytes for primitive numeric types
impl AsBytes for u64 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const u64 as *const u8, std::mem::size_of::<u64>())
        }
    }
}

impl AsBytes for u32 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const u32 as *const u8, std::mem::size_of::<u32>())
        }
    }
}

impl AsBytes for i64 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const i64 as *const u8, std::mem::size_of::<i64>())
        }
    }
}

impl AsBytes for i32 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const i32 as *const u8, std::mem::size_of::<i32>())
        }
    }
}

/// Ordered tree structure for range queries and prefix searches
///
/// Currently implemented using BTreeMap for optimal performance.
/// Thread-safe with RwLock allowing concurrent reads.
///
/// # Performance Characteristics
/// - Get: O(log n)
/// - Insert: O(log n)
/// - Delete: O(log n)
/// - Range: O(log n + k) where k is the number of items in range
///
/// # Example
/// ```ignore
/// use art::ArtTree;
///
/// let tree = ArtTree::new();
/// tree.insert(1, "one");
/// tree.insert(2, "two");
///
/// let range: Vec<_> = tree.range(Bound::Included(&1), Bound::Included(&2));
/// assert_eq!(range.len(), 2);
/// ```
pub struct ArtTree<K, V> {
    /// BTreeMap backend providing ordered access
    /// (Future: can be swapped with full ART implementation)
    inner: RwLock<BTreeMap<K, V>>,
}

impl<K: Ord + Clone, V: Clone> Default for ArtTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V: Clone> ArtTree<K, V> {
    /// Create a new empty ArtTree
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.read().get(key).cloned()
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.inner.write().insert(key, value)
    }

    pub fn delete(&self, key: &K) -> Option<V> {
        self.inner.write().remove(key)
    }

    pub fn range<'a>(
        &'a self,
        lower: Bound<&'a K>,
        upper: Bound<&'a K>,
    ) -> impl Iterator<Item = (K, V)> + 'a {
        self.range_limited(lower, upper, usize::MAX).into_iter()
    }

    pub fn range_limited<'a>(
        &'a self,
        lower: Bound<&'a K>,
        upper: Bound<&'a K>,
        limit: usize,
    ) -> Vec<(K, V)> {
        let guard = self.inner.read();
        let mut items = Vec::new();
        if limit == 0 {
            return items;
        }
        for (k, v) in guard.range((lower, upper)) {
            items.push((k.clone(), v.clone()));
            if items.len() >= limit {
                break;
            }
        }
        items
    }

    pub fn memory_usage(&self) -> usize {
        // This is a placeholder implementation.
        // A real implementation would require iterating through the tree
        // and summing up the size of nodes and key-value pairs.
        0
    }
}
