// Phase 8: Performance Optimizations
// Based on Weiss Ch.5 (Cache-aware design) and Lehman & Carey (T-Tree optimization)

use std::sync::atomic::{AtomicU64, Ordering};

/// Performance counters for monitoring and auto-tuning
#[derive(Debug, Default)]
pub struct PerfCounters {
    pub get_ops: AtomicU64,
    pub put_ops: AtomicU64,
    pub delete_ops: AtomicU64,
    pub range_ops: AtomicU64,
    pub join_ops: AtomicU64,

    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,

    pub cuckoo_kicks: AtomicU64,
    pub cuckoo_stash_uses: AtomicU64,
    pub cuckoo_resizes: AtomicU64,

    pub ttree_rotations: AtomicU64,
    pub ttree_splits: AtomicU64,
    pub ttree_merges: AtomicU64,
}

impl PerfCounters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get cache hit rate (0.0 to 1.0)
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        if hits + misses == 0.0 {
            0.0
        } else {
            hits / (hits + misses)
        }
    }

    /// Get operations per second (requires external timing)
    pub fn ops_per_sec(&self, elapsed_secs: f64) -> f64 {
        let total_ops = self.get_ops.load(Ordering::Relaxed)
            + self.put_ops.load(Ordering::Relaxed)
            + self.delete_ops.load(Ordering::Relaxed)
            + self.range_ops.load(Ordering::Relaxed)
            + self.join_ops.load(Ordering::Relaxed);

        total_ops as f64 / elapsed_secs
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.get_ops.store(0, Ordering::Relaxed);
        self.put_ops.store(0, Ordering::Relaxed);
        self.delete_ops.store(0, Ordering::Relaxed);
        self.range_ops.store(0, Ordering::Relaxed);
        self.join_ops.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.cuckoo_kicks.store(0, Ordering::Relaxed);
        self.cuckoo_stash_uses.store(0, Ordering::Relaxed);
        self.cuckoo_resizes.store(0, Ordering::Relaxed);
        self.ttree_rotations.store(0, Ordering::Relaxed);
        self.ttree_splits.store(0, Ordering::Relaxed);
        self.ttree_merges.store(0, Ordering::Relaxed);
    }

    /// Get summary statistics
    pub fn summary(&self) -> PerfSummary {
        PerfSummary {
            total_ops: self.get_ops.load(Ordering::Relaxed)
                + self.put_ops.load(Ordering::Relaxed)
                + self.delete_ops.load(Ordering::Relaxed),
            cache_hit_rate: self.cache_hit_rate(),
            cuckoo_kicks: self.cuckoo_kicks.load(Ordering::Relaxed),
            cuckoo_stash_uses: self.cuckoo_stash_uses.load(Ordering::Relaxed),
            cuckoo_resizes: self.cuckoo_resizes.load(Ordering::Relaxed),
            ttree_operations: self.ttree_rotations.load(Ordering::Relaxed)
                + self.ttree_splits.load(Ordering::Relaxed)
                + self.ttree_merges.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerfSummary {
    pub total_ops: u64,
    pub cache_hit_rate: f64,
    pub cuckoo_kicks: u64,
    pub cuckoo_stash_uses: u64,
    pub cuckoo_resizes: u64,
    pub ttree_operations: u64,
}

impl std::fmt::Display for PerfSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Performance Summary:")?;
        writeln!(f, "  Total Operations: {}", self.total_ops)?;
        writeln!(f, "  Cache Hit Rate: {:.2}%", self.cache_hit_rate * 100.0)?;
        writeln!(f, "  Cuckoo Kicks: {}", self.cuckoo_kicks)?;
        writeln!(f, "  Cuckoo Stash Uses: {}", self.cuckoo_stash_uses)?;
        writeln!(f, "  Cuckoo Resizes: {}", self.cuckoo_resizes)?;
        writeln!(f, "  T-Tree Operations: {}", self.ttree_operations)?;
        Ok(())
    }
}

/// Auto-tuning parameters based on runtime statistics
pub struct AutoTuner {
    counters: PerfCounters,
    config: TuningConfig,
}

#[derive(Debug, Clone)]
pub struct TuningConfig {
    /// Target cache hit rate (triggers optimization if below)
    pub target_cache_hit_rate: f64,

    /// Max acceptable cuckoo kicks before suggesting resize
    pub max_cuckoo_kicks_per_insert: f64,

    /// Enable adaptive prefetching
    pub adaptive_prefetch: bool,

    /// Enable SIMD optimizations
    pub enable_simd: bool,
}

impl Default for TuningConfig {
    fn default() -> Self {
        Self {
            target_cache_hit_rate: 0.95,
            max_cuckoo_kicks_per_insert: 10.0,
            adaptive_prefetch: true,
            enable_simd: cfg!(target_arch = "x86_64"),
        }
    }
}

impl AutoTuner {
    pub fn new(config: TuningConfig) -> Self {
        Self {
            counters: PerfCounters::new(),
            config,
        }
    }

    pub fn with_default() -> Self {
        Self::new(TuningConfig::default())
    }

    pub fn counters(&self) -> &PerfCounters {
        &self.counters
    }

    /// Analyze performance and suggest optimizations
    pub fn analyze(&self) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();
        let summary = self.counters.summary();

        // Check cache hit rate
        if summary.cache_hit_rate < self.config.target_cache_hit_rate {
            recommendations.push(Recommendation {
                priority: Priority::High,
                category: Category::CacheEfficiency,
                description: format!(
                    "Cache hit rate ({:.2}%) is below target ({:.2}%). Consider: \
                    1) Increasing shard count to improve locality, \
                    2) Enabling prefetching for range queries, \
                    3) Using smaller key/value sizes for better cache line utilization",
                    summary.cache_hit_rate * 100.0,
                    self.config.target_cache_hit_rate * 100.0
                ),
            });
        }

        // Check cuckoo performance
        if summary.total_ops > 0 {
            let kicks_per_op = summary.cuckoo_kicks as f64 / summary.total_ops as f64;
            if kicks_per_op > self.config.max_cuckoo_kicks_per_insert {
                recommendations.push(Recommendation {
                    priority: Priority::Medium,
                    category: Category::HashTableEfficiency,
                    description: format!(
                        "High cuckoo kick rate ({:.2} kicks/op). Consider: \
                        1) Increasing table size, \
                        2) Using better hash functions, \
                        3) Increasing bucket size from 4 to 8",
                        kicks_per_op
                    ),
                });
            }
        }

        // Check stash usage
        if summary.cuckoo_stash_uses > summary.total_ops / 100 {
            recommendations.push(Recommendation {
                priority: Priority::Medium,
                category: Category::HashTableEfficiency,
                description: format!(
                    "High stash usage ({} uses). The hash table may be undersized. \
                    Consider resizing or adjusting load factor threshold.",
                    summary.cuckoo_stash_uses
                ),
            });
        }

        // Check resize frequency
        if summary.cuckoo_resizes > 10 {
            recommendations.push(Recommendation {
                priority: Priority::Low,
                category: Category::MemoryAllocation,
                description: format!(
                    "Frequent resizes detected ({} resizes). Consider: \
                    1) Pre-allocating larger initial capacity, \
                    2) Adjusting load factor threshold",
                    summary.cuckoo_resizes
                ),
            });
        }

        // Suggest SIMD if not enabled
        if !self.config.enable_simd && cfg!(target_arch = "x86_64") {
            recommendations.push(Recommendation {
                priority: Priority::High,
                category: Category::CpuOptimization,
                description: "SIMD optimizations are available but not enabled. \
                    Enable AVX2/SSE4.2 for 2-4x faster bucket scans."
                    .to_string(),
            });
        }

        recommendations
    }

    /// Get current configuration
    pub fn config(&self) -> &TuningConfig {
        &self.config
    }
}

#[derive(Debug, Clone)]
pub struct Recommendation {
    pub priority: Priority,
    pub category: Category,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    CacheEfficiency,
    HashTableEfficiency,
    MemoryAllocation,
    CpuOptimization,
    ConcurrencyContention,
}

impl std::fmt::Display for Recommendation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{:?}] {:?}: {}",
            self.priority, self.category, self.description
        )
    }
}

/// Batch operation optimizer
/// Groups small operations to reduce overhead and improve cache efficiency
pub struct BatchOptimizer<K, V> {
    pending_puts: Vec<(K, V)>,
    pending_deletes: Vec<K>,
    batch_size: usize,
}

impl<K, V> BatchOptimizer<K, V> {
    pub fn new(batch_size: usize) -> Self {
        Self {
            pending_puts: Vec::with_capacity(batch_size),
            pending_deletes: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    pub fn add_put(&mut self, key: K, value: V) -> bool {
        self.pending_puts.push((key, value));
        self.pending_puts.len() >= self.batch_size
    }

    pub fn add_delete(&mut self, key: K) -> bool {
        self.pending_deletes.push(key);
        self.pending_deletes.len() >= self.batch_size
    }

    pub fn drain_puts(&mut self) -> Vec<(K, V)> {
        std::mem::take(&mut self.pending_puts)
    }

    pub fn drain_deletes(&mut self) -> Vec<K> {
        std::mem::take(&mut self.pending_deletes)
    }

    pub fn has_pending(&self) -> bool {
        !self.pending_puts.is_empty() || !self.pending_deletes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perf_counters() {
        let counters = PerfCounters::new();
        counters.get_ops.fetch_add(100, Ordering::Relaxed);
        counters.cache_hits.fetch_add(90, Ordering::Relaxed);
        counters.cache_misses.fetch_add(10, Ordering::Relaxed);

        assert_eq!(counters.cache_hit_rate(), 0.9);

        let summary = counters.summary();
        assert_eq!(summary.total_ops, 100);
        assert_eq!(summary.cache_hit_rate, 0.9);
    }

    #[test]
    fn test_auto_tuner_recommendations() {
        let tuner = AutoTuner::with_default();
        tuner.counters.get_ops.fetch_add(1000, Ordering::Relaxed);
        tuner.counters.cache_hits.fetch_add(800, Ordering::Relaxed);
        tuner
            .counters
            .cache_misses
            .fetch_add(200, Ordering::Relaxed);

        let recommendations = tuner.analyze();
        assert!(recommendations
            .iter()
            .any(|r| matches!(r.category, Category::CacheEfficiency)));
    }

    #[test]
    fn test_batch_optimizer() {
        let mut batch: BatchOptimizer<u64, String> = BatchOptimizer::new(3);

        assert!(!batch.add_put(1, "a".to_string()));
        assert!(!batch.add_put(2, "b".to_string()));
        assert!(batch.add_put(3, "c".to_string()));

        let puts = batch.drain_puts();
        assert_eq!(puts.len(), 3);
        assert!(!batch.has_pending());
    }
}
