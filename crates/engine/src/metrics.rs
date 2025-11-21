use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Metrics collector for monitoring and observability
pub struct MetricsCollector {
    // Operation counters
    gets: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,

    // Hit/miss tracking
    hits: AtomicU64,
    misses: AtomicU64,

    // Error tracking
    errors: AtomicU64,

    // Memory tracking
    memory_bytes: AtomicUsize,
    keys_count: AtomicUsize,

    // Latency tracking (simple moving average for now)
    total_get_latency_ns: AtomicU64,
    total_put_latency_ns: AtomicU64,
    total_delete_latency_ns: AtomicU64,

    // Start time for uptime calculation
    start_time: Instant,
}

/// Snapshot of current metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    // Operations
    pub total_gets: u64,
    pub total_puts: u64,
    pub total_deletes: u64,
    pub total_operations: u64,

    // Hit rate
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,

    // Errors
    pub errors: u64,

    // Memory
    pub memory_bytes: usize,
    pub keys_count: usize,

    // Average latencies (nanoseconds)
    pub avg_get_latency_ns: u64,
    pub avg_put_latency_ns: u64,
    pub avg_delete_latency_ns: u64,

    // Uptime
    pub uptime_seconds: u64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            memory_bytes: AtomicUsize::new(0),
            keys_count: AtomicUsize::new(0),
            total_get_latency_ns: AtomicU64::new(0),
            total_put_latency_ns: AtomicU64::new(0),
            total_delete_latency_ns: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    // === Operation tracking ===

    /// Record a GET operation
    pub fn record_get(&self, latency_ns: u64, hit: bool) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.total_get_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);

        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a PUT operation
    pub fn record_put(&self, latency_ns: u64) {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.total_put_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record a DELETE operation
    pub fn record_delete(&self, latency_ns: u64) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.total_delete_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    // === Memory tracking ===

    /// Update memory usage
    pub fn set_memory_bytes(&self, bytes: usize) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Update keys count
    pub fn set_keys_count(&self, count: usize) {
        self.keys_count.store(count, Ordering::Relaxed);
    }

    // === Snapshot ===

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let gets = self.gets.load(Ordering::Relaxed);
        let puts = self.puts.load(Ordering::Relaxed);
        let deletes = self.deletes.load(Ordering::Relaxed);
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);

        let total_get_latency = self.total_get_latency_ns.load(Ordering::Relaxed);
        let total_put_latency = self.total_put_latency_ns.load(Ordering::Relaxed);
        let total_delete_latency = self.total_delete_latency_ns.load(Ordering::Relaxed);

        let avg_get_latency_ns = if gets > 0 {
            total_get_latency / gets
        } else {
            0
        };

        let avg_put_latency_ns = if puts > 0 {
            total_put_latency / puts
        } else {
            0
        };

        let avg_delete_latency_ns = if deletes > 0 {
            total_delete_latency / deletes
        } else {
            0
        };

        let total_lookups = hits + misses;
        let hit_rate = if total_lookups > 0 {
            (hits as f64) / (total_lookups as f64)
        } else {
            0.0
        };

        MetricsSnapshot {
            total_gets: gets,
            total_puts: puts,
            total_deletes: deletes,
            total_operations: gets + puts + deletes,
            hits,
            misses,
            hit_rate,
            errors: self.errors.load(Ordering::Relaxed),
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            keys_count: self.keys_count.load(Ordering::Relaxed),
            avg_get_latency_ns,
            avg_put_latency_ns,
            avg_delete_latency_ns,
            uptime_seconds: self.start_time.elapsed().as_secs(),
        }
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let snapshot = self.snapshot();

        let mut output = String::new();

        // Help text and type definitions
        output.push_str("# HELP neuroindex_operations_total Total number of operations by type\n");
        output.push_str("# TYPE neuroindex_operations_total counter\n");
        output.push_str(&format!(
            "neuroindex_operations_total{{operation=\"get\"}} {}\n",
            snapshot.total_gets
        ));
        output.push_str(&format!(
            "neuroindex_operations_total{{operation=\"put\"}} {}\n",
            snapshot.total_puts
        ));
        output.push_str(&format!(
            "neuroindex_operations_total{{operation=\"delete\"}} {}\n",
            snapshot.total_deletes
        ));
        output.push_str("\n");

        output.push_str("# HELP neuroindex_cache_requests_total Total cache requests by result\n");
        output.push_str("# TYPE neuroindex_cache_requests_total counter\n");
        output.push_str(&format!(
            "neuroindex_cache_requests_total{{result=\"hit\"}} {}\n",
            snapshot.hits
        ));
        output.push_str(&format!(
            "neuroindex_cache_requests_total{{result=\"miss\"}} {}\n",
            snapshot.misses
        ));
        output.push_str("\n");

        output.push_str("# HELP neuroindex_cache_hit_rate Cache hit rate (0.0 to 1.0)\n");
        output.push_str("# TYPE neuroindex_cache_hit_rate gauge\n");
        output.push_str(&format!(
            "neuroindex_cache_hit_rate {:.4}\n",
            snapshot.hit_rate
        ));
        output.push_str("\n");

        output.push_str("# HELP neuroindex_errors_total Total number of errors\n");
        output.push_str("# TYPE neuroindex_errors_total counter\n");
        output.push_str(&format!("neuroindex_errors_total {}\n", snapshot.errors));
        output.push_str("\n");

        output.push_str("# HELP neuroindex_memory_bytes Current memory usage in bytes\n");
        output.push_str("# TYPE neuroindex_memory_bytes gauge\n");
        output.push_str(&format!(
            "neuroindex_memory_bytes {}\n",
            snapshot.memory_bytes
        ));
        output.push_str("\n");

        output.push_str("# HELP neuroindex_keys_count Current number of keys\n");
        output.push_str("# TYPE neuroindex_keys_count gauge\n");
        output.push_str(&format!("neuroindex_keys_count {}\n", snapshot.keys_count));
        output.push_str("\n");

        output.push_str(
            "# HELP neuroindex_operation_latency_nanoseconds Average operation latency\n",
        );
        output.push_str("# TYPE neuroindex_operation_latency_nanoseconds gauge\n");
        output.push_str(&format!(
            "neuroindex_operation_latency_nanoseconds{{operation=\"get\"}} {}\n",
            snapshot.avg_get_latency_ns
        ));
        output.push_str(&format!(
            "neuroindex_operation_latency_nanoseconds{{operation=\"put\"}} {}\n",
            snapshot.avg_put_latency_ns
        ));
        output.push_str(&format!(
            "neuroindex_operation_latency_nanoseconds{{operation=\"delete\"}} {}\n",
            snapshot.avg_delete_latency_ns
        ));
        output.push_str("\n");

        output.push_str("# HELP neuroindex_uptime_seconds Uptime in seconds\n");
        output.push_str("# TYPE neuroindex_uptime_seconds counter\n");
        output.push_str(&format!(
            "neuroindex_uptime_seconds {}\n",
            snapshot.uptime_seconds
        ));

        output
    }

    /// Reset all metrics (useful for testing)
    pub fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_get_latency_ns.store(0, Ordering::Relaxed);
        self.total_put_latency_ns.store(0, Ordering::Relaxed);
        self.total_delete_latency_ns.store(0, Ordering::Relaxed);
        // Note: memory_bytes and keys_count are not reset as they reflect current state
        // Note: start_time is not reset
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "NeuroIndex Metrics:")?;
        writeln!(f, "  Operations:")?;
        writeln!(f, "    - GET:    {}", self.total_gets)?;
        writeln!(f, "    - PUT:    {}", self.total_puts)?;
        writeln!(f, "    - DELETE: {}", self.total_deletes)?;
        writeln!(f, "    - TOTAL:  {}", self.total_operations)?;
        writeln!(f, "  Cache:")?;
        writeln!(f, "    - Hits:      {}", self.hits)?;
        writeln!(f, "    - Misses:    {}", self.misses)?;
        writeln!(f, "    - Hit Rate:  {:.2}%", self.hit_rate * 100.0)?;
        writeln!(f, "  Memory:")?;
        writeln!(
            f,
            "    - Usage: {} bytes ({:.2} MB)",
            self.memory_bytes,
            self.memory_bytes as f64 / 1_048_576.0
        )?;
        writeln!(f, "    - Keys:  {}", self.keys_count)?;
        writeln!(f, "  Latency (avg):")?;
        writeln!(
            f,
            "    - GET:    {:6} ns ({:.2} µs)",
            self.avg_get_latency_ns,
            self.avg_get_latency_ns as f64 / 1000.0
        )?;
        writeln!(
            f,
            "    - PUT:    {:6} ns ({:.2} µs)",
            self.avg_put_latency_ns,
            self.avg_put_latency_ns as f64 / 1000.0
        )?;
        writeln!(
            f,
            "    - DELETE: {:6} ns ({:.2} µs)",
            self.avg_delete_latency_ns,
            self.avg_delete_latency_ns as f64 / 1000.0
        )?;
        writeln!(f, "  Errors: {}", self.errors)?;
        writeln!(f, "  Uptime: {}s", self.uptime_seconds)?;
        Ok(())
    }
}

/// Helper to measure operation latency
pub struct LatencyTimer {
    start: Instant,
}

impl LatencyTimer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_metrics_collector_operations() {
        let metrics = MetricsCollector::new();

        metrics.record_get(1000, true);
        metrics.record_get(2000, false);
        metrics.record_put(1500);
        metrics.record_delete(500);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.total_gets, 2);
        assert_eq!(snapshot.total_puts, 1);
        assert_eq!(snapshot.total_deletes, 1);
        assert_eq!(snapshot.hits, 1);
        assert_eq!(snapshot.misses, 1);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let metrics = MetricsCollector::new();

        metrics.record_get(100, true);
        metrics.record_get(100, true);
        metrics.record_get(100, true);
        metrics.record_get(100, false);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.hit_rate, 0.75); // 3 hits out of 4
    }

    #[test]
    fn test_latency_tracking() {
        let metrics = MetricsCollector::new();

        metrics.record_get(1000, true);
        metrics.record_get(3000, true);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.avg_get_latency_ns, 2000); // (1000 + 3000) / 2
    }

    #[test]
    fn test_memory_tracking() {
        let metrics = MetricsCollector::new();

        metrics.set_memory_bytes(1024);
        metrics.set_keys_count(100);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.memory_bytes, 1024);
        assert_eq!(snapshot.keys_count, 100);
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = MetricsCollector::new();

        metrics.record_get(1000, true);
        metrics.record_put(2000);
        metrics.set_memory_bytes(1024);

        let prometheus = metrics.export_prometheus();

        assert!(prometheus.contains("neuroindex_operations_total{operation=\"get\"} 1"));
        assert!(prometheus.contains("neuroindex_operations_total{operation=\"put\"} 1"));
        assert!(prometheus.contains("neuroindex_memory_bytes 1024"));
    }

    #[test]
    fn test_uptime() {
        let metrics = MetricsCollector::new();

        thread::sleep(Duration::from_millis(100));

        let snapshot = metrics.snapshot();

        assert!(snapshot.uptime_seconds >= 0);
    }

    #[test]
    fn test_reset() {
        let metrics = MetricsCollector::new();

        metrics.record_get(1000, true);
        metrics.record_put(2000);

        metrics.reset();

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.total_gets, 0);
        assert_eq!(snapshot.total_puts, 0);
    }

    #[test]
    fn test_latency_timer() {
        let timer = LatencyTimer::start();
        thread::sleep(Duration::from_micros(100));
        let elapsed = timer.elapsed_ns();

        assert!(elapsed > 0);
    }
}
