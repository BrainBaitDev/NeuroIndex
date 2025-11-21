use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Prometheus-compatible metrics collector
#[derive(Clone)]
pub struct MetricsCollector {
    /// Total operations by type
    ops_get: Arc<AtomicU64>,
    ops_set: Arc<AtomicU64>,
    ops_del: Arc<AtomicU64>,
    ops_scan: Arc<AtomicU64>,

    /// Total errors
    errors_total: Arc<AtomicU64>,

    /// Start time for uptime calculation
    start_time: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            ops_get: Arc::new(AtomicU64::new(0)),
            ops_set: Arc::new(AtomicU64::new(0)),
            ops_del: Arc::new(AtomicU64::new(0)),
            ops_scan: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    /// Record a GET operation
    pub fn record_get(&self) {
        self.ops_get.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a SET operation
    pub fn record_set(&self) {
        self.ops_set.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a DELETE operation
    pub fn record_delete(&self) {
        self.ops_del.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a SCAN operation
    pub fn record_scan(&self) {
        self.ops_scan.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    #[allow(dead_code)]
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Export metrics in Prometheus text format
    pub fn export_prometheus(&self, total_keys: u64, memory_mb: u64) -> String {
        let uptime_seconds = self.start_time.elapsed().as_secs();

        let gets = self.ops_get.load(Ordering::Relaxed);
        let sets = self.ops_set.load(Ordering::Relaxed);
        let dels = self.ops_del.load(Ordering::Relaxed);
        let scans = self.ops_scan.load(Ordering::Relaxed);
        let errors = self.errors_total.load(Ordering::Relaxed);

        format!(
            r#"# HELP neuroindex_uptime_seconds Time since server started
# TYPE neuroindex_uptime_seconds gauge
neuroindex_uptime_seconds {}

# HELP neuroindex_operations_total Total number of operations by type
# TYPE neuroindex_operations_total counter
neuroindex_operations_total{{operation="get"}} {}
neuroindex_operations_total{{operation="set"}} {}
neuroindex_operations_total{{operation="delete"}} {}
neuroindex_operations_total{{operation="scan"}} {}

# HELP neuroindex_errors_total Total number of errors
# TYPE neuroindex_errors_total counter
neuroindex_errors_total {}

# HELP neuroindex_keys_total Current number of keys in database
# TYPE neuroindex_keys_total gauge
neuroindex_keys_total {}

# HELP neuroindex_memory_bytes Approximate memory usage in bytes
# TYPE neuroindex_memory_bytes gauge
neuroindex_memory_bytes {}

# HELP neuroindex_build_info Build information
# TYPE neuroindex_build_info gauge
neuroindex_build_info{{version="{}",rust_version="1.75"}} 1
"#,
            uptime_seconds,
            gets,
            sets,
            dels,
            scans,
            errors,
            total_keys,
            memory_mb * 1024 * 1024,
            env!("CARGO_PKG_VERSION")
        )
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector() {
        let metrics = MetricsCollector::new();

        metrics.record_get();
        metrics.record_get();
        metrics.record_set();
        metrics.record_delete();
        metrics.record_error();

        assert_eq!(metrics.ops_get.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.ops_set.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.ops_del.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.errors_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = MetricsCollector::new();

        metrics.record_get();
        metrics.record_set();

        let output = metrics.export_prometheus(100, 50);

        assert!(output.contains("neuroindex_operations_total{operation=\"get\"} 1"));
        assert!(output.contains("neuroindex_operations_total{operation=\"set\"} 1"));
        assert!(output.contains("neuroindex_keys_total 100"));
        assert!(output.contains("neuroindex_memory_bytes"));
    }
}
