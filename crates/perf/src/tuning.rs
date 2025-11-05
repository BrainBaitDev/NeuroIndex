// Performance tuning parameters for NeuroIndex
// Based on empirical analysis from papers (Lehman & Carey 1986, Weiss 2014)

use std::env;

/// T-Tree node parameters
#[derive(Debug, Clone, Copy)]
pub struct TTreeParams {
    /// Max keys per node (cache-line friendly: 4, 8, 16)
    pub max_keys: usize,
    /// Split threshold (when to split a full node)
    pub split_threshold: usize,
    /// Merge threshold (when to merge sparse nodes)
    pub merge_threshold: usize,
}

impl Default for TTreeParams {
    fn default() -> Self {
        // Optimal for L1 cache (64 bytes)
        // With u64 keys + values: 8 keys = 128 bytes (fits in 2 cache lines)
        Self {
            max_keys: 8,
            split_threshold: 8,
            merge_threshold: 2,
        }
    }
}

impl TTreeParams {
    /// Conservative: smaller nodes, faster splits
    pub fn conservative() -> Self {
        Self {
            max_keys: 4,
            split_threshold: 4,
            merge_threshold: 1,
        }
    }

    /// Aggressive: larger nodes, more data per node
    pub fn aggressive() -> Self {
        Self {
            max_keys: 16,
            split_threshold: 16,
            merge_threshold: 4,
        }
    }

    /// Cache-optimized for specific key/value sizes
    pub fn for_kv_size(key_size: usize, val_size: usize) -> Self {
        let pair_size = key_size + val_size;
        // Target: fit in 1-2 cache lines (64-128 bytes)
        let max_keys = (128 / pair_size).max(2).min(16);
        Self {
            max_keys,
            split_threshold: max_keys,
            merge_threshold: max_keys / 4,
        }
    }

    /// Load from environment variables
    pub fn from_env() -> Self {
        let max_keys = env::var("NEUROINDEX_TTREE_MAX_KEYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        Self {
            max_keys,
            split_threshold: max_keys,
            merge_threshold: max_keys / 4,
        }
    }
}

/// Cuckoo hash parameters
#[derive(Debug, Clone, Copy)]
pub struct CuckooParams {
    /// Bucket size (typically 4 or 8)
    pub bucket_size: usize,
    /// Max kick-out attempts before using stash
    pub max_kicks: usize,
    /// Stash size (overflow area)
    pub stash_size: usize,
    /// Load factor threshold for resize (0.0 - 1.0)
    pub resize_load: f64,
}

impl Default for CuckooParams {
    fn default() -> Self {
        // Balanced for general workloads (Weiss 2014)
        Self {
            bucket_size: 4,
            max_kicks: 96,
            stash_size: 16,
            resize_load: 0.90,
        }
    }
}

impl CuckooParams {
    /// High-throughput: larger buckets, more kicks
    pub fn high_throughput() -> Self {
        Self {
            bucket_size: 8,
            max_kicks: 128,
            stash_size: 32,
            resize_load: 0.95,
        }
    }

    /// Low-latency: smaller buckets, fewer kicks, aggressive resize
    pub fn low_latency() -> Self {
        Self {
            bucket_size: 4,
            max_kicks: 64,
            stash_size: 8,
            resize_load: 0.85,
        }
    }

    /// Load from environment
    pub fn from_env() -> Self {
        Self {
            bucket_size: env::var("NEUROINDEX_CUCKOO_BUCKET_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4),
            max_kicks: env::var("NEUROINDEX_CUCKOO_MAX_KICKS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(96),
            stash_size: env::var("NEUROINDEX_CUCKOO_STASH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16),
            resize_load: env::var("NEUROINDEX_CUCKOO_RESIZE_LOAD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.90),
        }
    }
}

/// Arena allocator parameters
#[derive(Debug, Clone, Copy)]
pub struct ArenaParams {
    /// Chunk size for bump allocator (bytes)
    pub chunk_size: usize,
    /// Slab sizes for value storage
    pub slab_sizes: [usize; 8],
}

impl Default for ArenaParams {
    fn default() -> Self {
        Self {
            // 1 MB chunks
            chunk_size: 1 << 20,
            // Slab sizes: 16, 32, 64, 128, 256, 512, 1024, 2048 bytes
            slab_sizes: [16, 32, 64, 128, 256, 512, 1024, 2048],
        }
    }
}

impl ArenaParams {
    /// Memory-intensive workload: larger chunks
    pub fn large_memory() -> Self {
        Self {
            chunk_size: 16 << 20, // 16 MB
            slab_sizes: [32, 64, 128, 256, 512, 1024, 2048, 4096],
        }
    }

    /// Small memory footprint
    pub fn small_memory() -> Self {
        Self {
            chunk_size: 256 << 10, // 256 KB
            slab_sizes: [8, 16, 32, 64, 128, 256, 512, 1024],
        }
    }
}

/// Prefetch parameters
#[derive(Debug, Clone, Copy)]
pub struct PrefetchParams {
    /// Enable prefetching
    pub enabled: bool,
    /// Prefetch distance (levels ahead in tree)
    pub distance: usize,
    /// Prefetch hint (T0, T1, T2, NTA)
    pub hint: i32,
}

impl Default for PrefetchParams {
    fn default() -> Self {
        Self {
            enabled: true,
            distance: 2,
            hint: 3, // T0 (all cache levels)
        }
    }
}

impl PrefetchParams {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            distance: 0,
            hint: 0,
        }
    }

    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            distance: 3,
            hint: 3,
        }
    }
}

/// Global performance configuration
#[derive(Debug, Clone, Copy)]
pub struct PerfConfig {
    pub ttree: TTreeParams,
    pub cuckoo: CuckooParams,
    pub arena: ArenaParams,
    pub prefetch: PrefetchParams,
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            ttree: TTreeParams::default(),
            cuckoo: CuckooParams::default(),
            arena: ArenaParams::default(),
            prefetch: PrefetchParams::default(),
        }
    }
}

impl PerfConfig {
    /// High-performance profile (low latency, aggressive caching)
    pub fn high_performance() -> Self {
        Self {
            ttree: TTreeParams::aggressive(),
            cuckoo: CuckooParams::low_latency(),
            arena: ArenaParams::default(),
            prefetch: PrefetchParams::aggressive(),
        }
    }

    /// High-throughput profile (larger buffers, batch-friendly)
    pub fn high_throughput() -> Self {
        Self {
            ttree: TTreeParams::aggressive(),
            cuckoo: CuckooParams::high_throughput(),
            arena: ArenaParams::large_memory(),
            prefetch: PrefetchParams::default(),
        }
    }

    /// Memory-constrained profile
    pub fn low_memory() -> Self {
        Self {
            ttree: TTreeParams::conservative(),
            cuckoo: CuckooParams::low_latency(),
            arena: ArenaParams::small_memory(),
            prefetch: PrefetchParams::disabled(),
        }
    }

    /// Load from environment variables
    pub fn from_env() -> Self {
        let profile = env::var("NEUROINDEX_PROFILE").unwrap_or_default();
        match profile.as_str() {
            "high-performance" => Self::high_performance(),
            "high-throughput" => Self::high_throughput(),
            "low-memory" => Self::low_memory(),
            _ => Self {
                ttree: TTreeParams::from_env(),
                cuckoo: CuckooParams::from_env(),
                arena: ArenaParams::default(),
                prefetch: PrefetchParams::default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttree_params() {
        let params = TTreeParams::default();
        assert_eq!(params.max_keys, 8);

        let conservative = TTreeParams::conservative();
        assert_eq!(conservative.max_keys, 4);

        let aggressive = TTreeParams::aggressive();
        assert_eq!(aggressive.max_keys, 16);
    }

    #[test]
    fn test_cuckoo_params() {
        let params = CuckooParams::default();
        assert_eq!(params.bucket_size, 4);
        assert_eq!(params.max_kicks, 96);

        let low_lat = CuckooParams::low_latency();
        assert!(low_lat.resize_load < 0.90);
    }

    #[test]
    fn test_kv_size_tuning() {
        // 8-byte key + 8-byte value = 16 bytes/pair
        // 128 / 16 = 8 keys per node
        let params = TTreeParams::for_kv_size(8, 8);
        assert_eq!(params.max_keys, 8);

        // 4-byte key + 4-byte value = 8 bytes/pair
        // 128 / 8 = 16 keys per node
        let params = TTreeParams::for_kv_size(4, 4);
        assert_eq!(params.max_keys, 16);
    }

    #[test]
    fn test_perf_profiles() {
        let hp = PerfConfig::high_performance();
        assert_eq!(hp.ttree.max_keys, 16);

        let ht = PerfConfig::high_throughput();
        assert_eq!(ht.cuckoo.bucket_size, 8);

        let lm = PerfConfig::low_memory();
        assert!(!lm.prefetch.enabled);
    }
}
