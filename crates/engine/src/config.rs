use crate::eviction::{EvictionPolicy, ResourceLimits};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use wal::{SnapshotOptions, WalOptions};

/// Configuration for the NeuroIndex engine, loaded from TOML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub resources: ResourcesConfig,

    #[serde(default)]
    pub persistence: PersistenceConfigSettings,
}

/// Server settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Port for RESP server (default: 6379)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Port for HTTP server (default: 8080)
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// Bind address (default: "127.0.0.1")
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Number of worker threads (default: num_cpus)
    #[serde(default = "default_workers")]
    pub workers: usize,
}

/// Resource limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcesConfig {
    /// Maximum memory in bytes (supports units: KB, MB, GB)
    /// Example: "2GB" or 2147483648
    #[serde(default = "default_max_memory")]
    pub max_memory: String,

    /// Maximum number of keys (0 = unlimited)
    #[serde(default)]
    pub max_keys: usize,

    /// Maximum concurrent clients (0 = unlimited)
    #[serde(default = "default_max_clients")]
    pub max_clients: usize,

    /// Maximum request size in bytes
    #[serde(default = "default_max_request_size")]
    pub max_request_size: usize,

    /// Eviction policy: "none", "lru", "lfu", "random", "volatile-lru"
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String,

    /// Eviction threshold (0.0 - 1.0)
    #[serde(default = "default_eviction_threshold")]
    pub eviction_threshold: f64,
}

/// Persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfigSettings {
    /// Enable persistence (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// WAL file path
    #[serde(default = "default_wal_path")]
    pub wal_path: String,

    /// Snapshot file path
    #[serde(default = "default_snapshot_path")]
    pub snapshot_path: String,

    /// Snapshot interval in seconds (0 = disabled)
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,

    /// Enable compression (default: true)
    #[serde(default = "default_true")]
    pub compression: bool,

    /// Compression level (1-22 for zstd)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,

    /// Number of shards (must be power of 2)
    #[serde(default = "default_shards")]
    pub shards: usize,

    /// Initial capacity per shard
    #[serde(default = "default_capacity")]
    pub capacity_per_shard: usize,
}

#[derive(Clone)]
pub struct PersistenceConfig {
    pub shards_pow2: usize,
    pub shard_cap_pow2: usize,
    pub wal_path: PathBuf,
    pub wal_options: WalOptions,
    pub snapshot_path: PathBuf,
    pub snapshot_options: SnapshotOptions,
    pub resource_limits: Option<ResourceLimits>,
}

impl PersistenceConfig {
    pub fn new<P, Q>(
        shards_pow2: usize,
        shard_cap_pow2: usize,
        wal_path: P,
        snapshot_path: Q,
    ) -> Self
    where
        P: Into<PathBuf>,
        Q: Into<PathBuf>,
    {
        Self {
            shards_pow2,
            shard_cap_pow2,
            wal_path: wal_path.into(),
            wal_options: WalOptions::default(),
            snapshot_path: snapshot_path.into(),
            snapshot_options: SnapshotOptions::default(),
            resource_limits: None,
        }
    }

    pub fn with_wal_options(mut self, wal_options: WalOptions) -> Self {
        self.wal_options = wal_options;
        self
    }

    pub fn with_snapshot_options(mut self, snapshot_options: SnapshotOptions) -> Self {
        self.snapshot_options = snapshot_options;
        self
    }

    pub fn with_resource_limits(mut self, resource_limits: ResourceLimits) -> Self {
        self.resource_limits = Some(resource_limits);
        self
    }
}

// Default functions for serde
fn default_port() -> u16 {
    6379
}
fn default_http_port() -> u16 {
    8080
}
fn default_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_workers() -> usize {
    num_cpus::get()
}

fn default_max_memory() -> String {
    "2GB".to_string()
}
fn default_max_clients() -> usize {
    10000
}
fn default_max_request_size() -> usize {
    512 * 1024 * 1024
} // 512MB
fn default_eviction_policy() -> String {
    "lru".to_string()
}
fn default_eviction_threshold() -> f64 {
    0.95
}

fn default_true() -> bool {
    true
}
fn default_wal_path() -> String {
    "./data/neuroindex.wal".to_string()
}
fn default_snapshot_path() -> String {
    "./data/neuroindex.snap".to_string()
}
fn default_snapshot_interval() -> u64 {
    300
} // 5 minutes
fn default_compression_level() -> i32 {
    3
}
fn default_shards() -> usize {
    16
}
fn default_capacity() -> usize {
    1024
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            http_port: default_http_port(),
            bind: default_bind(),
            workers: default_workers(),
        }
    }
}

impl Default for ResourcesConfig {
    fn default() -> Self {
        Self {
            max_memory: default_max_memory(),
            max_keys: 0,
            max_clients: default_max_clients(),
            max_request_size: default_max_request_size(),
            eviction_policy: default_eviction_policy(),
            eviction_threshold: default_eviction_threshold(),
        }
    }
}

impl Default for PersistenceConfigSettings {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            wal_path: default_wal_path(),
            snapshot_path: default_snapshot_path(),
            snapshot_interval: default_snapshot_interval(),
            compression: default_true(),
            compression_level: default_compression_level(),
            shards: default_shards(),
            capacity_per_shard: default_capacity(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            resources: ResourcesConfig::default(),
            persistence: PersistenceConfigSettings::default(),
        }
    }
}

impl EngineConfig {
    /// Load configuration from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let contents = fs::read_to_string(path)?;
        toml::from_str(&contents).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("TOML parse error: {}", e),
            )
        })
    }

    /// Save configuration to TOML file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let contents = toml::to_string_pretty(self).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("TOML serialize error: {}", e),
            )
        })?;
        fs::write(path, contents)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate eviction policy
        let valid_policies = ["none", "lru", "lfu", "random", "volatile-lru"];
        if !valid_policies.contains(&self.resources.eviction_policy.as_str()) {
            return Err(format!(
                "Invalid eviction_policy: '{}'. Must be one of: {}",
                self.resources.eviction_policy,
                valid_policies.join(", ")
            ));
        }

        // Validate eviction threshold
        if self.resources.eviction_threshold < 0.0 || self.resources.eviction_threshold > 1.0 {
            return Err(format!(
                "eviction_threshold must be between 0.0 and 1.0, got {}",
                self.resources.eviction_threshold
            ));
        }

        // Validate shards (must be power of 2)
        if !self.persistence.shards.is_power_of_two() {
            return Err(format!(
                "shards must be a power of 2, got {}",
                self.persistence.shards
            ));
        }

        // Validate compression level
        if self.persistence.compression_level < 1 || self.persistence.compression_level > 22 {
            return Err(format!(
                "compression_level must be between 1 and 22, got {}",
                self.persistence.compression_level
            ));
        }

        // Validate ports
        if self.server.port == 0 {
            return Err("port cannot be 0".to_string());
        }
        if self.server.http_port == 0 {
            return Err("http_port cannot be 0".to_string());
        }
        if self.server.port == self.server.http_port {
            return Err(format!(
                "port and http_port cannot be the same ({})",
                self.server.port
            ));
        }

        Ok(())
    }

    /// Parse memory string (e.g., "2GB", "512MB") to bytes
    pub fn parse_memory(&self) -> Result<usize, String> {
        parse_memory_string(&self.resources.max_memory)
    }

    pub fn to_persistence_config(&self) -> Result<PersistenceConfig, String> {
        let resource_limits = ResourceLimits {
            max_memory: self.parse_memory().unwrap() as u64,
            max_keys: self.resources.max_keys as u64,
            eviction_policy: match self.resources.eviction_policy.as_str() {
                "lru" => EvictionPolicy::LRU,
                "lfu" => EvictionPolicy::LFU,
                "random" => EvictionPolicy::Random,
                "volatile-lru" => EvictionPolicy::VolatileLRU,
                "none" | _ => EvictionPolicy::NoEviction,
            },
            eviction_target_percentage: self.resources.eviction_threshold as u8,
        };

        Ok(PersistenceConfig {
            shards_pow2: self.persistence.shards.next_power_of_two().trailing_zeros() as usize,
            shard_cap_pow2: self
                .persistence
                .capacity_per_shard
                .next_power_of_two()
                .trailing_zeros() as usize,
            wal_path: self.persistence.wal_path.clone().into(),
            wal_options: WalOptions::default(),
            snapshot_path: self.persistence.snapshot_path.clone().into(),
            snapshot_options: SnapshotOptions {
                ..Default::default()
            },
            resource_limits: Some(resource_limits),
        })
    }
}

/// Parse memory string like "2GB", "512MB", "1024KB" to bytes
pub fn parse_memory_string(s: &str) -> Result<usize, String> {
    let s = s.trim().to_uppercase();

    // Try to parse as plain number first
    if let Ok(bytes) = s.parse::<usize>() {
        return Ok(bytes);
    }

    // Parse with unit
    let (num_str, unit) = if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with("B") {
        (&s[..s.len() - 1], 1)
    } else {
        return Err(format!(
            "Invalid memory format: '{}'. Use format like '2GB', '512MB', or plain bytes",
            s
        ));
    };

    let num: usize = num_str
        .trim()
        .parse()
        .map_err(|_| format!("Invalid number in memory string: '{}'", num_str))?;

    Ok(num * unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = EngineConfig::default();
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.http_port, 8080);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.resources.eviction_policy, "lru");
        assert_eq!(config.persistence.shards, 16);
    }

    #[test]
    fn test_validate_good_config() {
        let config = EngineConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_bad_eviction_policy() {
        let mut config = EngineConfig::default();
        config.resources.eviction_policy = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_bad_threshold() {
        let mut config = EngineConfig::default();
        config.resources.eviction_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_non_power_of_two_shards() {
        let mut config = EngineConfig::default();
        config.persistence.shards = 10;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_same_ports() {
        let mut config = EngineConfig::default();
        config.server.port = 8080;
        config.server.http_port = 8080;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_parse_memory_gb() {
        assert_eq!(parse_memory_string("2GB").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_string("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_mb() {
        assert_eq!(parse_memory_string("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_string("100mb").unwrap(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_kb() {
        assert_eq!(parse_memory_string("1024KB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_bytes() {
        assert_eq!(parse_memory_string("1048576").unwrap(), 1048576);
        assert_eq!(parse_memory_string("1024B").unwrap(), 1024);
    }

    #[test]
    fn test_parse_memory_invalid() {
        assert!(parse_memory_string("2XB").is_err());
        assert!(parse_memory_string("invalid").is_err());
    }
}
