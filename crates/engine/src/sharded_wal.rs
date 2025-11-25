use crate::Engine;
use ahash::AHasher;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wal::{WalOptions, WalRecord, WalWriter};

/// Sharded Write-Ahead Log for parallel write throughput
///
/// Instead of a single WAL file (bottleneck), writes are distributed
/// across multiple WAL shards based on key hash. Each shard can be
/// written to independently, enabling parallel writes.
///
/// Benefits:
/// - Higher write throughput (N shards = ~N× throughput)
/// - Reduced lock contention
/// - Better cache locality
/// - Simpler than single-writer queue
pub struct ShardedWal<K, V> {
    shards: Vec<Arc<Mutex<WalWriter<K, V>>>>,
    num_shards: usize,
    base_path: PathBuf,
}

impl<K, V> ShardedWal<K, V>
where
    K: Serialize + Clone + Hash + art::AsBytes,
    V: Serialize + Clone,
{
    /// Create a new sharded WAL with N shards
    ///
    /// # Arguments
    /// * `base_path` - Base path for WAL files (e.g., "data/neuroindex.wal")
    /// * `num_shards` - Number of WAL shards (typically num_cpus or power of 2)
    /// * `options` - WAL options (compression, etc.)
    ///
    /// If base_path ends with .wal, creates: base_path.0, base_path.1, ...
    /// Otherwise creates: base_path/wal.0, base_path/wal.1, ...
    pub fn new<P: AsRef<Path>>(
        base_path: P,
        num_shards: usize,
        options: WalOptions,
    ) -> io::Result<Self> {
        assert!(num_shards > 0, "num_shards must be > 0");
        assert!(
            num_shards.is_power_of_two(),
            "num_shards should be power of 2 for better distribution"
        );

        let base_path = base_path.as_ref();

        // Determine shard naming strategy
        let (parent_dir, shard_pattern) = if let Some(path_str) = base_path.to_str() {
            if path_str.ends_with(".wal") {
                // e.g., "data/neuroindex.wal" -> create "data/neuroindex.wal.0", etc.
                let parent = base_path.parent().unwrap_or(Path::new("."));
                (parent.to_path_buf(), base_path.to_path_buf())
            } else {
                // e.g., "data/wal_dir" -> create "data/wal_dir/wal.0", etc.
                (base_path.to_path_buf(), base_path.join("wal"))
            }
        } else {
            (base_path.to_path_buf(), base_path.join("wal"))
        };

        std::fs::create_dir_all(&parent_dir)?;

        let mut shards = Vec::with_capacity(num_shards);

        for shard_id in 0..num_shards {
            let shard_path = PathBuf::from(format!("{}.{}", shard_pattern.display(), shard_id));
            let writer = WalWriter::open(shard_path, options)?;
            shards.push(Arc::new(Mutex::new(writer)));
        }

        Ok(Self {
            shards,
            num_shards,
            base_path: shard_pattern,
        })
    }

    /// Get shard index for a key using AHash
    fn shard_for_key(&self, key: &K) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize) % self.num_shards
    }

    /// Append a PUT record to the appropriate shard
    pub fn append_put(&self, key: &K, value: &V) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_put(key, value)
    }

    /// Append a DELETE record to the appropriate shard
    pub fn append_delete(&self, key: &K) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_delete(key)
    }

    /// Append a TAG record to the appropriate shard
    pub fn append_tag(&self, key: &K, tag: u64) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_tag(key, tag)
    }

    /// Append an UNTAG record to the appropriate shard
    pub fn append_untag(&self, key: &K, tag: u64) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_untag(key, tag)
    }

    /// Append a TTL SET record to the appropriate shard
    pub fn append_ttl_set(&self, key: &K, expires_at_ms: u64) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_ttl_set(key, expires_at_ms)
    }

    /// Append a TTL REMOVE record to the appropriate shard
    pub fn append_ttl_remove(&self, key: &K) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_ttl_remove(key)
    }

    /// Append a generic record to the appropriate shard
    pub fn append_record(&self, key: &K, record: &WalRecord<K, V>) -> io::Result<()> {
        let shard_idx = self.shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.append_record(record)
    }

    /// Flush all shards to disk
    pub fn flush_all(&self) -> io::Result<()> {
        for shard in &self.shards {
            let mut writer = shard.lock();
            writer.flush()?;
        }
        Ok(())
    }

    /// Truncate all shards (clear WAL after snapshot)
    pub fn truncate_all(&self) -> io::Result<()> {
        for shard in &self.shards {
            let mut writer = shard.lock();
            writer.reset()?;
        }
        Ok(())
    }

    /// Get number of shards
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Get base path
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Get shard paths for recovery
    pub fn shard_paths(&self) -> Vec<PathBuf> {
        (0..self.num_shards)
            .map(|i| PathBuf::from(format!("{}.{}", self.base_path.display(), i)))
            .collect()
    }
}

/// Recovery from sharded WAL
///
/// Reads all shard files and replays records into the engine
pub struct ShardedWalRecovery;

impl ShardedWalRecovery {
    /// Recover an engine from sharded WAL files
    ///
    /// Reads all WAL shards in parallel and replays operations.
    /// Order within a shard is preserved, but cross-shard ordering may vary.
    /// This is acceptable because operations on different keys commute.
    pub fn recover<K, V>(
        engine: &mut Engine<K, V>,
        base_path: &Path,
        num_shards: usize,
    ) -> io::Result<ShardedWalRecoveryStats>
    where
        K: Serialize
            + DeserializeOwned
            + Clone
            + Hash
            + Eq
            + Ord
            + Send
            + Sync
            + art::AsBytes
            + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        // Mirror shard naming used by ShardedWal::new
        let shard_pattern = if let Some(path_str) = base_path.to_str() {
            if path_str.ends_with(".wal") {
                base_path.to_path_buf()
            } else {
                base_path.join("wal")
            }
        } else {
            base_path.join("wal")
        };

        let mut stats = ShardedWalRecoveryStats {
            shards_recovered: 0,
            total_records: 0,
            put_records: 0,
            delete_records: 0,
            tag_records: 0,
            corrupted_records: 0,
        };

        for shard_id in 0..num_shards {
            let shard_path = PathBuf::from(format!("{}.{}", shard_pattern.display(), shard_id));

            if !shard_path.exists() {
                continue;
            }

            let mut reader = wal::WalReader::<K, V>::open(&shard_path)?;

            loop {
                match reader.next() {
                    Ok(Some(record)) => {
                        stats.total_records += 1;

                        match record {
                            WalRecord::Put { key, value } => {
                                stats.put_records += 1;
                                let _ = engine.put(key, value);
                            }
                            WalRecord::Delete { key } => {
                                stats.delete_records += 1;
                                let _ = engine.delete(&key);
                            }
                            WalRecord::Tag { key, tag } => {
                                stats.tag_records += 1;
                                let _ = engine.tag(&key, tag);
                            }
                            WalRecord::Untag { key, tag } => {
                                stats.tag_records += 1;
                                let _ = engine.untag(&key, tag);
                            }
                            WalRecord::TtlSet { key, expires_at_ms } => {
                                let _ = engine.set_ttl_epoch_ms(key, expires_at_ms);
                            }
                            WalRecord::TtlRemove { key } => {
                                let _ = engine.persist_internal(&key, false);
                            }
                        }
                    }
                    Ok(None) => break, // End of WAL
                    Err(_) => {
                        stats.corrupted_records += 1;
                        break; // Stop on corruption
                    }
                }
            }

            stats.shards_recovered += 1;
        }

        Ok(stats)
    }
}

/// Statistics from sharded WAL recovery
#[derive(Debug, Clone)]
pub struct ShardedWalRecoveryStats {
    pub shards_recovered: usize,
    pub total_records: usize,
    pub put_records: usize,
    pub delete_records: usize,
    pub tag_records: usize,
    pub corrupted_records: usize,
}

impl std::fmt::Display for ShardedWalRecoveryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sharded WAL Recovery: {} shards, {} total records ({} PUT, {} DELETE, {} TAG, {} corrupted)",
            self.shards_recovered,
            self.total_records,
            self.put_records,
            self.delete_records,
            self.tag_records,
            self.corrupted_records
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sharded_wal_creation() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            ShardedWal::<String, String>::new(temp_dir.path(), 4, WalOptions::default()).unwrap();

        assert_eq!(wal.num_shards(), 4);
        assert_eq!(wal.shard_paths().len(), 4);
    }

    #[test]
    fn test_shard_distribution() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            ShardedWal::<String, String>::new(temp_dir.path(), 4, WalOptions::default()).unwrap();

        // Write 100 keys, should distribute across shards
        for i in 0..100 {
            let key = format!("key:{}", i);
            let value = format!("value:{}", i);
            wal.append_put(&key, &value).unwrap();
        }

        wal.flush_all().unwrap();

        // All shard files should exist
        for path in wal.shard_paths() {
            assert!(path.exists());
        }
    }

    #[test]
    fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // Write some data
        {
            let wal = ShardedWal::<String, String>::new(temp_dir.path(), 4, WalOptions::default())
                .unwrap();

            for i in 0..10 {
                let key = format!("key:{}", i);
                let value = format!("value:{}", i);
                wal.append_put(&key, &value).unwrap();
            }

            wal.flush_all().unwrap();
        }

        // Recover into new engine
        let mut engine = Engine::<String, String>::with_shards(16, 16);
        let stats = ShardedWalRecovery::recover(&mut engine, temp_dir.path(), 4).unwrap();

        assert_eq!(stats.total_records, 10);
        assert_eq!(stats.put_records, 10);
        assert_eq!(stats.shards_recovered, 4);

        // Verify data
        for i in 0..10 {
            let key = format!("key:{}", i);
            let value = engine.get(&key).unwrap();
            assert_eq!(value, format!("value:{}", i));
        }
    }

    #[test]
    fn test_recovery_with_wal_suffix_path() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("neuroindex.wal");

        // Write some data using .wal suffix (shards stored as neuroindex.wal.<id>)
        {
            let wal =
                ShardedWal::<String, String>::new(&wal_path, 4, WalOptions::default()).unwrap();

            for i in 0..6 {
                let key = format!("key:{}", i);
                let value = format!("value:{}", i);
                wal.append_put(&key, &value).unwrap();
            }

            wal.flush_all().unwrap();
        }

        // Recover into new engine using the same base path
        let mut engine = Engine::<String, String>::with_shards(8, 16);
        let stats =
            ShardedWalRecovery::recover(&mut engine, &wal_path, 4).expect("recovery failed");

        assert_eq!(stats.total_records, 6);
        assert_eq!(stats.put_records, 6);

        for i in 0..6 {
            let key = format!("key:{}", i);
            let value = engine.get(&key).unwrap();
            assert_eq!(value, format!("value:{}", i));
        }
    }
}
