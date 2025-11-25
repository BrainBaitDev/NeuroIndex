use crate::eviction::{EvictionPolicy, LfuTracker, LruTracker, ResourceStats, ResourceTracker};
use crate::optimize::AutoTuner;
use ahash::RandomState;
use arena::{Arena, Handle};
use art::ArtTree;
use cuckoo::CuckooTable;
use cuckoofilter::CuckooFilter;
use hopscotch::HopscotchMap;
use parking_lot::{Mutex, RwLock};
use serde::{de::DeserializeOwned, Serialize};
use serde_json;
use std::any::Any;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;
use std::io;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use wal::{read_snapshot, write_snapshot, SnapshotMeta, SnapshotOptions, WalReader, WalRecord};

// Phase 7: Advanced Query Engine
pub mod background_snapshot;
pub mod config;
pub mod eviction;
pub mod geospatial;
pub mod metrics;
pub mod optimize;
pub mod pubsub;

#[cfg(feature = "change-events")]
pub mod change_events;
pub mod query;
pub mod raft;
pub mod raft_rpc;
pub mod replication;
pub mod secondary_index;
pub mod sharded_wal;
pub mod sql;
pub mod transaction;
pub mod ttl;
pub use config::PersistenceConfig;
pub use query::{Aggregator, HashJoinBuilder, QueryPlanner};

const RANGE_STREAM_BATCH: usize = 64;

struct ShardRangeIter<'a, K, V> {
    shard: &'a Shard<K, V>,
    initial_lower: Bound<&'a K>,
    upper: Bound<&'a K>,
    last_key: Option<K>,
    batch: std::vec::IntoIter<(K, Handle<V>)>,
}

impl<'a, K: Clone + Ord, V: Clone + Send + Sync + 'static> ShardRangeIter<'a, K, V> {
    fn new(shard: &'a Shard<K, V>, lower: Bound<&'a K>, upper: Bound<&'a K>) -> Self {
        Self {
            shard,
            initial_lower: lower,
            upper,
            last_key: None,
            batch: Vec::new().into_iter(),
        }
    }

    fn current_lower_bound(&self) -> Bound<&K> {
        if let Some(ref key) = self.last_key {
            Bound::Excluded(key)
        } else {
            match self.initial_lower {
                Bound::Included(k) => Bound::Included(k),
                Bound::Excluded(k) => Bound::Excluded(k),
                Bound::Unbounded => Bound::Unbounded,
            }
        }
    }

    fn refill_batch(&mut self) {
        let lower = self.current_lower_bound();
        let chunk = self
            .shard
            .ordered
            .range_limited(lower, self.upper, RANGE_STREAM_BATCH);
        self.batch = chunk.into_iter();
    }
}

impl<'a, K: Clone + Ord, V: Clone + Send + Sync + 'static> Iterator for ShardRangeIter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((key, handle)) = self.batch.next() {
                let guard = self.shard.arena.pin();
                let value = self.shard.arena.clone_value(handle, &guard);
                self.last_key = Some(key.clone());
                return Some((key, value));
            }

            self.refill_batch();
            if self.batch.len() == 0 {
                return None;
            }
        }
    }
}

pub struct RangeStreamingIter<'a, K, V> {
    shard_iters: Vec<ShardRangeIter<'a, K, V>>,
    heap: BinaryHeap<HeapEntry<K, V>>,
}

struct HeapEntry<K, V> {
    key: K,
    value: V,
    shard_idx: usize,
}

impl<K: Ord, V> PartialEq for HeapEntry<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K: Ord, V> Eq for HeapEntry<K, V> {}

impl<K: Ord, V> PartialOrd for HeapEntry<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, V> Ord for HeapEntry<K, V> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other.key.cmp(&self.key)
    }
}

impl<'a, K, V> RangeStreamingIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Send + Sync + 'static,
{
    fn new(engine: &'a Engine<K, V>, lower: Bound<&'a K>, upper: Bound<&'a K>) -> Self {
        let mut shard_iters = Vec::with_capacity(engine.shards.len());
        let mut heap = BinaryHeap::new();

        for (idx, shard) in engine.shards.iter().enumerate() {
            let mut iter = ShardRangeIter::new(shard, lower, upper);
            if let Some((key, value)) = iter.next() {
                heap.push(HeapEntry {
                    key,
                    value,
                    shard_idx: idx,
                });
            }
            shard_iters.push(iter);
        }

        Self { shard_iters, heap }
    }
}

impl<'a, K, V> Iterator for RangeStreamingIter<'a, K, V>
where
    K: Clone + Ord,
    V: Clone + Send + Sync + 'static,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.heap.pop().map(|entry| {
            if let Some((next_key, next_value)) = self.shard_iters[entry.shard_idx].next() {
                self.heap.push(HeapEntry {
                    key: next_key,
                    value: next_value,
                    shard_idx: entry.shard_idx,
                });
            }
            (entry.key, entry.value)
        })
    }
}
pub use geospatial::{GeoIndex, GeoIndexManager, GeoIndexStats, GeoPoint};
pub use metrics::{LatencyTimer, MetricsCollector, MetricsSnapshot};
pub use pubsub::{Message, PubSubManager, Subscription};
pub use raft::{RaftCluster, RaftNode, RaftNodeStats, RaftState};
pub use replication::{
    ReplicationOp, ReplicationProtocol, ReplicationRole, ReplicationState, ReplicationStats,
};
pub use secondary_index::{IndexStats, IndexType, SecondaryIndexManager};
pub use sharded_wal::{ShardedWal, ShardedWalRecovery, ShardedWalRecoveryStats};
pub use sql::SqlExecutor;
pub use ttl::{TtlManager, TtlStats};
use std::time::SystemTime;

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub snapshot_records: usize,
    pub snapshot_tags: usize,
    pub wal_records_ok: usize,
    pub wal_records_skipped: usize,
    pub wal_records_corrupted: usize,
    pub total_recovered: usize,
    pub recovery_errors: Vec<String>,
}

impl RecoveryReport {
    fn new() -> Self {
        Self::default()
    }
}

impl std::fmt::Display for RecoveryReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Recovery Report:")?;
        writeln!(f, "  - Snapshot Records: {}", self.snapshot_records)?;
        writeln!(f, "  - Snapshot Tags: {}", self.snapshot_tags)?;
        writeln!(f, "  - WAL Records OK: {}", self.wal_records_ok)?;
        writeln!(f, "  - WAL Records Skipped: {}", self.wal_records_skipped)?;
        writeln!(
            f,
            "  - WAL Records Corrupted: {}",
            self.wal_records_corrupted
        )?;
        writeln!(f, "  - Total Records Recovered: {}", self.total_recovered)?;
        if !self.recovery_errors.is_empty() {
            writeln!(f, "  - Errors:")?;
            for err in &self.recovery_errors {
                writeln!(f, "    - {}", err)?;
            }
        }
        Ok(())
    }
}
pub struct Shard<K, V> {
    arena: Arc<Arena<V>>,
    kv: CuckooTable<K, Handle<V>, RandomState>,
    ordered: ArtTree<K, Handle<V>>,
    tags: HopscotchMap<u64, Vec<Handle<V>>>,
    reverse_tags: HopscotchMap<usize, Vec<u64>>,
    // Secondary index: Unified Value Index using ART
    // Stores: Normalized Value String -> List of Keys
    // Supports both exact match and prefix scan efficiently
    value_index: ArtTree<String, Vec<K>>,
    bloom: Mutex<CuckooFilter>, // Membership filter for negative lookup optimization
}

impl<K, V> Shard<K, V>
where
    K: Eq + Hash + Ord + Clone + art::AsBytes,
    V: Clone + Send + Sync + Serialize + 'static,
{
    pub fn new(cap_pow2: usize, arena: Arc<Arena<V>>) -> Self {
        Self {
            arena,
            kv: CuckooTable::with_capacity_pow2(cap_pow2),
            ordered: ArtTree::new(),
            tags: HopscotchMap::with_capacity_pow2(cap_pow2),
            reverse_tags: HopscotchMap::with_capacity_pow2(cap_pow2),
            value_index: ArtTree::new(), // Unified index
            bloom: Mutex::new(CuckooFilter::new(cap_pow2)), // cap_pow2 is already a power of 2
        }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        // Fast negative lookup: if bloom filter says "not present", definitely not there
        if !self.bloom.lock().contains(k) {
            return None;
        }

        let guard = self.arena.pin();
        self.kv
            .get(k)
            .map(|handle| self.arena.clone_value(handle, &guard))
    }

    pub fn put(&self, k: K, v: V) -> Option<V> {
        // Update bloom filter
        self.bloom.lock().insert(&k);

        let guard = self.arena.pin();
        let v_for_indexing = v.clone();
        let handle = self.arena.allocate(v);
        let prev = self.kv.put(k.clone(), handle);

        let tree_prev = self.ordered.insert(k.clone(), handle);

        debug_assert!(
            prev == tree_prev,
            "cuckoo and ttree returned different previous handles"
        );

        // Maintain value_index: remove old key from previous value bucket (if any)
        let old_value = prev.map(|old| {
            let val = self.arena.clone_value(old, &guard);

            // Move tag associations from old handle to new handle to preserve tags on updates.
            if let Some(tags_list) = self.reverse_tags.remove(&old.addr()) {
                // Update forward map
                for tag in &tags_list {
                    self.tags.update_in(tag, |handles| {
                        for h in handles.iter_mut() {
                            if *h == old {
                                *h = handle;
                            }
                        }
                        handles.is_empty()
                    });
                }
                // Register new reverse mapping
                self.reverse_tags.insert(handle.addr(), tags_list);
            }

            if let Ok(vjson) = serde_json::to_value(&val) {
                let key = match vjson {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "null".to_string(),
                    other => serde_json::to_string(&other).unwrap_or_else(|_| {
                        serde_json::to_string(&val)
                            .unwrap_or_else(|_| "<unserializable>".to_string())
                    }),
                };

                // Remove from unified ART index
                // We need to update the vector inside the tree
                if let Some(mut vec) = self.value_index.get(&key) {
                     vec.retain(|x| x != &k);
                     if vec.is_empty() {
                         self.value_index.delete(&key);
                     } else {
                         self.value_index.insert(key, vec);
                     }
                }
            }

            self.arena.retire(old, &guard);
            val
        });

        // Add new value to unified index
        if let Ok(vjson) = serde_json::to_value(&v_for_indexing) {
            let key_str = match vjson {
                serde_json::Value::String(s) => s,
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => "null".to_string(),
                other => serde_json::to_string(&other).unwrap_or_default(),
            };

            // Update or insert
            let mut vec = self.value_index.get(&key_str).unwrap_or_default();
            if !vec.contains(&k) {
                vec.push(k.clone());
            }
            self.value_index.insert(key_str, vec);
        }

        old_value
    }

    pub fn delete(&self, k: &K) -> Option<V> {
        // Update bloom filter
        self.bloom.lock().delete(k);

        let guard = self.arena.pin();
        let prev = self.kv.delete(k);

        let tree_prev = self.ordered.delete(k);

        debug_assert!(
            prev == tree_prev,
            "cuckoo and ttree returned different handles on delete"
        );
        prev.map(|handle| {
            // remove from value_index
            let val = self.arena.clone_value(handle, &guard);
            if let Ok(vjson) = serde_json::to_value(&val) {
                let key = match vjson {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "null".to_string(),
                    other => serde_json::to_string(&other).unwrap_or_else(|_| {
                        serde_json::to_string(&val)
                            .unwrap_or_else(|_| "<unserializable>".to_string())
                    }),
                };

                if let Some(mut vec) = self.value_index.get(&key) {
                    vec.retain(|x| x != k);
                    if vec.is_empty() {
                        self.value_index.delete(&key);
                    } else {
                        self.value_index.insert(key, vec);
                    }
                }
            }

            self.remove_from_tags(&handle);
            self.arena.retire(handle, &guard);
            val
        })
    }

    pub fn range(&self, lower: Bound<&K>, upper: Bound<&K>) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let guard = self.arena.pin();
        self.ordered
            .range(lower, upper)
            .map(|(k, v)| (k.clone(), self.arena.clone_value(v, &guard)))
            .collect()
    }

    pub fn tag(&self, key: &K, tag: u64) -> bool {
        let handle = match self.kv.get(key) {
            Some(h) => h,
            None => return false,
        };

        let existed = self.tags.update_in(&tag, |handles| {
            if !handles.iter().any(|h| *h == handle) {
                handles.push(handle);
            }
            handles.is_empty()
        });
        if !existed {
            self.tags.insert(tag, vec![handle]);
        }

        let addr = handle.addr();
        let existed = self.reverse_tags.update_in(&addr, |tags| {
            if !tags.contains(&tag) {
                tags.push(tag);
            }
            tags.is_empty()
        });
        if !existed {
            self.reverse_tags.insert(addr, vec![tag]);
        }
        true
    }

    pub fn untag(&self, key: &K, tag: u64) -> bool {
        let handle = match self.kv.get(key) {
            Some(h) => h,
            None => return false,
        };
        if let Some(mut handles) = self.tags.remove(&tag) {
            let original_len = handles.len();
            handles.retain(|h| *h != handle);
            let removed = handles.len() != original_len;
            if handles.is_empty() {
                // nothing to reinsert
            } else if removed {
                self.tags.insert(tag, handles.clone());
            } else {
                self.tags.insert(tag, handles);
                return false;
            }

            if removed {
                if let Some(mut reverse) = self.reverse_tags.remove(&handle.addr()) {
                    reverse.retain(|t| *t != tag);
                    if !reverse.is_empty() {
                        self.reverse_tags.insert(handle.addr(), reverse);
                    }
                }
            }
            removed
        } else {
            false
        }
    }

    pub fn tagged(&self, tag: u64) -> Vec<V> {
        match self.tags.get(&tag) {
            Some(handles) => {
                let guard = self.arena.pin();
                handles
                    .into_iter()
                    .map(|handle| self.arena.clone_value(handle, &guard))
                    .collect()
            }
            None => Vec::new(),
        }
    }

    pub fn export_entries(&self) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let handles: Vec<(K, Handle<V>)> = self
            .ordered
            .range(Bound::Unbounded, Bound::Unbounded)
            .map(|(k, v)| (k.clone(), v))
            .collect();
        let guard = self.arena.pin();
        let mut out = Vec::with_capacity(handles.len());
        for (key, handle) in handles {
            let value = self.arena.clone_value(handle, &guard);
            out.push((key, value));
        }
        out
    }

    pub fn export_tag_records(&self) -> Vec<(K, u64)>
    where
        K: Clone,
    {
        let handles: Vec<(K, Handle<V>)> = self
            .ordered
            .range(Bound::Unbounded, Bound::Unbounded)
            .map(|(k, v)| (k.clone(), v))
            .collect();
        let mut addr_to_key = HashMap::with_capacity(handles.len());
        for (key, handle) in &handles {
            addr_to_key.insert(handle.addr(), key.clone());
        }
        let mut out = Vec::new();
        for (addr, tags) in self.reverse_tags.entries() {
            if let Some(key) = addr_to_key.get(&addr) {
                for tag in tags {
                    out.push((key.clone(), tag));
                }
            }
        }
        out
    }

    fn remove_from_tags(&self, handle: &Handle<V>) {
        let addr = handle.addr();
        if let Some(tag_list) = self.reverse_tags.remove(&addr) {
            for tag in tag_list {
                self.tags.update_in(&tag, |handles| {
                    handles.retain(|h| *h != *handle);
                    handles.is_empty()
                });
            }
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.arena.memory_usage()
            + self.kv.memory_usage()
            + self.ordered.memory_usage()
            + self.tags.memory_usage()
            + self.reverse_tags.memory_usage()
            + self.value_index.memory_usage()
    }
}

struct EnginePersistence<K, V> {
    wal: Arc<crate::sharded_wal::ShardedWal<K, V>>,
    _wal_options: wal::WalOptions,
    _wal_path: PathBuf,
    snapshot_path: PathBuf,
    snapshot_options: SnapshotOptions,
}

pub struct Engine<K, V> {
    shards: Vec<Arc<Shard<K, V>>>,
    mask: usize,
    hash_builder: RandomState,
    shards_pow2: usize,
    per_shard_cap_pow2: usize,
    persistence: Option<EnginePersistence<K, V>>,
    snapshot_lock: Option<Arc<RwLock<()>>>,
    resource_tracker: Option<Arc<ResourceTracker>>,
    lru_tracker: Option<Arc<LruTracker<K>>>,
    lfu_tracker: Option<Arc<LfuTracker<K>>>,
    ttl_manager: Option<Arc<Mutex<TtlManager<K>>>>,
    metrics: Option<Arc<MetricsCollector>>,
    pubsub: Option<Arc<PubSubManager>>,

    // Change event subscribers (feature-gated)
    #[cfg(feature = "change-events")]
    change_subscribers: Arc<Mutex<Vec<Arc<dyn change_events::ChangeSubscriber>>>>,

    // Cheap atomic counters for O(1) metrics
    total_keys: Arc<AtomicU64>,
    total_ops: Arc<AtomicU64>,
    raft: Option<Arc<RaftNode<K, V>>>,
    raft_applier_running: Option<Arc<AtomicU64>>, // 0 = stopped, 1 = running
    auto_tuner: Option<Arc<AutoTuner>>,
    auto_tuner_running: Option<Arc<AtomicU64>>, // 0 = stopped, 1 = running
}

/// Best-effort estimation of total size (stack + heap) for common key/value types.
/// Falls back to `size_of_val` when the type is not recognized.
fn approximate_size<T: 'static>(value: &T) -> usize {
    let base = std::mem::size_of_val(value);
    let extra = if let Some(s) = (value as &dyn Any).downcast_ref::<String>() {
        s.capacity()
    } else if let Some(v) = (value as &dyn Any).downcast_ref::<Vec<u8>>() {
        v.capacity()
    } else if let Some(vs) = (value as &dyn Any).downcast_ref::<Vec<String>>() {
        // account for vector backing storage + each string buffer
        vs.capacity() * std::mem::size_of::<String>()
            + vs.iter().map(|s| s.capacity()).sum::<usize>()
    } else {
        0
    };

    base + extra
}

#[cfg(feature = "change-events")]
fn stringify_for_subscriber<T: Serialize>(value: &T) -> Option<String> {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(s)) => Some(s),
        Ok(other) => serde_json::to_string(&other).ok(),
        Err(_) => None,
    }
}

impl<K, V> Engine<K, V>
where
    K: Eq
        + Hash
        + Ord
        + Clone
        + art::AsBytes
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn with_shards(n_shards_pow2: usize, per_shard_cap_pow2: usize) -> Self {
        Self::new_internal(n_shards_pow2, per_shard_cap_pow2, None, None)
    }

    pub fn with_resource_limits(mut self, limits: eviction::ResourceLimits) -> Self {
        let (resource_tracker, lru_tracker, lfu_tracker) = {
            let lru = if matches!(
                limits.eviction_policy,
                EvictionPolicy::LRU | EvictionPolicy::VolatileLRU
            ) {
                Some(Arc::new(LruTracker::new()))
            } else {
                None
            };
            let lfu = if matches!(limits.eviction_policy, EvictionPolicy::LFU) {
                Some(Arc::new(LfuTracker::new()))
            } else {
                None
            };
            (Some(Arc::new(ResourceTracker::new(limits))), lru, lfu)
        };

        self.resource_tracker = resource_tracker;
        self.lru_tracker = lru_tracker;
        self.lfu_tracker = lfu_tracker;
        self
    }

    pub fn with_persistence(config: PersistenceConfig) -> io::Result<Self> {
        // Use ShardedWal with 16 shards for parallel writes
        let num_wal_shards = 16;
        let sharded_wal = crate::sharded_wal::ShardedWal::new(
            &config.wal_path,
            num_wal_shards,
            config.wal_options.clone(),
        )?;

        let persistence = EnginePersistence {
            wal: Arc::new(sharded_wal),
            _wal_options: config.wal_options,
            _wal_path: config.wal_path.clone(),
            snapshot_path: config.snapshot_path.clone(),
            snapshot_options: config.snapshot_options.clone(),
        };
        Ok(Self::new_internal(
            config.shards_pow2,
            config.shard_cap_pow2,
            Some(persistence),
            config.resource_limits,
        ))
    }

    pub fn with_raft(mut self, raft: Arc<RaftNode<K, V>>) -> Self {
        self.raft = Some(raft);
        self
    }

    /// Start the Raft applier thread (call this after wrapping Engine in Arc)
    pub fn start_raft_applier(engine: &Arc<Self>) {
        if let Some(raft_node) = &engine.raft {
            let running = Arc::new(AtomicU64::new(1));
            
            // Store running flag (we'll need to add this field)
            if let Some(existing) = &engine.raft_applier_running {
                existing.store(1, Ordering::Relaxed);
            }
            
            let engine_weak = Arc::downgrade(engine);
            let raft_clone = raft_node.clone();
            
            thread::spawn(move || {
                while running.load(Ordering::Relaxed) == 1 {
                    if let Some(engine) = engine_weak.upgrade() {
                        let entries = raft_clone.get_committed_entries();
                        
                        for entry in entries {
                            match &entry.command {
                                crate::replication::ReplicationOp::Put { key, value } => {
                                    let _ = engine.put_internal(key.clone(), value.clone(), false);
                                }
                                crate::replication::ReplicationOp::Delete { key } => {
                                    let _ = engine.delete_internal(key.clone(), false);
                                }
                                crate::replication::ReplicationOp::Tag { key, tag } => {
                                    let _ = engine.tag_internal(key.clone(), *tag, false);
                                }
                            }
                            
                            raft_clone.update_last_applied(entry.index);
                        }
                    } else {
                        break;
                    }
                    
                    thread::sleep(Duration::from_millis(10));
                }
            });
        }
    }

    /// Enable auto-tuning system (call this after wrapping Engine in Arc)
    pub fn enable_auto_tuning(engine: &Arc<Self>) {
        let tuner = Arc::new(AutoTuner::with_default());
        let running = Arc::new(AtomicU64::new(1));
        
        let engine_weak = Arc::downgrade(engine);
        let tuner_clone = tuner.clone();
        
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) == 1 {
                if let Some(_engine) = engine_weak.upgrade() {
                    // Analyze performance and get recommendations
                    let recommendations = tuner_clone.analyze();
                    
                    if !recommendations.is_empty() {
                        eprintln!("\n=== Auto-Tuning Recommendations ===");
                        for rec in recommendations {
                            eprintln!("{}", rec);
                        }
                        eprintln!("===================================\n");
                    }
                } else {
                    break;
                }
                
                // Run analysis every 60 seconds
                thread::sleep(Duration::from_secs(60));
            }
        });
    }

    pub fn recover(config: PersistenceConfig) -> io::Result<Self> {
        Self::recover_with_report(config).map(|(engine, _report)| engine)
    }

    /// Recover with detailed report
    pub fn recover_with_report(config: PersistenceConfig) -> io::Result<(Self, RecoveryReport)> {
        let mut report = RecoveryReport::new();

        eprintln!("Starting recovery...");
        eprintln!("  Snapshot: {}", config.snapshot_path.display());
        eprintln!("  WAL: {}", config.wal_path.display());

        // Load snapshot
        let snapshot = read_snapshot::<_, K, V>(&config.snapshot_path)?;
        let mut engine = Self::with_persistence(config.clone())?;
        engine.enable_ttl();

        if let Some(data) = snapshot {
            report.snapshot_records = data.entries.len();
            report.snapshot_tags = data.tags.len();

            eprintln!(
                "Loading snapshot: {} records, {} tags...",
                report.snapshot_records, report.snapshot_tags
            );

            for (key, value) in data.entries {
                engine.put_internal(key, value, false)?;
            }
            for (key, tag) in data.tags {
                let _ = engine.tag_internal(key, tag, false)?;
            }
            // Restore TTL deadlines (skip expired)
            for (key, expires_at_ms) in data.ttls {
                let _ = engine.set_ttl_epoch_ms(key, expires_at_ms);
            }

            report.total_recovered += report.snapshot_records;
        } else {
            eprintln!("No snapshot found, starting from empty state");
        }

        // Replay WAL from all shards
        eprintln!("Replaying WAL from sharded files...");
        let num_wal_shards = 16;

        // Determine shard naming pattern (same logic as ShardedWal::new)
        let shard_pattern = if let Some(path_str) = config.wal_path.to_str() {
            if path_str.ends_with(".wal") {
                // e.g., "data/neuroindex.wal" -> files are "data/neuroindex.wal.0", etc.
                config.wal_path.clone()
            } else {
                // e.g., "data/wal_dir" -> files are "data/wal_dir/wal.0", etc.
                config.wal_path.join("wal")
            }
        } else {
            config.wal_path.join("wal")
        };

        for shard_id in 0..num_wal_shards {
            let shard_path = PathBuf::from(format!("{}.{}", shard_pattern.display(), shard_id));

            if !shard_path.exists() {
                continue;
            }

            eprintln!(
                "  Replaying shard {} from {}...",
                shard_id,
                shard_path.display()
            );
            let mut reader = WalReader::<K, V>::open(&shard_path)?;

            loop {
                match reader.next() {
                    Ok(Some(record)) => match engine.apply_wal_record(record, false) {
                        Ok(_) => {
                            report.wal_records_ok += 1;
                            report.total_recovered += 1;
                        }
                        Err(e) => {
                            report.wal_records_skipped += 1;
                            report
                                .recovery_errors
                                .push(format!("Failed to apply record: {}", e));
                            eprintln!("Warning: Skipping corrupted record: {}", e);
                        }
                    },
                    Ok(None) => break,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        eprintln!("Warning: WAL shard {} ended unexpectedly", shard_id);
                        report
                            .recovery_errors
                            .push(format!("WAL shard {} ended unexpectedly", shard_id));
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                        report.wal_records_corrupted += 1;
                        report
                            .recovery_errors
                            .push(format!("Corrupted record in shard {}: {}", shard_id, e));
                        eprintln!(
                            "Warning: Corrupted record in shard {} (CRC mismatch), skipping: {}",
                            shard_id, e
                        );
                    }
                    Err(e) => {
                        report.wal_records_skipped += 1;
                        report
                            .recovery_errors
                            .push(format!("Error in shard {}: {}", shard_id, e));
                        eprintln!(
                            "Warning: Error reading WAL shard {}: {}, attempting to continue",
                            shard_id, e
                        );
                    }
                }
            }
        }

        eprintln!("\n{}", report);

        Ok((engine, report))
    }

    pub fn get(&self, k: &K) -> Option<V> {
        // Start metrics timer
        let timer = if self.metrics.is_some() {
            Some(LatencyTimer::start())
        } else {
            None
        };

        // Check if key has expired
        if let Some(ttl_mgr) = &self.ttl_manager {
            if ttl_mgr.lock().is_expired(k) {
                // Key expired, delete it asynchronously and return None
                let _ = self.delete(k);

                // Record miss
                if let (Some(metrics), Some(t)) = (&self.metrics, timer) {
                    metrics.record_get(t.elapsed_ns(), false);
                }

                return None;
            }
        }

        // Track access for LRU/LFU
        if let Some(lru) = &self.lru_tracker {
            lru.touch(k);
        }
        if let Some(lfu) = &self.lfu_tracker {
            lfu.touch(k);
        }

        let idx = self.shard_idx(k);
        let result = self.shards[idx].get(k);

        // Record metrics
        if let (Some(metrics), Some(t)) = (&self.metrics, timer) {
            metrics.record_get(t.elapsed_ns(), result.is_some());
        }

        result
    }

    /// Find entries with a normalized value (uses per-shard secondary index)
    pub fn find_by_value(&self, value_text: &str) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let mut out = Vec::new();
        for shard in &self.shards {
            let key_lookup = value_text.to_string();
            if let Some(keys) = shard.value_index.get(&key_lookup) {
                for key in keys.iter() {
                    if let Some(v) = shard.get(key) {
                        out.push((key.clone(), v));
                    }
                }
            }
        }
        out
    }

    /// Find entries whose normalized value matches a SQL LIKE pattern.
    /// This builds a regex from the SQL pattern and iterates per-shard value_index keys
    /// to find matching buckets, then returns all key/value pairs from those buckets.
    pub fn find_by_value_like(&self, like_pattern: &str) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        // Convert SQL LIKE pattern to regex
        let regex_pattern = like_pattern
            .replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("*", "\\*")
            .replace("+", "\\+")
            .replace("?", "\\?")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("{", "\\{")
            .replace("}", "\\}")
            .replace("%", ".*")
            .replace("_", ".");

        let re = match regex::Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };



        let mut out = Vec::new();
        for shard in &self.shards {
            // iterate value_index keys
            for (val_key, keys) in shard.value_index.range(Bound::Unbounded, Bound::Unbounded) {
                if re.is_match(&val_key) {
                    for key in keys.iter() {
                        if let Some(v) = shard.get(key) {
                            out.push((key.clone(), v));
                        }
                    }
                }
            }
        }
        out
    }

    /// Update all entries whose normalized value equals `value_text` to `new_value`.
    /// Returns the number of updated entries.
    pub fn update_by_value(&self, value_text: &str, new_value: V) -> io::Result<usize>
    where
        K: Clone,
        V: Clone,
    {
        let mut updated = 0usize;

        // Collect keys first per shard to avoid mutating the value_index while iterating
        for shard in &self.shards {
            let lookup = value_text.to_string();
            if let Some(keys) = shard.value_index.get(&lookup) {
                let keys_copy = keys.clone();
                for key in keys_copy {
                    // Use put_internal so WAL and index maintenance occur
                    let _ = self.put_internal(key.clone(), new_value.clone(), true)?;
                    updated += 1;
                }
            }
        }

        Ok(updated)
    }

    /// Delete all entries whose normalized value equals `value_text`.
    /// Returns the number of deleted entries.
    pub fn delete_by_value(&self, value_text: &str) -> io::Result<usize>
    where
        K: Clone,
        V: Clone,
    {
        let mut deleted = 0usize;

        for shard in &self.shards {
            let lookup = value_text.to_string();
            if let Some(keys) = shard.value_index.get(&lookup) {
                let keys_copy = keys.clone();
                for key in keys_copy {
                    if self.delete_internal(key.clone(), true)?.is_some() {
                        deleted += 1;
                    }
                }
            }
        }

        Ok(deleted)
    }

    pub fn put(&self, k: K, v: V) -> io::Result<Option<V>> {
        let timer = if self.metrics.is_some() {
            Some(LatencyTimer::start())
        } else {
            None
        };

        let result = self.put_internal(k, v, true);

        // Record metrics
        if let (Some(metrics), Some(t)) = (&self.metrics, timer) {
            metrics.record_put(t.elapsed_ns());
        }

        result
    }

    pub fn delete(&self, k: &K) -> io::Result<Option<V>> {
        let timer = if self.metrics.is_some() {
            Some(LatencyTimer::start())
        } else {
            None
        };

        let result = self.delete_internal(k.clone(), true);

        // Record metrics
        if let (Some(metrics), Some(t)) = (&self.metrics, timer) {
            metrics.record_delete(t.elapsed_ns());
        }

        result
    }

    fn collect_range_unordered(&self, lower: Bound<&K>, upper: Bound<&K>) -> Vec<(K, V)> {
        let mut out: Vec<(K, V)> = Vec::new();
        for shard in &self.shards {
            out.extend(shard.range(lower, upper));
        }
        out
    }

    pub fn range(&self, lower: Bound<&K>, upper: Bound<&K>) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let mut out = self.collect_range_unordered(lower, upper);
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn range_streaming<'a>(
        &'a self,
        lower: Bound<&'a K>,
        upper: Bound<&'a K>,
    ) -> RangeStreamingIter<'a, K, V>
    where
        K: Clone + Ord,
        V: Clone,
    {
        RangeStreamingIter::new(self, lower, upper)
    }

    pub fn tag(&self, key: &K, tag: u64) -> io::Result<bool> {
        self.tag_internal(key.clone(), tag, true)
    }

    pub fn untag(&self, key: &K, tag: u64) -> io::Result<bool> {
        self.untag_internal(key.clone(), tag, true)
    }

    pub fn tagged(&self, tag: u64) -> Vec<V> {
        self.shards
            .iter()
            .flat_map(|shard| shard.tagged(tag))
            .collect()
    }

    pub fn flush(&self) -> io::Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.wal.flush_all()?;
        }
        Ok(())
    }

    pub fn snapshot(&self) -> io::Result<()> {
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "persistence not configured"))?;
        // Exclusive lock so mutating ops (and WAL appends) wait until snapshot finishes,
        // avoiding WAL truncation that could drop concurrent writes.
        let _snapshot_guard = self
            .snapshot_lock
            .as_ref()
            .map(|lock| lock.write())
            .expect("snapshot lock should exist when persistence is configured");
        self.flush()?;
        let entries = self.export_entries();
        let tags = self.export_tags();
        let ttls = self.export_ttls();
        let meta = SnapshotMeta {
            shards_pow2: self.shards_pow2 as u32,
            shard_cap_pow2: self.per_shard_cap_pow2 as u32,
        };
        write_snapshot(
            &persistence.snapshot_path,
            persistence.snapshot_options.clone(),
            meta,
            entries,
            tags,
            ttls,
        )?;
        persistence.wal.truncate_all()?;
        Ok(())
    }

    pub fn flushdb(&self) -> io::Result<()> {
        use std::ops::Bound::Unbounded;

        for shard in &self.shards {
            let entries = shard.range(Unbounded, Unbounded);
            for key in entries.into_iter().map(|(k, _)| k) {
                let _ = self.delete(&key)?;
            }
        }
        Ok(())
    }

    fn new_internal(
        shards_pow2: usize,
        per_shard_cap_pow2: usize,
        persistence: Option<EnginePersistence<K, V>>,
        resource_limits: Option<eviction::ResourceLimits>,
    ) -> Self {
        assert!(shards_pow2.is_power_of_two());
        let shards = (0..shards_pow2)
            .map(|_| Arc::new(Shard::new(per_shard_cap_pow2, Arc::new(Arena::new()))))
            .collect();

        // When persistence is enabled, snapshots must exclude concurrent mutating operations
        // so that WAL truncation does not drop freshly appended records.
        let snapshot_lock = persistence.as_ref().map(|_| Arc::new(RwLock::new(())));

        let (resource_tracker, lru_tracker, lfu_tracker) = if let Some(limits) = resource_limits {
            let lru = if matches!(
                limits.eviction_policy,
                EvictionPolicy::LRU | EvictionPolicy::VolatileLRU
            ) {
                Some(Arc::new(LruTracker::new()))
            } else {
                None
            };
            let lfu = if matches!(limits.eviction_policy, EvictionPolicy::LFU) {
                Some(Arc::new(LfuTracker::new()))
            } else {
                None
            };
            (Some(Arc::new(ResourceTracker::new(limits))), lru, lfu)
        } else {
            (None, None, None)
        };

        Self {
            shards,
            mask: shards_pow2 - 1,
            hash_builder: RandomState::default(),
            shards_pow2,
            per_shard_cap_pow2,
            persistence,
            snapshot_lock,
            resource_tracker,
            lru_tracker,
            lfu_tracker,
            ttl_manager: None,
            metrics: None,
            pubsub: None,
            #[cfg(feature = "change-events")]
            change_subscribers: Arc::new(Mutex::new(Vec::new())),
            total_keys: Arc::new(AtomicU64::new(0)),
            total_ops: Arc::new(AtomicU64::new(0)),
            raft: None,
            raft_applier_running: None,
            auto_tuner: None,
            auto_tuner_running: None,
        }
    }

    fn shard_idx(&self, k: &K) -> usize {
        use std::hash::{BuildHasher, Hasher};
        let mut h = self.hash_builder.clone().build_hasher();
        k.hash(&mut h);
        (h.finish() as usize) & self.mask
    }

    fn put_internal(&self, k: K, v: V, log: bool) -> io::Result<Option<V>> {
        if let Some(raft) = &self.raft {
            if raft.state() == crate::raft::RaftState::Leader {
                raft.propose(crate::replication::ReplicationOp::Put { key: k, value: v });
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, "Not a leader"));
            }
        }

        // Prevent snapshots from truncating WAL while this mutation is in flight.
        let _snapshot_guard = if log {
            self.snapshot_lock.as_ref().map(|lock| lock.read())
        } else {
            None
        };

        // Check if key already exists (for resource limit check)
        let idx = self.shard_idx(&k);
        let key_exists = self.shards[idx].get(&k).is_some();

        // Perform eviction BEFORE inserting if needed
        if !key_exists {
            if let Some(tracker) = &self.resource_tracker {
                // Try eviction if threshold reached
                if tracker.needs_eviction() {
                    self.perform_eviction()?;
                }

                let key_size = approximate_size(&k);
                let value_size = approximate_size(&v);

                // Now check if we can insert
                tracker
                    .check_can_insert(key_size, value_size)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
        }

        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.append_put(&k, &v)?;
            }
        }

        let old_value = self.shards[idx].put(k.clone(), v.clone());

        // Update atomic counters
        if old_value.is_none() {
            self.total_keys.fetch_add(1, Ordering::Relaxed);
        }
        self.total_ops.fetch_add(1, Ordering::Relaxed);

        // Update resource tracking
        if let Some(tracker) = &self.resource_tracker {
            let key_size = approximate_size(&k);
            let value_size = approximate_size(&v);

            if old_value.is_none() {
                // New key inserted
                tracker.increment_keys();
                tracker.add_memory(key_size + value_size);
            } else {
                // Existing key updated - only count value size difference
                if let Some(ref old_v) = old_value {
                    let old_size = approximate_size(old_v);
                    if value_size > old_size {
                        tracker.add_memory(value_size - old_size);
                    } else {
                        tracker.sub_memory(old_size - value_size);
                    }
                }
            }

            // Track access for LRU/LFU
            if let Some(lru) = &self.lru_tracker {
                let is_volatile_policy = self
                    .resource_tracker
                    .as_ref()
                    .map(|t| t.stats().eviction_policy == EvictionPolicy::VolatileLRU)
                    .unwrap_or(false);

                if !is_volatile_policy || self.ttl(&k).is_some() {
                    lru.touch(&k);
                }
            }
            if let Some(lfu) = &self.lfu_tracker {
                lfu.touch(&k);
            }
        }

        // Notify change event subscribers
        #[cfg(feature = "change-events")]
        {
            // Convert K/V to String preserving plain strings
            if let (Some(key_txt), Some(value_txt)) =
                (stringify_for_subscriber(&k), stringify_for_subscriber(&v))
            {
                for subscriber in self.change_subscribers.lock().iter() {
                    subscriber.on_put(&key_txt, &value_txt);
                }
            }
        }

        Ok(old_value)
    }

    fn delete_internal(&self, key: K, log: bool) -> io::Result<Option<V>> {
        if let Some(raft) = &self.raft {
            if raft.state() == crate::raft::RaftState::Leader {
                raft.propose(crate::replication::ReplicationOp::Delete { key });
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, "Not a leader"));
            }
        }

        // Prevent snapshots from truncating WAL while this mutation is in flight.
        let _snapshot_guard = if log {
            self.snapshot_lock.as_ref().map(|lock| lock.read())
        } else {
            None
        };

        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.append_delete(&key)?;
            }
        }

        let idx = self.shard_idx(&key);
        let deleted = self.shards[idx].delete(&key);

        // Update atomic counters
        if deleted.is_some() {
            self.total_keys.fetch_sub(1, Ordering::Relaxed);
        }
        self.total_ops.fetch_add(1, Ordering::Relaxed);

        // Update resource tracking
        if let Some(tracker) = &self.resource_tracker {
            if let Some(ref value) = deleted {
                let key_size = approximate_size(&key);
                let value_size = approximate_size(value);
                tracker.sub_memory(key_size + value_size);
                tracker.decrement_keys();

                // Remove from eviction trackers
                if let Some(lru) = &self.lru_tracker {
                    lru.remove(&key);
                }
                if let Some(lfu) = &self.lfu_tracker {
                    lfu.remove(&key);
                }
            }
        }

        // Remove from TTL tracking
        if let Some(ttl_mgr) = &self.ttl_manager {
            if deleted.is_some() {
                ttl_mgr.lock().remove_ttl(&key);
            }
        }

        // Notify change event subscribers
        #[cfg(feature = "change-events")]
        if deleted.is_some() {
            if let Some(key_txt) = stringify_for_subscriber(&key) {
                for subscriber in self.change_subscribers.lock().iter() {
                    subscriber.on_delete(&key_txt);
                }
            }
        }

        Ok(deleted)
    }

    fn tag_internal(&self, key: K, tag: u64, log: bool) -> io::Result<bool> {
        let _snapshot_guard = if log {
            self.snapshot_lock.as_ref().map(|lock| lock.read())
        } else {
            None
        };

        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.append_tag(&key, tag)?;
            }
        }
        let idx = self.shard_idx(&key);
        Ok(self.shards[idx].tag(&key, tag))
    }

    fn untag_internal(&self, key: K, tag: u64, log: bool) -> io::Result<bool> {
        let _snapshot_guard = if log {
            self.snapshot_lock.as_ref().map(|lock| lock.read())
        } else {
            None
        };

        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.append_untag(&key, tag)?;
            }
        }
        let idx = self.shard_idx(&key);
        Ok(self.shards[idx].untag(&key, tag))
    }

    fn apply_wal_record(&self, record: WalRecord<K, V>, log: bool) -> io::Result<()> {
        match record {
            WalRecord::Put { key, value } => {
                self.put_internal(key, value, log)?;
            }
            WalRecord::Delete { key } => {
                self.delete_internal(key, log)?;
            }
            WalRecord::Tag { key, tag } => {
                let _ = self.tag_internal(key, tag, log)?;
            }
            WalRecord::Untag { key, tag } => {
                let _ = self.untag_internal(key, tag, log)?;
            }
            WalRecord::TtlSet { key, expires_at_ms } => {
                let _ = self.set_ttl_epoch_ms(key, expires_at_ms);
            }
            WalRecord::TtlRemove { key } => {
                let _ = self.persist_internal(&key, false);
            }
        }
        Ok(())
    }

    pub fn export_entries(&self) -> Vec<(K, V)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.export_entries());
        }
        out
    }

    pub fn export_tags(&self) -> Vec<(K, u64)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.export_tag_records());
        }
        out
    }

    pub fn export_ttls(&self) -> Vec<(K, u64)>
    where
        K: Clone,
    {
        if let Some(ttl_mgr) = &self.ttl_manager {
            return ttl_mgr.lock().export_deadlines_ms();
        }
        Vec::new()
    }

    pub fn shards_pow2(&self) -> usize {
        self.shards_pow2
    }

    pub fn per_shard_cap_pow2(&self) -> usize {
        self.per_shard_cap_pow2
    }

    pub fn persistence_config(&self) -> Option<(PathBuf, SnapshotOptions)> {
        self.persistence
            .as_ref()
            .map(|p| (p.snapshot_path.clone(), p.snapshot_options.clone()))
    }

    pub fn memory_usage(&self) -> usize {
        if let Some(rt) = &self.resource_tracker {
            return rt.stats().memory_used as usize;
        }

        // Fallback when resource tracking is disabled: best-effort structural usage
        self.shards.iter().map(|s| s.memory_usage()).sum::<usize>()
    }

    /// Get total keys count (O(1) with atomic counter)
    pub fn total_keys(&self) -> u64 {
        self.total_keys.load(Ordering::Relaxed)
    }

    /// Get total operations count (O(1) with atomic counter)
    pub fn total_ops(&self) -> u64 {
        self.total_ops.load(Ordering::Relaxed)
    }

    /// Get resource statistics (if limits enabled)
    pub fn resource_stats(&self) -> Option<ResourceStats> {
        self.resource_tracker.as_ref().map(|t| t.stats())
    }

    /// Check if resource limits are enabled
    pub fn has_resource_limits(&self) -> bool {
        self.resource_tracker.is_some()
    }

    // ===== TTL Methods =====

    /// Enable TTL support (must be called before using TTL features)
    pub fn enable_ttl(&mut self) {
        if self.ttl_manager.is_none() {
            self.ttl_manager = Some(Arc::new(Mutex::new(TtlManager::new())));
        }
    }

    /// Put a key-value pair with TTL (Time-To-Live)
    pub fn put_with_ttl(&self, k: K, v: V, ttl: Duration) -> io::Result<Option<V>> {
        let result = self.put(k.clone(), v)?;

        if let Some(ttl_mgr) = &self.ttl_manager {
            // Protect against concurrent snapshot truncation of the WAL.
            let _snapshot_guard = self.snapshot_lock.as_ref().map(|lock| lock.read());
            if let Some(persistence) = &self.persistence {
                if let Some(epoch_ms) = SystemTime::now()
                    .checked_add(ttl)
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as u64)
                {
                    persistence.wal.append_ttl_set(&k, epoch_ms)?;
                }
            }

            ttl_mgr.lock().set_ttl(k.clone(), ttl);
            
            // If VolatileLRU, we must start tracking this key now that it has a TTL
            if let Some(lru) = &self.lru_tracker {
                 let is_volatile_policy = self
                    .resource_tracker
                    .as_ref()
                    .map(|t| t.stats().eviction_policy == EvictionPolicy::VolatileLRU)
                    .unwrap_or(false);
                
                if is_volatile_policy {
                    lru.touch(&k);
                }
            }
        }

        Ok(result)
    }

    /// Internal helper to set TTL from an absolute epoch time (milliseconds).
    pub(crate) fn set_ttl_epoch_ms(&self, k: K, expires_at_ms: u64) -> bool {
        if let Some(ttl_mgr) = &self.ttl_manager {
            return ttl_mgr.lock().set_ttl_epoch_ms(k, expires_at_ms);
        }
        false
    }

    /// Set expiration on an existing key
    pub fn expire(&self, k: &K, ttl: Duration) -> bool {
        if let Some(ttl_mgr) = &self.ttl_manager {
            // Check if key exists
            if self.get(k).is_some() {
                // Protect WAL from being truncated mid-write
                let _snapshot_guard = self.snapshot_lock.as_ref().map(|lock| lock.read());
                if let Some(persistence) = &self.persistence {
                    if let Some(epoch_ms) = SystemTime::now()
                        .checked_add(ttl)
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as u64)
                    {
                        if let Err(e) = persistence.wal.append_ttl_set(k, epoch_ms) {
                            eprintln!("Failed to append TTL set to WAL: {}", e);
                        }
                    }
                }

                ttl_mgr.lock().set_ttl(k.clone(), ttl);

                // If VolatileLRU, we must start tracking this key now that it has a TTL
                if let Some(lru) = &self.lru_tracker {
                     let is_volatile_policy = self
                        .resource_tracker
                        .as_ref()
                        .map(|t| t.stats().eviction_policy == EvictionPolicy::VolatileLRU)
                        .unwrap_or(false);
                    
                    if is_volatile_policy {
                        lru.touch(k);
                    }
                }

                return true;
            }
        }
        false
    }

    /// Remove TTL from a key (make it persistent)
    pub fn persist(&self, k: &K) -> bool {
        self.persist_internal(k, true)
    }

    pub(crate) fn persist_internal(&self, k: &K, log: bool) -> bool {
        if let Some(ttl_mgr) = &self.ttl_manager {
            let removed = ttl_mgr.lock().remove_ttl(k);
            if removed && log {
                let _snapshot_guard = self.snapshot_lock.as_ref().map(|lock| lock.read());
                if let Some(persistence) = &self.persistence {
                    if let Err(e) = persistence.wal.append_ttl_remove(k) {
                        eprintln!("Failed to append TTL remove to WAL: {}", e);
                    }
                }
            }
            
            // If VolatileLRU, we must stop tracking this key now that it is persistent
            if removed {
                if let Some(lru) = &self.lru_tracker {
                     let is_volatile_policy = self
                        .resource_tracker
                        .as_ref()
                        .map(|t| t.stats().eviction_policy == EvictionPolicy::VolatileLRU)
                        .unwrap_or(false);
                    
                    if is_volatile_policy {
                        lru.remove(k);
                    }
                }
            }

            removed
        } else {
            false
        }
    }

    /// Get remaining TTL for a key
    pub fn ttl(&self, k: &K) -> Option<Duration> {
        if let Some(ttl_mgr) = &self.ttl_manager {
            ttl_mgr.lock().ttl(k)
        } else {
            None
        }
    }

    /// Get TTL statistics
    pub fn ttl_stats(&self) -> Option<TtlStats> {
        self.ttl_manager.as_ref().map(|mgr| mgr.lock().stats())
    }

    /// Check if TTL is enabled
    pub fn has_ttl(&self) -> bool {
        self.ttl_manager.is_some()
    }

    /// Cleanup expired keys (best-effort) and return number of deletions.
    /// Can be called from a background task to enforce TTL without reads.
    pub fn cleanup_expired_keys(&self) -> usize {
        let Some(ttl_mgr) = &self.ttl_manager else {
            return 0;
        };

        let expired = ttl_mgr.lock().get_expired_keys();
        if expired.is_empty() {
            return 0;
        }

        let mut deleted = 0usize;
        for key in &expired {
            #[cfg(feature = "change-events")]
            if let Some(txt) = stringify_for_subscriber(key) {
                for subscriber in self.change_subscribers.lock().iter() {
                    subscriber.on_expire(&txt);
                }
            }
            if self.delete(key).ok().flatten().is_some() {
                deleted += 1;
            }
        }

        // Remove from tracking regardless of delete outcome to avoid reprocessing
        ttl_mgr.lock().cleanup_expired_keys(&expired);

        deleted
    }

    // ===== Metrics Methods =====

    /// Enable metrics collection
    pub fn enable_metrics(&mut self) {
        if self.metrics.is_none() {
            self.metrics = Some(Arc::new(MetricsCollector::new()));
        }
    }

    /// Get metrics snapshot
    pub fn metrics_snapshot(&self) -> Option<MetricsSnapshot> {
        self.metrics.as_ref().map(|m| m.snapshot())
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> Option<String> {
        self.metrics.as_ref().map(|m| m.export_prometheus())
    }

    /// Check if metrics are enabled
    pub fn has_metrics(&self) -> bool {
        self.metrics.is_some()
    }

    // ===== Pub/Sub Methods =====

    /// Enable Pub/Sub messaging
    pub fn enable_pubsub(&mut self) {
        if self.pubsub.is_none() {
            self.pubsub = Some(Arc::new(PubSubManager::new()));
        }
    }

    /// Subscribe to a topic pattern
    ///
    /// Patterns support wildcards:
    /// - `*` matches any single segment (e.g., `user.*` matches `user.created`, `user.deleted`)
    pub fn subscribe(&self, pattern: String) -> Option<Subscription> {
        self.pubsub.as_ref().map(|ps| ps.subscribe(pattern))
    }

    /// Unsubscribe by subscription ID
    pub fn unsubscribe(&self, subscription_id: u64) -> bool {
        self.pubsub
            .as_ref()
            .map(|ps| ps.unsubscribe(subscription_id))
            .unwrap_or(false)
    }

    /// Publish a message to a topic
    ///
    /// Returns the number of subscribers that received the message
    pub fn publish(&self, topic: String, payload: String) -> Option<usize> {
        self.pubsub.as_ref().map(|ps| ps.publish(topic, payload))
    }

    // ===== Change Events Methods =====

    /// Register a change event subscriber
    ///
    /// The subscriber will receive notifications for all PUT and DELETE operations.
    /// Multiple subscribers can be registered and will all receive events.
    ///
    /// **Note**: This feature requires the `change-events` feature flag.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use engine::change_events::ChangeSubscriber;
    ///
    /// #[derive(Debug)]
    /// struct MySubscriber;
    ///
    /// impl ChangeSubscriber for MySubscriber {
    ///     fn on_put(&self, key: &str, value: &str) {
    ///         println!("PUT: {} = {}", key, value);
    ///     }
    ///     fn on_delete(&self, key: &str) {
    ///         println!("DELETE: {}", key);
    ///     }
    /// }
    ///
    /// let mut engine = Engine::with_shards(4, 16);
    /// engine.register_subscriber(Arc::new(MySubscriber));
    /// ```
    #[cfg(feature = "change-events")]
    pub fn register_subscriber(&self, subscriber: Arc<dyn change_events::ChangeSubscriber>) {
        self.change_subscribers.lock().push(subscriber);
    }

    /// Get the number of registered change subscribers
    #[cfg(feature = "change-events")]
    pub fn change_subscriber_count(&self) -> usize {
        self.change_subscribers.lock().len()
    }

    /// Get number of active pubsub subscribers
    pub fn pubsub_subscriber_count(&self) -> Option<usize> {
        self.pubsub.as_ref().map(|ps| ps.subscriber_count())
    }

    /// Get total messages published
    pub fn message_count(&self) -> Option<u64> {
        self.pubsub.as_ref().map(|ps| ps.message_count())
    }

    /// Check if Pub/Sub is enabled
    pub fn has_pubsub(&self) -> bool {
        self.pubsub.is_some()
    }

    /// Perform eviction based on configured policy
    fn perform_eviction(&self) -> io::Result<()> {
        let tracker = match &self.resource_tracker {
            Some(t) => t,
            None => return Ok(()),
        };

        let stats = tracker.stats();
        let bytes_to_free = tracker.bytes_to_evict();
        
        // If we are at or above the limit, we need to evict.
        // Even if we are exactly AT the limit, we need to evict 1 to make room for the new insert.
        let keys_over_limit = if stats.keys_count >= stats.limits.max_keys {
            (stats.keys_count + 1).saturating_sub(stats.limits.max_keys)
        } else {
            0
        };

        if bytes_to_free == 0 && keys_over_limit == 0 {
            return Ok(());
        }

        // Estimate how many keys to evict
        // Assume average key+value size from current memory usage
        let avg_size = if stats.keys_count > 0 {
            stats.memory_used / stats.keys_count
        } else {
            1024 // Default 1KB if unknown
        };

        let estimated_keys_mem = (bytes_to_free / avg_size).max(if bytes_to_free > 0 { 1 } else { 0 });
        let estimated_keys = estimated_keys_mem.max(keys_over_limit);

        // Get keys to evict based on policy
        let keys_to_evict = match stats.eviction_policy {
            EvictionPolicy::LRU | EvictionPolicy::VolatileLRU => {
                if let Some(lru) = &self.lru_tracker {
                    lru.get_lru_keys(estimated_keys as usize)
                } else {
                    Vec::new()
                }
            }
            EvictionPolicy::LFU => {
                if let Some(lfu) = &self.lfu_tracker {
                    lfu.get_lfu_keys(estimated_keys as usize)
                } else {
                    Vec::new()
                }
            }
            EvictionPolicy::Random => {
                // For random eviction, we'll just take keys from the first shard
                // This is a simplified implementation
                use std::ops::Bound;
                let entries = self.shards[0].range(Bound::Unbounded, Bound::Unbounded);
                entries
                    .into_iter()
                    .take(estimated_keys as usize)
                    .map(|(k, _)| k)
                    .collect()
            }
            EvictionPolicy::NoEviction => Vec::new(),
        };

        // Evict keys
        let mut evicted = 0;
        for key in keys_to_evict {
            match self.delete_internal(key, true) {
                Ok(Some(_)) => {
                    evicted += 1;
                    tracker.record_eviction(true);
                }
                Ok(None) => {
                    // Key already deleted
                    tracker.record_eviction(false);
                }
                Err(e) => {
                    eprintln!("Eviction error: {}", e);
                    tracker.record_eviction(false);
                }
            }

            // Check if we've freed enough
            if tracker.bytes_to_evict() == 0 {
                break;
            }
        }

        if evicted > 0 {
            eprintln!(
                "Evicted {} keys to free memory (policy: {:?})",
                evicted, stats.eviction_policy
            );
        }

        Ok(())
    }

    pub fn range_query<'a>(
        &'a self,
        lower: Bound<&'a K>,
        upper: Bound<&'a K>,
    ) -> RangeQuery<'a, K, V> {
        RangeQuery {
            engine: self,
            lower,
            upper,
        }
    }

    pub fn join(&self) -> JoinBuilder<'_, K, V> {
        JoinBuilder::new(self)
    }
}

pub struct RangeQuery<'a, K, V> {
    engine: &'a Engine<K, V>,
    lower: Bound<&'a K>,
    upper: Bound<&'a K>,
}

impl<'a, K, V> RangeQuery<'a, K, V>
where
    K: Eq
        + Hash
        + Ord
        + Clone
        + art::AsBytes
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn sorted(self) -> Vec<(K, V)> {
        self.engine.range(self.lower, self.upper)
    }

    pub fn filter<P>(self, predicate: P) -> FilteredRangeQuery<'a, K, V, P>
    where
        P: FnMut(&K, &V) -> bool,
    {
        FilteredRangeQuery {
            engine: self.engine,
            lower: self.lower,
            upper: self.upper,
            predicate,
            limit: None,
        }
    }

    pub fn limit(self, n: usize) -> FilteredRangeQuery<'a, K, V, impl FnMut(&K, &V) -> bool> {
        self.filter(|_, _| true).with_limit(n)
    }
}

pub struct FilteredRangeQuery<'a, K, V, P>
where
    P: FnMut(&K, &V) -> bool,
{
    engine: &'a Engine<K, V>,
    lower: Bound<&'a K>,
    upper: Bound<&'a K>,
    predicate: P,
    limit: Option<usize>,
}

impl<'a, K, V, P> FilteredRangeQuery<'a, K, V, P>
where
    K: Eq
        + Hash
        + Ord
        + Clone
        + art::AsBytes
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    P: FnMut(&K, &V) -> bool,
{
    pub fn with_limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn collect(mut self) -> Vec<(K, V)> {
        let mut out = Vec::new();
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        for (k, v) in self.engine.collect_range_unordered(self.lower, self.upper) {
            if (self.predicate)(&k, &v) {
                out.push((k, v));
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    break;
                }
            }
        }
        out
    }

    pub fn aggregate_sum<T, F>(mut self, mut map: F) -> T
    where
        T: Default + std::ops::AddAssign,
        F: FnMut(&K, &V) -> T,
    {
        let mut acc = T::default();
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        for (k, v) in self.engine.collect_range_unordered(self.lower, self.upper) {
            if (self.predicate)(&k, &v) {
                acc += map(&k, &v);
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    break;
                }
            }
        }
        acc
    }

    pub fn aggregate_count(mut self) -> usize {
        let mut count = 0usize;
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        for (k, v) in self.engine.collect_range_unordered(self.lower, self.upper) {
            if (self.predicate)(&k, &v) {
                count += 1;
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    break;
                }
            }
        }
        count
    }
}

#[derive(Debug, Clone)]
pub struct HashJoinResult<J, LK, LV, RK, RV> {
    pub join_key: J,
    pub left_key: LK,
    pub left_value: LV,
    pub right_key: RK,
    pub right_value: RV,
}

pub struct JoinBuilder<'a, K, V> {
    engine: &'a Engine<K, V>,
}

impl<'a, K, V> JoinBuilder<'a, K, V>
where
    K: Eq
        + Hash
        + Ord
        + Clone
        + art::AsBytes
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(engine: &'a Engine<K, V>) -> Self {
        Self { engine }
    }

    pub fn hash_join<K2, V2, J, FLeft, FRight>(
        &self,
        other: &'a Engine<K2, V2>,
        left_range: (Bound<&'a K>, Bound<&'a K>),
        right_range: (Bound<&'a K2>, Bound<&'a K2>),
        mut left_key: FLeft,
        mut right_key: FRight,
    ) -> Vec<HashJoinResult<J, K, V, K2, V2>>
    where
        K2: Eq
            + Hash
            + Ord
            + Clone
            + art::AsBytes
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + 'static,
        V2: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        J: Eq + Hash + Clone,
        FLeft: FnMut(&K, &V) -> J,
        FRight: FnMut(&K2, &V2) -> J,
    {
        let left_rows = self
            .engine
            .collect_range_unordered(left_range.0, left_range.1);
        let mut hash_table: HashMap<J, Vec<(K, V)>> =
            HashMap::with_capacity(left_rows.len().next_power_of_two().max(16));

        for (k, v) in left_rows {
            let join_key = left_key(&k, &v);
            hash_table.entry(join_key).or_default().push((k, v));
        }

        let mut results = Vec::new();
        let right_rows = other.collect_range_unordered(right_range.0, right_range.1);
        results.reserve(right_rows.len());
        for (rk, rv) in right_rows {
            let join_key = right_key(&rk, &rv);
            if let Some(matches) = hash_table.get(&join_key) {
                for (lk, lv) in matches {
                    results.push(HashJoinResult {
                        join_key: join_key.clone(),
                        left_key: lk.clone(),
                        left_value: lv.clone(),
                        right_key: rk.clone(),
                        right_value: rv.clone(),
                    });
                }
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included};
    use tempfile::tempdir;
    use wal::WalOptions;

    #[test]
    fn ordered_range_across_shards() {
        let engine = Engine::<u32, u32>::with_shards(4, 1 << 6);
        for i in 0..200u32 {
            engine.put(i, i * 10).unwrap();
        }
        let slice = engine.range(Included(&25), Excluded(&42));
        let expected: Vec<_> = (25..42).map(|k| (k, k * 10)).collect();
        assert_eq!(slice, expected);

        engine.delete(&30).unwrap();
        let slice = engine.range(Included(&25), Excluded(&42));
        let expected: Vec<_> = (25..42).filter(|k| *k != 30).map(|k| (k, k * 10)).collect();
        assert_eq!(slice, expected);
    }

    #[test]
    fn hopscotch_secondary_tags() {
        let engine = Engine::with_shards(4, 1 << 6);
        for i in 0..64u64 {
            engine.put(i, i * 100).unwrap();
        }
        assert!(engine.tag(&5, 42).unwrap());
        assert!(engine.tag(&9, 42).unwrap());
        let mut tagged = engine.tagged(42);
        tagged.sort();
        assert_eq!(tagged, vec![500, 900]);

        engine.put(5, 7_000).unwrap();
        let mut tagged = engine.tagged(42);
        tagged.sort();
        assert_eq!(tagged, vec![900, 7_000]);

        engine.delete(&9).unwrap();
        let tagged = engine.tagged(42);
        assert_eq!(tagged, vec![7_000]);

        assert!(!engine.tag(&999, 42).unwrap());
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("engine.wal");
        let snapshot_path = dir.path().join("engine.snap");
        let config = PersistenceConfig {
            shards_pow2: 4,
            shard_cap_pow2: 1 << 6,
            wal_path: wal_path.clone(),
            wal_options: WalOptions::default(),
            snapshot_path: snapshot_path.clone(),
            snapshot_options: SnapshotOptions::default(),
            resource_limits: None,
        };
        let engine = Engine::with_persistence(config.clone()).unwrap();
        for i in 0..32u64 {
            engine.put(i, i * 10).unwrap();
            if i % 3 == 0 {
                engine.tag(&i, 100).unwrap();
            }
        }
        engine.flush().unwrap();
        engine.snapshot().unwrap();

        let recovered = Engine::recover(config).unwrap();
        for i in 0..32u64 {
            assert_eq!(recovered.get(&i), Some(i * 10));
        }
        let mut tagged = recovered.tagged(100);
        tagged.sort();
        let expected: Vec<_> = (0..32u64).filter(|i| i % 3 == 0).map(|i| i * 10).collect();
        assert_eq!(tagged, expected);
    }

    #[test]
    fn range_query_with_filters_and_aggregates() {
        let engine = Engine::with_shards(2, 1 << 6);
        for i in 0..50u32 {
            engine.put(i, i * 2).unwrap();
        }
        let mut filtered = engine
            .range_query(Included(&10), Included(&30))
            .filter(|k, _| *k % 5 == 0)
            .with_limit(3)
            .collect();
        filtered.sort_by(|a, b| a.0.cmp(&b.0));
        // Keys divisible by 5 in range [10, 30]: 10, 15, 20, 25, 30
        assert!(filtered.len() <= 3);
        assert!(filtered.iter().all(|(k, v)| k % 5 == 0 && *v == k * 2));

        let sum: u64 = engine
            .range_query(Included(&0), Excluded(&20))
            .filter(|_, v| *v % 4 == 0)
            .aggregate_sum(|_, v| *v as u64);
        // 0, 4, 8, 12, 16, 20, 24, 28, 32, 36
        // 0+8+16+24+32 = 80
        // 0,2,4,6,8,10,12,14,16,18
        // 0,4,8,12,16
        // 0, 8, 16, 24, 32
        let expected_sum = (0..20).map(|i| i * 2).filter(|v| v % 4 == 0).sum::<u32>();
        assert_eq!(sum, expected_sum as u64);

        let count = engine
            .range_query(Included(&0), Included(&49))
            .filter(|k, _| *k < 25)
            .aggregate_count();
        assert_eq!(count, 25);
    }

    #[test]
    fn hash_join_simple() {
        let left = Engine::<u64, String>::with_shards(2, 1 << 6);
        let right = Engine::<u64, String>::with_shards(2, 1 << 6);
        for i in 0..20u64 {
            left.put(i, format!("L{}", i)).unwrap();
            if i % 2 == 0 {
                right.put(i, format!("R{}", i)).unwrap();
            }
        }
        let results = left.join().hash_join(
            &right,
            (Included(&0), Included(&19)),
            (Included(&0), Included(&19)),
            |k, _| *k,
            |k, _| *k,
        );
        assert_eq!(results.len(), 10);
        for res in &results {
            assert_eq!(res.left_key, res.right_key);
            assert_eq!(res.left_value, format!("L{}", res.join_key));
            assert_eq!(res.right_value, format!("R{}", res.join_key));
        }
    }
}


