use ahash::RandomState;
use arena::{Arena, Handle};
use cuckoo::CuckooTable;
use hopscotch::HopscotchMap;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use ttree::TTree;
use wal::{
    read_snapshot, write_snapshot, SnapshotMeta, SnapshotOptions, WalOptions, WalReader, WalRecord,
    WalWriter,
};

// Phase 7: Advanced Query Engine
pub mod query;
pub use query::{Aggregator, HashJoinBuilder, QueryPlanner};

// Phase 8: Performance Optimizations
pub mod optimize;
pub use optimize::{
    AutoTuner, BatchOptimizer, PerfCounters, PerfSummary, Recommendation, TuningConfig,
};

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub shards_pow2: usize,
    pub shard_cap_pow2: usize,
    pub wal_path: PathBuf,
    pub wal_options: WalOptions,
    pub snapshot_path: PathBuf,
    pub snapshot_options: SnapshotOptions,
}

impl PersistenceConfig {
    pub fn new<P: Into<PathBuf>>(
        shards_pow2: usize,
        shard_cap_pow2: usize,
        wal_path: P,
        snapshot_path: P,
    ) -> Self {
        Self {
            shards_pow2,
            shard_cap_pow2,
            wal_path: wal_path.into(),
            wal_options: WalOptions::default(),
            snapshot_path: snapshot_path.into(),
            snapshot_options: SnapshotOptions::default(),
        }
    }
}

struct EnginePersistence<K, V> {
    wal: Arc<Mutex<WalWriter<K, V>>>,
    _wal_options: WalOptions,
    _wal_path: PathBuf,
    snapshot_path: PathBuf,
    snapshot_options: SnapshotOptions,
}

pub struct Shard<K, V> {
    arena: Arc<Arena<V>>,
    kv: CuckooTable<K, Handle<V>, RandomState>,
    ordered: TTree<K, Handle<V>>,
    tags: HopscotchMap<u64, Vec<Handle<V>>>,
    reverse_tags: HopscotchMap<usize, Vec<u64>>,
}

impl<K, V> Shard<K, V>
where
    K: Eq + Hash + Ord + Clone,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(cap_pow2: usize, arena: Arc<Arena<V>>) -> Self {
        Self {
            arena,
            kv: CuckooTable::with_capacity_pow2(cap_pow2),
            ordered: TTree::new(),
            tags: HopscotchMap::with_capacity_pow2(cap_pow2),
            reverse_tags: HopscotchMap::with_capacity_pow2(cap_pow2),
        }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        let guard = self.arena.pin();
        self.kv
            .get(k)
            .map(|handle| self.arena.clone_value(handle, &guard))
    }

    pub fn put(&self, k: K, v: V) -> Option<V> {
        let guard = self.arena.pin();
        let handle = self.arena.allocate(v);
        let prev = self.kv.put(k.clone(), handle);
        let tree_prev = self.ordered.insert(k, handle);
        debug_assert!(
            prev == tree_prev,
            "cuckoo and ttree returned different previous handles"
        );
        prev.map(|old| {
            let val = self.arena.clone_value(old, &guard);
            self.replace_handle_in_tags(old, handle);
            self.arena.retire(old, &guard);
            val
        })
    }

    pub fn delete(&self, k: &K) -> Option<V> {
        let guard = self.arena.pin();
        let prev = self.kv.delete(k);
        let tree_prev = self.ordered.delete(k);
        debug_assert!(
            prev == tree_prev,
            "cuckoo and ttree returned different handles on delete"
        );
        prev.map(|handle| {
            self.remove_from_tags(handle);
            let val = self.arena.clone_value(handle, &guard);
            self.arena.retire(handle, &guard);
            val
        })
    }

    pub fn range(&self, lower: &Bound<&K>, upper: &Bound<&K>) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let handles = self.ordered.range(lower.clone(), upper.clone());
        let guard = self.arena.pin();
        let mut out = Vec::with_capacity(handles.len());
        for (k, handle) in handles {
            let v = self.arena.clone_value(handle, &guard);
            out.push((k, v));
        }
        out
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

    fn replace_handle_in_tags(&self, old: Handle<V>, new: Handle<V>) {
        let old_addr = old.addr();
        if let Some(tag_list) = self.reverse_tags.remove(&old_addr) {
            for tag in &tag_list {
                self.tags.update_in(tag, |handles| {
                    if let Some(pos) = handles.iter().position(|h| *h == old) {
                        handles[pos] = new;
                    }
                    handles.is_empty()
                });
            }
            self.reverse_tags.insert(new.addr(), tag_list);
        }
    }

    fn remove_from_tags(&self, handle: Handle<V>) {
        let addr = handle.addr();
        if let Some(tag_list) = self.reverse_tags.remove(&addr) {
            for tag in tag_list {
                self.tags.update_in(&tag, |handles| {
                    handles.retain(|h| *h != handle);
                    handles.is_empty()
                });
            }
        }
    }
}

pub struct Engine<K, V> {
    shards: Vec<Shard<K, V>>,
    mask: usize,
    hash_builder: RandomState,
    shards_pow2: usize,
    per_shard_cap_pow2: usize,
    persistence: Option<EnginePersistence<K, V>>,
}

impl<K, V> Engine<K, V>
where
    K: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn with_shards(n_shards_pow2: usize, per_shard_cap_pow2: usize) -> Self {
        Self::new_internal(n_shards_pow2, per_shard_cap_pow2, None)
    }

    pub fn with_persistence(config: PersistenceConfig) -> io::Result<Self> {
        let wal_writer = WalWriter::<K, V>::open(&config.wal_path, config.wal_options)?;
        let persistence = EnginePersistence {
            wal: Arc::new(Mutex::new(wal_writer)),
            _wal_options: config.wal_options,
            _wal_path: config.wal_path.clone(),
            snapshot_path: config.snapshot_path.clone(),
            snapshot_options: config.snapshot_options.clone(),
        };
        Ok(Self::new_internal(
            config.shards_pow2,
            config.shard_cap_pow2,
            Some(persistence),
        ))
    }

    pub fn recover(config: PersistenceConfig) -> io::Result<Self> {
        let snapshot = read_snapshot::<_, K, V>(&config.snapshot_path)?;
        let engine = Self::with_persistence(config.clone())?;
        if let Some(data) = snapshot {
            for (key, value) in data.entries {
                engine.put_internal(key, value, false)?;
            }
            for (key, tag) in data.tags {
                let _ = engine.tag_internal(key, tag, false)?;
            }
        }
        if config.wal_path.exists() {
            let mut reader = WalReader::<K, V>::open(&config.wal_path)?;
            let mut records_applied = 0;
            loop {
                match reader.next() {
                    Ok(Some(record)) => {
                        engine.apply_wal_record(record, false)?;
                        records_applied += 1;
                    }
                    Ok(None) => break,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        // WAL ended unexpectedly, but we've applied what we could
                        eprintln!("Warning: WAL ended unexpectedly after {} records. Continuing with partial recovery.", records_applied);
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        // NOTE: WAL is not reset here to allow persistence across restarts.
        // It will be reset on SNAPSHOT command to compact the log.
        // For multi-server scenarios, use snapshots to sync data.
        Ok(engine)
    }

    pub fn get(&self, k: &K) -> Option<V> {
        let idx = self.shard_idx(k);
        self.shards[idx].get(k)
    }

    pub fn put(&self, k: K, v: V) -> io::Result<Option<V>> {
        self.put_internal(k, v, true)
    }

    pub fn delete(&self, k: &K) -> io::Result<Option<V>> {
        self.delete_internal(k.clone(), true)
    }

    fn collect_range_unordered(&self, lower: &Bound<&K>, upper: &Bound<&K>) -> Vec<(K, V)> {
        let mut out: Vec<(K, V)> = Vec::new();
        for shard in &self.shards {
            out.extend(shard.range(lower, upper));
        }
        out
    }

    pub fn range(&self, lower: Bound<&K>, upper: Bound<&K>) -> Vec<(K, V)> {
        let mut out = self.collect_range_unordered(&lower, &upper);
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
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

    pub fn join<'a>(&'a self) -> JoinBuilder<'a, K, V> {
        JoinBuilder::new(self)
    }

    pub fn tag(&self, key: &K, tag: u64) -> io::Result<bool> {
        self.tag_internal(key.clone(), tag, true)
    }

    pub fn tagged(&self, tag: u64) -> Vec<V> {
        self.shards
            .iter()
            .flat_map(|shard| shard.tagged(tag))
            .collect()
    }

    pub fn flush(&self) -> io::Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.wal.lock().flush()?;
        }
        Ok(())
    }

    pub fn snapshot(&self) -> io::Result<()> {
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "persistence not configured"))?;
        self.flush()?;
        let entries = self.export_entries();
        let tags = self.export_tags();
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
        )?;
        persistence.wal.lock().reset()?;
        Ok(())
    }

    pub fn flushdb(&self) -> io::Result<()> {
        use std::ops::Bound::Unbounded;

        for shard in &self.shards {
            let entries = shard.range(&Unbounded, &Unbounded);
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
    ) -> Self {
        assert!(shards_pow2.is_power_of_two());
        let shards = (0..shards_pow2)
            .map(|_| Shard::new(per_shard_cap_pow2, Arc::new(Arena::new())))
            .collect();
        Self {
            shards,
            mask: shards_pow2 - 1,
            hash_builder: RandomState::default(),
            shards_pow2,
            per_shard_cap_pow2,
            persistence,
        }
    }

    fn shard_idx(&self, k: &K) -> usize {
        use std::hash::{BuildHasher, Hasher};
        let mut h = self.hash_builder.clone().build_hasher();
        k.hash(&mut h);
        (h.finish() as usize) & self.mask
    }

    fn put_internal(&self, k: K, v: V, log: bool) -> io::Result<Option<V>> {
        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.lock().append_put(&k, &v)?;
            }
        }
        let idx = self.shard_idx(&k);
        Ok(self.shards[idx].put(k, v))
    }

    fn delete_internal(&self, key: K, log: bool) -> io::Result<Option<V>> {
        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.lock().append_delete(&key)?;
            }
        }
        let idx = self.shard_idx(&key);
        Ok(self.shards[idx].delete(&key))
    }

    fn tag_internal(&self, key: K, tag: u64, log: bool) -> io::Result<bool> {
        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.lock().append_tag(&key, tag)?;
            }
        }
        let idx = self.shard_idx(&key);
        Ok(self.shards[idx].tag(&key, tag))
    }

    fn untag_internal(&self, key: K, tag: u64, log: bool) -> io::Result<bool> {
        if log {
            if let Some(persistence) = &self.persistence {
                persistence.wal.lock().append_untag(&key, tag)?;
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
        }
        Ok(())
    }

    fn export_entries(&self) -> Vec<(K, V)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.export_entries());
        }
        out
    }

    fn export_tags(&self) -> Vec<(K, u64)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.export_tag_records());
        }
        out
    }
}

pub struct RangeQuery<'a, K, V> {
    engine: &'a Engine<K, V>,
    lower: Bound<&'a K>,
    upper: Bound<&'a K>,
}

impl<'a, K, V> RangeQuery<'a, K, V>
where
    K: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn sorted(self) -> Vec<(K, V)> {
        self.engine.range(self.lower.clone(), self.upper.clone())
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
    K: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
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
        for (k, v) in self
            .engine
            .collect_range_unordered(&self.lower, &self.upper)
        {
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
        for (k, v) in self
            .engine
            .collect_range_unordered(&self.lower, &self.upper)
        {
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
        for (k, v) in self
            .engine
            .collect_range_unordered(&self.lower, &self.upper)
        {
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
    K: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
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
        K2: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        V2: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        J: Eq + Hash + Clone,
        FLeft: FnMut(&K, &V) -> J,
        FRight: FnMut(&K2, &V2) -> J,
    {
        let left_rows = self
            .engine
            .collect_range_unordered(&left_range.0, &left_range.1);
        let mut hash_table: HashMap<J, Vec<(K, V)>> =
            HashMap::with_capacity(left_rows.len().next_power_of_two().max(16));

        for (k, v) in left_rows {
            let join_key = left_key(&k, &v);
            hash_table.entry(join_key).or_default().push((k, v));
        }

        let mut results = Vec::new();
        let right_rows = other.collect_range_unordered(&right_range.0, &right_range.1);
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

    #[test]
    fn ordered_range_across_shards() {
        let engine = Engine::with_shards(4, 1 << 6);
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
        // Keys divisible by 5 in range [10, 30]: 10, 20, 25, 30
        // With limit(3), depending on shard ordering we might get different combinations
        assert!(filtered.len() <= 3);
        assert!(filtered.iter().all(|(k, v)| k % 5 == 0 && *v == k * 2));

        let sum: u64 = engine
            .range_query(Included(&0), Excluded(&20))
            .filter(|_, v| *v % 4 == 0)
            .aggregate_sum(|_, v| *v as u64);
        assert_eq!(sum, 180);

        let count = engine
            .range_query(Included(&0), Included(&49))
            .filter(|k, _| *k < 25)
            .aggregate_count();
        assert_eq!(count, 25);
    }

    #[test]
    fn hash_join_simple() {
        let left = Engine::with_shards(2, 1 << 6);
        let right = Engine::with_shards(2, 1 << 6);
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
