use ahash::RandomState;
use parking_lot::{Mutex, RwLock};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub mod filter;
mod opt;
mod simd;

const BUCKET_SIZE: usize = 4;
const MAX_KICKS: usize = 96;
const STASH_SIZE: usize = 16;
const RESIZE_LOAD: f64 = 0.90;
const REHASH_BATCH: usize = 16; // Number of buckets to move per operation

#[derive(Clone)]
pub struct Entry<K, V> {
    pub key: K,
    pub val: V,
}

struct Bucket<K, V> {
    slots: [Option<Entry<K, V>>; BUCKET_SIZE],
}

impl<K, V> Default for Bucket<K, V> {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
        }
    }
}

pub struct Table<K, V, S = RandomState> {
    buckets: Vec<Mutex<Bucket<K, V>>>,
    mask: usize,
    stash: Mutex<Vec<Entry<K, V>>>,
    hasher: S,
    size: AtomicUsize, // Changed to Atomic for easier sharing
}

impl<K, V, S: BuildHasher + Clone> Table<K, V, S> {
    fn with_pow2(cap_pow2: usize, hasher: S) -> Self {
        assert!(cap_pow2.is_power_of_two());
        let buckets = (0..cap_pow2)
            .map(|_| Mutex::new(Bucket::default()))
            .collect();
        Self {
            buckets,
            mask: cap_pow2 - 1,
            stash: Mutex::new(Vec::with_capacity(STASH_SIZE)),
            hasher,
            size: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn hash_idx(&self, sep: u8, k: &K) -> usize
    where
        K: Hash,
    {
        let mut h = self.hasher.clone().build_hasher();
        sep.hash(&mut h);
        k.hash(&mut h);
        (h.finish() as usize) & self.mask
    }

    #[inline]
    fn h1(&self, k: &K) -> usize
    where
        K: Hash,
    {
        self.hash_idx(0, k)
    }

    #[inline]
    fn h2(&self, k: &K) -> usize
    where
        K: Hash,
    {
        self.hash_idx(1, k)
    }

    fn capacity(&self) -> usize {
        self.buckets.len() * BUCKET_SIZE + STASH_SIZE
    }

    fn load_factor(&self) -> f64 {
        let sz = self.size.load(Ordering::Relaxed);
        (sz as f64) / (self.capacity() as f64)
    }
}

pub struct CuckooTable<K, V, S = RandomState> {
    // Primary table (newest)
    table: RwLock<Arc<Table<K, V, S>>>,
    // Old table during resizing
    old_table: RwLock<Option<Arc<Table<K, V, S>>>>,
    // Current rehash index in old_table
    rehash_idx: AtomicUsize,
}

impl<K: Eq + Hash + Clone, V: Clone, S: BuildHasher + Clone + Default> CuckooTable<K, V, S> {
    pub fn with_capacity_pow2(cap_pow2: usize) -> Self {
        let t = Arc::new(Table::with_pow2(cap_pow2, S::default()));
        Self {
            table: RwLock::new(t),
            old_table: RwLock::new(None),
            rehash_idx: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn read_table(&self) -> Arc<Table<K, V, S>> {
        self.table.read().clone()
    }

    #[inline]
    fn read_old_table(&self) -> Option<Arc<Table<K, V, S>>> {
        self.old_table.read().clone()
    }

    pub fn len(&self) -> usize {
        let t = self.read_table();
        let s1 = t.size.load(Ordering::Relaxed);
        if let Some(old) = self.read_old_table() {
            s1 + old.size.load(Ordering::Relaxed)
        } else {
            s1
        }
    }

    pub fn capacity(&self) -> usize {
        self.read_table().capacity()
    }

    pub fn load_factor(&self) -> f64 {
        self.read_table().load_factor()
    }

    pub fn memory_usage(&self) -> usize {
        let t = self.read_table();
        let mut mem = t.capacity() * (std::mem::size_of::<K>() + std::mem::size_of::<V>());
        if let Some(old) = self.read_old_table() {
            mem += old.capacity() * (std::mem::size_of::<K>() + std::mem::size_of::<V>());
        }
        mem
    }

    pub fn get(&self, key: &K) -> Option<V> {
        // 1. Check primary table
        let t = self.read_table();
        if let Some(v) = Self::get_in_table(&t, key) {
            return Some(v);
        }

        // 2. Check old table if resizing
        if let Some(old) = self.read_old_table() {
            if let Some(v) = Self::get_in_table(&old, key) {
                // Optional: Passive migration could happen here, but we'll stick to active migration in PUT
                // to keep GET read-only and lock-free (mostly).
                return Some(v);
            }
        }
        None
    }

    fn get_in_table(t: &Table<K, V, S>, key: &K) -> Option<V> {
        let i1 = t.h1(key);
        let i2 = t.h2(key);

        // Prefetch
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::_mm_prefetch;
            const _MM_HINT_T0: i32 = 3;
            _mm_prefetch(
                (&t.buckets[i2] as *const Mutex<Bucket<K, V>>) as *const i8,
                _MM_HINT_T0,
            );
        }

        {
            let b = t.buckets[i1].lock();
            for s in &b.slots {
                if let Some(e) = s {
                    if &e.key == key {
                        return Some(e.val.clone());
                    }
                }
            }
        }
        {
            let b = t.buckets[i2].lock();
            for s in &b.slots {
                if let Some(e) = s {
                    if &e.key == key {
                        return Some(e.val.clone());
                    }
                }
            }
        }
        let stash = t.stash.lock();
        for e in stash.iter() {
            if &e.key == key {
                return Some(e.val.clone());
            }
        }
        None
    }

    pub fn delete(&self, key: &K) -> Option<V> {
        let t = self.read_table();
        
        // Try delete from primary
        if let Some(v) = Self::delete_from_table(&t, key) {
            return Some(v);
        }

        // Try delete from old
        if let Some(old) = self.read_old_table() {
             if let Some(v) = Self::delete_from_table(&old, key) {
                return Some(v);
            }
        }
        
        // While we are here, might as well help with rehash
        self.help_rehash();
        
        None
    }

    fn delete_from_table(t: &Table<K, V, S>, key: &K) -> Option<V> {
        let i1 = t.h1(key);
        {
            let mut b = t.buckets[i1].lock();
            for s in &mut b.slots {
                if let Some(e) = s {
                    if &e.key == key {
                        let v = s.take().unwrap().val;
                        t.size.fetch_sub(1, Ordering::Relaxed);
                        return Some(v);
                    }
                }
            }
        }
        let i2 = t.h2(key);
        {
            let mut b = t.buckets[i2].lock();
            for s in &mut b.slots {
                if let Some(e) = s {
                    if &e.key == key {
                        let v = s.take().unwrap().val;
                        t.size.fetch_sub(1, Ordering::Relaxed);
                        return Some(v);
                    }
                }
            }
        }
        let mut stash = t.stash.lock();
        if let Some(i) = stash.iter().position(|e| &e.key == key) {
            t.size.fetch_sub(1, Ordering::Relaxed);
            return Some(stash.swap_remove(i).val);
        }
        None
    }

    pub fn put(&self, key: K, val: V) -> Option<V> {
        // Help rehash before inserting
        self.help_rehash();

        // Check if we need to start a new resize
        if self.read_old_table().is_none() && self.load_factor() >= RESIZE_LOAD {
            self.start_resize();
        }

        // If key exists in old table, remove it first (effectively migrating it)
        let mut old_val = None;
        if let Some(old) = self.read_old_table() {
            if let Some(v) = Self::delete_from_table(&old, &key) {
                old_val = Some(v);
            }
        }

        // Insert into new table
        let mut t = self.read_table();
        let mut current_key = key;
        let mut current_val = val;

        loop {
            match self.put_in_table(&t, current_key, current_val) {
                Ok(res) => {
                    // If we found a value in old table, that's the "previous value" we should return
                    // unless put_in_table also found one (which shouldn't happen if consistent).
                    return old_val.or(res);
                }
                Err(entry) => {
                    // Stash overflow!
                    // 1. If resizing, finish it synchronously to free up space/simplify state
                    if self.read_old_table().is_some() {
                        self.finish_resize();
                        // After finish_resize, 't' might be stale if we swapped tables? 
                        // Actually finish_resize just drains old_table. 't' (primary) remains the same.
                        // But we should re-read just in case logic changes.
                        t = self.read_table();
                    } else {
                        // Not resizing, but full. Force resize.
                        self.start_resize();
                        t = self.read_table();
                    }
                    
                    // Retry with the rejected entry
                    current_key = entry.key;
                    current_val = entry.val;
                }
            }
        }
    }

    fn finish_resize(&self) {
        while self.read_old_table().is_some() {
            self.help_rehash();
            // Yield to avoid starving other threads if this takes long?
            // For now, tight loop is fine as it makes progress.
            std::thread::yield_now();
        }
    }

    fn start_resize(&self) {
        // Check again under write lock
        let mut old_lock = self.old_table.write();
        if old_lock.is_some() {
            return; // Already resizing
        }

        let current = self.table.read().clone();
        let new_cap = current.buckets.len() * 2;
        let new_table = Arc::new(Table::with_pow2(new_cap, current.hasher.clone()));

        // Move current to old, set new as current
        *old_lock = Some(current);
        *self.table.write() = new_table;
        self.rehash_idx.store(0, Ordering::SeqCst);
    }

    fn help_rehash(&self) {
        let old_table_arc = match self.read_old_table() {
            Some(t) => t,
            None => return,
        };

        let start_idx = self.rehash_idx.fetch_add(REHASH_BATCH, Ordering::SeqCst);
        if start_idx >= old_table_arc.buckets.len() {
            // Check if stash is also empty, then finish
            let stash_empty = old_table_arc.stash.lock().is_empty();
            if stash_empty && start_idx >= old_table_arc.buckets.len() + 1 { // +1 buffer
                 // Done!
                 let mut old_lock = self.old_table.write();
                 *old_lock = None;
            } else if !stash_empty {
                 // Move stash items
                 let mut stash = old_table_arc.stash.lock();
                 let t = self.read_table();
                 for e in stash.drain(..) {
                     // We must handle overflow here too!
                     // If put_in_table fails during rehash, we are in trouble.
                     // The new table is full. We can't easily resize *again* recursively inside help_rehash
                     // without potentially deadlocking or exploding complexity.
                     // However, help_rehash is called from put/delete/get.
                     // If we fail here, we should probably panic or force a blocking resize of the NEW table?
                     // But we are already holding locks on old_table buckets maybe? No, we just have the stash lock here.
                     
                     // For now, let's try to insert. If it fails, we are in a bad spot.
                     // A robust implementation would chain resizes.
                     // Simplification: just panic for now if rehash fails, as it implies 
                     // we doubled capacity and STILL can't fit the old items? Unlikely with 2x growth unless hash attack.
                     if let Err(_entry) = self.put_in_table(&t, e.key, e.val) {
                         // Emergency: we can't fit an item from old table into new table.
                         // This is catastrophic.
                         panic!("CuckooTable: Failed to move item during rehash. New table full.");
                     }
                 }
            }
            return;
        }

        let end_idx = (start_idx + REHASH_BATCH).min(old_table_arc.buckets.len());
        let t = self.read_table();

        for i in start_idx..end_idx {
            let mut bucket = old_table_arc.buckets[i].lock();
            for slot in bucket.slots.iter_mut() {
                if let Some(e) = slot.take() {
                    if let Err(_entry) = self.put_in_table(&t, e.key, e.val) {
                         panic!("CuckooTable: Failed to move item during rehash. New table full.");
                    }
                    old_table_arc.size.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn put_in_table(&self, t: &Arc<Table<K, V, S>>, key: K, val: V) -> Result<Option<V>, Entry<K, V>> {
        let mut new_e = Entry { key, val };
        
        // Try insert in h1
        let i1 = t.h1(&new_e.key);
        {
            let mut b = t.buckets[i1].lock();
            // Check for update
            for s in &mut b.slots {
                if let Some(e) = s {
                    if e.key == new_e.key {
                        let old = std::mem::replace(&mut e.val, new_e.val);
                        return Ok(Some(old));
                    }
                }
            }
            // Insert empty
            for s in &mut b.slots {
                if s.is_none() {
                    *s = Some(new_e);
                    t.size.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        }

        // Try insert in h2
        let i2 = t.h2(&new_e.key);
        {
            let mut b = t.buckets[i2].lock();
            for s in &mut b.slots {
                if let Some(e) = s {
                    if e.key == new_e.key {
                        let old = std::mem::replace(&mut e.val, new_e.val);
                        return Ok(Some(old));
                    }
                }
            }
            for s in &mut b.slots {
                if s.is_none() {
                    *s = Some(new_e);
                    t.size.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        }

        // Kick
        let mut idx = i1;
        for _ in 0..MAX_KICKS {
            let mut b = t.buckets[idx].lock();
            let ev = b.slots[0]
                .take()
                .expect("bucket unexpectedly empty during kick");
            b.slots[0] = Some(new_e);
            drop(b);

            new_e = ev;
            let alt = if idx == t.h1(&new_e.key) {
                t.h2(&new_e.key)
            } else {
                t.h1(&new_e.key)
            };
            idx = alt;

            let mut b2 = t.buckets[idx].lock();
            for s in &mut b2.slots {
                if s.is_none() {
                    *s = Some(new_e);
                    t.size.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        }

        // Stash
        {
            let mut stash = t.stash.lock();
            if stash.len() < STASH_SIZE {
                stash.push(new_e);
                t.size.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
        
        // Table full/stuck. Return the entry so caller can handle it (e.g. resize)
        Err(new_e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_put_get_del() {
        let t = CuckooTable::<u64, u64, RandomState>::with_capacity_pow2(64);
        for i in 0..10_000 {
            t.put(i, i + 1);
        }
        for i in 0..10_000 {
            assert_eq!(t.get(&i), Some(i + 1));
        }
        for i in 0..10_000 {
            assert_eq!(t.delete(&i), Some(i + 1));
            assert_eq!(t.get(&i), None);
        }
    }

    #[test]
    fn resize_growth() {
        let t = CuckooTable::<u64, u64, RandomState>::with_capacity_pow2(16);
        for i in 0..50_000 {
            t.put(i, i);
        }
        for i in (0..50_000).step_by(7) {
            assert_eq!(t.get(&i), Some(i));
        }
        assert!(t.capacity() >= 50_000);
    }
}


