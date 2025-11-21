use ahash::RandomState;
use parking_lot::{Mutex, RwLock};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

pub mod filter;
mod opt;
mod simd;

const BUCKET_SIZE: usize = 4;
const MAX_KICKS: usize = 96;
const STASH_SIZE: usize = 16;
const RESIZE_LOAD: f64 = 0.90;

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
    size: Mutex<usize>,
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
            size: Mutex::new(0),
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
        let sz = *self.size.lock();
        (sz as f64) / (self.capacity() as f64)
    }
}

pub struct CuckooTable<K, V, S = RandomState> {
    inner: RwLock<Arc<Table<K, V, S>>>,
}

impl<K: Eq + Hash, V: Clone, S: BuildHasher + Clone + Default> CuckooTable<K, V, S> {
    pub fn with_capacity_pow2(cap_pow2: usize) -> Self {
        let t = Arc::new(Table::with_pow2(cap_pow2, S::default()));
        Self {
            inner: RwLock::new(t),
        }
    }

    #[inline]
    fn read(&self) -> Arc<Table<K, V, S>> {
        self.inner.read().clone()
    }

    #[inline]
    fn swap_table(&self, t: Arc<Table<K, V, S>>) {
        *self.inner.write() = t;
    }

    pub fn len(&self) -> usize {
        *self.read().size.lock()
    }

    pub fn capacity(&self) -> usize {
        self.read().capacity()
    }

    pub fn load_factor(&self) -> f64 {
        self.read().load_factor()
    }

    pub fn memory_usage(&self) -> usize {
        self.read().capacity() * (std::mem::size_of::<K>() + std::mem::size_of::<V>())
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let t = self.read();
        let i1 = t.h1(key);

        // Prefetch bucket i2 while processing i1
        let i2 = t.h2(key);
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
        let t = self.read();
        let i1 = t.h1(key);
        {
            let mut b = t.buckets[i1].lock();
            for s in &mut b.slots {
                if let Some(e) = s {
                    if &e.key == key {
                        let v = s.take().unwrap().val;
                        *t.size.lock() -= 1;
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
                        *t.size.lock() -= 1;
                        return Some(v);
                    }
                }
            }
        }
        let mut stash = t.stash.lock();
        if let Some(i) = stash.iter().position(|e| &e.key == key) {
            *t.size.lock() -= 1;
            return Some(stash.swap_remove(i).val);
        }
        None
    }

    pub fn put(&self, key: K, val: V) -> Option<V> {
        if self.load_factor() >= RESIZE_LOAD {
            self.resize(self.capacity() * 2);
        }
        self.put_no_resize(key, val)
    }

    fn put_no_resize(&self, key: K, val: V) -> Option<V> {
        let t = self.read();
        let mut new_e = Entry { key, val };
        let i1 = t.h1(&new_e.key);
        {
            let mut b = t.buckets[i1].lock();
            for s in &mut b.slots {
                if let Some(e) = s {
                    if e.key == new_e.key {
                        let old = std::mem::replace(&mut e.val, new_e.val);
                        return Some(old);
                    }
                }
            }
            for s in &mut b.slots {
                if s.is_none() {
                    *s = Some(new_e);
                    *t.size.lock() += 1;
                    return None;
                }
            }
        }
        let i2 = t.h2(&new_e.key);
        {
            let mut b = t.buckets[i2].lock();
            for s in &mut b.slots {
                if let Some(e) = s {
                    if e.key == new_e.key {
                        let old = std::mem::replace(&mut e.val, new_e.val);
                        return Some(old);
                    }
                }
            }
            for s in &mut b.slots {
                if s.is_none() {
                    *s = Some(new_e);
                    *t.size.lock() += 1;
                    return None;
                }
            }
        }
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
                    *t.size.lock() += 1;
                    return None;
                }
            }
        }
        {
            let mut stash = t.stash.lock();
            if stash.len() < STASH_SIZE {
                stash.push(new_e);
                *t.size.lock() += 1;
                return None;
            }
        }
        drop(t);
        self.resize(self.capacity() * 2);
        self.put_no_resize(new_e.key, new_e.val)
    }

    pub fn resize(&self, new_capacity_slots: usize) {
        let mut buckets = (new_capacity_slots / BUCKET_SIZE).max(8);
        buckets = buckets.next_power_of_two();
        let new_tab = Arc::new(Table::with_pow2(buckets, S::default()));

        let old = self.read();
        for m in &old.buckets {
            let mut b = m.lock();
            for slot in b.slots.iter_mut() {
                if let Some(e) = slot.take() {
                    Self::insert_into_table(&new_tab, e);
                    *old.size.lock() -= 1;
                }
            }
        }
        let mut stash = old.stash.lock();
        for e in stash.drain(..) {
            Self::insert_into_table(&new_tab, e);
            *old.size.lock() -= 1;
        }

        self.swap_table(new_tab);
    }

    fn insert_into_table(table: &Arc<Table<K, V, S>>, mut e: Entry<K, V>) {
        let i1 = table.h1(&e.key);
        {
            let mut b = table.buckets[i1].lock();
            for s in &mut b.slots {
                if s.is_none() {
                    *s = Some(e);
                    *table.size.lock() += 1;
                    return;
                }
            }
        }
        let i2 = table.h2(&e.key);
        {
            let mut b = table.buckets[i2].lock();
            for s in &mut b.slots {
                if s.is_none() {
                    *s = Some(e);
                    *table.size.lock() += 1;
                    return;
                }
            }
        }
        let mut idx = i1;
        for _ in 0..MAX_KICKS {
            let mut b = table.buckets[idx].lock();
            let ev = b.slots[0].take().unwrap();
            b.slots[0] = Some(e);
            drop(b);
            e = ev;
            let alt = if idx == table.h1(&e.key) {
                table.h2(&e.key)
            } else {
                table.h1(&e.key)
            };
            idx = alt;
            let mut b2 = table.buckets[idx].lock();
            for s in &mut b2.slots {
                if s.is_none() {
                    *s = Some(e);
                    *table.size.lock() += 1;
                    return;
                }
            }
        }
        let mut st = table.stash.lock();
        if st.len() < STASH_SIZE {
            st.push(e);
            *table.size.lock() += 1;
        } else {
            panic!("target table too small to complete resize");
        }
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
