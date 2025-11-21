use ahash::RandomState;
use parking_lot::RwLock;
use std::hash::{BuildHasher, Hash, Hasher};

const NEIGHBORHOOD: usize = 32;
const MAX_DISTANCE: usize = 256;
const MAX_LOAD_PERCENT: usize = 90;

struct Entry<K, V> {
    key: K,
    value: V,
    home: usize,
}

enum InsertOutcome<K, V> {
    Placed(Option<V>),
    NeedsResize(Entry<K, V>),
}

struct HopTable<K, V> {
    slots: Vec<Option<Entry<K, V>>>,
    mask: usize,
    len: usize,
}

impl<K, V> HopTable<K, V> {
    fn with_capacity_pow2(cap: usize) -> Self {
        assert!(cap.is_power_of_two());
        let mut slots = Vec::with_capacity(cap);
        slots.resize_with(cap, || None);
        Self {
            mask: cap - 1,
            slots,
            len: 0,
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    fn distance(&self, home: usize, idx: usize) -> usize {
        (idx.wrapping_sub(home)) & self.mask
    }

    fn load_exceeded(&self) -> bool {
        self.len * 100 >= self.capacity() * MAX_LOAD_PERCENT
    }

    fn insert_entry(&mut self, mut entry: Entry<K, V>) -> InsertOutcome<K, V>
    where
        K: Eq,
        V: Clone,
    {
        if self.load_exceeded() {
            return InsertOutcome::NeedsResize(entry);
        }

        let home = entry.home;
        for offset in 0..NEIGHBORHOOD {
            let idx = (home + offset) & self.mask;
            if let Some(existing) = self.slots[idx].as_mut() {
                if existing.home == home && existing.key == entry.key {
                    let old = existing.value.clone();
                    existing.value = entry.value;
                    return InsertOutcome::Placed(Some(old));
                }
            }
        }

        let mut free_idx = None;
        for dist in 0..MAX_DISTANCE {
            let idx = (home + dist) & self.mask;
            if self.slots[idx].is_none() {
                free_idx = Some(idx);
                break;
            }
        }

        let mut free_idx = match free_idx {
            Some(idx) => idx,
            None => return InsertOutcome::NeedsResize(entry),
        };

        while self.distance(home, free_idx) >= NEIGHBORHOOD {
            let mut moved = false;
            for offset in (1..NEIGHBORHOOD).rev() {
                let candidate_idx = (free_idx.wrapping_sub(offset)) & self.mask;
                if let Some(existing) = self.slots[candidate_idx].as_ref() {
                    let candidate_home = existing.home;
                    if self.distance(candidate_home, free_idx) < NEIGHBORHOOD {
                        let moved_entry = self.slots[candidate_idx].take().unwrap();
                        self.slots[free_idx] = Some(moved_entry);
                        free_idx = candidate_idx;
                        moved = true;
                        break;
                    }
                }
            }
            if !moved {
                return InsertOutcome::NeedsResize(entry);
            }
        }

        entry.home = home;
        self.slots[free_idx] = Some(entry);
        self.len += 1;
        InsertOutcome::Placed(None)
    }

    fn get<'a>(&'a self, key: &K, home: usize) -> Option<&'a V>
    where
        K: Eq,
    {
        for offset in 0..NEIGHBORHOOD {
            let idx = (home + offset) & self.mask;
            if let Some(entry) = self.slots[idx].as_ref() {
                if entry.home == home && &entry.key == key {
                    return Some(&entry.value);
                }
            }
        }
        None
    }

    fn get_mut<'a>(&'a mut self, key: &K, home: usize) -> Option<&'a mut V>
    where
        K: Eq,
    {
        let base_ptr = self.slots.as_mut_ptr();
        for offset in 0..NEIGHBORHOOD {
            let idx = (home + offset) & self.mask;
            unsafe {
                let slot = &mut *base_ptr.add(idx);
                if let Some(entry) = slot.as_mut() {
                    if entry.home == home && &entry.key == key {
                        return Some(&mut entry.value);
                    }
                }
            }
        }
        None
    }

    fn remove(&mut self, key: &K, home: usize) -> Option<V>
    where
        K: Eq,
    {
        for offset in 0..NEIGHBORHOOD {
            let idx = (home + offset) & self.mask;
            if let Some(entry) = self.slots[idx].as_mut() {
                if entry.home == home && &entry.key == key {
                    self.len -= 1;
                    return self.slots[idx].take().map(|e| e.value);
                }
            }
        }
        None
    }
}

pub struct HopscotchMap<K, V, S = RandomState> {
    table: RwLock<HopTable<K, V>>,
    hasher: S,
}

impl<K, V> HopscotchMap<K, V, RandomState>
where
    K: Eq + Hash,
    V: Clone,
{
    pub fn with_capacity_pow2(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, RandomState::default())
    }
}

impl<K, V, S> HopscotchMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Clone,
{
    pub fn with_capacity_and_hasher(cap: usize, hasher: S) -> Self {
        assert!(cap.is_power_of_two());
        Self {
            table: RwLock::new(HopTable::with_capacity_pow2(cap)),
            hasher,
        }
    }

    #[inline]
    fn index_for(&self, key: &K, mask: usize) -> usize {
        let mut state = self.hasher.clone().build_hasher();
        key.hash(&mut state);
        (state.finish() as usize) & mask
    }

    fn resize(&self, table: &mut HopTable<K, V>) {
        let mut new_cap = table.capacity() * 2;
        if new_cap < NEIGHBORHOOD * 2 {
            new_cap = NEIGHBORHOOD * 2;
        }
        if !new_cap.is_power_of_two() {
            new_cap = new_cap.next_power_of_two();
        }
        let mut new_table = HopTable::with_capacity_pow2(new_cap);
        for slot in table.slots.iter_mut() {
            if let Some(mut entry) = slot.take() {
                entry.home = self.index_for(&entry.key, new_table.mask);
                loop {
                    match new_table.insert_entry(entry) {
                        InsertOutcome::Placed(_) => break,
                        InsertOutcome::NeedsResize(rest) => {
                            let bigger = new_table.capacity() * 2;
                            new_table = HopTable::with_capacity_pow2(bigger);
                            entry = rest;
                        }
                    }
                }
            }
        }
        *table = new_table;
    }

    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Clone,
    {
        let mut table = self.table.write();
        let mut entry = Entry {
            home: self.index_for(&key, table.mask),
            key,
            value,
        };
        loop {
            match table.insert_entry(entry) {
                InsertOutcome::Placed(old) => return old,
                InsertOutcome::NeedsResize(rest) => {
                    self.resize(&mut table);
                    entry = Entry {
                        home: self.index_for(&rest.key, table.mask),
                        key: rest.key,
                        value: rest.value,
                    };
                }
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let table = self.table.read();
        let home = self.index_for(key, table.mask);
        table.get(key, home).cloned()
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut table = self.table.write();
        let home = self.index_for(key, table.mask);
        table.remove(key, home)
    }

    pub fn len(&self) -> usize {
        self.table.read().len
    }

    pub fn memory_usage(&self) -> usize {
        self.table.read().capacity() * (std::mem::size_of::<K>() + std::mem::size_of::<V>())
    }

    pub fn entries(&self) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let table = self.table.read();
        let mut out = Vec::with_capacity(table.len);
        for slot in table.slots.iter() {
            if let Some(entry) = slot.as_ref() {
                out.push((entry.key.clone(), entry.value.clone()));
            }
        }
        out
    }

    pub fn update_in<F>(&self, key: &K, f: F) -> bool
    where
        F: FnOnce(&mut V) -> bool,
    {
        let mut table = self.table.write();
        let home = self.index_for(key, table.mask);
        if let Some(val) = table.get_mut(key, home) {
            let remove = f(val);
            if remove {
                let _ = table.remove(key, home);
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let map = HopscotchMap::<u64, u64>::with_capacity_pow2(64);
        for i in 0..10_000 {
            assert_eq!(map.insert(i, i + 1), None);
        }
        for i in 0..10_000 {
            assert_eq!(map.get(&i), Some(i + 1));
        }
        for i in (0..10_000).step_by(3) {
            let old = map.insert(i, i + 5);
            if old != Some(i + 1) {
                panic!(
                    "unexpected old value for key {}: {:?} (expected {})",
                    i,
                    old,
                    i + 1
                );
            }
        }
        for i in (0..10_000).step_by(11) {
            let removed = map.remove(&i);
            let expected = if i % 3 == 0 { i + 5 } else { i + 1 };
            if removed != Some(expected) {
                panic!(
                    "unexpected removed value for key {}: {:?} (expected {})",
                    i, removed, expected
                );
            }
            assert_eq!(map.get(&i), None);
        }
    }

    #[test]
    fn high_collision_inserts() {
        let map = HopscotchMap::<u64, u64>::with_capacity_pow2(64);
        let base = 1u64 << 40;
        for i in 0..5_000 {
            let key = base + (i & 0xFF);
            map.insert(key, i);
        }
        for i in 0..5_000 {
            let key = base + (i & 0xFF);
            assert!(map.get(&key).is_some());
        }
    }

    #[test]
    fn update_in_mutates_value() {
        let map = HopscotchMap::<u64, Vec<u64>>::with_capacity_pow2(64);
        map.insert(7, vec![1, 2]);
        let existed = map.update_in(&7, |vals| {
            vals.push(3);
            false
        });
        assert!(existed);
        assert_eq!(map.get(&7), Some(vec![1, 2, 3]));
        let removed = map.update_in(&7, |vals| {
            vals.clear();
            true
        });
        assert!(removed);
        assert_eq!(map.get(&7), None);
    }
}
