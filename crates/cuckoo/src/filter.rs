use ahash::RandomState;
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;

const BUCKET_SIZE: usize = 4; // slots per bucket
const MAX_KICKS: usize = 500;

type Fingerprint = u16;

struct Bucket {
    slots: [Option<Fingerprint>; BUCKET_SIZE],
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            slots: [None; BUCKET_SIZE],
        }
    }
}

pub struct CuckooFilter {
    buckets: Vec<Bucket>,
    hasher: RandomState,
    len: usize,
    capacity: usize,
}

impl CuckooFilter {
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let num_buckets = (capacity / BUCKET_SIZE).max(1);
        Self {
            buckets: (0..num_buckets).map(|_| Default::default()).collect(),
            hasher: RandomState::new(),
            len: 0,
            capacity,
        }
    }

    fn hash<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut h1 = self.hasher.build_hasher();
        let mut h2 = DefaultHasher::new();
        item.hash(&mut h1);
        item.hash(&mut h2);
        (h1.finish(), h2.finish())
    }

    fn fingerprint_and_indices<T: Hash>(&self, item: &T) -> (Fingerprint, usize, usize) {
        let (hash1, hash2) = self.hash(item);
        let fp = (hash2 & 0xFFFF) as Fingerprint;
        let i1 = hash1 as usize % self.buckets.len();
        let i2 = (i1 ^ (hash2 as usize)) % self.buckets.len();
        (fp, i1, i2)
    }

    pub fn insert<T: Hash>(&mut self, item: &T) -> bool {
        if self.contains(item) {
            return true;
        }
        if self.len >= self.capacity {
            return false; // Filter is full
        }

        let (mut fp, i1, i2) = self.fingerprint_and_indices(item);

        if self.try_insert(i1, fp) || self.try_insert(i2, fp) {
            self.len += 1;
            return true;
        }

        // If both buckets are full, we need to kick an item
        let mut current_index = i1;
        for _ in 0..MAX_KICKS {
            fp = self.swap(current_index, fp);

            // Calculate alternate index for the kicked fingerprint
            let (_, hash2) = self.hash(&fp);
            current_index = (current_index ^ (hash2 as usize)) % self.buckets.len();

            if self.try_insert(current_index, fp) {
                self.len += 1;
                return true;
            }
        }

        false // Filter is full, needs resize or a stash
    }

    fn try_insert(&mut self, index: usize, fp: Fingerprint) -> bool {
        let bucket = &mut self.buckets[index];
        for slot in &mut bucket.slots {
            if slot.is_none() {
                *slot = Some(fp);
                return true;
            }
        }
        false
    }

    fn swap(&mut self, index: usize, fp: Fingerprint) -> Fingerprint {
        // For simplicity, always kick the first slot. A real implementation might choose randomly.
        let bucket = &mut self.buckets[index];
        bucket.slots[0].replace(fp).unwrap_or(fp)
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let (fp, i1, i2) = self.fingerprint_and_indices(item);

        let b1 = &self.buckets[i1];
        for &slot in &b1.slots {
            if slot == Some(fp) {
                return true;
            }
        }

        let b2 = &self.buckets[i2];
        for &slot in &b2.slots {
            if slot == Some(fp) {
                return true;
            }
        }

        false
    }

    pub fn delete<T: Hash>(&mut self, item: &T) -> bool {
        let (fp, i1, i2) = self.fingerprint_and_indices(item);

        if self.try_delete(i1, fp) || self.try_delete(i2, fp) {
            self.len -= 1;
            return true;
        }
        false
    }

    fn try_delete(&mut self, index: usize, fp: Fingerprint) -> bool {
        let bucket = &mut self.buckets[index];
        for slot in &mut bucket.slots {
            if *slot == Some(fp) {
                *slot = None;
                return true;
            }
        }
        false
    }
}
