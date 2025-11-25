// Optimized specializations for common key types
use super::*;
use crate::simd;

/// Optimized Cuckoo table for u64 keys with SIMD scanning
impl<V: Clone, S: BuildHasher + Clone + Default> CuckooTable<u64, V, S> {
    /// SIMD-optimized get for u64 keys
    pub fn get_u64_fast(&self, key: &u64) -> Option<V> {
        // 1. Check primary table
        let t = self.read_table();
        if let Some(v) = Self::get_u64_in_table(&t, key) {
            return Some(v);
        }

        // 2. Check old table if resizing
        if let Some(old) = self.read_old_table() {
            if let Some(v) = Self::get_u64_in_table(&old, key) {
                return Some(v);
            }
        }
        None
    }

    fn get_u64_in_table(t: &Table<u64, V, S>, key: &u64) -> Option<V> {
        let i1 = t.h1(key);
        let i2 = t.h2(key);

        // Prefetch second bucket
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::_mm_prefetch;
            const _MM_HINT_T0: i32 = 3;
            _mm_prefetch(
                (&t.buckets[i2] as *const Mutex<Bucket<u64, V>>) as *const i8,
                _MM_HINT_T0,
            );
        }

        // Check bucket 1 with SIMD
        {
            let b = t.buckets[i1].lock();

            // Extract keys for SIMD scan
            let mut keys = [0u64; 4];
            let mut vals: [Option<&Entry<u64, V>>; 4] = [None, None, None, None];

            for (idx, slot) in b.slots.iter().enumerate() {
                if let Some(e) = slot {
                    keys[idx] = e.key;
                    vals[idx] = Some(e);
                }
            }

            #[cfg(target_arch = "x86_64")]
            {
                if let Some(idx) = simd::find_u64_simd_safe(&keys, *key) {
                    if let Some(e) = vals[idx] {
                        return Some(e.val.clone());
                    }
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            {
                for (idx, &k) in keys.iter().enumerate() {
                    if k == *key {
                        if let Some(e) = vals[idx] {
                            return Some(e.val.clone());
                        }
                    }
                }
            }
        }

        // Check bucket 2 with SIMD
        {
            let b = t.buckets[i2].lock();

            let mut keys = [0u64; 4];
            let mut vals: [Option<&Entry<u64, V>>; 4] = [None, None, None, None];

            for (idx, slot) in b.slots.iter().enumerate() {
                if let Some(e) = slot {
                    keys[idx] = e.key;
                    vals[idx] = Some(e);
                }
            }

            #[cfg(target_arch = "x86_64")]
            {
                if let Some(idx) = simd::find_u64_simd_safe(&keys, *key) {
                    if let Some(e) = vals[idx] {
                        return Some(e.val.clone());
                    }
                }
            }

            #[cfg(not(target_arch = "x86_64"))]
            {
                for (idx, &k) in keys.iter().enumerate() {
                    if k == *key {
                        if let Some(e) = vals[idx] {
                            return Some(e.val.clone());
                        }
                    }
                }
            }
        }

        // Stash fallback
        let stash = t.stash.lock();
        for e in stash.iter() {
            if &e.key == key {
                return Some(e.val.clone());
            }
        }
        None
    }
}

/// Optimized implementation for u32 keys
impl<V: Clone, S: BuildHasher + Clone + Default> CuckooTable<u32, V, S> {
    pub fn get_u32_fast(&self, key: &u32) -> Option<V> {
        // 1. Check primary table
        let t = self.read_table();
        if let Some(v) = Self::get_u32_in_table(&t, key) {
            return Some(v);
        }

        // 2. Check old table if resizing
        if let Some(old) = self.read_old_table() {
            if let Some(v) = Self::get_u32_in_table(&old, key) {
                return Some(v);
            }
        }
        None
    }

    fn get_u32_in_table(t: &Table<u32, V, S>, key: &u32) -> Option<V> {
        let i1 = t.h1(key);
        let i2 = t.h2(key);

        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::_mm_prefetch;
            const _MM_HINT_T0: i32 = 3;
            _mm_prefetch(
                (&t.buckets[i2] as *const Mutex<Bucket<u32, V>>) as *const i8,
                _MM_HINT_T0,
            );
        }

        {
            let b = t.buckets[i1].lock();
            let mut keys = [0u32; 4];
            let mut vals: [Option<&Entry<u32, V>>; 4] = [None, None, None, None];

            for (idx, slot) in b.slots.iter().enumerate() {
                if let Some(e) = slot {
                    keys[idx] = e.key;
                    vals[idx] = Some(e);
                }
            }

            if let Some(idx) = simd::find_u32_simd_safe(&keys, *key) {
                if let Some(e) = vals[idx] {
                    return Some(e.val.clone());
                }
            }
        }

        {
            let b = t.buckets[i2].lock();
            let mut keys = [0u32; 4];
            let mut vals: [Option<&Entry<u32, V>>; 4] = [None, None, None, None];

            for (idx, slot) in b.slots.iter().enumerate() {
                if let Some(e) = slot {
                    keys[idx] = e.key;
                    vals[idx] = Some(e);
                }
            }

            if let Some(idx) = simd::find_u32_simd_safe(&keys, *key) {
                if let Some(e) = vals[idx] {
                    return Some(e.val.clone());
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
}
