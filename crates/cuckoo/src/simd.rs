// SIMD optimizations for Cuckoo Hash bucket scanning
// Uses AVX2/SSE4.2 on x86_64 for parallel key comparison

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD-accelerated search in bucket for u64 keys
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn find_u64_avx2(keys: &[u64; 4], target: u64) -> Option<usize> {
    // Load 4 keys into AVX2 register (256-bit)
    let keys_vec = _mm256_loadu_si256(keys.as_ptr() as *const __m256i);

    // Broadcast target to all lanes
    let target_vec = _mm256_set1_epi64x(target as i64);

    // Compare for equality
    let cmp = _mm256_cmpeq_epi64(keys_vec, target_vec);

    // Extract mask
    let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));

    if mask != 0 {
        Some(mask.trailing_zeros() as usize)
    } else {
        None
    }
}

/// SIMD-accelerated search in bucket for u32 keys
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
pub unsafe fn find_u32_sse42(keys: &[u32; 4], target: u32) -> Option<usize> {
    // Load 4 keys into SSE register (128-bit)
    let keys_vec = _mm_loadu_si128(keys.as_ptr() as *const __m128i);

    // Broadcast target
    let target_vec = _mm_set1_epi32(target as i32);

    // Compare
    let cmp = _mm_cmpeq_epi32(keys_vec, target_vec);

    // Extract mask
    let mask = _mm_movemask_ps(_mm_castsi128_ps(cmp));

    if mask != 0 {
        Some(mask.trailing_zeros() as usize)
    } else {
        None
    }
}

/// Fallback scalar search
#[allow(dead_code)]
#[inline(always)]
pub fn find_scalar<K: Eq>(keys: &[Option<K>], target: &K) -> Option<usize> {
    keys.iter().position(|slot| {
        if let Some(k) = slot {
            k == target
        } else {
            false
        }
    })
}

/// Safe wrapper for SIMD u64 search with runtime detection
#[cfg(target_arch = "x86_64")]
pub fn find_u64_simd_safe(keys: &[u64; 4], target: u64) -> Option<usize> {
    if is_x86_feature_detected!("avx2") {
        unsafe { find_u64_avx2(keys, target) }
    } else {
        // Scalar fallback
        keys.iter().position(|&k| k == target)
    }
}

/// Safe wrapper for SIMD u32 search
#[cfg(target_arch = "x86_64")]
pub fn find_u32_simd_safe(keys: &[u32; 4], target: u32) -> Option<usize> {
    if is_x86_feature_detected!("sse4.2") {
        unsafe { find_u32_sse42(keys, target) }
    } else {
        keys.iter().position(|&k| k == target)
    }
}

/// Non-x86 platforms: scalar only
#[cfg(not(target_arch = "x86_64"))]
pub fn find_u64_simd_safe(keys: &[u64; 4], target: u64) -> Option<usize> {
    keys.iter().position(|&k| k == target)
}

#[cfg(not(target_arch = "x86_64"))]
pub fn find_u32_simd_safe(keys: &[u32; 4], target: u32) -> Option<usize> {
    keys.iter().position(|&k| k == target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_u64_found() {
        let keys = [10u64, 20, 30, 40];
        assert_eq!(find_u64_simd_safe(&keys, 20), Some(1));
        assert_eq!(find_u64_simd_safe(&keys, 40), Some(3));
    }

    #[test]
    fn test_simd_u64_not_found() {
        let keys = [10u64, 20, 30, 40];
        assert_eq!(find_u64_simd_safe(&keys, 99), None);
    }

    #[test]
    fn test_simd_u32() {
        let keys = [5u32, 15, 25, 35];
        assert_eq!(find_u32_simd_safe(&keys, 15), Some(1));
        assert_eq!(find_u32_simd_safe(&keys, 99), None);
    }
}
