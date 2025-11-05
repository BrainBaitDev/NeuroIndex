// Software prefetching for T-Tree range scans
// Reduces cache miss latency by prefetching next nodes during traversal

/// Prefetch hint levels (as constants for _mm_prefetch)
#[cfg(target_arch = "x86_64")]
pub mod hint {
    use std::arch::x86_64::{_MM_HINT_NTA, _MM_HINT_T0, _MM_HINT_T1, _MM_HINT_T2};

    /// Temporal data; fetch into all cache levels
    pub const T0: i32 = _MM_HINT_T0;
    /// Temporal data; fetch into L2 and L3
    #[allow(dead_code)]
    pub const T1: i32 = _MM_HINT_T1;
    /// Temporal data; fetch into L3 only
    #[allow(dead_code)]
    pub const T2: i32 = _MM_HINT_T2;
    /// Non-temporal data; minimize cache pollution
    #[allow(dead_code)]
    pub const NTA: i32 = _MM_HINT_NTA;
}

/// Prefetch a memory address into cache with T0 hint
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub unsafe fn prefetch<T>(ptr: *const T, _hint: i32) {
    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
    // Note: _mm_prefetch requires a compile-time constant hint
    // We default to T0 for now; use specific prefetch functions for other hints
    _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
}

/// No-op prefetch for non-x86 platforms
#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub unsafe fn prefetch<T>(_ptr: *const T, _hint: i32) {
    // No-op
}

/// Prefetch a boxed node
#[inline(always)]
pub fn prefetch_node<T>(node_opt: &Option<Box<T>>, hint: i32) {
    if let Some(node) = node_opt {
        unsafe {
            prefetch(node.as_ref() as *const T, hint);
        }
    }
}

/// Prefetch both children of a node for sequential scan
#[inline(always)]
pub fn prefetch_children<T>(left: &Option<Box<T>>, right: &Option<Box<T>>, hint: i32) {
    prefetch_node(left, hint);
    prefetch_node(right, hint);
}

/// Prefetch with specific hint using macros for compile-time constants
#[cfg(target_arch = "x86_64")]
macro_rules! prefetch_with_hint {
    ($ptr:expr, T0) => {{
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch($ptr as *const i8, _MM_HINT_T0)
    }};
    ($ptr:expr, T1) => {{
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T1};
        _mm_prefetch($ptr as *const i8, _MM_HINT_T1)
    }};
    ($ptr:expr, T2) => {{
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T2};
        _mm_prefetch($ptr as *const i8, _MM_HINT_T2)
    }};
    ($ptr:expr, NTA) => {{
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_NTA};
        _mm_prefetch($ptr as *const i8, _MM_HINT_NTA)
    }};
}

/// Multi-level prefetch: prefetch N levels deep
/// Useful for deep range scans
#[allow(dead_code)]
#[cfg(target_arch = "x86_64")]
pub fn prefetch_tree_depth<K, V>(node: &Option<Box<super::Node<K, V>>>, depth: usize) {
    if depth == 0 || node.is_none() {
        return;
    }

    if let Some(n) = node {
        // Prefetch current node data with T0
        unsafe {
            prefetch_with_hint!(n.as_ref() as *const super::Node<K, V>, T0);
        }

        // Recursively prefetch children
        if depth > 1 {
            prefetch_tree_depth(&n.left, depth - 1);
            prefetch_tree_depth(&n.right, depth - 1);
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn prefetch_tree_depth<K, V>(_node: &Option<Box<super::Node<K, V>>>, _depth: usize) {
    // No-op on non-x86
}
