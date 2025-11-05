// Performance utilities: cache-line alignment, padding, and memory layout optimization

use std::alloc::{alloc, dealloc, Layout};
use std::mem;
use std::ptr::NonNull;

pub mod tuning;

/// Standard cache line size for modern CPUs (x86-64, ARM)
pub const CACHE_LINE_SIZE: usize = 64;

/// L2 cache line size (some CPUs)
pub const CACHE_LINE_SIZE_L2: usize = 128;

/// Macro to define a cache-line aligned type
#[macro_export]
macro_rules! cache_aligned {
    ($name:ident, $inner:ty) => {
        #[repr(align(64))]
        pub struct $name(pub $inner);
    };
}

/// Cache-line aligned wrapper for any type
#[repr(align(64))]
#[derive(Debug, Clone, Copy)]
pub struct CacheAligned<T>(pub T);

impl<T> CacheAligned<T> {
    pub fn new(val: T) -> Self {
        CacheAligned(val)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Padding to prevent false sharing between adjacent cache lines
/// Note: Due to const generic limitations, we pad to a full cache line
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct Padded<T> {
    pub value: T,
}

impl<T> Padded<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> std::ops::Deref for Padded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Padded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// Allocate cache-line aligned memory
pub struct AlignedAlloc<T> {
    ptr: NonNull<T>,
    layout: Layout,
}

impl<T> AlignedAlloc<T> {
    pub fn new(value: T) -> Self {
        let layout = Layout::from_size_align(
            mem::size_of::<T>(),
            CACHE_LINE_SIZE.max(mem::align_of::<T>()),
        )
        .expect("invalid layout");

        unsafe {
            let ptr = alloc(layout) as *mut T;
            if ptr.is_null() {
                panic!("allocation failed");
            }
            ptr.write(value);
            Self {
                ptr: NonNull::new_unchecked(ptr),
                layout,
            }
        }
    }

    pub fn get(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }

    pub fn get_mut(&mut self) -> &mut T {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T> Drop for AlignedAlloc<T> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.ptr.as_ptr());
            dealloc(self.ptr.as_ptr() as *mut u8, self.layout);
        }
    }
}

unsafe impl<T: Send> Send for AlignedAlloc<T> {}
unsafe impl<T: Sync> Sync for AlignedAlloc<T> {}

/// Cache-line aligned vector for SIMD and cache-friendly sequential access
pub struct AlignedVec<T> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
}

impl<T> AlignedVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let layout = Layout::from_size_align(mem::size_of::<T>() * cap, CACHE_LINE_SIZE)
            .expect("invalid layout");

        unsafe {
            let ptr = alloc(layout) as *mut T;
            if ptr.is_null() {
                panic!("allocation failed");
            }
            Self {
                ptr: NonNull::new_unchecked(ptr),
                len: 0,
                cap,
            }
        }
    }

    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            panic!("AlignedVec capacity exceeded");
        }
        unsafe {
            self.ptr.as_ptr().add(self.len).write(value);
        }
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> Drop for AlignedVec<T> {
    fn drop(&mut self) {
        unsafe {
            for i in 0..self.len {
                std::ptr::drop_in_place(self.ptr.as_ptr().add(i));
            }
            let layout =
                Layout::from_size_align_unchecked(mem::size_of::<T>() * self.cap, CACHE_LINE_SIZE);
            dealloc(self.ptr.as_ptr() as *mut u8, layout);
        }
    }
}

unsafe impl<T: Send> Send for AlignedVec<T> {}
unsafe impl<T: Sync> Sync for AlignedVec<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_aligned() {
        let val = CacheAligned::new(42u64);
        assert_eq!(*val, 42);
        assert_eq!(mem::align_of_val(&val), CACHE_LINE_SIZE);
    }

    #[test]
    fn test_padded() {
        let val = Padded::new(100u32);
        assert_eq!(*val, 100);
        // With align(64), the struct is aligned to cache line but may not be padded in size
        assert_eq!(mem::align_of_val(&val), CACHE_LINE_SIZE);
    }

    #[test]
    fn test_aligned_alloc() {
        let mut alloc = AlignedAlloc::new(999u64);
        assert_eq!(*alloc.get(), 999);
        *alloc.get_mut() = 1000;
        assert_eq!(*alloc.get(), 1000);
    }

    #[test]
    fn test_aligned_vec() {
        let mut vec = AlignedVec::with_capacity(10);
        vec.push(1u64);
        vec.push(2);
        vec.push(3);
        assert_eq!(vec.len(), 3);
        assert_eq!(vec.as_slice(), &[1, 2, 3]);
        // Check alignment
        assert_eq!((vec.as_slice().as_ptr() as usize) % CACHE_LINE_SIZE, 0);
    }
}
