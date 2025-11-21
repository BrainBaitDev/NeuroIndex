use crossbeam_epoch::{pin, Guard};
use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::mem::{size_of, ManuallyDrop, MaybeUninit};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

const BLOCK_SIZE: usize = 1024;
const INLINE_THRESHOLD: usize = 32;

pub struct Arena<V> {
    inner: Arc<Inner<V>>,
}

struct Inner<V> {
    freelist: SegQueue<usize>,
    blocks: Mutex<Vec<Box<[Slot<V>]>>>,
    next_version: AtomicU64,
}

struct Slot<V> {
    state: AtomicU8,
    record: UnsafeCell<MaybeUninit<Record<V>>>,
}

unsafe impl<V: Send + Sync> Send for Slot<V> {}
unsafe impl<V: Send + Sync> Sync for Slot<V> {}

impl<V> Slot<V> {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(0),
            record: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    unsafe fn write(&self, record: Record<V>) {
        debug_assert_eq!(self.state.load(Ordering::Acquire), 0);
        (*self.record.get()) = MaybeUninit::new(record);
        self.state.store(1, Ordering::Release);
    }

    unsafe fn record(&self) -> &Record<V> {
        debug_assert_eq!(self.state.load(Ordering::Acquire), 1);
        &*(*self.record.get()).as_ptr()
    }

    unsafe fn drop_record(&self) {
        if self.state.swap(0, Ordering::AcqRel) == 1 {
            (*self.record.get()).assume_init_drop();
        }
    }

    fn mark_retired(&self) {
        self.state.store(2, Ordering::Release);
    }
}

pub struct Handle<V> {
    ptr: NonNull<Slot<V>>,
    _marker: std::marker::PhantomData<V>,
}

unsafe impl<V: Send + Sync> Send for Handle<V> {}
unsafe impl<V: Send + Sync> Sync for Handle<V> {}

impl<V> Handle<V> {
    fn new(ptr: NonNull<Slot<V>>) -> Self {
        Self {
            ptr,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn addr(&self) -> usize {
        self.ptr.as_ptr() as usize
    }
}

impl<V> Copy for Handle<V> {}

impl<V> Clone for Handle<V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> PartialEq for Handle<V> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<V> Eq for Handle<V> {}

pub struct Record<V> {
    version: AtomicU64,
    tombstone: AtomicBool,
    value: ValueCell<V>,
}

impl<V> Record<V> {
    fn new(value: V, version: u64) -> Self {
        Self {
            version: AtomicU64::new(version),
            tombstone: AtomicBool::new(false),
            value: ValueCell::new(value),
        }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn set_version(&self, version: u64) {
        self.version.store(version, Ordering::Release);
    }

    pub fn is_tombstone(&self) -> bool {
        self.tombstone.load(Ordering::Acquire)
    }

    pub fn mark_tombstone(&self) {
        self.tombstone.store(true, Ordering::Release);
    }

    pub fn clone_value(&self) -> V
    where
        V: Clone,
    {
        self.value.clone_value()
    }
}

enum ValueCell<V> {
    Inline(ManuallyDrop<V>),
    Heap(Box<V>),
}

impl<V> ValueCell<V> {
    fn new(value: V) -> Self {
        if size_of::<V>() <= INLINE_THRESHOLD && size_of::<V>() != 0 {
            ValueCell::Inline(ManuallyDrop::new(value))
        } else {
            ValueCell::Heap(Box::new(value))
        }
    }

    fn clone_value(&self) -> V
    where
        V: Clone,
    {
        match self {
            ValueCell::Inline(v) => (**v).clone(),
            ValueCell::Heap(b) => (**b).clone(),
        }
    }
}

impl<V> Drop for ValueCell<V> {
    fn drop(&mut self) {
        unsafe {
            match self {
                ValueCell::Inline(v) => ManuallyDrop::drop(v),
                ValueCell::Heap(_) => {}
            }
        }
    }
}

impl<V> Arena<V>
where
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                freelist: SegQueue::new(),
                blocks: Mutex::new(Vec::new()),
                next_version: AtomicU64::new(1),
            }),
        }
    }

    pub fn pin(&self) -> Guard {
        pin()
    }

    pub fn allocate(&self, value: V) -> Handle<V> {
        let slot_ptr = self.acquire_slot();
        let version = self.inner.next_version.fetch_add(1, Ordering::Relaxed);
        let record = Record::new(value, version);
        unsafe {
            slot_ptr.as_ref().write(record);
        }
        Handle::new(slot_ptr)
    }

    pub fn clone_value(&self, handle: Handle<V>, _guard: &Guard) -> V {
        unsafe { handle.ptr.as_ref().record().clone_value() }
    }

    pub fn retire(&self, handle: Handle<V>, guard: &Guard) {
        let inner = Arc::clone(&self.inner);
        let slot_ptr = handle.ptr;
        unsafe {
            slot_ptr.as_ref().mark_retired();
        }
        let raw = slot_ptr.as_ptr() as usize;
        guard.defer(move || unsafe {
            let slot = &*(raw as *mut Slot<V>);
            slot.drop_record();
            inner.freelist.push(raw);
        });
    }

    fn acquire_slot(&self) -> NonNull<Slot<V>> {
        if let Some(raw) = self.inner.freelist.pop() {
            return unsafe { NonNull::new_unchecked(raw as *mut Slot<V>) };
        }
        self.grow();
        let raw = self
            .inner
            .freelist
            .pop()
            .expect("freelist must contain slots after grow");
        unsafe { NonNull::new_unchecked(raw as *mut Slot<V>) }
    }

    fn grow(&self) {
        let mut block: Vec<Slot<V>> = Vec::with_capacity(BLOCK_SIZE);
        block.resize_with(BLOCK_SIZE, Slot::new);
        let mut boxed = block.into_boxed_slice();
        for slot in boxed.iter_mut() {
            let ptr = slot as *mut Slot<V>;
            self.inner.freelist.push(ptr as usize);
        }
        self.inner.blocks.lock().push(boxed);
    }

    pub fn memory_usage(&self) -> usize {
        self.inner.blocks.lock().len() * BLOCK_SIZE * size_of::<Slot<V>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_allocate_clone_retire() {
        let arena = Arena::new();
        let guard = arena.pin();
        let handle = arena.allocate(42u64);
        assert_eq!(arena.clone_value(handle, &guard), 42);
        arena.retire(handle, &guard);
        guard.flush();
    }

    #[test]
    fn reuse_slot_after_retire() {
        let arena = Arena::new();
        {
            let guard = arena.pin();
            let handle = arena.allocate(10u32);
            assert_eq!(arena.clone_value(handle, &guard), 10);
            arena.retire(handle, &guard);
            guard.flush();
        }
        let guard = arena.pin();
        let handle2 = arena.allocate(20u32);
        assert_eq!(arena.clone_value(handle2, &guard), 20);
        arena.retire(handle2, &guard);
        guard.flush();
    }

    #[derive(Clone)]
    struct Large([u8; 64]);

    #[test]
    fn large_values_use_heap() {
        let arena = Arena::new();
        let guard = arena.pin();
        let payload = Large([7; 64]);
        let handle = arena.allocate(payload.clone());
        let got = arena.clone_value(handle, &guard);
        assert_eq!(got.0[0], 7);
        arena.retire(handle, &guard);
        guard.flush();
    }
}
