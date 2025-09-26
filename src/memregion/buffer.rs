use std::{marker::PhantomData, ops::Range, ptr::NonNull, sync::atomic::AtomicUsize};

use tracing::trace;

use crate::{
    lamellae::{CommSlice, Remote},
    memregion::{
        LamellarMemoryRegion, OneSidedMemoryRegion, RegisteredMemoryRegion, SharedMemoryRegion,
    },
};

pub trait AsLamellarBuffer<T: Remote>: Send + 'static {
    fn as_slice(&self) -> &[T];
    fn as_mut_slice(&mut self) -> &mut [T];
}

impl<T: Remote> AsLamellarBuffer<T> for Vec<T> {
    fn as_slice(&self) -> &[T] {
        self.as_slice()
    }
    fn as_mut_slice(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}
impl<T: Remote> AsLamellarBuffer<T> for LamellarMemoryRegion<T> {
    fn as_slice(&self) -> &[T] {
        unsafe { self.as_slice() }
    }
    fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { LamellarMemoryRegion::as_mut_slice(self) }
    }
}
impl<T: Remote> AsLamellarBuffer<T> for SharedMemoryRegion<T> {
    fn as_slice(&self) -> &[T] {
        unsafe { self.as_slice() }
    }
    fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { SharedMemoryRegion::as_mut_slice(self) }
    }
}
impl<T: Remote> AsLamellarBuffer<T> for OneSidedMemoryRegion<T> {
    fn as_slice(&self) -> &[T] {
        unsafe { self.as_slice() }
    }
    fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { OneSidedMemoryRegion::as_mut_slice(self) }
    }
}

impl<T: Remote> AsLamellarBuffer<T> for CommSlice<T> {
    fn as_slice(&self) -> &[T] {
        self.as_slice()
    }
    fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { self.as_mut_slice() }
    }
}

struct BufferInner<T> {
    cnt: AtomicUsize,
    data: T,
}

unsafe impl<T: Send> Send for BufferInner<T> {}
unsafe impl<T: Sync> Sync for BufferInner<T> {}

impl<T> BufferInner<T> {
    fn new(data: T) -> Self {
        BufferInner {
            cnt: AtomicUsize::new(1),
            data,
        }
    }
}

pub struct LamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    data: NonNull<BufferInner<B>>,
    range: Range<usize>,
    _phantom: PhantomData<T>,
}
unsafe impl<T: Remote, B: AsLamellarBuffer<T>> Send for LamellarBuffer<T, B> {}
unsafe impl<T: Remote, B: AsLamellarBuffer<T>> Sync for LamellarBuffer<T, B> {}

impl<T: Remote, B: AsLamellarBuffer<T>> std::fmt::Debug for LamellarBuffer<T, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LamellarBuffer(range={:?}, current cnt={:?})",
            self.range,
            unsafe {
                self.data
                    .as_ref()
                    .cnt
                    .load(std::sync::atomic::Ordering::SeqCst)
            }
        )
    }
}

impl<T: Remote> LamellarBuffer<T, LamellarMemoryRegion<T>> {
    pub(crate) unsafe fn from_lamellar_memory_region(mem_region: LamellarMemoryRegion<T>) -> Self {
        let len = mem_region.len();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(mem_region))).into())
                .unwrap(),
            range: 0..len,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote> LamellarBuffer<T, SharedMemoryRegion<T>> {
    /// unsafe because multiple handles to the same memory region can be created,
    /// thus user must ensure that nothing else is mutating the memory region
    /// while this buffer exists
    pub unsafe fn from_shared_memory_region(mem_region: SharedMemoryRegion<T>) -> Self {
        let len = mem_region.len();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(mem_region))).into())
                .unwrap(),
            range: 0..len,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote> LamellarBuffer<T, OneSidedMemoryRegion<T>> {
    /// unsafe because multiple handles to the same memory region can be created,
    /// thus user must ensure that nothing else is mutating the memory region
    /// while this buffer exists
    pub unsafe fn from_one_sided_memory_region(mem_region: OneSidedMemoryRegion<T>) -> Self {
        let len = mem_region.len();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(mem_region))).into())
                .unwrap(),
            range: 0..len,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote> LamellarBuffer<T, CommSlice<T>> {
    /// unsafe because multiple handles to the same memory region can be created,
    /// thus user must ensure that nothing else is mutating the memory region
    /// while this buffer exists
    pub(crate) unsafe fn from_comm_slice(comm_slice: CommSlice<T>) -> Self {
        let len = comm_slice.len();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(comm_slice))).into())
                .unwrap(),
            range: 0..len,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote> LamellarBuffer<T, Vec<T>> {
    /// safe because the buffer takes ownership of the vec
    pub fn from_vec(vec: Vec<T>) -> Self {
        let len = vec.len();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(vec))).into()).unwrap(),
            range: 0..len,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LamellarBuffer<T, B> {
    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn split(self, at: usize) -> (Self, Self) {
        unsafe {
            self.data
                .as_ref()
                .cnt
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        };
        assert!(at <= self.len());
        let left = LamellarBuffer {
            data: self.data.clone(),
            range: self.range.start..(self.range.start + at),
            _phantom: PhantomData,
        };
        let right = LamellarBuffer {
            data: self.data,
            range: (self.range.start + at)..self.range.end,
            _phantom: PhantomData,
        };
        (left, right)
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        unsafe {
            self.data
                .as_ref()
                .cnt
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        };
        assert!(at <= self.len());
        self.range = self.range.start..(self.range.start + at);

        let right = LamellarBuffer {
            data: self.data,
            range: (self.range.start + at)..self.range.end,
            _phantom: PhantomData,
        };
        right
    }

    pub fn try_unwrap(self) -> Result<B, Self> {
        if unsafe {
            self.data
                .as_ref()
                .cnt
                .load(std::sync::atomic::Ordering::SeqCst)
        } == 1
        {
            let data = unsafe { Box::from_raw(self.data.as_ptr()) };
            Ok(data.data)
        } else {
            Err(self)
        }
    }

    pub fn try_reset(&mut self) -> bool {
        if unsafe {
            self.data
                .as_ref()
                .cnt
                .load(std::sync::atomic::Ordering::SeqCst)
        } == 1
        {
            let len = self.as_slice().len();
            self.range = 0..len;
            true
        } else {
            false
        }
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { &self.data.as_ref().data.as_slice()[self.range.clone()] }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { &mut self.data.as_mut().data.as_mut_slice()[self.range.clone()] }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Drop for LamellarBuffer<T, B> {
    fn drop(&mut self) {
        if unsafe {
            self.data
                .as_ref()
                .cnt
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
        } == 1
        {
            trace!("Dropping LamellarBuffer: {:?}", self);
            unsafe {
                let _ = Box::from_raw(self.data.as_ptr());
            }
        }
    }
}
