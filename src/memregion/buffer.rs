use std::{marker::PhantomData, ops::Range, ptr::NonNull, sync::{Arc,atomic::AtomicUsize}};

use tracing::trace;

use crate::{
    lamellae::{CommSlice, Remote,Lamellae,CommProgress},
    memregion::{LamellarMemoryRegion, OneSidedMemoryRegion, SharedMemoryRegion},
    lamellar_team::{IntoLamellarTeam},
};

/// Trait implemented by types that can serve as the backing store for a [`LamellarBuffer`].
///
/// Implementors include [`Vec<T>`], [`OneSidedMemoryRegion<T>`][crate::memregion::OneSidedMemoryRegion],
/// [`SharedMemoryRegion<T>`][crate::memregion::SharedMemoryRegion], and internal
/// [`CommSlice<T>`][crate::lamellae::CommSlice].
pub trait AsLamellarBuffer<T: Remote>: Send + 'static {
    /// Returns a shared slice of the backing data.
    fn as_slice(&self) -> &[T];
    /// Returns a mutable slice of the backing data.
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

    // fn as_slice(&self) -> &[T] {
    //     self.data.as_slice()
    // }

    // fn as_mut_slice(&mut self) -> &mut [T] {
    //     self.data.as_mut_slice()
    // }
}

/// A reference-counted, possibly sub-sliced wrapper around a buffer used as the destination
/// for RDMA get operations.
///
/// `LamellarBuffer<T, B>` is the type returned by and passed into `get_into_buffer` /
/// `get_into_buffer_unmanaged` methods on arrays and memory regions. It tracks the number of
/// outstanding references so that the runtime can determine when the backing store is safe to
/// reclaim or inspect.
///
/// # Constructors
///
/// | Method | Backing store | Safety |
/// |--------|---------------|--------|
/// | [`from_vec`][Self::from_vec] | `Vec<T>` | safe — takes ownership |
/// | [`from_one_sided_memory_region`][Self::from_one_sided_memory_region] | [`OneSidedMemoryRegion<T>`][crate::memregion::OneSidedMemoryRegion] | `unsafe` |
/// | [`from_shared_memory_region`][Self::from_shared_memory_region] | [`SharedMemoryRegion<T>`][crate::memregion::SharedMemoryRegion] | `unsafe` |
///
/// # Examples
///```
/// use lamellar::memregion::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let num_pes = world.num_pes();
///
/// let mem_region: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(num_pes * 10);
/// unsafe {
///     for (i, elem) in mem_region.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
///         *elem = i;
///     }
///     let buf = LamellarBuffer::from_vec(vec![0usize; 10]);
///     mem_region.get_into_buffer(my_pe * 10, buf).block();
/// }
///```
pub struct LamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    data: NonNull<BufferInner<B>>,
    range: Range<usize>,
    lamellae: Arc<Lamellae>,
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

// impl<T: Remote> LamellarBuffer<T, LamellarMemoryRegion<T>> {
//     /// Unsafe because multiple handles to the same memory region can be created,
//     /// thus the caller must ensure no other references mutate the region while
//     /// the buffer exists.
//     pub(crate) unsafe fn from_lamellar_memory_region(
//         mem_region: LamellarMemoryRegion<T>,
//     ) -> Self {
//         let len = mem_region.len();
//         LamellarBuffer {
//             data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(mem_region))).into())
//                 .unwrap(),
//             range: 0..len,
//             _phantom: PhantomData,
//         }
//     }
// }

// impl<T: Remote> From<LamellarMemoryRegion<T>> for LamellarBuffer<T, LamellarMemoryRegion<T>> {
//     fn from(mem_region: LamellarMemoryRegion<T>) -> Self {
//         unsafe { LamellarBuffer::from_lamellar_memory_region(mem_region) }
//     }
// }

impl<T: Remote> LamellarBuffer<T, SharedMemoryRegion<T>> {
    /// Wraps a [`SharedMemoryRegion`] as a [`LamellarBuffer`] for use with RDMA get operations.
    ///
    /// # Safety
    /// Multiple [`LamellarBuffer`] handles to the same region can be created. The caller must
    /// ensure that no other reference mutates the memory region while this buffer exists.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let src: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(num_pes * 10);
    /// let dst: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * 10).block();
    /// unsafe {
    ///     for (i, elem) in src.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_shared_memory_region(dst);
    ///     src.get_into_buffer(0, buf).block();
    /// }
    ///```
    pub unsafe fn from_shared_memory_region(mem_region: SharedMemoryRegion<T>) -> Self {
        let len = mem_region.len();
        let lamellae = mem_region.lamellae();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(mem_region))).into())
                .unwrap(),
            range: 0..len,
            lamellae,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote> From<SharedMemoryRegion<T>> for LamellarBuffer<T, SharedMemoryRegion<T>> {
    fn from(mem_region: SharedMemoryRegion<T>) -> Self {
        unsafe { LamellarBuffer::from_shared_memory_region(mem_region) }
    }
}

impl<T: Remote> LamellarBuffer<T, OneSidedMemoryRegion<T>> {
    /// Wraps a [`OneSidedMemoryRegion`] as a [`LamellarBuffer`] for use with RDMA get operations.
    ///
    /// Using an RDMA-registered one-sided region as the destination avoids an extra copy compared
    /// to [`from_vec`][LamellarBuffer::<T, Vec<T>>::from_vec].
    ///
    /// # Safety
    /// Multiple [`LamellarBuffer`] handles to the same region can be created. The caller must
    /// ensure that no other reference mutates the memory region while this buffer exists.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let src: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(num_pes * 10);
    /// let dst: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(10);
    /// unsafe {
    ///     for (i, elem) in src.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_one_sided_memory_region(dst);
    ///     src.get_into_buffer(my_pe * 10, buf).block();
    /// }
    ///```
    pub unsafe fn from_one_sided_memory_region(mem_region: OneSidedMemoryRegion<T>) -> Self {
        let len = mem_region.len();
        let lamellae = mem_region.lamellae();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(mem_region))).into())
                .unwrap(),
            range: 0..len,
            lamellae,
            _phantom: PhantomData,
        }
    }
}

impl<T: Remote> From<OneSidedMemoryRegion<T>> for LamellarBuffer<T, OneSidedMemoryRegion<T>> {
    fn from(mem_region: OneSidedMemoryRegion<T>) -> Self {
        unsafe { LamellarBuffer::from_one_sided_memory_region(mem_region) }
    }
}

impl<T: Remote> LamellarBuffer<T, CommSlice<T>> {
    /// unsafe because multiple handles to the same memory region can be created,
    /// thus user must ensure that nothing else is mutating the memory region
    /// while this buffer exists
    pub(crate) unsafe fn from_comm_slice(comm_slice: CommSlice<T>,lamellae: Arc<Lamellae>) -> Self {
        let len = comm_slice.len();
        trace!(target: "lamellae_debug", "creating LamellarBuffer from CommSlice with len {:?} lamellae cnt: {:?}", len, Arc::strong_count(&lamellae));
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(comm_slice))).into())
                .unwrap(),
            range: 0..len,
            lamellae,
            _phantom: PhantomData,
        }
    }
}

// impl<T: Remote> From<CommSlice<T>> for LamellarBuffer<T, CommSlice<T>> {
//     fn from(comm_slice: CommSlice<T>) -> Self {
//         unsafe { LamellarBuffer::from_comm_slice(comm_slice) }
//     }
// }

impl<T: Remote> LamellarBuffer<T, Vec<T>> {
    /// Wraps a `Vec<T>` as a [`LamellarBuffer`] for use with RDMA get operations.
    ///
    /// This is the simplest way to create a destination buffer. The [`LamellarBuffer`] takes
    /// ownership of the `Vec`, so no additional safety requirements apply. Retrieve the
    /// `Vec` back with [`try_unwrap`][Self::try_unwrap] or [`async_unwrap`][Self::async_unwrap]
    /// once the transfer is complete.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(num_pes * 10);
    /// unsafe {
    ///     for (i, elem) in mem_region.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_vec(vec![0usize; 10]);
    ///     mem_region.get_into_buffer(my_pe * 10, buf).block();
    /// }
    ///```
    pub fn from_vec<U: Into<IntoLamellarTeam>>(team: U,vec: Vec<T>) -> Self {
        let len = vec.len();
        let lamellae = team.into().team.lamellae.clone();
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(vec))).into()).unwrap(),
            range: 0..len,
            lamellae,
            _phantom: PhantomData,
        }
    }
    pub(crate) fn from_vec_with_lamellae(vec: Vec<T>,lamellae: Arc<Lamellae>) -> Self {
        let len = vec.len();
        trace!(target: "lamellae_debug", "creating LamellarBuffer from Vec with len {:?} lamellae cnt: {:?}", len, Arc::strong_count(&lamellae));
        LamellarBuffer {
            data: NonNull::new(Box::into_raw(Box::new(BufferInner::new(vec))).into()).unwrap(),
            range: 0..len,
            lamellae,
            _phantom: PhantomData,
        }
    }
}

// impl<T: Remote> From<Vec<T>> for LamellarBuffer<T, Vec<T>> {
//     fn from(vec: Vec<T>) -> Self {
//         LamellarBuffer::from_vec(vec)
//     }
// }

impl<T: Remote, B: AsLamellarBuffer<T>> LamellarBuffer<T, B> {
    /// Returns the number of elements in the (possibly sub-sliced) buffer.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3]);
    /// assert_eq!(buf.len(), 3);
    ///```
    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    /// Returns `true` if the buffer contains no elements.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf: LamellarBuffer<usize, Vec<usize>> = LamellarBuffer::from_vec(&world, vec![]);
    /// assert!(buf.is_empty());
    ///```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Splits the buffer at `at`, consuming `self` and returning two sub-buffers that share
    /// the same backing store.
    ///
    /// Both halves must be driven to completion (or dropped) before the backing store is reclaimed.
    ///
    /// # Panics
    /// Panics if `at > self.len()`.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3, 4, 5]);
    /// let (left, right) = buf.split(2);
    /// assert_eq!(left.len(), 2);
    /// assert_eq!(right.len(), 3);
    ///```
    pub fn split(self, at: usize) -> (Self, Self) {
        unsafe {
            self.data
                .as_ref()
                .cnt
                .fetch_add(2, std::sync::atomic::Ordering::SeqCst)
        }; //+ 2 as we technically creating two new buffers and dropping this one
        assert!(at <= self.len());
        let left = LamellarBuffer {
            data: self.data.clone(),
            range: self.range.start..(self.range.start + at),
            lamellae: self.lamellae.clone(),
            _phantom: PhantomData,
        };
        let right = LamellarBuffer {
            data: self.data,
            range: (self.range.start + at)..self.range.end,
            lamellae: self.lamellae.clone(),
            _phantom: PhantomData,
        };
        trace!(target: "lamellae_debug", "split LamellarBuffer at {:?} into left range {:?} and right range {:?} lamellae cnt: {:?}", at, left.range, right.range, Arc::strong_count(&self.lamellae));
        (left, right)
    }

    /// Splits off the tail of this buffer starting at `at`, returning it as a new sub-buffer.
    /// `self` is truncated to `[0, at)` in-place.
    ///
    /// # Panics
    /// Panics if `at > self.len()`.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let mut buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3, 4, 5]);
    /// let tail = buf.split_off(3);
    /// assert_eq!(buf.len(), 3);
    /// assert_eq!(tail.len(), 2);
    ///```
    pub fn split_off(&mut self, at: usize) -> Self {
        unsafe {
            self.data
                .as_ref()
                .cnt
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        };
        assert!(at <= self.len());

        let right = LamellarBuffer {
            data: self.data,
            range: (self.range.start + at)..self.range.end,
            lamellae: self.lamellae.clone(),
            _phantom: PhantomData,
        };
        self.range = self.range.start..(self.range.start + at);
        trace!(target: "lamellae_debug", "split_off LamellarBuffer at {:?} into left range {:?} and right range {:?} lamellae cnt: {:?}", at, self.range, right.range, Arc::strong_count(&self.lamellae));
        right
    }

    /// Attempts to reclaim ownership of the backing store.
    ///
    /// Succeeds (returns `Ok(B)`) when this is the sole remaining reference; otherwise
    /// returns `Err(self)` so the caller can retry.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3]);
    /// match buf.try_unwrap() {
    ///     Ok(vec) => println!("Reclaimed Vec: {:?}", vec),
    ///     Err(buf) => println!("Other references exist"),
    /// }
    ///```
    pub fn try_unwrap(self) -> Result<B, Self> {
        if unsafe {
            self.data
                .as_ref()
                .cnt
                .load(std::sync::atomic::Ordering::SeqCst)
        } == 1
        {
            let mut this = std::mem::ManuallyDrop::new(self);
            let data = unsafe { Box::from_raw(this.data.as_ptr()) };
            // drop Arc<Lamellae> that ManuallyDrop suppresses
            unsafe { std::ptr::drop_in_place(&mut this.lamellae as *mut Arc<Lamellae>) };
            Ok(data.data)
        } else {
            Err(self)
        }
    }

    /// Asynchronously waits until this is the sole remaining reference, then reclaims the
    /// backing store.
    ///
    /// Yields via `async_std::task::yield_now` while other references exist.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// async fn example() {
    ///     let world = LamellarWorldBuilder::new().build();
    ///     let buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3]);
    ///     let vec = buf.async_unwrap().await;
    ///     println!("Unwrapped: {:?}", vec);
    /// }
    ///```
    pub async fn async_unwrap(self) -> B {
        while unsafe {
            self.data
                .as_ref()
                .cnt
                .load(std::sync::atomic::Ordering::SeqCst)
        } != 1
        {
            // println!("Waiting to unwrap LamellarBuffer: {:?}", self);
            trace!("Waiting to unwrap LamellarBuffer: {:?}", self);
            async_std::task::yield_now().await;
        }
        let mut this = std::mem::ManuallyDrop::new(self);
        let data = unsafe { Box::from_raw(this.data.as_ptr()) };
        trace!(target: "lamellae_debug", "successfully unwrapped LamellarBuffer, lamellae cnt: {:?}", Arc::strong_count(&this.lamellae));
        // drop Arc<Lamellae> that ManuallyDrop suppresses
        unsafe { std::ptr::drop_in_place(&mut this.lamellae as *mut Arc<Lamellae>) };
        data.data
    }

    /// Resets the active slice window back to the full backing buffer, if this is the sole
    /// remaining reference.
    ///
    /// Returns `true` on success; `false` if other references still exist.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let mut buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3, 4, 5]);
    /// let _tail = buf.split_off(2);
    /// // After split, window is [0, 2); try_reset returns false due to other reference
    /// assert_eq!(buf.try_reset(), false);
    ///```
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

    /// Returns a shared slice of the active window of the buffer.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3]);
    /// let slice = buf.as_slice();
    /// assert_eq!(slice.len(), 3);
    ///```
    pub fn as_slice(&self) -> &[T] {
        unsafe { &self.data.as_ref().data.as_slice()[self.range.clone()] }
    }

    /// Returns a mutable slice of the active window of the buffer.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let mut buf = LamellarBuffer::from_vec(&world, vec![1, 2, 3]);
    /// let slice = buf.as_mut_slice();
    /// slice[0] = 42;
    ///```
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { &mut self.data.as_mut().data.as_mut_slice()[self.range.clone()] }
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn orig_as_slice(&self) -> &[T] {
        unsafe { self.data.as_ref().data.as_slice() }
    }
    #[allow(dead_code)]
    pub(crate) unsafe fn orig_as_casted_slice<P>(&self) -> &[P] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ref().data.as_slice().as_ptr() as *const P,
                self.data.as_ref().data.as_slice().len() * std::mem::size_of::<T>()
                    / std::mem::size_of::<P>(),
            )
        }
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn orig_as_ptr(&self) -> *const T {
        unsafe { self.data.as_ref().data.as_slice().as_ptr() as *const T }
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn orig_as_casted_ptr<P>(&self) -> *const P {
        unsafe { self.data.as_ref().data.as_slice().as_ptr() as *const P }
    }

    #[allow(dead_code)]
    pub(crate) fn orig_num_bytes(&self) -> usize {
        unsafe { &self.data.as_ref().data.as_slice().len() * std::mem::size_of::<T>() }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Drop for LamellarBuffer<T, B> {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop LamellarBuffer");
        // println!("LamellarBuffer dropped: {:?}", self);
        if unsafe {
            self.data
                .as_ref()
                .cnt
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
        } == 1
        {
            // ensure all pending RDMA operations using this buffer are flushed before we drop the backing store
            self.lamellae.comm().flush_all();
            unsafe {
                let _ = Box::from_raw(self.data.as_ptr());
            }
        }
        trace!(target: "lamellae_debug", "end drop LamellarBuffer lamellae cnt: {:?}", Arc::strong_count(&self.lamellae));
    }
}
