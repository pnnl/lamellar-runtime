use std::sync::Arc;

use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        CommAllocAddr, CommAllocInner, CommAllocRdma, RdmaGetBufferHandle, RdmaGetHandle,
        RdmaGetIntoBufferHandle,
    },
    memregion::MemregionRdmaInputInner,
    scheduler::Scheduler,
    AsLamellarBuffer, LamellarBuffer, RdmaHandle, Remote,
};

#[derive(Debug, Clone)]
pub(crate) struct CommSlice<T> {
    pub(crate) inner_alloc: CommAllocInner,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

// unsafe impl<T> Send for CommSlice<T> {}
// unsafe impl<T> Sync for CommSlice<T> {}

impl<T> CommSlice<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
    pub(crate) unsafe fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
    pub(crate) unsafe fn as_casted_slice<R>(&self) -> Option<&[R]> {
        let len = self.len() * std::mem::size_of::<T>() / std::mem::size_of::<R>();
        if len * std::mem::size_of::<R>() != self.len() * std::mem::size_of::<T>() {
            return None; // size mismatch
        }
        let ptr = self.as_mut_ptr() as *mut R;
        Some(std::slice::from_raw_parts_mut(ptr, len))
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn sub_slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&index) => index,
            std::ops::Bound::Excluded(&index) => index + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&index) => index + 1,
            std::ops::Bound::Excluded(&index) => index,
            std::ops::Bound::Unbounded => self.len(),
        };
        assert!(start <= end);
        assert!(end <= self.len());
        trace!(
            "subslice start: {} end: {} new size: {} ({} {})",
            start,
            end,
            end - start,
            start * std::mem::size_of::<T>(),
            (end - start) * std::mem::size_of::<T>()
        );
        CommSlice {
            inner_alloc: self.inner_alloc.sub_alloc(
                start * std::mem::size_of::<T>(),
                (end - start) * std::mem::size_of::<T>(),
            ),
            _phantom: std::marker::PhantomData,
        }
    }
    pub(crate) fn as_mut_ptr(&self) -> *mut T {
        unsafe { self.inner_alloc.addr().as_mut_ptr() }
    }
    pub(crate) fn as_ptr(&self) -> *const T {
        unsafe { self.inner_alloc.addr().as_ptr() }
    }
    pub(crate) fn len(&self) -> usize {
        self.inner_alloc.size() / std::mem::size_of::<T>()
    }

    pub(crate) fn usize_addr(&self) -> usize {
        self.inner_alloc.addr().into()
    }

    pub(crate) fn index_addr(&self, index: usize) -> CommAllocAddr {
        assert!(index < self.len());
        self.inner_alloc.addr() + index * std::mem::size_of::<T>()
    }

    pub(crate) unsafe fn from_raw_parts(data: *const T, len: usize) -> Self {
        CommSlice {
            inner_alloc: CommAllocInner::Raw(data as usize, len * std::mem::size_of::<T>()),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        self.inner_alloc.contains(&addr)
    }

    pub(crate) fn wait(&self) {
        self.inner_alloc.wait()
    }

    // pub(crate) fn num_bytes(&self) -> usize {
    //     self.info.size() * std::mem::size_of::<T>()
    // }
}

impl<T> CommAllocRdma for CommSlice<T> {
    fn put<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: U,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put: CommSlice<{:?}> vs put<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.put(scheduler, counters, src, pe, offset)
    }
    fn put_blocking<U: Remote>(&self, src: U, pe: usize, offset: usize) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_blocking: CommSlice<{:?}> vs put_blocking<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.put_blocking(src, pe, offset)
    }
    fn put_unmanaged<U: Remote>(&self, src: U, pe: usize, offset: usize) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_unmanaged: CommSlice<{:?}> vs put_unmanaged<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        trace!("put_unmanaged called on CommSlice {:?}", self.as_ptr(),);
        self.inner_alloc.put_unmanaged(src, pe, offset)
    }
    fn put_buffer<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<U>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_buffer: CommSlice<{:?}> vs put_buffer<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc
            .put_buffer(scheduler, counters, src, pe, offset)
    }
    fn put_buffer_unmanaged<U: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<U>>,
        pe: usize,
        offset: usize,
    ) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_buffer_unmanaged: CommSlice<{:?}> vs put_buffer_unmanaged<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.put_buffer_unmanaged(src, pe, offset)
    }

    fn put_all<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: U,
        offset: usize,
    ) -> RdmaHandle<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_all: CommSlice<{:?}> vs put_all<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.put_all(scheduler, counters, src, offset)
    }
    fn put_all_unmanaged<U: Remote>(&self, src: U, offset: usize) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_all_unmanaged: CommSlice<{:?}> vs put_all_unmanaged<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.put_all_unmanaged(src, offset)
    }
    fn put_all_buffer<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<U>>,
        offset: usize,
    ) -> RdmaHandle<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_all_buffer: CommSlice<{:?}> vs put_all_buffer<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc
            .put_all_buffer(scheduler, counters, src, offset)
    }
    fn put_all_buffer_unmanaged<U: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<U>>,
        offset: usize,
    ) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in put_all_buffer_unmanaged: CommSlice<{:?}> vs put_all_buffer_unmanaged<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.put_all_buffer_unmanaged(src, offset)
    }
    fn get<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get: CommSlice<{:?}> vs get<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.get(scheduler, counters, pe, offset)
    }
    fn blocking_get<U: Remote>(&self, pe: usize, offset: usize) -> U {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get: CommSlice<{:?}> vs get<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.blocking_get(pe, offset)
    }
    fn get_buffer<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get_buffer: CommSlice<{:?}> vs get_buffer<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc
            .get_buffer(scheduler, counters, pe, offset, len)
    }
    fn blocking_get_buffer<U: Remote>(&self, pe: usize, offset: usize, len: usize) -> Vec<U> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get_buffer: CommSlice<{:?}> vs get_buffer<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.blocking_get_buffer(pe, offset, len)
    }
    fn get_into_buffer<U: Remote, B: AsLamellarBuffer<U>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<U, B>,
    ) -> RdmaGetIntoBufferHandle<U, B> {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get_into_buffer: CommSlice<{:?}> vs get_into_buffer<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc
            .get_into_buffer(scheduler, counters, pe, offset, dst)
    }
    fn blocking_get_into_buffer<U: Remote, B: AsLamellarBuffer<U>>(
        &self,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<U, B>,
    ) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get_into_buffer: CommSlice<{:?}> vs get_into_buffer<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.blocking_get_into_buffer(pe, offset, dst)
    }
    fn get_into_buffer_unmanaged<U: Remote, B: AsLamellarBuffer<U>>(
        &self,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<U, B>,
    ) {
        if std::any::type_name::<T>() != std::any::type_name::<U>() {
            println!(
                "Type mismatch in get_into_buffer_unmanaged: CommSlice<{:?}> vs get_into_buffer_unmanaged<{:?}>",
                std::any::type_name::<T>(),
                std::any::type_name::<U>()
            );
        }
        self.inner_alloc.get_into_buffer_unmanaged(pe, offset, dst)
    }
}

impl<T> std::ops::Deref for CommSlice<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

impl<T> std::ops::DerefMut for CommSlice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
}
