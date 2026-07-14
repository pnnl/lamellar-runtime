use crate::array::generic_atomic::*;
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::rdma::private::{LamellarRdmaGet, LamellarRdmaPut, Sealed};
use crate::array::*;
use crate::memregion::{
    AsLamellarBuffer, Dist, LamellarBuffer, MemregionRdmaInput, MemregionRdmaInputInner,
    RemoteMemoryRegion,
};
use futures_util::future::join_all;
use parking_lot::Mutex;

impl<T: Dist> GenericAtomicArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Performs an atomic put of a single element into the array at the given global `index`.
    ///
    /// Uses an Active Message to the target PE to atomically store the element under a
    /// per-element lock. The element is written to whichever PE owns that index according to
    /// the array's [`Distribution`].
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes, Distribution::Block).block();
    ///
    /// // PE 0 atomically writes its PE index into every element
    /// if my_pe == 0 {
    ///     for i in 0..array.len() {
    ///         array.put(i, my_pe).block();
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget atomic put of a single element at the given global `index`.
    ///
    /// No completion handle is returned; use [`GenericAtomicArray::wait_all`] or a barrier to
    /// ensure the transfer is complete before accessing the destination.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes, Distribution::Block).block();
    ///
    /// if my_pe == 0 {
    ///     for i in 0..array.len() {
    ///         array.put_unmanaged(i, my_pe);
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a put of a buffer into the array starting at global `index`.
    ///
    /// Each element is written atomically under a per-element lock on the target PE, but the
    /// buffer as a whole is not written atomically. The runtime distributes writes across PEs
    /// according to the array's [`Distribution`].
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic — concurrent readers may observe
    /// a partially-written state. The index range `index..index+buf.len()` must be within bounds
    /// of the global array.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(10);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     unsafe { array.put_buffer(0, &src).block(); }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub unsafe fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into(), Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget put of a buffer starting at global `index`.
    ///
    /// No completion handle is returned; use [`GenericAtomicArray::wait_all`] or a barrier to
    /// ensure all transfers complete.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. The index range
    /// `index..index+buf.len()` must be within bounds of the global array.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(10);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     unsafe { array.put_buffer_unmanaged(0, &src); }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into(), Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs an atomic put of a single element directly to PE `pe` at `offset` within that
    /// PE's local segment of the array.
    ///
    /// Uses an Active Message to atomically store the element under a per-element lock on the
    /// target PE. Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// // PE 0 atomically writes 42 into PE 1's local offset 0
    /// if my_pe == 0 && num_pes > 1 {
    ///     array.put_pe(1, 0, 42).block();
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe(self, pe, offset, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget atomic put of a single element directly to PE `pe` at `offset`.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// if my_pe == 0 && num_pes > 1 {
    ///     array.put_pe_unmanaged(1, 0, 42);
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe_unmanaged(self, pe, offset, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a put of a buffer directly to PE `pe` starting at `offset` within that PE's
    /// local segment of the array.
    ///
    /// Each element is written atomically, but the buffer as a whole is not collectively atomic.
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+buf.len()` must be within bounds of that PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 && num_pes > 1 {
    ///     unsafe { array.put_pe_buffer(1, 0, &src).block(); }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub unsafe fn put_pe_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_pe_buffer(self, pe, offset, buf.into(), Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget put of a buffer directly to PE `pe` at `offset`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+buf.len()` must be within bounds of that PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 && num_pes > 1 {
    ///     unsafe { array.put_pe_buffer_unmanaged(1, 0, &src); }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(self, pe, offset, buf.into(), Sealed);
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts an atomic put of a single element to all PEs at `offset` within each PE's
    /// local segment of the array.
    ///
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// // PE 0 atomically writes 42 into offset 0 on every PE's local segment
    /// if my_pe == 0 {
    ///     array.put_all(0, 42).block();
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all(self, offset, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fire-and-forget broadcast of an atomic put of a single element to all PEs at `offset`.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// if my_pe == 0 {
    ///     array.put_all_unmanaged(0, 42);
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_all_unmanaged(&self, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a buffer to all PEs starting at `offset` within each PE's local segment of the array.
    ///
    /// Each element is written atomically, but the buffer as a whole is not collectively atomic.
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `offset..offset+buf.len()` must
    /// be within bounds of every PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     unsafe { array.put_all_buffer(0, &src).block(); }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub unsafe fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into(), Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fire-and-forget broadcast of a buffer to all PEs starting at `offset`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `offset..offset+buf.len()` must
    /// be within bounds of every PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     unsafe { array.put_all_buffer_unmanaged(0, &src); }
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into(), Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs an atomic get of a single element at the given global `index`.
    ///
    /// Uses an Active Message to atomically read the element under a per-element lock on the
    /// target PE. Returns an [`ArrayRdmaGetHandle`] that resolves to `T` when `spawn()`ed or
    /// `block()`ed.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// // Every PE atomically reads the element owned by PE 0
    /// let val = array.get(0).block();
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    pub fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get(self, index, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs an atomic get of a single element at the given global `index`,
    /// blocking the calling thread until the transfer completes.
    ///
    /// Uses an Active Message to atomically read the element under a per-element lock on the
    /// target PE.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let val = array.blocking_get(0);
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    pub fn blocking_get(&self, index: usize) -> T {
        unsafe { <Self as LamellarRdmaGet<T>>::blocking_get(self, index, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of `num_elems` elements starting at global `index`.
    ///
    /// Each element is read atomically, but the snapshot as a whole is not collectively atomic.
    /// The runtime handles cross-PE distribution automatically. Returns an
    /// [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic — the snapshot may contain
    /// elements read at different points in time. The range `index..index+num_elems` must be
    /// within bounds of the global array.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = unsafe { array.get_buffer(0, 10).block() };
    /// println!("PE{my_pe} first 10 elements: {:?}", data);
    ///```
    pub unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of `num_elems` elements starting at global `index`, blocking
    /// the calling thread until the transfer completes.
    ///
    /// Each element is read atomically, but the snapshot as a whole is not collectively atomic.
    /// The runtime handles cross-PE distribution automatically.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic — the snapshot may contain
    /// elements read at different points in time. The range `index..index+num_elems` must be
    /// within bounds of the global array.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = unsafe { array.blocking_get_buffer(0, 10) };
    /// println!("PE{my_pe} first 10 elements: {:?}", data);
    ///```
    pub unsafe fn blocking_get_buffer(&self, index: usize, num_elems: usize) -> Vec<T> {
        <Self as LamellarRdmaGet<T>>::blocking_get_buffer(self, index, num_elems, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of elements starting at global `index` into the provided pre-allocated
    /// [`LamellarBuffer`].
    ///
    /// Each element is read atomically, but the snapshot as a whole is not collectively atomic.
    /// The number of elements transferred equals `data.len()`. Returns an
    /// [`ArrayRdmaGetIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    /// Use [`LamellarBuffer::from_vec`] to wrap an owned `Vec` as the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. The range
    /// `index..index+data.len()` must be within bounds of the global array and `data` must be
    /// large enough to hold all results.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 10];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// let buf = unsafe { array.get_into_buffer(0, buf).block() };
    /// let result = buf.try_unwrap().expect("no other references exist");
    /// println!("PE{my_pe} first 10 elements: {:?}", result);
    ///```
    pub unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of elements starting at global `index` into the provided
    /// pre-allocated [`LamellarBuffer`], blocking the calling thread until the transfer completes.
    ///
    /// Each element is read atomically, but the snapshot as a whole is not collectively atomic.
    /// The number of elements transferred equals `data.len()`.
    /// Use [`LamellarBuffer::from_vec`] to wrap an owned `Vec` as the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. The range
    /// `index..index+data.len()` must be within bounds of the global array and `data` must be
    /// large enough to hold all results.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 10];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// unsafe { array.blocking_get_into_buffer(0, buf); }
    ///```
    pub unsafe fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::blocking_get_into_buffer(self, index, data, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget get into the provided [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`GenericAtomicArray::wait_all`] or a barrier to
    /// determine when the transfer is complete before reading from `data`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. The range
    /// `index..index+data.len()` must be within bounds of the global array. `data` must remain
    /// valid until the transfer completes.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfers.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 10];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// unsafe { array.get_into_buffer_unmanaged(0, buf); }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(self, index, data, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs an atomic get of a single element directly from PE `pe` at `offset` within that
    /// PE's local segment of the array.
    ///
    /// Uses an Active Message to atomically read the element under a per-element lock on the
    /// target PE. Returns an [`ArrayRdmaGetHandle`] that resolves to `T` when `spawn()`ed or
    /// `block()`ed.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// // Atomically read PE 0's first local element directly
    /// let val = array.get_pe(0, 0).block();
    /// println!("PE{my_pe} read PE0[0] = {val}");
    ///```
    pub fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs an atomic get of a single element directly from PE `pe` at
    /// `offset`, blocking the calling thread until the transfer completes.
    ///
    /// Uses an Active Message to atomically read the element under a per-element lock on the
    /// target PE.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let val = array.blocking_get_pe(0, 0);
    /// println!("PE{my_pe} read PE0[0] = {val}");
    ///```
    pub fn blocking_get_pe(&self, pe: usize, offset: usize) -> T {
        unsafe { <Self as LamellarRdmaGet<T>>::blocking_get_pe(self, pe, offset, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of `num_elems` elements from PE `pe` starting at `offset`.
    ///
    /// Each element is read atomically under a per-element lock. Returns an
    /// [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+num_elems` must be within bounds of that PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// // Fetch 5 elements starting at PE 0's local offset 0
    /// let data = unsafe { array.get_buffer_pe(0, 0, 5).block() };
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", data);
    ///```
    pub unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of `num_elems` elements from PE `pe` starting at `offset`,
    /// blocking the calling thread until the transfer completes.
    ///
    /// Each element is read atomically under a per-element lock.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+num_elems` must be within bounds of that PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = unsafe { array.blocking_get_buffer_pe(0, 0, 5) };
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", data);
    ///```
    pub unsafe fn blocking_get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> Vec<T> {
        <Self as LamellarRdmaGet<T>>::blocking_get_buffer_pe(self, pe, offset, num_elems, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get from PE `pe` at `offset` into the provided pre-allocated [`LamellarBuffer`].
    ///
    /// Each element is read atomically, but the snapshot as a whole is not collectively atomic.
    /// The number of elements transferred equals `data.len()`. Returns an
    /// [`ArrayRdmaGetIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    /// Use [`LamellarBuffer::from_one_sided_memory_region`] or [`LamellarBuffer::from_vec`] to
    /// construct the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+data.len()` must be within bounds of that PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst = world.alloc_one_sided_mem_region::<usize>(5);
    /// let buf = unsafe { LamellarBuffer::from_one_sided_memory_region(dst.clone()) };
    /// unsafe { array.get_into_buffer_pe(0, 0, buf).block(); }
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", unsafe { dst.as_slice() });
    ///```
    pub unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get from PE `pe` at `offset` into the provided pre-allocated
    /// [`LamellarBuffer`], blocking the calling thread until the transfer completes.
    ///
    /// Each element is read atomically, but the snapshot as a whole is not collectively atomic.
    /// The number of elements transferred equals `data.len()`.
    /// Use [`LamellarBuffer::from_one_sided_memory_region`] or [`LamellarBuffer::from_vec`] to
    /// construct the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+data.len()` must be within bounds of that PE's local memory region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst = world.alloc_one_sided_mem_region::<usize>(5);
    /// let buf = unsafe { LamellarBuffer::from_one_sided_memory_region(dst.clone()) };
    /// unsafe { array.blocking_get_into_buffer_pe(0, 0, buf); }
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", unsafe { dst.as_slice() });
    ///```
    pub unsafe fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(self, pe, offset, data, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget get from PE `pe` at `offset` into the provided [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`GenericAtomicArray::wait_all`] or a barrier to
    /// determine when the transfer is complete before reading from `data`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not collectively atomic. `pe` must be a valid PE index
    /// and `offset..offset+data.len()` must be within bounds of that PE's local memory region.
    /// `data` must remain valid until the transfer completes.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: GenericAtomicArray<usize> = GenericAtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 5];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// unsafe { array.get_into_buffer_unmanaged_pe(0, 0, buf); }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(self, pe, offset, data, Sealed)
    }
}
impl<T: Dist> LamellarRdmaPut<T> for GenericAtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        let am = self.store(index, data);
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::StoreOp(am),
            spawned: false,
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T, _: Sealed) {
        let _ = self.store(index, data).spawn();
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local(InitPutBufferAm {
            array: self.clone(),
            index,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
        _: Sealed,
    ) {
        let _ = self
            .spawn_am_local_tg(InitPutBufferAm {
                array: self.clone(),
                index: index,
                buf: buf.into(),
            });
    }

    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicRemotePePutAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                offset,
                elem_size: std::mem::size_of::<T>(),
                data: unsafe {
                    std::slice::from_raw_parts(
                        (&data as *const T) as *const u8,
                        std::mem::size_of::<T>(),
                    )
                    .to_vec()
                },
            },
        );
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RemoteAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T, _: Sealed) {
        let _ = self
            .spawn_am_pe_tg(
                pe,
                GenericAtomicRemotePePutAm {
                    array: self.clone().into(), //inner of the indices we need to place data into
                    offset,
                    elem_size: std::mem::size_of::<T>(),
                    data: unsafe {
                        std::slice::from_raw_parts(
                            (&data as *const T) as *const u8,
                            std::mem::size_of::<T>(),
                        )
                        .to_vec()
                    },
                },
            );
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicRemotePePutAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                offset,
                elem_size: std::mem::size_of::<T>(),
                data: buf.into().to_bytes(),
            },
        );
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RemoteAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        let _ = self.spawn_am_pe_tg(
            pe,
            GenericAtomicRemotePePutAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                offset,
                elem_size: std::mem::size_of::<T>(),
                data: buf.into().to_bytes(),
            },
        );
    }
    unsafe fn put_all(&self, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: unsafe {
                std::slice::from_raw_parts(
                    (&data as *const T) as *const u8,
                    std::mem::size_of::<T>(),
                )
                .to_vec()
            },
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RemoteAmPutAll(req),
            spawned: false,
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T, _: Sealed) {
        let _ = self.spawn_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: unsafe {
                std::slice::from_raw_parts(
                    (&data as *const T) as *const u8,
                    std::mem::size_of::<T>(),
                )
                .to_vec()
            },
        });
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: buf.into().to_bytes(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RemoteAmPutAll(req),
            spawned: false,
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        let _ = self.spawn_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: buf.into().to_bytes(),
        });
    }
}

impl<T: Dist> LamellarRdmaGet<T> for GenericAtomicArray<T> {
    unsafe fn get(&self, index: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let req = self.exec_am_pe_tg(
                pe,
                GenericAtomicGetPeAm {
                    array: self.clone().into(), //inner of the indices we need to place data into
                    local_index: offset,
                },
            );
            ArrayRdmaGetHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetState::RemoteAmGet(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds in LamellarArray get");
        }
    }
    unsafe fn blocking_get(&self, index: usize, _: Sealed) -> T {
        <Self as LamellarRdmaGet<T>>::get(self, index, Sealed).block()
    }
    unsafe fn get_buffer(
        &self,
        index: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        let req = self.exec_am_local(GenericAtomicInitGetBufferAm {
            array: self.clone(),
            index: index,
            len: num_elems,
        });
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::LocalAmGet(req),
            spawned: false,
        }
    }

    unsafe fn blocking_get_buffer(&self, index: usize, num_elems: usize, _: Sealed) -> Vec<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems, Sealed).block()
    }
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let req = self.exec_am_local(GenericAtomicInitGetIntoBufferAm {
            array: self.clone(),
            index: index,
            buf: Mutex::new(data),
        });
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::LocalAmGet(req),
            spawned: false,
        }
    }

    unsafe fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data, Sealed).block()
    }
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        let _ = <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data, Sealed).spawn();
    }

    unsafe fn get_pe(&self, pe: usize, offset: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicGetPeAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                local_index: offset,
            },
        );
        ArrayRdmaGetHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetState::RemoteAmGet(req),
            spawned: false,
        }
    }
    unsafe fn blocking_get_pe(&self, pe: usize, offset: usize, _: Sealed) -> T {
        <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset, Sealed).block()
    }

    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        let buf = self.array.team_rt().alloc_one_sided_mem_region(num_elems);
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicRemoteGetBufferPeAm {
                array: self.clone().into(),
                offset,
                num_elems,
                buf: unsafe { buf.clone().to_base::<u8>() },
            },
        );
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::RemoteAmGet(req, buf),
            spawned: false,
        }
    }
    unsafe fn blocking_get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> Vec<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems, Sealed).block()
    }

    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicRemoteGetIntoBufferPeAm {
                array: self.clone().into(),
                offset,
                num_elems: data.len(),
            },
        );
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::RemoteAmGet(data, req),
            spawned: false,
        }
    }
    unsafe fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data, Sealed).block()
    }
    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        let _ = <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data, Sealed)
            .spawn();
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicGetPeAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    local_index: usize,              //local index
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicGetPeAm<T> {
    async fn exec(self) -> Vec<u8> {
        unsafe {
            let _lock = self.array.lock_index(self.local_index);
            self.array
                .array
                .element_for_local_index(self.local_index)
                .to_vec()
        }
    }
}
#[lamellar_impl::AmLocalDataRT]
pub(crate) struct GenericAtomicInitGetBufferAm<T: Dist> {
    array: GenericAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    len: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for GenericAtomicInitGetBufferAm<T> {
    async fn exec(self) -> Vec<T> {
        let mut reqs = vec![];
        let mut cur_index = 0;
        let buf = lamellar::team.alloc_one_sided_mem_region::<T>(self.len);
        let mut bufs = vec![];
        for pe in self
            .array
            .array
            .pes_for_range(self.index, self.len)
            .into_iter()
        {
            if let Some(len) = self
                .array
                .array
                .num_elements_on_pe_for_range(pe, self.index, self.len)
            {
                let temp_buf = buf.sub_region(cur_index..cur_index + len);

                let remote_am = GenericAtomicRemoteGetBufferAm {
                    array: self.array.clone().into(),
                    start_index: self.index,
                    len: self.len,
                    buf: unsafe { temp_buf.clone().to_base::<u8>() },
                };
                bufs.push(temp_buf.clone());
                reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                cur_index += len;
            }
        }
        let num_pes = reqs.len();
        join_all(reqs).await;
        match self.array.array.inner.distribution {
            Distribution::Block => unsafe { buf.as_slice().to_vec() },
            Distribution::Cyclic => {
                let mut data: Vec<T> = unsafe {
                    let mut v = Vec::with_capacity(self.len);
                    v.set_len(self.len);
                    v
                };
                for (k, buf) in bufs.iter().enumerate() {
                    let buf_slice = unsafe { buf.as_slice() };
                    for (i, val) in buf_slice.iter().enumerate() {
                        data[i * num_pes + k] = *val;
                    }
                }
                data
            }
        }
    }
}
#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicRemoteGetBufferAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    buf: OneSidedMemoryRegion<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemoteGetBufferAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) {
        // println!("in GenericAtomic remotegetam {:?} {:?}",self.start_index,self.len);

        unsafe {
            let data = match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, indices)) => {
                    let mut locks = Vec::new();
                    for i in indices {
                        //for simplicity lets lock all the indicies we are concerned about
                        let full_i = self
                            .array
                            .array
                            .inner
                            .pe_full_offset_for_local_index(self.array.array.inner.data.my_pe, i)
                            .expect("invalid local index");
                        locks.push(self.array.locks[full_i].lock());
                    }
                    elems.to_vec() //copy the data
                } //locks dropped
                None => vec![],
            };
            self.buf.put_buffer(0, data).await;
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicRemoteGetBufferPeAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    offset: usize,
    num_elems: usize,
    buf: OneSidedMemoryRegion<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemoteGetBufferPeAm {
    async fn exec(self) {
        let mut locks = Vec::new();
        let full_offset = self
            .array
            .array
            .inner
            .pe_full_offset_for_local_index(self.array.array.inner.data.my_pe, self.offset)
            .expect("invalid local index");
        for i in full_offset..full_offset + self.num_elems {
            locks.push(self.array.locks[i].lock());
        }
        // let mut data = vec![0u8; self.num_elems * orig_t_size];
        let local_ptr = unsafe { self.array.array.ptr_for_local_index(self.offset) };
        let data = unsafe {
            std::slice::from_raw_parts(
                local_ptr as *const u8,
                self.num_elems * self.array.array.inner.elem_size,
            )
            .to_vec()
        };
        unsafe { self.buf.put_buffer(0, data).await };
    }
}

#[lamellar_impl::AmLocalDataRT]
struct GenericAtomicInitGetIntoBufferAm<T: Dist, B: AsLamellarBuffer<T>> {
    array: GenericAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: Mutex<LamellarBuffer<T, B>>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static, B: AsLamellarBuffer<T>> LamellarAm
    for GenericAtomicInitGetIntoBufferAm<T, B>
{
    async fn exec(self) {
        let mut reqs = vec![];
        let mut buf = self.buf.lock().split_off(0);
        for pe in self
            .array
            .array
            .pes_for_range(self.index, buf.len())
            .into_iter()
        {
            // println!("pe {:?}",pe);
            let remote_am = GenericAtomicRemoteGetIntoBufferAm {
                array: self.array.clone().into(),
                start_index: self.index,
                len: buf.len(),
            };
            reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
        }

        unsafe {
            match self.array.array.inner.distribution {
                Distribution::Block => {
                    let mut cur_index = 0;

                    let buf_slice = buf.as_mut_slice();
                    let buf_u8_slice = std::slice::from_raw_parts_mut(
                        buf_slice.as_mut_ptr() as *mut u8,
                        buf_slice.len() * std::mem::size_of::<T>(),
                    );
                    for req in reqs.drain(..) {
                        let data = req.await;
                        buf_u8_slice[cur_index..(cur_index + data.len())].copy_from_slice(&data);
                        cur_index += data.len();
                    }
                }
                Distribution::Cyclic => {
                    let buf_slice = buf.as_mut_slice();
                    let num_pes = reqs.len();
                    for (start_index, req) in reqs.drain(..).enumerate() {
                        let data = req.await;
                        let data_aligned = data.as_ptr() as usize % std::mem::align_of::<T>() == 0;
                        if data_aligned {
                            let data_t_slice = std::slice::from_raw_parts(
                                data.as_ptr() as *const T,
                                data.len() / std::mem::size_of::<T>(),
                            );
                            for (i, val) in data_t_slice.iter().enumerate() {
                                buf_slice[start_index + i * num_pes] = *val;
                            }
                        } else {
                            let data_t_ptr = data.as_ptr() as *mut T;
                            for i in 0..(data.len() / std::mem::size_of::<T>()) {
                                buf_slice[start_index + i * num_pes] =
                                    std::ptr::read_unaligned(data_t_ptr.offset(i as isize));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicRemoteGetIntoBufferAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemoteGetIntoBufferAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) -> Vec<u8> {
        // println!("in GenericAtomic remotegetam {:?} {:?}",self.start_index,self.len);
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, indices)) => {
                    let mut locks = Vec::new();
                    for i in indices {
                        //for simplicity lets lock all the indicies we are concerned about
                        let full_i = self
                            .array
                            .array
                            .inner
                            .pe_full_offset_for_local_index(self.array.array.inner.data.my_pe, i)
                            .expect("invalid local index");
                        locks.push(self.array.locks[full_i].lock());
                    }
                    elems.to_vec() //copy the data
                } //locks dropped
                None => vec![],
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicRemoteGetIntoBufferPeAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    offset: usize,
    num_elems: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemoteGetIntoBufferPeAm {
    async fn exec(self) -> Vec<u8> {
        let mut locks = Vec::new();
        let full_offset = self
            .array
            .array
            .inner
            .pe_full_offset_for_local_index(self.array.array.inner.data.my_pe, self.offset)
            .expect("invalid local index");
        for i in full_offset..full_offset + self.num_elems {
            locks.push(self.array.locks[i].lock());
        }
        let local_ptr = unsafe { self.array.array.ptr_for_local_index(self.offset) };
        unsafe {
            std::slice::from_raw_parts(
                local_ptr as *const u8,
                self.num_elems * self.array.array.inner.elem_size,
            )
            .to_vec()
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutBufferAm<T: Dist> {
    array: GenericAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutBufferAm<T> {
    async fn exec(self) {
        unsafe {
            // let u8_buf = self.buf.clone().to_base::<u8>();
            let mut reqs = vec![];
            match self.array.array.inner.distribution {
                Distribution::Block => {
                    let mut cur_index = 0;
                    for pe in self
                        .array
                        .array
                        .pes_for_range(self.index, self.buf.len())
                        .into_iter()
                    {
                        if let Some(len) = self.array.array.num_elements_on_pe_for_range(
                            pe,
                            self.index,
                            self.buf.len(),
                        ) {
                            // let u8_buf_len = len * std::mem::size_of::<T>();
                            // println!("pe {:?} index: {:?} len {:?} buflen {:?} putting {:?}",pe,self.index,len, self.buf.len(),&u8_buf.as_slice().unwrap()[cur_index..(cur_index+u8_buf_len)]);
                            let remote_am = GenericAtomicRemotePutAm {
                                array: self.array.clone().into(), //inner of the indices we need to place data into
                                start_index: self.index,
                                len: self.buf.len(),
                                data: self.buf.sub_region(cur_index..(cur_index + len)).to_bytes(),
                            };
                            reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                            cur_index += len;
                        } else {
                            panic!("this should not be possible");
                        }
                    }
                }
                Distribution::Cyclic => {
                    let num_pes = ArrayExecAm::team_rt(&self.array).num_pes();
                    let mut pe_u8_vecs: HashMap<usize, Vec<u8>> = HashMap::new();
                    let mut pe_t_slices: HashMap<usize, &mut [T]> = HashMap::new();
                    let buf_slice = self.buf.as_slice();
                    for pe in self
                        .array
                        .array
                        .pes_for_range(self.index, self.buf.len())
                        .into_iter()
                    {
                        if let Some(len) = self.array.array.num_elements_on_pe_for_range(
                            pe,
                            self.index,
                            self.buf.len(),
                        ) {
                            // println!("len {:?} buf len {:?} ",len,self.buf.len());
                            let mut u8_vec = vec![0u8; len * std::mem::size_of::<T>()];
                            let t_slice =
                                std::slice::from_raw_parts_mut(u8_vec.as_mut_ptr() as *mut T, len);
                            pe_u8_vecs.insert(pe, u8_vec);
                            pe_t_slices.insert(pe, t_slice);
                        }
                    }
                    for (buf_index, index) in
                        (self.index..(self.index + self.buf.len())).enumerate()
                    {
                        let pe = match self.array.array.pe_for_dist_index(index) {
                            Some(pe) => pe % num_pes,
                            None => panic!(
                                "Index: {index} is out of bounds for array of length: {:?}",
                                self.array.array.inner.size
                            ),
                        };
                        // println!("pe {:?} tslice index {:?} buf_index {:?}",pe,buf_index/num_pes,buf_index);
                        pe_t_slices.get_mut(&pe).unwrap()[buf_index / num_pes] =
                            buf_slice[buf_index];
                    }
                    for (pe, vec) in pe_u8_vecs.drain() {
                        // println!("pe {:?} vec {:?}",pe,vec);
                        let remote_am = GenericAtomicRemotePutAm {
                            array: self.array.clone().into(), //inner of the indices we need to place data into
                            start_index: self.index,
                            len: self.buf.len(),
                            data: vec,
                        };
                        reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                    }
                }
            }
            for req in reqs.drain(..) {
                req.await;
            }
            // println!("done local put");
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicRemotePutAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemotePutAm {
    async fn exec(self) {
        // println!("in remote put {:?} {:?} {:?}",self.start_index,self.len,self.data);
        // let _lock = self.array.lock.write();
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, indices)) => {
                    // println!("elems: {:?}",elems);
                    let mut locks = Vec::new();
                    for i in indices {
                        //for simplicity lets lock all the indicies we are concerned about
                        let full_i = self
                            .array
                            .array
                            .inner
                            .pe_full_offset_for_local_index(self.array.array.inner.data.my_pe, i)
                            .expect("invalid local index");
                        locks.push(self.array.locks[full_i].lock());
                    }
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr(),
                        elems.as_mut_ptr(),
                        elems.len(),
                    )
                }
                None => {}
            }
        }
        // println!("done remote put");
    }
}

#[lamellar_impl::AmDataRT(Debug)]
pub(crate) struct GenericAtomicRemotePePutAm {
    array: __GenericAtomicByteArray, //inner of the indices we need to place data into
    offset: usize,
    elem_size: usize,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemotePePutAm {
    async fn exec(self) {
        let byte_start_index = self.offset * self.elem_size;
        let u8_slice = &mut self.array.array.mut_local_data::<u8>()
            [byte_start_index..(byte_start_index + self.data.len())];
        let src_ptr = self.data.as_ptr();
        let dst_ptr = u8_slice.as_mut_ptr();
        let mut locks = Vec::new();

        for i in self.offset..(self.offset + self.data.len() / self.elem_size) {
            locks.push(self.array.locks[i].lock());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, self.data.len());
        }
    }
}
