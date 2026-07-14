use parking_lot::Mutex;

use crate::{
    array::{
        global_lock_atomic::GlobalLockArray,
        private::{ArrayExecAm, LamellarArrayPrivate},
        rdma::private::{LamellarRdmaGet, LamellarRdmaPut, Sealed},
        ArrayRdmaGetBufferHandle, ArrayRdmaGetBufferState, ArrayRdmaGetHandle,
        ArrayRdmaGetIntoBufferHandle, ArrayRdmaGetIntoBufferState, ArrayRdmaGetState,
        ArrayRdmaPutHandle, ArrayRdmaPutState,
    },
    memregion::{
        AsLamellarBuffer, Dist, LamellarBuffer, MemregionRdmaInput, MemregionRdmaInputInner,
    },
    ActiveMessaging,
};
impl<T: Dist> GlobalLockArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Performs a put of a single element into the array at the given global `index`.
    ///
    /// Acquires the global write lock on the affected PE before transferring data, ensuring
    /// exclusive access to the entire PE's local segment for the duration of the write.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes, Distribution::Block).block();
    ///
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
    /// Performs a fire-and-forget put of a single element at the given global `index`.
    ///
    /// No completion handle is returned; use [`GlobalLockArray::wait_all`] or a barrier to
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes, Distribution::Block).block();
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
    /// Acquires the global write lock on each target PE before writing to its local segment.
    /// The runtime distributes writes across PEs according to the array's [`Distribution`].
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(10);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     array.put_buffer(0, &src).block();
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into(), Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget put of a buffer starting at global `index`.
    ///
    /// No completion handle is returned; use [`GlobalLockArray::wait_all`] or a barrier to
    /// ensure all transfers complete.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(10);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     array.put_buffer_unmanaged(0, &src);
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(&self, index: usize, buf: U) {
        unsafe {
            <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into(), Sealed)
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a put of a single element directly to PE `pe` at `offset` within that PE's
    /// local segment of the array.
    ///
    /// Acquires the global write lock on PE `pe` before the transfer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
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
    /// Performs a fire-and-forget put of a single element directly to PE `pe` at `offset`.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Acquires the global write lock on PE `pe` for the duration of the transfer.
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 && num_pes > 1 {
    ///     array.put_pe_buffer(1, 0, &src).block();
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_pe_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe_buffer(self, pe, offset, buf.into(), Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget put of a buffer directly to PE `pe` at `offset`.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 && num_pes > 1 {
    ///     array.put_pe_buffer_unmanaged(1, 0, &src);
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        unsafe {
            <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                self,
                pe,
                offset,
                buf.into(),
                Sealed,
            )
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a put of a single element to all PEs at `offset` within each PE's local
    /// segment of the array.
    ///
    /// Acquires the global write lock on each target PE before writing.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
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
    /// Fire-and-forget broadcast of a single element to all PEs at `offset`.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Broadcasts a buffer to all PEs starting at `offset` within each PE's local segment of
    /// the array.
    ///
    /// Acquires the global write lock on each target PE before writing.
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     array.put_all_buffer(0, &src).block();
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into(), Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fire-and-forget broadcast of a buffer to all PEs starting at `offset`.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// let src = world.alloc_one_sided_mem_region::<usize>(5);
    /// unsafe { for elem in src.as_mut_slice() { *elem = my_pe; } }
    ///
    /// if my_pe == 0 {
    ///     array.put_all_buffer_unmanaged(0, &src);
    /// }
    /// array.wait_all();
    /// array.barrier();
    ///```
    pub fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(&self, offset: usize, buf: U) {
        unsafe {
            <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into(), Sealed)
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of a single element at the given global `index`.
    ///
    /// Acquires the global read lock on the affected PE, allowing concurrent reads but excluding
    /// writes for the duration of the transfer. Returns an [`ArrayRdmaGetHandle`] that resolves
    /// to `T` when `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let val = array.get(0).block();
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    pub fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get(self, index, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of a single element at the given global `index`, blocking
    /// the calling thread until the transfer completes.
    ///
    /// Acquires the global read lock on the affected PE, allowing concurrent reads but excluding
    /// writes for the duration of the transfer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes, Distribution::Block).block();
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
    /// Acquires the global read lock on each target PE for its portion of the transfer. Returns
    /// an [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = array.get_buffer(0, 10).block();
    /// println!("PE{my_pe} first 10 elements: {:?}", data);
    ///```
    pub fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of `num_elems` elements starting at global `index`, blocking
    /// the calling thread until the transfer completes.
    ///
    /// Acquires the global read lock on each target PE for its portion of the transfer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = array.blocking_get_buffer(0, 10);
    /// println!("PE{my_pe} first 10 elements: {:?}", data);
    ///```
    pub fn blocking_get_buffer(&self, index: usize, num_elems: usize) -> Vec<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::blocking_get_buffer(self, index, num_elems, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of elements starting at global `index` into the provided pre-allocated
    /// [`LamellarBuffer`].
    ///
    /// The number of elements transferred equals `data.len()`. Returns an
    /// [`ArrayRdmaGetIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    /// Use [`LamellarBuffer::from_vec`] to wrap an owned `Vec` as the destination buffer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 10];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// let buf = array.get_into_buffer(0, buf).block();
    /// let result = buf.try_unwrap().expect("no other references exist");
    /// println!("PE{my_pe} first 10 elements: {:?}", result);
    ///```
    pub fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of elements starting at global `index` into the provided
    /// pre-allocated [`LamellarBuffer`], blocking the calling thread until the transfer completes.
    ///
    /// The number of elements transferred equals `data.len()`.
    /// Use [`LamellarBuffer::from_vec`] to wrap an owned `Vec` as the destination buffer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 10];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// array.blocking_get_into_buffer(0, buf);
    ///```
    pub fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        unsafe { <Self as LamellarRdmaGet<T>>::blocking_get_into_buffer(self, index, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget get into the provided [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`GlobalLockArray::wait_all`] or a barrier to
    /// determine when the transfer is complete before reading from `data`.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 10];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// array.get_into_buffer_unmanaged(0, buf);
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        unsafe {
            <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(self, index, data, Sealed)
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of a single element directly from PE `pe` at `offset` within that PE's
    /// local segment of the array.
    ///
    /// Acquires the global read lock on PE `pe` for the duration of the transfer.
    /// Returns an [`ArrayRdmaGetHandle`] that resolves to `T` when `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let val = array.get_pe(0, 0).block();
    /// println!("PE{my_pe} read PE0[0] = {val}");
    ///```
    pub fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of a single element directly from PE `pe` at `offset`,
    /// blocking the calling thread until the transfer completes.
    ///
    /// Acquires the global read lock on PE `pe` for the duration of the transfer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Acquires the global read lock on PE `pe` for the duration of the transfer. Returns an
    /// [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or `block()`ed.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = array.get_buffer_pe(0, 0, 5).block();
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", data);
    ///```
    pub fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get of `num_elems` elements from PE `pe` starting at `offset`,
    /// blocking the calling thread until the transfer completes.
    ///
    /// Acquires the global read lock on PE `pe` for the duration of the transfer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let data = array.blocking_get_buffer_pe(0, 0, 5);
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", data);
    ///```
    pub fn blocking_get_buffer_pe(&self, pe: usize, offset: usize, num_elems: usize) -> Vec<T> {
        unsafe {
            <Self as LamellarRdmaGet<T>>::blocking_get_buffer_pe(self, pe, offset, num_elems, Sealed)
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get from PE `pe` at `offset` into the provided pre-allocated [`LamellarBuffer`].
    ///
    /// Acquires the global read lock on PE `pe` for the duration of the transfer. The number of
    /// elements transferred equals `data.len()`. Returns an [`ArrayRdmaGetIntoBufferHandle`] that
    /// must be `spawn()`ed or `block()`ed.
    /// Use [`LamellarBuffer::from_one_sided_memory_region`] or [`LamellarBuffer::from_vec`] to
    /// construct the destination buffer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst = world.alloc_one_sided_mem_region::<usize>(5);
    /// let buf = unsafe { LamellarBuffer::from_one_sided_memory_region(dst.clone()) };
    /// array.get_into_buffer_pe(0, 0, buf).block();
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", unsafe { dst.as_slice() });
    ///```
    pub fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously performs a get from PE `pe` at `offset` into the provided pre-allocated
    /// [`LamellarBuffer`], blocking the calling thread until the transfer completes.
    ///
    /// Acquires the global read lock on PE `pe` for the duration of the transfer. The number of
    /// elements transferred equals `data.len()`.
    /// Use [`LamellarBuffer::from_one_sided_memory_region`] or [`LamellarBuffer::from_vec`] to
    /// construct the destination buffer.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst = world.alloc_one_sided_mem_region::<usize>(5);
    /// let buf = unsafe { LamellarBuffer::from_one_sided_memory_region(dst.clone()) };
    /// array.blocking_get_into_buffer_pe(0, 0, buf);
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", unsafe { dst.as_slice() });
    ///```
    pub fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        unsafe {
            <Self as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(self, pe, offset, data, Sealed)
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget get from PE `pe` at `offset` into the provided [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`GlobalLockArray::wait_all`] or a barrier to
    /// determine when the transfer is complete before reading from `data`.
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let dst: Vec<usize> = vec![0usize; 5];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// array.get_into_buffer_unmanaged_pe(0, 0, buf);
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        unsafe {
            <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                self, pe, offset, data, Sealed,
            )
        }
    }
}
impl<T: Dist> LamellarRdmaPut<T> for GlobalLockArray<T> {
    unsafe fn put(&self, index: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        if let Some((pe, offset)) = self.array.pe_and_rdma_offset_for_global_index(index) {
            self.put_pe(pe, offset, data)
        } else {
            panic!("index out of bounds");
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T, _: Sealed) {
        if let Some((pe, offset)) = self.array.pe_and_rdma_offset_for_global_index(index) {
            let _ = self.put_pe(pe, offset, data).spawn();
        } else {
            panic!("index out of bounds");
        }
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
        let req = self.exec_am_local_tg(InitPePutAm {
            array: self.clone(),
            offset,
            pe,
            val: data,
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T, _: Sealed) {
        let _ = self.spawn_am_local_tg(InitPePutAm {
            array: self.clone(),
            offset,
            pe,
            val: data,
        });
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPePutBufferAm {
            array: self.clone(),
            offset,
            pe,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
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
        let _ = self.spawn_am_local_tg(InitPePutBufferAm {
            array: self.clone(),
            offset,
            pe: pe,
            buf: buf.into(),
        });
    }
    unsafe fn put_all(&self, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPutAllAm {
            array: self.clone(),
            offset,
            val: data,
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T, _: Sealed) {
        let _ = self.spawn_am_local_tg(InitPutAllAm {
            array: self.clone(),
            offset,
            val: data,
        });
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPutAllBufferAm {
            array: self.clone(),
            offset,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        let _ = self.spawn_am_local_tg(InitPutAllBufferAm {
            array: self.clone(),
            offset,
            buf: buf.into(),
        });
    }
}

impl<T: Dist> LamellarRdmaGet<T> for GlobalLockArray<T> {
    unsafe fn get(&self, index: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        if let Some((pe, offset)) = self.array.pe_and_rdma_offset_for_global_index(index) {
            let req = self.exec_am_local_tg(InitGetPeAm {
                array: self.clone(),
                offset,
                pe,
            });
            ArrayRdmaGetHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetState::LocalAmGet(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds");
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
        let req = self.exec_am_local_tg(InitGetBufferAm {
            array: self.clone(),
            index,
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
        let req = self.exec_am_local_tg(InitGetIntoBufferAm {
            array: self.clone(),
            index,
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
        let req = self.exec_am_local_tg(InitGetPeAm {
            array: self.clone(),
            offset,
            pe,
        });
        ArrayRdmaGetHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetState::LocalAmGet(req),
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
        let req = self.exec_am_local_tg(InitGetBufferPeAm {
            array: self.clone(),
            offset,
            pe,
            len: num_elems,
        });
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::LocalAmGet(req),
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
        let req = self.exec_am_local_tg(InitGetIntoBufferPeAm {
            array: self.clone(),
            offset,
            pe,
            buf: Mutex::new(data),
        });
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::LocalAmGet(req),
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

#[lamellar_impl::AmLocalDataRT]
struct InitGetPeAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetPeAm<T> {
    async fn exec(self) -> T {
        let _global_lock = self.array.read_lock().await;
        unsafe { self.array.array.get_pe(self.pe, self.offset).await }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    index: usize,
    len: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetBufferAm<T> {
    async fn exec(self) -> Vec<T> {
        let _global_lock = self.array.read_lock().await;
        unsafe { self.array.array.get_buffer(self.index, self.len).await }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetBufferPeAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    len: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetBufferPeAm<T> {
    async fn exec(self) -> Vec<T> {
        let _global_lock = self.array.read_lock().await;
        unsafe {
            self.array
                .array
                .get_buffer_pe(self.pe, self.offset, self.len)
                .await
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetIntoBufferAm<T: Dist, B: AsLamellarBuffer<T>> {
    array: GlobalLockArray<T>,
    index: usize,
    buf: Mutex<LamellarBuffer<T, B>>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static, B: AsLamellarBuffer<T>> LamellarAm for InitGetIntoBufferAm<T, B> {
    async fn exec(self) {
        let _global_lock = self.array.read_lock().await;
        let buf = self.buf.lock().split_off(0);
        unsafe {
            self.array.array.get_into_buffer(self.index, buf).await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetIntoBufferPeAm<T: Dist, B: AsLamellarBuffer<T>> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    buf: Mutex<LamellarBuffer<T, B>>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static, B: AsLamellarBuffer<T>> LamellarAm for InitGetIntoBufferPeAm<T, B> {
    async fn exec(self) {
        let _global_lock = self.array.read_lock().await;
        let buf = self.buf.lock().split_off(0);
        unsafe {
            self.array
                .array
                .get_into_buffer_pe(self.pe, self.offset, buf)
                .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    index: usize,
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutBufferAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            <crate::UnsafeArray<T> as LamellarRdmaPut<T>>::put_buffer(
                &self.array.array,
                self.index,
                self.buf.clone(),
                Sealed,
            )
            .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPePutAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    val: T,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPePutAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            self.array
                .array
                .put_pe(self.pe, self.offset, self.val)
                .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutAllAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    val: T,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutAllAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            self.array.array.put_all(self.offset, self.val).await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPePutBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPePutBufferAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            <crate::UnsafeArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                &self.array.array,
                self.pe,
                self.offset,
                self.buf.clone(),
                Sealed,
            )
            .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutAllBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutAllBufferAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            <crate::UnsafeArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                &self.array.array,
                self.offset,
                self.buf.clone(),
                Sealed,
            )
            .await;
        }
    }
}
