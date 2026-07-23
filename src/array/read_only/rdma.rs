use crate::{
    array::{
        rdma::private::{LamellarRdmaGet, Sealed},
        *,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer},
};

impl<T: Dist> ReadOnlyArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously gets a single element at the given global `index`, blocking until the
    /// transfer completes.
    ///
    /// Because the array is read-only, no atomicity or mutation constraints apply to the source.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes, Distribution::Block).block();
    ///
    /// let val = array.blocking_get(0);
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    pub fn blocking_get(&self, index: usize) -> T {
        unsafe { <Self as LamellarRdmaGet<T>>::blocking_get(self, index, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of a single element at the given global `index`.
    ///
    /// Because the array is read-only, no atomicity or mutation constraints apply to the source.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes, Distribution::Block).block();
    ///
    /// let val = array.get(0).block();
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    pub fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get(self, index, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get of `num_elems` elements starting at global `index`.
    ///
    /// Because the array is read-only, concurrent gets are always safe with respect to the source.
    /// Returns an [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or
    /// `block()`ed.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// let data = array.get_buffer(0, 10).block();
    /// println!("PE{my_pe} first 10 elements: {:?}", data);
    ///```
    pub fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously gets `num_elems` elements starting at global `index`, blocking until the
    /// transfer completes.
    ///
    /// Because the array is read-only, concurrent gets are always safe with respect to the source.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Synchronously gets elements starting at global `index` into the provided pre-allocated
    /// [`LamellarBuffer`], blocking until the transfer completes.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// No completion handle is returned; use [`ReadOnlyArray::wait_all`] or a barrier to
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// // Read PE 0's first local element directly
    /// let val = array.get_pe(0, 0).block();
    /// println!("PE{my_pe} read PE0[0] = {val}");
    ///```
    pub fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset, Sealed) }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously gets a single element from PE `pe` at `offset`, blocking until the transfer
    /// completes.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Returns an [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Synchronously gets `num_elems` elements from PE `pe` starting at `offset`, blocking until
    /// the transfer completes.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// let data = array.blocking_get_buffer_pe(0, 0, 5);
    /// println!("PE{my_pe} PE0 data[0..5]: {:?}", data);
    ///```
    pub fn blocking_get_buffer_pe(&self, pe: usize, offset: usize, num_elems: usize) -> Vec<T> {
        unsafe {
            <Self as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                self, pe, offset, num_elems, Sealed,
            )
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a get from PE `pe` at `offset` into the provided pre-allocated [`LamellarBuffer`].
    ///
    /// The number of elements transferred equals `data.len()`. Returns an
    /// [`ArrayRdmaGetIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Synchronously gets from PE `pe` at `offset` into the provided pre-allocated
    /// [`LamellarBuffer`], blocking until the transfer completes.
    ///
    /// The number of elements transferred equals `data.len()`.
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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
            <Self as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                self, pe, offset, data, Sealed,
            )
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a fire-and-forget get from PE `pe` at `offset` into the provided [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`ReadOnlyArray::wait_all`] or a barrier to
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
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

impl<T: Dist> LamellarRdmaGet<T> for ReadOnlyArray<T> {
    unsafe fn get(&self, index: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get(&self.array, index, Sealed)
    }
    unsafe fn blocking_get(&self, index: usize, _: Sealed) -> T {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::blocking_get(&self.array, index, Sealed)
    }
    unsafe fn get_buffer(
        &self,
        index: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_buffer(&self.array, index, num_elems, Sealed)
    }
    unsafe fn blocking_get_buffer(&self, index: usize, num_elems: usize, _: Sealed) -> Vec<T> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
            &self.array,
            index,
            num_elems,
            Sealed,
        )
    }
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_into_buffer(&self.array, index, data, Sealed)
    }

    unsafe fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
            &self.array,
            index,
            data,
            Sealed,
        )
    }
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
            &self.array,
            index,
            data,
            Sealed,
        )
    }
    unsafe fn get_pe(&self, pe: usize, offset: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_pe(&self.array, pe, offset, Sealed)
    }
    unsafe fn blocking_get_pe(&self, pe: usize, offset: usize, _: Sealed) -> T {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(&self.array, pe, offset, Sealed)
    }
    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
            &self.array,
            pe,
            offset,
            num_elems,
            Sealed,
        )
    }
    unsafe fn blocking_get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> Vec<T> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
            &self.array,
            pe,
            offset,
            num_elems,
            Sealed,
        )
    }
    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
            &self.array,
            pe,
            offset,
            data,
            Sealed,
        )
    }
    unsafe fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
            &self.array,
            pe,
            offset,
            data,
            Sealed,
        )
    }
    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        <UnsafeArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
            &self.array,
            pe,
            offset,
            data,
            Sealed,
        )
    }
}
