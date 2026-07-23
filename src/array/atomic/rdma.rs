use crate::array::atomic::*;
use crate::array::rdma::private::{LamellarRdmaGet, LamellarRdmaPut, Sealed};
use crate::array::*;
use crate::memregion::AsLamellarBuffer;
use crate::memregion::Dist;
use crate::memregion::LamellarBuffer;
use crate::memregion::MemregionRdmaInput;
use crate::memregion::MemregionRdmaInputInner;

impl<T: Dist> AtomicArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Performs a one-sided RDMA put of a single element into the array at the given global `index`.
    ///
    /// The write is performed atomically on the target PE. The element is written to whichever PE
    /// owns that index according to the array's [`Distribution`].
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed to drive the transfer.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes, Distribution::Block).block();
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
    /// Performs a fire-and-forget one-sided RDMA put of a single element at the given global `index`.
    ///
    /// The write is performed atomically on the target PE. No completion handle is returned;
    /// use [`AtomicArray::wait_all`] or a barrier to ensure the transfer is complete before
    /// accessing the destination.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes, Distribution::Block).block();
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
    /// Performs a one-sided RDMA put of a buffer into the array starting at global `index`.
    ///
    /// The buffer length determines how many elements are transferred; the runtime automatically
    /// distributes writes across PEs according to the array's [`Distribution`].
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic — concurrent reads or writes to
    /// overlapping elements can observe a partially-written state. The index range
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a fire-and-forget one-sided RDMA put of a buffer starting at global `index`.
    ///
    /// No completion handle is returned; use [`AtomicArray::wait_all`] or a barrier to ensure
    /// all transfers complete before accessing the destination.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. The index range
    /// `index..index+buf.len()` must be within bounds of the global array. The source buffer must
    /// remain valid until the NIC completes the transfer.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a one-sided RDMA put of a single element directly to PE `pe` at `offset` within
    /// that PE's local segment of the array.
    ///
    /// The write is performed atomically on the target PE. Returns an [`ArrayRdmaPutHandle`] that
    /// must be `spawn()`ed or `block()`ed.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a fire-and-forget one-sided RDMA put of a single element directly to PE `pe`
    /// at `offset`.
    ///
    /// The write is performed atomically on the target PE.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a one-sided RDMA put of a buffer directly to PE `pe` starting at `offset` within
    /// that PE's local segment of the array.
    ///
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. `pe` must be a valid PE index
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a fire-and-forget one-sided RDMA put of a buffer directly to PE `pe` at `offset`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. `pe` must be a valid PE index
    /// and `offset..offset+buf.len()` must be within bounds of that PE's local memory region.
    /// The source buffer must remain valid until the NIC completes the transfer.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
        <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(self, pe, offset, buf.into(), Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a single element atomically to all PEs at `offset` within each PE's local
    /// segment of the array.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Fire-and-forget broadcast of a single element atomically to all PEs at `offset`.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Returns an [`ArrayRdmaPutHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. `offset..offset+buf.len()` must
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Multi-element buffer transfers are not individually atomic. `offset..offset+buf.len()` must
    /// be within bounds of every PE's local memory region. The source buffer must remain valid until
    /// all NIC transfers complete.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a one-sided RDMA get of a single element at the given global `index`.
    ///
    /// The read is performed atomically on the target PE. Returns an [`ArrayRdmaGetHandle`] that
    /// resolves to `T` when `spawn()`ed or `block()`ed.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes, Distribution::Block).block();
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
    /// Synchronously gets a single element at the given global `index`, blocking until the
    /// transfer completes.
    ///
    /// The read is performed atomically on the target PE.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes, Distribution::Block).block();
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
    /// Performs a one-sided RDMA get of `num_elems` elements starting at global `index`.
    ///
    /// The runtime handles cross-PE distribution automatically. Returns an
    /// [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic — the snapshot may observe
    /// elements written at different points in time. The range `index..index+num_elems` must be
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Synchronously gets `num_elems` elements starting at global `index`, blocking until the
    /// transfer completes.
    ///
    /// The runtime handles cross-PE distribution automatically.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic — the snapshot may observe
    /// elements written at different points in time. The range `index..index+num_elems` must be
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a one-sided RDMA get of elements starting at global `index` into the provided
    /// pre-allocated [`LamellarBuffer`].
    ///
    /// The number of elements transferred equals `data.len()`. Returns an
    /// [`ArrayRdmaGetIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    /// Use [`LamellarBuffer::from_vec`] to wrap an owned `Vec` as the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. The range
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Synchronously gets elements starting at global `index` into the provided pre-allocated
    /// [`LamellarBuffer`], blocking until the transfer completes.
    ///
    /// The number of elements transferred equals `data.len()`.
    /// Use [`LamellarBuffer::from_vec`] to wrap an owned `Vec` as the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. The range
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a fire-and-forget one-sided RDMA get into the provided [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`AtomicArray::wait_all`] or a barrier to determine
    /// when the transfer is complete before reading from `data`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. The range
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a one-sided RDMA get of a single element directly from PE `pe` at `offset` within
    /// that PE's local segment of the array.
    ///
    /// The read is performed atomically on the target PE. Returns an [`ArrayRdmaGetHandle`] that
    /// resolves to `T` when `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `pe` must be a valid PE index and `offset` must be within bounds of that PE's local memory
    /// region.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// // Atomically read PE 0's first local element
    /// let val = unsafe { array.get_pe(0, 0).block() };
    /// println!("PE{my_pe} read PE0[0] = {val}");
    ///```
    pub unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Synchronously gets a single element from PE `pe` at `offset`, blocking until the transfer
    /// completes.
    ///
    /// The read is performed atomically on the target PE.
    ///
    /// # Safety
    /// `pe` must be a valid PE index and `offset` must be within bounds of that PE's local memory
    /// region.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// let val = unsafe { array.blocking_get_pe(0, 0) };
    /// println!("PE{my_pe} read PE0[0] = {val}");
    ///```
    pub unsafe fn blocking_get_pe(&self, pe: usize, offset: usize) -> T {
        <Self as LamellarRdmaGet<T>>::blocking_get_pe(self, pe, offset, Sealed)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a one-sided RDMA get of `num_elems` elements from PE `pe` starting at `offset`.
    ///
    /// Returns an [`ArrayRdmaGetBufferHandle`] that resolves to `Vec<T>` when `spawn()`ed or `block()`ed.
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
    ///
    /// // Fetch 5 elements starting at PE 0's local offset 0
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
    /// array.dist_iter_mut().enumerate().for_each(|(i, elem)| *elem = i).block();
    /// array.barrier();
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
    /// Performs a one-sided RDMA get from PE `pe` at `offset` into the provided pre-allocated
    /// [`LamellarBuffer`].
    ///
    /// The number of elements transferred equals `data.len()`. Returns an
    /// [`ArrayRdmaGetIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    /// Use [`LamellarBuffer::from_one_sided_memory_region`] or [`LamellarBuffer::from_vec`] to
    /// construct the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. `pe` must be a valid PE index
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Synchronously gets from PE `pe` at `offset` into the provided pre-allocated
    /// [`LamellarBuffer`], blocking until the transfer completes.
    ///
    /// The number of elements transferred equals `data.len()`.
    /// Use [`LamellarBuffer::from_one_sided_memory_region`] or [`LamellarBuffer::from_vec`] to
    /// construct the destination buffer.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. `pe` must be a valid PE index
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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
    /// Performs a fire-and-forget one-sided RDMA get from PE `pe` at `offset` into the provided
    /// [`LamellarBuffer`].
    ///
    /// No completion handle is returned; use [`AtomicArray::wait_all`] or a barrier to determine
    /// when the transfer is complete before reading from `data`.
    ///
    /// # Safety
    /// Multi-element buffer transfers are not individually atomic. `pe` must be a valid PE index
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
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes * 10, Distribution::Block).block();
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

impl<T: Dist> LamellarRdmaPut<T> for AtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data, Sealed)
            }
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T, _: Sealed) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(
                    array, index, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf, Sealed)
            }
        }
    }
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T, _: Sealed) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                    array, pe, offset, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_all(&self, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data, Sealed)
            }
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T, _: Sealed) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                    array, offset, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf, Sealed,
                )
            }
        }
    }
}

impl<T: Dist> LamellarRdmaGet<T> for AtomicArray<T> {
    unsafe fn get(&self, index: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index, Sealed)
            }
        }
    }

    unsafe fn blocking_get(&self, index: usize, _: Sealed) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get(array, index, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get(array, index, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get(array, index, Sealed)
            }
        }
    }

    unsafe fn get_buffer(
        &self,
        index: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_buffer(&self, index: usize, num_elems: usize, _: Sealed) -> Vec<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(
                    array, index, data, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
                    array, index, data, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data, Sealed,
                )
            }
        }
    }

    unsafe fn get_pe(&self, pe: usize, offset: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset, Sealed)
            }
        }
    }

    unsafe fn blocking_get_pe(&self, pe: usize, offset: usize, _: Sealed) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(
                    array, pe, offset, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(
                    array, pe, offset, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(
                    array, pe, offset, Sealed,
                )
            }
        }
    }

    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> Vec<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }
}
