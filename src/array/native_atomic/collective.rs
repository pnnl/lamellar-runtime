use crate::{
    array::{
        collective::{
            algorithm::{
                do_all_gather, do_all_gather_in_buffer, do_all_reduce, do_all_reduce_bitwise,
                do_all_reduce_bitwise_in_buffer, do_all_reduce_comparison,
                do_all_reduce_comparison_in_buffer, do_all_reduce_in_buffer, do_all_to_all,
                do_all_to_all_in_buffer, do_broadcast, do_broadcast_in_buffer, do_gather,
                do_gather_in_buffer, do_reduce, do_reduce_bitwise, do_reduce_bitwise_in_buffer,
                do_reduce_comparison, do_reduce_comparison_in_buffer, do_reduce_in_buffer,
                do_reduce_scatter, do_reduce_scatter_bitwise, do_reduce_scatter_bitwise_in_buffer,
                do_reduce_scatter_comparison, do_reduce_scatter_comparison_in_buffer,
                do_reduce_scatter_in_buffer, do_scatter, do_scatter_in_buffer,
            },
            broadcast_handle::{
                ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle,
                ArrayCollectiveAllToAllIntoBufferState, ArrayCollectiveAllToAllState,
                ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle,
                ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState,
                ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle,
                ArrayCollectiveScatterIntoBufferState, ArrayCollectiveScatterState,
                CollectiveAllToAllIntoBufferManualOpHandle, CollectiveAllToAllManualOpHandle,
                CollectiveBroadcastIntoBufferManualOpHandle, CollectiveBroadcastManualOpHandle,
                CollectiveScatterIntoBufferManualOpHandle, CollectiveScatterManualOpHandle,
            },
            gather_handle::{
                ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle,
                ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState,
                ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle,
                ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState,
                CollectiveAllGatherIntoBufferManualOpHandle, CollectiveAllGatherManualOpHandle,
                CollectiveGatherIntoBufferManualOpHandle, CollectiveGatherManualOpHandle,
            },
            reduce_handle::{
                ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceIntoBufferHandle,
                ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState,
                ArrayCollectiveReduceHandle, ArrayCollectiveReduceIntoBufferHandle,
                ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState,
                CollectiveAllReduceIntoBufferManualOpHandle, CollectiveAllReduceManualOpHandle,
                CollectiveReduceIntoBufferManualOpHandle, CollectiveReduceManualOpHandle,
            },
            reduce_scatter_handle::{
                ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle,
                ArrayCollectiveReduceScatterIntoBufferState, ArrayCollectiveReduceScatterState,
                CollectiveReduceScatterIntoBufferManualOpHandle,
                CollectiveReduceScatterManualOpHandle,
            },
        },
        private::LamellarArrayPrivate,
        NativeAtomicArray,
    },
    lamellae::collective::{
        BroadcastInput, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput,
    },
    AsLamellarBuffer, Dist, ElementArithmeticOps, ElementBitWiseOps, ElementComparePartialEqOps,
    LamellarBuffer, LamellarEnv,
};

impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
    /// All-reduce sum of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// sum is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.sum_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_sum {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Sum,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.sum_all(index, len)
        }
    }

    /// All-reduce product of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// product is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.prod_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_prod {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Prod,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.prod_all(index, len)
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> NativeAtomicArray<T> {
    /// All-reduce max of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// maximum is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.max_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_max {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce_comparison(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Max,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.max_all(index, len)
        }
    }

    /// All-reduce min of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// minimum is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.min_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_min {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce_comparison(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Min,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.min_all(index, len)
        }
    }
}

impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
    /// All-reduce bitwise AND of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise AND is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_and_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn bit_and_all(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_bit_and {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitAnd,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_and_all(index, len)
        }
    }

    /// All-reduce bitwise XOR of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise XOR is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_xor_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn bit_xor_all(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_bit_xor {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitXor,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_xor_all(index, len)
        }
    }

    /// All-reduce bitwise OR of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise OR is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_or_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if !self.array.collective_support.all_bit_or {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(
                    CollectiveAllReduceManualOpHandle {
                        future: Box::pin(do_all_reduce_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitOr,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_or_all(index, len)
        }
    }
}

impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
    /// All-reduce sum of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`sum_all`](NativeAtomicArray::sum_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.sum_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_sum {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Sum,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.sum_all_into_buffer(index, len, buffer)
        }
    }

    /// All-reduce product of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`prod_all`](NativeAtomicArray::prod_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.prod_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_prod {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Prod,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.prod_all_into_buffer(index, len, buffer)
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> NativeAtomicArray<T> {
    /// All-reduce max of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`max_all`](NativeAtomicArray::max_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.max_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_max {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_comparison_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Max,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.max_all_into_buffer(index, len, buffer)
        }
    }

    /// All-reduce min of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`min_all`](NativeAtomicArray::min_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.min_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_min {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_comparison_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Min,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.min_all_into_buffer(index, len, buffer)
        }
    }
}

impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
    /// All-reduce bitwise AND of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`bit_and_all`](NativeAtomicArray::bit_and_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.bit_and_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_bit_and {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_bitwise_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitAnd,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_and_all_into_buffer(index, len, buffer)
        }
    }

    /// All-reduce bitwise XOR of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`bit_xor_all`](NativeAtomicArray::bit_xor_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.bit_xor_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_bit_xor {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_bitwise_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitXor,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_xor_all_into_buffer(index, len, buffer)
        }
    }

    /// All-reduce bitwise OR of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`bit_or_all`](NativeAtomicArray::bit_or_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.bit_or_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.all_bit_or {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(
                    CollectiveAllReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_all_reduce_bitwise_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitOr,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_or_all_into_buffer(index, len, buffer)
        }
    }
}

// impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
//     pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .sum_all_in_place(src_and_dst)
//     }

//     pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .max_all_in_place(src_and_dst)
//     }

//     pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .min_all_in_place(src_and_dst)
//     }

//     pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .prod_all_in_place(src_and_dst)
//     }
// }

// impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
//     pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .bit_and_all_in_place(src_and_dst)
//     }

//     pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .bit_xor_all_in_place(src_and_dst)
//     }

//     pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
//         self.array
//             .bit_or_all_in_place(src_and_dst)
//     }
// }

impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
    /// Reduce sum of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// sum is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.sum_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn sum_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.sum {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::Sum,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.sum_at_pe(index, len, pe)
        }
    }

    /// Reduce product of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// product is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.prod_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn prod_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.prod {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::Prod,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.prod_at_pe(index, len, pe)
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> NativeAtomicArray<T> {
    /// Reduce max of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// maximum is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.max_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn max_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.max {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce_comparison(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::Max,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.max_at_pe(index, len, pe)
        }
    }

    /// Reduce min of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// minimum is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.min_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn min_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.min {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce_comparison(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::Min,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.min_at_pe(index, len, pe)
        }
    }
}

impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
    /// Reduce bitwise AND of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise AND is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_and_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn bit_and_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.bit_and {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::BitAnd,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_and_at_pe(index, len, pe)
        }
    }

    /// Reduce bitwise XOR of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise XOR is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_xor_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn bit_xor_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.bit_xor {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::BitXor,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_xor_at_pe(index, len, pe)
        }
    }

    /// Reduce bitwise OR of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise OR is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_or_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn bit_or_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.collective_support.bit_or {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(
                    CollectiveReduceManualOpHandle {
                        future: Box::pin(do_reduce_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                            ReduceOp::BitOr,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_or_at_pe(index, len, pe)
        }
    }
}

impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
    /// Reduce sum of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`sum_at_pe`](NativeAtomicArray::sum_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.sum_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.sum {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Sum,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.sum_at_pe_into_buffer(index, len, target)
        }
    }

    /// Reduce product of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`prod_at_pe`](NativeAtomicArray::prod_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.prod_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.prod {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Prod,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.prod_at_pe_into_buffer(index, len, target)
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> NativeAtomicArray<T> {
    /// Reduce max of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`max_at_pe`](NativeAtomicArray::max_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.max_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.max {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_comparison_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Max,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.max_at_pe_into_buffer(index, len, target)
        }
    }

    /// Reduce min of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`min_at_pe`](NativeAtomicArray::min_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.min_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.min {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_comparison_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Min,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.min_at_pe_into_buffer(index, len, target)
        }
    }
}

impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
    /// Reduce bitwise AND of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`bit_and_at_pe`](NativeAtomicArray::bit_and_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.bit_and_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.bit_and {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_bitwise_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitAnd,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_and_at_pe_into_buffer(index, len, target)
        }
    }

    /// Reduce bitwise XOR of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`bit_xor_at_pe`](NativeAtomicArray::bit_xor_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.bit_xor_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.bit_xor {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_bitwise_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitXor,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_xor_at_pe_into_buffer(index, len, target)
        }
    }

    /// Reduce bitwise OR of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`bit_or_at_pe`](NativeAtomicArray::bit_or_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(1);
    ///     let _result = unsafe { array.bit_or_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.collective_support.bit_or {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(
                    CollectiveReduceIntoBufferManualOpHandle {
                        future: Box::pin(do_reduce_bitwise_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitOr,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_or_at_pe_into_buffer(index, len, target)
        }
    }
}

// impl<T: Dist + Default> NativeAtomicArray<T> {
//     pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .sum_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .max_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .min_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .prod_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .bit_and_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .bit_xor_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => {
//                 array
//                     .array
//                     .bit_or_at_pe_in_place(pe)
//             }
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//          }
//     }
// }

impl<T: Dist + Default> NativeAtomicArray<T> {
    /// Gathers `len` elements starting at `index` from every PE, delivering the concatenated result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The gathered
    /// data from all PEs is concatenated and every PE receives the full result.
    /// Returns an [`ArrayCollectiveAllGatherHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.gather_all(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        if !self.array.collective_support.allgather {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllGatherHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllGatherState::CollectiveAllGatherManual(
                    CollectiveAllGatherManualOpHandle {
                        future: Box::pin(do_all_gather(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.gather_all(index, len)
        }
    }

    /// Gathers `len` elements starting at `index` from every PE into a caller-supplied `buffer`, delivering to all PEs.
    ///
    /// Like [`gather_all`](NativeAtomicArray::gather_all) but places the concatenated result into the
    /// caller-supplied [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllGatherIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must
    /// be large enough to hold `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(world.num_pes());
    ///     let _result = unsafe { array.gather_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        if !self.array.collective_support.allgather {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllGatherIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBufferManual(
                    CollectiveAllGatherIntoBufferManualOpHandle {
                        future: Box::pin(do_all_gather_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.gather_all_into_buffer(index, len, buffer)
        }
    }
}

impl<T: Dist + Default> NativeAtomicArray<T> {
    /// Gathers `len` elements starting at `index` from every PE, delivering the concatenated result only to PE `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The gathered
    /// data is concatenated and delivered only to the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveGatherHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.gather_at_pe(0, array.num_elems_local(), 0) }.block();
    /// }
    ///```
    pub unsafe fn gather_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveGatherHandle<T> {
        if !self.array.collective_support.gather {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveGatherHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveGatherState::CollectiveGatherManual(
                    CollectiveGatherManualOpHandle {
                        future: Box::pin(do_gather(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            pe,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.gather_at_pe(index, len, pe)
        }
    }

    /// Gathers `len` elements starting at `index` from every PE into a caller-supplied `dst`, delivering only to the root PE.
    ///
    /// Like [`gather_at_pe`](NativeAtomicArray::gather_at_pe) but places the concatenated result into the
    /// caller-supplied `dst: RootOrLamellarBuffer`. The root PE is encoded in the `dst` value.
    /// Returns an [`ArrayCollectiveGatherIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(world.num_pes());
    ///     let _result = unsafe { array.gather_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    /// }
    ///```
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        if !self.array.collective_support.gather {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveGatherIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBufferManual(
                    CollectiveGatherIntoBufferManualOpHandle {
                        future: Box::pin(do_gather_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.gather_at_pe_into_buffer(index, len, target)
        }
    }
}

impl<T: Dist + Default> NativeAtomicArray<T> {
    /// All-to-all exchange: each PE sends `len` elements starting at `index` to every other PE.
    ///
    /// Each PE sends `len` elements from its local segment beginning at `index` to every other PE,
    /// and receives one segment per PE. The result on each PE is the concatenation of segments
    /// received from all PEs.
    /// Returns an [`ArrayCollectiveAllToAllHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.alltoall(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn alltoall(&self, index: usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        if !self.array.collective_support.alltoall {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllToAllHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllToAllState::CollectiveAllToAllManual(
                    CollectiveAllToAllManualOpHandle {
                        future: Box::pin(do_all_to_all(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.alltoall(index, len)
        }
    }

    /// All-to-all exchange placing received data into caller-supplied `buffer`.
    ///
    /// Like [`alltoall`](NativeAtomicArray::alltoall) but places the received data into the caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllToAllIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must
    /// be large enough to hold `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(world.num_pes());
    ///     let _result = unsafe { array.alltoall_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn alltoall_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        if !self.array.collective_support.alltoall {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveAllToAllIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBufferManual(
                    CollectiveAllToAllIntoBufferManualOpHandle {
                        future: Box::pin(do_all_to_all_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            buffer,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.alltoall_into_buffer(index, len, buffer)
        }
    }
}

impl<T: Dist + Default> NativeAtomicArray<T> {
    /// Broadcasts `len` elements from the root PE specified by `src_or_root_pe` to all PEs.
    ///
    /// `src_or_root_pe: BroadcastInput` encodes both the root PE index and, on the root, the source
    /// buffer. All non-root PEs receive the broadcast data into their local array segment.
    /// Returns an [`ArrayCollectiveBroadcastHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.broadcast_from_pe(BroadcastInput::Root(0), array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn broadcast_from_pe(
        &self,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> ArrayCollectiveBroadcastHandle<T> {
        if !self.array.collective_support.broadcast {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            match src_or_root_pe {
                BroadcastInput::Root(index) => ArrayCollectiveBroadcastHandle {
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveBroadcastState::CollectiveBroadcastManual(
                        CollectiveBroadcastManualOpHandle {
                            future: Box::pin(do_broadcast(
                                self.clone(),
                                self.array.inner.data.mem_region.scheduler.clone(),
                                alloc.clone(),
                                my_ticket,
                                now_serving,
                                index,
                                len,
                                self.my_pe(),
                            )),
                            scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                            counters: self.array.inner.data.mem_region.counters.clone(),
                        },
                    ),
                    spawned: false,
                },
                BroadcastInput::NotRoot(root) => ArrayCollectiveBroadcastHandle {
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveBroadcastState::CollectiveBroadcastManual(
                        CollectiveBroadcastManualOpHandle {
                            future: Box::pin(do_broadcast(
                                self.clone(),
                                self.array.inner.data.mem_region.scheduler.clone(),
                                alloc.clone(),
                                my_ticket,
                                now_serving,
                                0,
                                len,
                                root,
                            )),
                            scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                            counters: self.array.inner.data.mem_region.counters.clone(),
                        },
                    ),
                    spawned: false,
                },
            }
        } else {
            self.array.broadcast_from_pe(src_or_root_pe, len)
        }
    }

    /// Broadcasts `len` elements from the root PE, placing received data into caller-supplied `dst`.
    ///
    /// Like [`broadcast_from_pe`](NativeAtomicArray::broadcast_from_pe) but places received data into the
    /// caller-supplied `dst: RootSrcOrLamellarBuffer`. On the root PE `dst` encodes both the source
    /// data and the destination; on non-root PEs it is the destination buffer.
    /// Returns an [`ArrayCollectiveBroadcastIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `len` elements. All
    /// destination buffers must be large enough to hold `len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.broadcast_from_pe_into_buffer(RootSrcOrLamellarBuffer::<usize, OneSidedMemoryRegion<usize>>::Root(0), 1) }.block();
    /// }
    ///```
    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        target: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        if !self.array.collective_support.broadcast {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveBroadcastIntoBufferHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBufferManual(
                    CollectiveBroadcastIntoBufferManualOpHandle {
                        future: Box::pin(do_broadcast_in_buffer(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            alloc.clone(),
                            my_ticket,
                            now_serving,
                            len,
                            target,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.broadcast_from_pe_into_buffer(target, len)
        }
    }
}

impl<T: Dist + Default> NativeAtomicArray<T> {
    /// Scatters segments of data from the root PE to each PE, with `len` elements per PE.
    ///
    /// `src_or_root_pe: ScatterInput` encodes the root PE index and, on the root, the source buffer
    /// containing `num_pes * len` elements. Each PE receives its disjoint `len`-element segment.
    /// Returns an [`ArrayCollectiveScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.scatter_from_pe(ScatterInput::Root(0), array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn scatter_from_pe(
        &self,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> ArrayCollectiveScatterHandle<T> {
        if !self.array.collective_support.scatter {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            match src_or_root_pe {
                ScatterInput::Root(index) => ArrayCollectiveScatterHandle {
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterState::CollectiveScatterManual(
                        CollectiveScatterManualOpHandle {
                            future: Box::pin(do_scatter(
                                self.clone(),
                                self.array.inner.data.mem_region.scheduler.clone(),
                                alloc.clone(),
                                my_ticket,
                                now_serving,
                                index,
                                len,
                                self.my_pe(),
                            )),
                            scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                            counters: self.array.inner.data.mem_region.counters.clone(),
                        },
                    ),
                    spawned: false,
                },
                ScatterInput::NotRoot(root) => ArrayCollectiveScatterHandle {
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterState::CollectiveScatterManual(
                        CollectiveScatterManualOpHandle {
                            future: Box::pin(do_scatter(
                                self.clone(),
                                self.array.inner.data.mem_region.scheduler.clone(),
                                alloc.clone(),
                                my_ticket,
                                now_serving,
                                0,
                                len,
                                root,
                            )),
                            scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                            counters: self.array.inner.data.mem_region.counters.clone(),
                        },
                    ),
                    spawned: false,
                },
            }
        } else {
            self.array.scatter_from_pe(src_or_root_pe, len)
        }
    }

    /// Scatters segments from the root PE, placing each PE's received segment into caller-supplied `buffer`.
    ///
    /// Like [`scatter_from_pe`](NativeAtomicArray::scatter_from_pe) but places the received `len`-element
    /// segment into the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// `src_or_root_pe: ScatterInput` encodes the root PE.
    /// Returns an [`ArrayCollectiveScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `num_pes * len` elements.
    /// Each PE's `buffer` must be large enough to hold `len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.scatter_from_pe_into_buffer(buf.into(), ScatterInput::Root(0), array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        buf: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.scatter {
            let sync_alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let alloc = sync_alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            match src_or_root_pe {
                ScatterInput::Root(index) => ArrayCollectiveScatterIntoBufferHandle {
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(
                        CollectiveScatterIntoBufferManualOpHandle {
                            future: Box::pin(do_scatter_in_buffer(
                                self.clone(),
                                self.array.inner.data.mem_region.scheduler.clone(),
                                alloc.clone(),
                                my_ticket,
                                now_serving,
                                index,
                                len,
                                self.my_pe(),
                                buf,
                            )),
                            scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                            counters: self.array.inner.data.mem_region.counters.clone(),
                        },
                    ),
                    spawned: false,
                },
                ScatterInput::NotRoot(root) => ArrayCollectiveScatterIntoBufferHandle {
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(
                        CollectiveScatterIntoBufferManualOpHandle {
                            future: Box::pin(do_scatter_in_buffer(
                                self.clone(),
                                self.array.inner.data.mem_region.scheduler.clone(),
                                alloc.clone(),
                                my_ticket,
                                now_serving,
                                0,
                                len,
                                root,
                                buf,
                            )),
                            scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                            counters: self.array.inner.data.mem_region.counters.clone(),
                        },
                    ),
                    spawned: false,
                },
            }
        } else {
            self.array
                .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
        }
    }
}

impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
    /// Reduce-scatter sum: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// sum is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.sum_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn sum_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.sum_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Sum,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.sum_scatter(index, len)
        }
    }

    /// Reduce-scatter product: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// product is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.prod_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn prod_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.prod_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Prod,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.prod_scatter(index, len)
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> NativeAtomicArray<T> {
    /// Reduce-scatter max: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// maximum is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.max_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn max_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.max_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter_comparison(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Max,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.max_scatter(index, len)
        }
    }

    /// Reduce-scatter min: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// minimum is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.min_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn min_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.min_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter_comparison(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::Min,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.min_scatter(index, len)
        }
    }
}

impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
    /// Reduce-scatter bitwise AND: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise AND is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_and_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn bit_and_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.bit_and_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitAnd,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_and_scatter(index, len)
        }
    }

    /// Reduce-scatter bitwise XOR: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise XOR is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_xor_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn bit_xor_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.bit_xor_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitXor,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_xor_scatter(index, len)
        }
    }

    /// Reduce-scatter bitwise OR: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise OR is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let _result = unsafe { array.bit_or_scatter(0, array.num_elems_local()) }.block();
    /// }
    ///```
    pub unsafe fn bit_or_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.collective_support.bit_or_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterHandle {
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(
                    CollectiveReduceScatterManualOpHandle {
                        future: Box::pin(do_reduce_scatter_bitwise(
                            self.clone(),
                            self.array.inner.data.mem_region.scheduler.clone(),
                            sync_alloc,
                            my_ticket,
                            now_serving,
                            index,
                            len,
                            ReduceOp::BitOr,
                        )),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    },
                ),
                spawned: false,
            }
        } else {
            self.array.bit_or_scatter(index, len)
        }
    }
}

impl<T: ElementArithmeticOps + Default> NativeAtomicArray<T> {
    /// Reduce-scatter sum, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`sum_scatter`](NativeAtomicArray::sum_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.sum_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.sum_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::Sum, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.sum_scatter_into_buffer(index, len, buffer)
        }
    }

    /// Reduce-scatter product, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`prod_scatter`](NativeAtomicArray::prod_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.prod_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.prod_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::Prod, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.prod_scatter_into_buffer(index, len, buffer)
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> NativeAtomicArray<T> {
    /// Reduce-scatter max, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`max_scatter`](NativeAtomicArray::max_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.max_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.max_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_comparison_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::Max, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.max_scatter_into_buffer(index, len, buffer)
        }
    }

    /// Reduce-scatter min, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`min_scatter`](NativeAtomicArray::min_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.min_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.min_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_comparison_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::Min, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.min_scatter_into_buffer(index, len, buffer)
        }
    }
}

impl<T: ElementBitWiseOps + Default> NativeAtomicArray<T> {
    /// Reduce-scatter bitwise AND, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`bit_and_scatter`](NativeAtomicArray::bit_and_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.bit_and_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.bit_and_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::BitAnd, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.bit_and_scatter_into_buffer(index, len, buffer)
        }
    }

    /// Reduce-scatter bitwise XOR, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`bit_xor_scatter`](NativeAtomicArray::bit_xor_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.bit_xor_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.bit_xor_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::BitXor, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.bit_xor_scatter_into_buffer(index, len, buffer)
        }
    }

    /// Reduce-scatter bitwise OR, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`bit_or_scatter`](NativeAtomicArray::bit_or_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `NativeAtomicArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// if let AtomicArray::NativeAtomicArray(array) = array {
    ///     world.barrier();
    ///     let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    ///     let _result = unsafe { array.bit_or_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    /// }
    ///```
    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.collective_support.bit_or_scatter {
            let alloc = self.array.inner.data.mem_region.get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            let (ticket_ctr, now_serving) = self
                .array
                .inner
                .data
                .mem_region
                .get_collective_ticket_state();
            let my_ticket = ticket_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ArrayCollectiveReduceScatterIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.clone(), self.array.inner.data.mem_region.scheduler.clone(), sync_alloc, my_ticket, now_serving, index, len, ReduceOp::BitOr, buffer)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            self.array.bit_or_scatter_into_buffer(index, len, buffer)
        }
    }
}
