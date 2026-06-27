use crate::{AsLamellarBuffer, Dist, ElementArithmeticOps, ElementBitWiseOps, LamellarBuffer, LamellarEnv, array::{collective::{algorithm::{do_all_gather, do_all_gather_in_buffer, do_all_reduce, do_all_reduce_bitwise, do_all_reduce_bitwise_in_buffer, do_all_reduce_in_buffer, do_all_to_all, do_all_to_all_in_buffer, do_broadcast, do_broadcast_in_buffer, do_gather, do_gather_in_buffer, do_reduce, do_reduce_bitwise, do_reduce_bitwise_in_buffer, do_reduce_in_buffer, do_reduce_scatter, do_reduce_scatter_bitwise, do_reduce_scatter_bitwise_in_buffer, do_reduce_scatter_in_buffer, do_scatter, do_scatter_in_buffer}, broadcast_handle::{ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle, ArrayCollectiveAllToAllIntoBufferState, ArrayCollectiveAllToAllState, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle, ArrayCollectiveScatterIntoBufferState, ArrayCollectiveScatterState, CollectiveAllToAllIntoBufferManualOpHandle, CollectiveAllToAllManualOpHandle, CollectiveBroadcastIntoBufferManualOpHandle, CollectiveBroadcastManualOpHandle, CollectiveScatterIntoBufferManualOpHandle, CollectiveScatterManualOpHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle, ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState, CollectiveAllGatherIntoBufferManualOpHandle, CollectiveAllGatherManualOpHandle, CollectiveGatherIntoBufferManualOpHandle, CollectiveGatherManualOpHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceIntoBufferHandle, ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState, CollectiveAllReduceIntoBufferManualOpHandle, CollectiveAllReduceManualOpHandle, CollectiveReduceIntoBufferManualOpHandle, CollectiveReduceManualOpHandle}, reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle, ArrayCollectiveReduceScatterIntoBufferState, ArrayCollectiveReduceScatterState, CollectiveReduceScatterIntoBufferManualOpHandle, CollectiveReduceScatterManualOpHandle}}, global_lock_atomic::GlobalLockCollectiveMutLocalData, private::LamellarArrayPrivate}, lamellae::collective::{BroadcastInput, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput}};

impl<T: ElementArithmeticOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.sum_all(0, 1).block();
    ///```
    pub fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_sum {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Sum)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .sum_all(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.max_all(0, 1).block();
    ///```
    pub fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_max {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Max)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .max_all(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.min_all(0, 1).block();
    ///```
    pub fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_min {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Min)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .min_all(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.prod_all(0, 1).block();
    ///```
    pub fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_prod {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Prod)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .prod_all(index, len)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.bit_and_all(0, 1).block();
    ///```
    pub fn bit_and_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_bit_and {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitAnd)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .bit_and_all(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.bit_or_all(0, 1).block();
    ///```
    pub fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_bit_or {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitOr)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .bit_or_all(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.bit_xor_all(0, 1).block();
    ///```
    pub fn bit_xor_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        if ! self.array.array.collective_support.all_bit_xor {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                    future: Box::pin(do_all_reduce_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitXor)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .bit_xor_all(index, len)
            }
        }
    }

}


impl<T: ElementArithmeticOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.sum_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_sum {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Sum, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .sum_all_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.max_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_max {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Max, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .max_all_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.min_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_min {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Min, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .min_all_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.prod_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_prod {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Prod, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .prod_all_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_and_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_bit_and {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitAnd, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .bit_and_all_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_or_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_bit_or {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitOr, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .bit_or_all_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_xor_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        if ! self.array.array.collective_support.all_bit_xor {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllReduceIntoBufferHandle{
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_all_reduce_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitXor, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        }
        else {
            unsafe {
                self
                    .array
                    .array
                    .bit_xor_all_into_buffer(index, len, buffer)
            }
        }
    }

}

impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.sum_all_in_place(buf).block();
    ///```
    pub fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_all_in_place(src_and_dst)
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.max_all_in_place(buf).block();
    ///```
    pub fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_all_in_place(src_and_dst)
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.min_all_in_place(buf).block();
    ///```
    pub fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_all_in_place(src_and_dst)
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.prod_all_in_place(buf).block();
    ///```
    pub fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_all_in_place(src_and_dst)
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_and_all_in_place(buf).block();
    ///```
    pub fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_all_in_place(src_and_dst)
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_or_all_in_place(buf).block();
    ///```
    pub fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_all_in_place(src_and_dst)
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_xor_all_in_place(buf).block();
    ///```
    pub fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all_in_place(src_and_dst)
        }
    }

}


impl<T: ElementArithmeticOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.sum_at_pe(0, 1, 0).block();
    ///```
    pub fn sum_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.sum {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::Sum)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .sum_at_pe(index, len, pe)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.max_at_pe(0, 1, 0).block();
    ///```
    pub fn max_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.max {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::Max)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .max_at_pe(index, len, pe)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.min_at_pe(0, 1, 0).block();
    ///```
    pub fn min_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.min {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::Min)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .min_at_pe(index, len, pe)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.prod_at_pe(0, 1, 0).block();
    ///```
    pub fn prod_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.prod {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::Prod)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .prod_at_pe(index, len, pe)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.bit_and_at_pe(0, 1, 0).block();
    ///```
    pub fn bit_and_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.bit_and {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::BitAnd)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_and_at_pe(index, len, pe)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.bit_or_at_pe(0, 1, 0).block();
    ///```
    pub fn bit_or_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.bit_or {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::BitOr)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_or_at_pe(index, len, pe)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.bit_xor_at_pe(0, 1, 0).block();
    ///```
    pub fn bit_xor_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        if !self.array.array.collective_support.bit_xor {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                    future: Box::pin(do_reduce_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe, ReduceOp::BitXor)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_xor_at_pe(index, len, pe)
            }
        }
    }
}

impl<T: ElementArithmeticOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.sum_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.sum {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Sum, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .sum_at_pe_into_buffer(index, len, target)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.max_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.max {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Max, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .max_at_pe_into_buffer(index, len, target)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.min_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.min {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Min, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .min_at_pe_into_buffer(index, len, target)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.prod_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.prod {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Prod, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .prod_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.bit_and_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.bit_and {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitAnd, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_and_at_pe_into_buffer(index, len, target)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.bit_or_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.bit_or {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitOr, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_or_at_pe_into_buffer(index, len, target)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.bit_xor_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.bit_xor {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitXor, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_xor_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

// impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
//     pub fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .sum_at_pe_in_place(pe)
//         }
//     }
//     pub fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .max_at_pe_in_place(pe)
//         }
//     }
//     pub fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .min_at_pe_in_place(pe)
//         }
//     }
//     pub fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .prod_at_pe_in_place(pe)
//         }
//     }
//     pub fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .bit_and_at_pe_in_place(pe)
//         }
//     }
//     pub fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .bit_or_at_pe_in_place(pe)
//         }
//     }
//     pub fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         unsafe {
//             self
//                 .array
//                 .array
//                 .bit_xor_at_pe_in_place(pe)
//         }
//     }
// }

impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.gather_all(0, 1).block();
    ///```
    pub fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        if !self.array.array.collective_support.allgather {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllGatherHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllGatherState::CollectiveAllGatherManual(CollectiveAllGatherManualOpHandle {
                    future: Box::pin(do_all_gather(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .gather_all(index, len)
            }
        }
    }

    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.gather_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.allgather {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllGatherIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBufferManual(CollectiveAllGatherIntoBufferManualOpHandle {
                    future: Box::pin(do_all_gather_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .gather_all_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.gather_at_pe(0, 1, 0).block();
    ///```
    pub fn gather_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        if !self.array.array.collective_support.gather {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveGatherHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveGatherState::CollectiveGatherManual(CollectiveGatherManualOpHandle {
                    future: Box::pin(do_gather(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, pe)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .gather_at_pe(index, len, pe)
            }
        }
    }

    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.gather_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)).block();
    ///```
    pub fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.gather {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveGatherIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBufferManual(CollectiveGatherIntoBufferManualOpHandle {
                    future: Box::pin(do_gather_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .gather_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.broadcast_all(0, 1).block();
    ///```
    pub fn broadcast_all(&self,  index: usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        if !self.array.array.collective_support.alltoall {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllToAllHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllToAllState::CollectiveAllToAllManual(CollectiveAllToAllManualOpHandle {
                    future: Box::pin(do_all_to_all(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .alltoall(index, len)
            }
        }
    }

    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.broadcast_all_into_buffer(0, 1, buf).block();
    ///```
    pub fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.alltoall {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveAllToAllIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBufferManual(CollectiveAllToAllIntoBufferManualOpHandle {
                    future: Box::pin(do_all_to_all_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
        
            unsafe {
                self
                    .array
                    .array
                    .alltoall_into_buffer(index, len, buffer)
            }
        }
    }
}


impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.broadcast_from_pe(BroadcastInput::Root(0), 1).block();
    ///```
    pub fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> ArrayCollectiveBroadcastHandle<T> {
        let alloc = self.array
            .array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();

        match src_or_root_pe {
            BroadcastInput::Root(index) => ArrayCollectiveBroadcastHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveBroadcastState::CollectiveBroadcastManual(CollectiveBroadcastManualOpHandle {
                    future: Box::pin(do_broadcast(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, self.array.array.my_pe())),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            },
            BroadcastInput::NotRoot(root) => ArrayCollectiveBroadcastHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveBroadcastState::CollectiveBroadcastManual(CollectiveBroadcastManualOpHandle {
                    future: Box::pin(do_broadcast(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, 0, len, root)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            },
        }
    }

    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.broadcast_from_pe_into_buffer(RootSrcOrLamellarBuffer::Root(0), 1).block();
    ///```
    pub fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>, len: usize) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.broadcast {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveBroadcastIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBufferManual(CollectiveBroadcastIntoBufferManualOpHandle {
                    future: Box::pin(do_broadcast_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, len, target)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .broadcast_from_pe_into_buffer(target, len)
            }
        }
    }
}

impl<T: Dist + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// guard.scatter_from_pe(ScatterInput::Root(0), 1).block();
    ///```
    pub fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterHandle<T> {
        if !self.array.array.collective_support.scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();

            match src_or_root_pe {
                ScatterInput::Root(index) => ArrayCollectiveScatterHandle {
                    array: self.array.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterState::CollectiveScatterManual(CollectiveScatterManualOpHandle {
                        future: Box::pin(do_scatter(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, self.array.array.my_pe())),
                        scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                },
                ScatterInput::NotRoot(root) => ArrayCollectiveScatterHandle {
                    array: self.array.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterState::CollectiveScatterManual(CollectiveScatterManualOpHandle {
                        future: Box::pin(do_scatter(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, 0, len, root)),
                        scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                },
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .scatter_from_pe(src_or_root_pe, len)
            }
        }
    }

    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.scatter_from_pe_into_buffer(buf, ScatterInput::Root(0), 1).block();
    ///```
    pub fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();

            match src_or_root_pe {
                ScatterInput::Root(index) => ArrayCollectiveScatterIntoBufferHandle {
                    array: self.array.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(CollectiveScatterIntoBufferManualOpHandle {
                        future: Box::pin(do_scatter_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, self.array.array.my_pe(), buf)),
                        scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                },
                ScatterInput::NotRoot(root) => ArrayCollectiveScatterIntoBufferHandle {
                    array: self.array.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(CollectiveScatterIntoBufferManualOpHandle {
                        future: Box::pin(do_scatter_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, 0, len, root, buf)),
                        scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                },
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            }
        }
    }
}



impl<T: ElementArithmeticOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.sum_scatter(0, 1).block();
    ///```
    pub fn sum_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.sum_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Sum)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .sum_scatter(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.max_scatter(0, 1).block();
    ///```
    pub fn max_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.max_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Max)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .max_scatter(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.min_scatter(0, 1).block();
    ///```
    pub fn min_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.min_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Min)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .min_scatter(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.prod_scatter(0, 1).block();
    ///```
    pub fn prod_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.prod_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Prod)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .prod_scatter(index, len)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.bit_and_scatter(0, 1).block();
    ///```
    pub fn bit_and_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.bit_and_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitAnd)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_and_scatter(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.bit_or_scatter(0, 1).block();
    ///```
    pub fn bit_or_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.bit_or_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitOr)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_or_scatter(index, len)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let _result = guard.bit_xor_scatter(0, 1).block();
    ///```
    pub fn bit_xor_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        if !self.array.array.collective_support.bit_xor_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitXor)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_xor_scatter(index, len)
            }
        }
    }

}


impl<T: ElementArithmeticOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.sum_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.sum_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Sum, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .sum_scatter_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.max_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.max_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Max, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .max_scatter_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.min_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.min_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Min, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .min_scatter_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.prod_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.prod_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::Prod, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .prod_scatter_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> GlobalLockCollectiveMutLocalData<T> {
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_and_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.bit_and_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitAnd, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_and_scatter_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_or_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.bit_or_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitOr, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_or_scatter_into_buffer(index, len, buffer)
            }
        }
    }
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// let guard = array.collective_write_local_data().block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// guard.bit_xor_scatter_into_buffer(0, 1, buf).block();
    ///```
    pub fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        if !self.array.array.collective_support.bit_xor_scatter {
            let alloc = self.array
                .array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

            let sync_alloc = alloc.unwrap();
            ArrayCollectiveReduceScatterIntoBufferHandle {
                array: self.array.array.as_lamellar_byte_array(),
                state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                    future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.array.clone(), self.array.array.inner.data.mem_region.scheduler.clone(), sync_alloc, index, len, ReduceOp::BitXor, buffer)),
                    scheduler: self.array.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
        } else {
            unsafe {
                self
                    .array
                    .array
                    .bit_xor_scatter_into_buffer(index, len, buffer)
            }
        }
    }

}