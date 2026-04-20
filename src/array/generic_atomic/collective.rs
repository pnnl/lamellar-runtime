use crate::{AsLamellarBuffer, BroadcastInput, Dist, ElementArithmeticOps, ElementBitWiseOps, GenericAtomicArray, LamellarBuffer, LamellarEnv, ScatterInput, array::{collective::{algorithm::{do_all_gather, do_all_gather_in_buffer, do_all_reduce, do_all_reduce_bitwise, do_all_reduce_bitwise_in_buffer, do_all_reduce_in_buffer, do_all_to_all, do_all_to_all_in_buffer, do_broadcast, do_broadcast_in_buffer, do_gather, do_gather_in_buffer, do_reduce, do_reduce_bitwise, do_reduce_bitwise_in_buffer, do_reduce_in_buffer, do_reduce_scatter, do_reduce_scatter_bitwise, do_reduce_scatter_bitwise_in_buffer, do_reduce_scatter_in_buffer, do_scatter, do_scatter_in_buffer}, broadcast_handle::{ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle, ArrayCollectiveAllToAllIntoBufferState, ArrayCollectiveAllToAllState, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle, ArrayCollectiveScatterIntoBufferState, ArrayCollectiveScatterState, CollectiveAllToAllIntoBufferManualOpHandle, CollectiveAllToAllManualOpHandle, CollectiveBroadcastIntoBufferManualOpHandle, CollectiveBroadcastManualOpHandle, CollectiveScatterIntoBufferManualOpHandle, CollectiveScatterManualOpHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle, ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState, CollectiveAllGatherIntoBufferManualOpHandle, CollectiveAllGatherManualOpHandle, CollectiveGatherIntoBufferManualOpHandle, CollectiveGatherManualOpHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceIntoBufferHandle, ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState, CollectiveAllReduceIntoBufferManualOpHandle, CollectiveAllReduceManualOpHandle, CollectiveReduceIntoBufferManualOpHandle, CollectiveReduceManualOpHandle}, reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle, ArrayCollectiveReduceScatterIntoBufferState, ArrayCollectiveReduceScatterState, CollectiveReduceScatterIntoBufferManualOpHandle, CollectiveReduceScatterManualOpHandle}}, private::LamellarArrayPrivate}, lamellae::collective::{ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer}};


impl<T: ElementArithmeticOps> GenericAtomicArray<T> {
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce(self.clone(), sync_alloc, index, len, ReduceOp::Sum)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce(self.clone(), sync_alloc, index, len, ReduceOp::Max)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce(self.clone(), sync_alloc, index, len, ReduceOp::Min)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce(self.clone(), sync_alloc, index, len, ReduceOp::Prod)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: ElementBitWiseOps> GenericAtomicArray<T> {

    pub unsafe fn bit_and_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce_bitwise(self.clone(), sync_alloc, index, len, ReduceOp::BitAnd)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce_bitwise(self.clone(), sync_alloc, index, len, ReduceOp::BitXor)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }

    }

    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduceManual(CollectiveAllReduceManualOpHandle {
                future: Box::pin(do_all_reduce_bitwise(self.clone(), sync_alloc, index, len, ReduceOp::BitOr)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}


impl<T: ElementArithmeticOps> GenericAtomicArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Sum, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Max, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Min, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Prod, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: ElementBitWiseOps> GenericAtomicArray<T> {
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitAnd, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitXor, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveAllReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(CollectiveAllReduceIntoBufferManualOpHandle {
                future: Box::pin(do_all_reduce_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitOr, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: ElementArithmeticOps> GenericAtomicArray<T> {
    pub unsafe fn sum_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce(self.clone(), sync_alloc, index, len, pe, ReduceOp::Sum)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce(self.clone(), sync_alloc, index, len, pe, ReduceOp::Max)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce(self.clone(), sync_alloc, index, len, pe, ReduceOp::Min)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce(self.clone(), sync_alloc, index, len, pe, ReduceOp::Prod)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: ElementBitWiseOps> GenericAtomicArray<T> {
    pub unsafe fn bit_and_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce_bitwise(self.clone(), sync_alloc, index, len, pe, ReduceOp::BitAnd)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce_bitwise(self.clone(), sync_alloc, index, len, pe, ReduceOp::BitXor)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T>
    {
        let alloc = self.array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduceManual(CollectiveReduceManualOpHandle {
                future: Box::pin(do_reduce_bitwise(self.clone(), sync_alloc.clone(), index, len, pe, ReduceOp::BitOr)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}


impl<T: ElementArithmeticOps> GenericAtomicArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Sum, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Max, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Min, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Prod, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: ElementBitWiseOps> GenericAtomicArray<T> {
    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitAnd, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitXor, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(CollectiveReduceIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitOr, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}



impl<T: ElementArithmeticOps> GenericAtomicArray<T> {
    pub unsafe fn sum_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter(self.clone(), sync_alloc, index, len, ReduceOp::Sum)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn max_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter(self.clone(), sync_alloc, index, len, ReduceOp::Max)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn min_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter(self.clone(), sync_alloc, index, len, ReduceOp::Min)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn prod_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter(self.clone(), sync_alloc, index, len, ReduceOp::Prod)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: ElementBitWiseOps> GenericAtomicArray<T> {

    pub unsafe fn bit_and_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter_bitwise(self.clone(), sync_alloc, index, len, ReduceOp::BitAnd)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T>
    {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter_bitwise(self.clone(), sync_alloc, index, len, ReduceOp::BitXor)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T>
    {
        let alloc = self.array
                .inner
                .data
                .mem_region
                .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(CollectiveReduceScatterManualOpHandle {
                future: Box::pin(do_reduce_scatter_bitwise(self.clone(), sync_alloc, index, len, ReduceOp::BitOr)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}



impl<T: Dist> GenericAtomicArray<T> {
    pub unsafe fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterHandle<T> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();

        match src_or_root_pe {
            ScatterInput::Root(index) => {
                ArrayCollectiveScatterHandle{
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterState::CollectiveScatterManual(CollectiveScatterManualOpHandle {
                        future: Box::pin(do_scatter(self.clone(), alloc.clone(), index, len, self.my_pe())),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                }
            },
            ScatterInput::NotRoot(root) => {
                ArrayCollectiveScatterHandle{
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterState::CollectiveScatterManual(CollectiveScatterManualOpHandle {
                        future: Box::pin(do_scatter(self.clone(), alloc.clone(), 0, len, root)),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                }
            }
        }
    }

    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();

        match src_or_root_pe {
            ScatterInput::Root(index) => {
                ArrayCollectiveScatterIntoBufferHandle{
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(CollectiveScatterIntoBufferManualOpHandle {
                        future: Box::pin(do_scatter_in_buffer(self.clone(), alloc.clone(), index, len, self.my_pe(), buf)),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                }
            },
            ScatterInput::NotRoot(root) => {
                ArrayCollectiveScatterIntoBufferHandle{
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(CollectiveScatterIntoBufferManualOpHandle {
                        future: Box::pin(do_scatter_in_buffer(self.clone(), alloc.clone(), 0, len, root, buf)),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                }
            }
        }
    }
}


impl<T: Dist> GenericAtomicArray<T> {
    pub unsafe fn gather_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveGatherHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherState::CollectiveGatherManual(CollectiveGatherManualOpHandle {
                future: Box::pin(do_gather(self.clone(), alloc.clone(), index, len, pe)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveGatherIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBufferManual(CollectiveGatherIntoBufferManualOpHandle {
                future: Box::pin(do_gather_in_buffer(self.clone(), alloc.clone(), index, len, target)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: Dist> GenericAtomicArray<T> {
    pub unsafe fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> ArrayCollectiveBroadcastHandle<T> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();

        match src_or_root_pe {
            BroadcastInput::Root(index) => {
                ArrayCollectiveBroadcastHandle{
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveBroadcastState::CollectiveBroadcastManual(CollectiveBroadcastManualOpHandle {
                        future: Box::pin(do_broadcast(self.clone(), alloc.clone(), index, len, self.my_pe())),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                }
            },
            BroadcastInput::NotRoot(root) => {
                ArrayCollectiveBroadcastHandle{
                    array: self.array.as_lamellar_byte_array(),
                    state: ArrayCollectiveBroadcastState::CollectiveBroadcastManual(CollectiveBroadcastManualOpHandle {
                        future: Box::pin(do_broadcast(self.clone(), alloc.clone(), 0, len, root)),
                        scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                        counters: self.array.inner.data.mem_region.counters.clone(),
                    }),
                    spawned: false,
                }
            }
        }
    }

    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>, len: usize) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveBroadcastIntoBufferHandle{
                array: self.array.as_lamellar_byte_array(),
                state: ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBufferManual(CollectiveBroadcastIntoBufferManualOpHandle {
                    future: Box::pin(do_broadcast_in_buffer(self.clone(), alloc.clone(), len, target)),
                    scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                    counters: self.array.inner.data.mem_region.counters.clone(),
                }),
                spawned: false,
            }
    }
}

impl<T: Dist> GenericAtomicArray<T> {
    pub unsafe fn alltoall(&self,  index:usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveAllToAllHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllToAllState::CollectiveAllToAllManual(CollectiveAllToAllManualOpHandle {
                future: Box::pin(do_all_to_all(self.clone(), alloc.clone(), index, len)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn alltoall_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
                .get_collective_sync_alloc();

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveAllToAllIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBufferManual(CollectiveAllToAllIntoBufferManualOpHandle {
                future: Box::pin(do_all_to_all_in_buffer(self.clone(), alloc.clone(), index, len, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

impl<T: Dist> GenericAtomicArray<T> {
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();
        

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveAllGatherHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherState::CollectiveAllGatherManual(CollectiveAllGatherManualOpHandle {
                future: Box::pin(do_all_gather(self.clone(), alloc.clone(), index, len)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        let sync_alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();
        

        let alloc  = sync_alloc.unwrap();
        ArrayCollectiveAllGatherIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBufferManual(CollectiveAllGatherIntoBufferManualOpHandle {
                future: Box::pin(do_all_gather_in_buffer(self.clone(), alloc.clone(), index, len, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}



impl<T: ElementArithmeticOps> GenericAtomicArray<T> {
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Sum, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Max, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Min, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::Prod, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }

    }
}

impl<T: ElementBitWiseOps> GenericAtomicArray<T> {
    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitAnd, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }

    }

    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitXor, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let alloc = self.array
            .inner
            .data
            .mem_region
            .get_collective_sync_alloc();

        let sync_alloc = alloc.unwrap();
        ArrayCollectiveReduceScatterIntoBufferHandle{
            array: self.array.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(CollectiveReduceScatterIntoBufferManualOpHandle {
                future: Box::pin(do_reduce_scatter_bitwise_in_buffer(self.clone(), sync_alloc, index, len, ReduceOp::BitOr, buffer)),
                scheduler: self.array.inner.data.mem_region.scheduler.clone(),
                counters: self.array.inner.data.mem_region.counters.clone(),
            }),
            spawned: false,
        }
    }
}

