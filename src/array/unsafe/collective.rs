use crate::array::collective::broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveAllBroadcastIntoBufferState, ArrayCollectiveAllBroadcastState, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState};
use crate::array::collective::gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle, ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState};
use crate::array::collective::reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceInPlaceState, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceInPlaceState, ArrayCollectiveReduceIntoBufferHandle, ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState};
use crate::array::private::LamellarArrayPrivate;
use crate::lamellae::collective::{CollectiveAllReduceOpHandle, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer};
use crate::memregion::buffer;
use crate::{AsLamellarBuffer, LamellarBuffer, UnsafeArray};
use crate::Dist;

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::Sum);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::Max);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::Min);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::Prod);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::BitAnd);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::BitXor);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(ReduceOp::BitOr);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Sum, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Max, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Min, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Prod, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::BitAnd, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::BitXor, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::BitOr, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::Sum);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::Max);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::Min);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::Prod);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::BitAnd);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::BitXor);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(ReduceOp::BitOr);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Sum, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Max, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Min, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Prod, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitOr, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitAnd, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitXor, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Sum, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Max, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Min, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Prod, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitOr, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitAnd, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitXor, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::Sum, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::Max, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::Min, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::Prod, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::BitOr, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::BitAnd, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_in_place(ReduceOp::BitXor, pe);

        ArrayCollectiveReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_all(&self) -> ArrayCollectiveAllGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all();

        ArrayCollectiveAllGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherState::CollectiveAllGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all_into_buffer(buffer);

        ArrayCollectiveAllGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_at_pe(&self, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather(pe);

        ArrayCollectiveGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherState::CollectiveGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_into_buffer(dst);

        ArrayCollectiveGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_all(&self) -> ArrayCollectiveAllBroadcastHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_all();

        ArrayCollectiveAllBroadcastHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllBroadcastState::CollectiveAllBroadcast(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_all_into_buffer(buffer);

        ArrayCollectiveAllBroadcastIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllBroadcastIntoBufferState::CollectiveAllBroadcastIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_from_pe(&self, pe: usize) -> ArrayCollectiveBroadcastHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast(pe);

        ArrayCollectiveBroadcastHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveBroadcastState::CollectiveBroadcast(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootSrcOrLamellarBuffer<T, B>) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_into_buffer(dst);

        ArrayCollectiveBroadcastIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req),
            spawned: false,
        }
    }
}