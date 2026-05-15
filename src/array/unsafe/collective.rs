use crate::array::collective::broadcast_handle::{ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle, ArrayCollectiveAllToAllIntoBufferState, ArrayCollectiveAllToAllState, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle, ArrayCollectiveScatterIntoBufferState, ArrayCollectiveScatterState};
use crate::array::collective::gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle, ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState};
use crate::array::collective::reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceInPlaceState, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceIntoBufferHandle, ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState};
use crate::array::collective::reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle, ArrayCollectiveReduceScatterIntoBufferState, ArrayCollectiveReduceScatterState};
use crate::array::private::LamellarArrayPrivate;
use crate::lamellae::collective::{BroadcastInput, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput};
use crate::{AsLamellarBuffer, LamellarBuffer, UnsafeArray};
use crate::Dist;

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Sum);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Max);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Min);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Prod);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitAnd);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitXor);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitOr);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Sum, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Max, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Min, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Prod, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitAnd, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitXor, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitOr, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Sum);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Max);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Min);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Prod);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitAnd);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitXor);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitOr);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Sum, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Max, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Min, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Prod, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitOr, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitAnd, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitXor, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Sum, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Max, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Min, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Prod, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitOr, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitAnd, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitXor, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }
}

// impl<T: Dist> UnsafeArray<T> {
//     pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Sum, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Max, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Min, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Prod, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::BitOr, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::BitAnd, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::BitXor, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }
// }

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all(index, len);

        ArrayCollectiveAllGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherState::CollectiveAllGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all_into_buffer(index, len, buffer);

        ArrayCollectiveAllGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather(index, len, pe);

        ArrayCollectiveGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherState::CollectiveGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_into_buffer(index, len, dst);

        ArrayCollectiveGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn alltoall(&self,  index: usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .alltoall(index, len);

        ArrayCollectiveAllToAllHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllToAllState::CollectiveAllToAll(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn alltoall_into_buffer<B: AsLamellarBuffer<T>>(&self,  index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .alltoall_into_buffer(index, len, buffer);

        ArrayCollectiveAllToAllIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> ArrayCollectiveBroadcastHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast(src_or_root_pe, len);

        ArrayCollectiveBroadcastHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveBroadcastState::CollectiveBroadcast(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, dst: RootSrcOrLamellarBuffer<T, B>, len: usize) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_into_buffer(dst, len);

        ArrayCollectiveBroadcastIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .scatter(src_or_root_pe, len);

        ArrayCollectiveScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveScatterState::CollectiveScatter(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .scatter_into_buffer(buffer, src_or_root_pe, len);

        ArrayCollectiveScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req),
            spawned: false,
        }
    }
}


impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Sum, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn max_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Max, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn min_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Min, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Prod, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitAnd, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitXor, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitOr, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Sum, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Max, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Min, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Prod, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitAnd, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitXor, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitOr, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }
}
