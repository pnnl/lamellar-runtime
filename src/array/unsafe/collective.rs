use crate::array::collective::broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveAllBroadcastIntoBufferState, ArrayCollectiveAllBroadcastState, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle, ArrayCollectiveScatterIntoBufferState, ArrayCollectiveScatterState};
use crate::array::collective::gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle, ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState};
use crate::array::collective::reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceInPlaceState, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceInPlaceState, ArrayCollectiveReduceIntoBufferHandle, ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState};
use crate::array::collective::reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle, ArrayCollectiveReduceScatterIntoBufferState, ArrayCollectiveReduceScatterState};
use crate::array::private::LamellarArrayPrivate;
use crate::lamellae::collective::{BroadcastInput, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput};
use crate::memregion::MemregionRdmaInput;
use crate::{AsLamellarBuffer, LamellarBuffer, UnsafeArray};
use crate::Dist;

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::Sum);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::Max);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::Min);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::Prod);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::BitAnd);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::BitXor);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(src.into(), ReduceOp::BitOr);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Sum, src.into(), buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Max, src.into(), buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Min, src.into(), buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::Prod, src.into(), buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::BitAnd, src.into(), buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::BitXor, src.into(), buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(ReduceOp::BitOr, src.into(), buffer);

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
    pub unsafe fn sum_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Sum, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Max, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Min, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Prod, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitOr, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitAnd, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitXor, src.into(), pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Sum, src.into(), dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Max, src.into(), dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Min, src.into(), dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Prod, src.into(), dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitOr, src.into(), dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitAnd, src.into(), dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitXor, src.into(), dst);

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
    pub unsafe fn gather_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all(src.into());

        ArrayCollectiveAllGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherState::CollectiveAllGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all_into_buffer(src.into(), buffer);

        ArrayCollectiveAllGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather(src.into(), pe);

        ArrayCollectiveGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherState::CollectiveGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, dst: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_into_buffer(src.into(), dst);

        ArrayCollectiveGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_all(&self,  src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllBroadcastHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_all(src.into());

        ArrayCollectiveAllBroadcastHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllBroadcastState::CollectiveAllBroadcast(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self,  src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_all_into_buffer(src.into(), buffer);

        ArrayCollectiveAllBroadcastIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllBroadcastIntoBufferState::CollectiveAllBroadcastIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput<T>) -> ArrayCollectiveBroadcastHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast(src_or_root_pe);

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

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn scatter_from_pe(&self, src_or_root_pe: ScatterInput<T>) -> ArrayCollectiveScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .scatter(src_or_root_pe);

        ArrayCollectiveScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveScatterState::CollectiveScatter(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput<T>) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .scatter_into_buffer(buffer, src_or_root_pe);

        ArrayCollectiveScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req),
            spawned: false,
        }
    }
}


impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Sum, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn max_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Max, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn min_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Min, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Prod, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitAnd, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitXor, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitOr, src.into(), len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Sum, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Max, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Min, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Prod, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitAnd, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitXor, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }

    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitOr, src.into(), buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req),
            spawned: false,
        }
    }
}
