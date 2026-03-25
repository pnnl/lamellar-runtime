use crate::{array::{collective::{broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceIntoBufferHandle}, reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle}}, global_lock_atomic::GlobalLockCollectiveMutLocalData}, lamellae::collective::{BroadcastInput, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput}, memregion::MemregionRdmaInput, AsLamellarBuffer, Dist, LamellarBuffer};


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_all(index, len)
        }
    }
    pub fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_all(index, len)
        }
    }
    pub fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_all(index, len)
        }
    }
    pub fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_all(index, len)
        }
    }
    pub fn bit_and_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_all(index, len)
        }
    }
    pub fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_all(index, len)
        }
    }
    pub fn bit_xor_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all(index, len)
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_all_into_buffer(index, len, buffer)
        }
    }
    pub fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_all_into_buffer(index, len, buffer)
        }
    }
    pub fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_all_into_buffer(index, len, buffer)
        }
    }
    pub fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_all_into_buffer(index, len, buffer)
        }
    }
    pub fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_all_into_buffer(index, len, buffer)
        }
    }
    pub fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_all_into_buffer(index, len, buffer)
        }
    }
    pub fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all_into_buffer(index, len, buffer)
        }
    }

}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_all_in_place(src_and_dst)
        }
    }
    pub fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_all_in_place(src_and_dst)
        }
    }
    pub fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_all_in_place(src_and_dst)
        }
    }
    pub fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_all_in_place(src_and_dst)
        }
    }
    pub fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_all_in_place(src_and_dst)
        }
    }
    pub fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_all_in_place(src_and_dst)
        }
    }
    pub fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all_in_place(src_and_dst)
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe(index, len, pe)
        }
    }
    pub fn max_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_at_pe(index, len, pe)
        }
    }
    pub fn min_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_at_pe(index, len, pe)
        }
    }
    pub fn prod_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe(index, len, pe)
        }
    }
    pub fn bit_and_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe(index, len, pe)
        }
    }
    pub fn bit_or_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe(index, len, pe)
        }
    }
    pub fn bit_xor_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe(index, len, pe)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe_into_buffer(index, len, target)
        }
    }
    pub fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_at_pe_into_buffer(index, len, target)
        }
    }
    pub fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_at_pe_into_buffer(index, len, target)
        }
    }
    pub fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe_into_buffer(index, len, target)
        }
    }
    pub fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe_into_buffer(index, len, target)
        }
    }
    pub fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe_into_buffer(index, len, target)
        }
    }
    pub fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe_into_buffer(index, len, target)
        }
    }
}

// impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
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

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        unsafe {
            self
                .array
                .array
                .gather_all(index, len)
        }
    }

    pub fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .gather_all_into_buffer(index, len, buffer)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn gather_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        unsafe {
            self
                .array
                .array
                .gather_at_pe(index, len, pe)
        }
    }

    pub fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .gather_at_pe_into_buffer(index, len, target)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn broadcast_all(&self,  src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllBroadcastHandle<T> {
        unsafe {
            self
                .array
                .array
                .broadcast_all(src)
        }
    }

    pub fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .broadcast_all_into_buffer(src, buffer)
        }
    }
}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> ArrayCollectiveBroadcastHandle<T> {
        unsafe {
            self
                .array
                .array
                .broadcast_from_pe(src_or_root_pe, len)
        }
    }

    pub fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>, len: usize) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .broadcast_from_pe_into_buffer(target, len)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .scatter_from_pe(src_or_root_pe, len)
        }
    }

    pub fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
        }
    }
}



impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_scatter(index, len)
        }
    }
    pub fn max_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_scatter(index, len)
        }
    }
    pub fn min_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_scatter(index, len)
        }
    }
    pub fn prod_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_scatter(index, len)
        }
    }
    pub fn bit_and_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_scatter(index, len)
        }
    }
    pub fn bit_or_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_scatter(index, len)
        }
    }
    pub fn bit_xor_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_scatter(index, len)
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_scatter_into_buffer(index, len, buffer)
        }
    }
    pub fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_scatter_into_buffer(index, len, buffer)
        }
    }
    pub fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_scatter_into_buffer(index, len, buffer)
        }
    }
    pub fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_scatter_into_buffer(index, len, buffer)
        }
    }
    pub fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_scatter_into_buffer(index, len, buffer)
        }
    }
    pub fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_scatter_into_buffer(index, len, buffer)
        }
    }
    pub fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_scatter_into_buffer(index, len, buffer)
        }
    }

}