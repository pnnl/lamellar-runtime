use crate::{array::{collective::{broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceIntoBufferHandle}, reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle}}, global_lock_atomic::GlobalLockCollectiveMutLocalData}, lamellae::collective::{BroadcastInput, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput}, memregion::MemregionRdmaInput, AsLamellarBuffer, Dist, LamellarBuffer};


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_all(src)
        }
    }
    pub fn max_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_all(src)
        }
    }
    pub fn min_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_all(src)
        }
    }
    pub fn prod_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_all(src)
        }
    }
    pub fn bit_and_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_all(src)
        }
    }
    pub fn bit_or_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_all(src)
        }
    }
    pub fn bit_xor_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all(src)
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_all_into_buffer(src, buffer)
        }
    }
    pub fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_all_into_buffer(src, buffer)
        }
    }
    pub fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_all_into_buffer(src, buffer)
        }
    }
    pub fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_all_into_buffer(src, buffer)
        }
    }
    pub fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_all_into_buffer(src, buffer)
        }
    }
    pub fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_all_into_buffer(src, buffer)
        }
    }
    pub fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all_into_buffer(src, buffer)
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
    pub fn sum_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe(src, pe)
        }
    }
    pub fn max_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_at_pe(src, pe)
        }
    }
    pub fn min_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_at_pe(src, pe)
        }
    }
    pub fn prod_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe(src, pe)
        }
    }
    pub fn bit_and_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe(src, pe)
        }
    }
    pub fn bit_or_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe(src, pe)
        }
    }
    pub fn bit_xor_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe(src, pe)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe_into_buffer(src, target)
        }
    }
    pub fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_at_pe_into_buffer(src, target)
        }
    }
    pub fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_at_pe_into_buffer(src, target)
        }
    }
    pub fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe_into_buffer(src, target)
        }
    }
    pub fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe_into_buffer(src, target)
        }
    }
    pub fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe_into_buffer(src, target)
        }
    }
    pub fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe_into_buffer(src, target)
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
    pub fn gather_all(&self, src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllGatherHandle<T> {
        unsafe {
            self
                .array
                .array
                .gather_all(src)
        }
    }

    pub fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .gather_all_into_buffer(src, buffer)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn gather_at_pe(&self, src: impl Into<MemregionRdmaInput<T>>, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        unsafe {
            self
                .array
                .array
                .gather_at_pe(src, pe)
        }
    }

    pub fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .gather_at_pe_into_buffer(src, target)
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
    pub fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput<T>) -> ArrayCollectiveBroadcastHandle<T> {
        unsafe {
            self
                .array
                .array
                .broadcast_from_pe(src_or_root_pe)
        }
    }

    pub fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .broadcast_from_pe_into_buffer(target)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn scatter_from_pe(&self, src_or_root_pe: ScatterInput<T>) -> ArrayCollectiveScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .scatter_from_pe(src_or_root_pe)
        }
    }

    pub fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput<T>) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .scatter_from_pe_into_buffer(buf, src_or_root_pe)
        }
    }
}



impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_scatter(src, len)
        }
    }
    pub fn max_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_scatter(src, len)
        }
    }
    pub fn min_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_scatter(src, len)
        }
    }
    pub fn prod_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_scatter(src, len)
        }
    }
    pub fn bit_and_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_scatter(src, len)
        }
    }
    pub fn bit_or_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_scatter(src, len)
        }
    }
    pub fn bit_xor_scatter(&self, src: impl Into<MemregionRdmaInput<T>>, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_scatter(src, len)
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_scatter_into_buffer(src, buffer)
        }
    }
    pub fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_scatter_into_buffer(src, buffer)
        }
    }
    pub fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_scatter_into_buffer(src, buffer)
        }
    }
    pub fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_scatter_into_buffer(src, buffer)
        }
    }
    pub fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_scatter_into_buffer(src, buffer)
        }
    }
    pub fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_scatter_into_buffer(src, buffer)
        }
    }
    pub fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_scatter_into_buffer(src, buffer)
        }
    }

}