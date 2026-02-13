use crate::{array::{collective::{broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceIntoBufferHandle}}, global_lock_atomic::GlobalLockCollectiveMutLocalData}, lamellae::collective::{RootOrLamellarBuffer, RootSrcOrLamellarBuffer}, AsLamellarBuffer, Dist, LamellarBuffer};


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_all()
        }
    }
    pub fn max_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_all()
        }
    }
    pub fn min_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_all()
        }
    }
    pub fn prod_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_all()
        }
    }
    pub fn bit_and_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_all()
        }
    }
    pub fn bit_or_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_all()
        }
    }
    pub fn bit_xor_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all()
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_all_into_buffer(buffer)
        }
    }
    pub fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_all_into_buffer(buffer)
        }
    }
    pub fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_all_into_buffer(buffer)
        }
    }
    pub fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_all_into_buffer(buffer)
        }
    }
    pub fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_all_into_buffer(buffer)
        }
    }
    pub fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_all_into_buffer(buffer)
        }
    }
    pub fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all_into_buffer(buffer)
        }
    }

}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_all_in_place()
        }
    }
    pub fn max_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_all_in_place()
        }
    }
    pub fn min_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_all_in_place()
        }
    }
    pub fn prod_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_all_in_place()
        }
    }
    pub fn bit_and_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_all_in_place()
        }
    }
    pub fn bit_or_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_all_in_place()
        }
    }
    pub fn bit_xor_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_all_in_place()
        }
    }

}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe(pe)
        }
    }
    pub fn max_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_at_pe(pe)
        }
    }
    pub fn min_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_at_pe(pe)
        }
    }
    pub fn prod_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe(pe)
        }
    }
    pub fn bit_and_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe(pe)
        }
    }
    pub fn bit_or_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe(pe)
        }
    }
    pub fn bit_xor_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe(pe)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe_into_buffer(target)
        }
    }
    pub fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .max_at_pe_into_buffer(target)
        }
    }
    pub fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .min_at_pe_into_buffer(target)
        }
    }
    pub fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe_into_buffer(target)
        }
    }
    pub fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe_into_buffer(target)
        }
    }
    pub fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe_into_buffer(target)
        }
    }
    pub fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe_into_buffer(target)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .sum_at_pe_in_place(pe)
        }
    }
    pub fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .max_at_pe_in_place(pe)
        }
    }
    pub fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .min_at_pe_in_place(pe)
        }
    }
    pub fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .prod_at_pe_in_place(pe)
        }
    }
    pub fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_and_at_pe_in_place(pe)
        }
    }
    pub fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_or_at_pe_in_place(pe)
        }
    }
    pub fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        unsafe {
            self
                .array
                .array
                .bit_xor_at_pe_in_place(pe)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn gather_all(&self) -> ArrayCollectiveAllGatherHandle<T> {
        unsafe {
            self
                .array
                .array
                .gather_all()
        }
    }

    pub fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .gather_all_into_buffer(buffer)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn gather_at_pe(&self, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        unsafe {
            self
                .array
                .array
                .gather_at_pe(pe)
        }
    }

    pub fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .gather_at_pe_into_buffer(target)
        }
    }
}

impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn broadcast_all(&self) -> ArrayCollectiveAllBroadcastHandle<T> {
        unsafe {
            self
                .array
                .array
                .broadcast_all()
        }
    }

    pub fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
        unsafe {
            self
                .array
                .array
                .broadcast_all_into_buffer(buffer)
        }
    }
}


impl<T: Dist> GlobalLockCollectiveMutLocalData<T> {
    pub fn broadcast_from_pe(&self, pe: usize) -> ArrayCollectiveBroadcastHandle<T> {
        unsafe {
            self
                .array
                .array
                .broadcast_from_pe(pe)
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