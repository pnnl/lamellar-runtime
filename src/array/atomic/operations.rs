use crate::array::atomic::*;
use crate::array::operations::handle::{ArrayFetchOpHandle, ArrayResultOpHandle};
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for AtomicArray<T> {
    fn load<'a>(&self, index: usize) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.load(index),
            AtomicArray::GenericAtomicArray(array) => array.load(index),
            AtomicArray::NetworkAtomicArray(array) => array.load(index),
        }
    }
    fn blocking_load(&self, index: usize) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_load(index),
            AtomicArray::GenericAtomicArray(array) => array.blocking_load(index),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_load(index),
        }
    }
}

impl<T: ElementOps + 'static> AccessOps<T> for AtomicArray<T> {
    fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.store(index, val),
            AtomicArray::GenericAtomicArray(array) => array.store(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.store(index, val),
        }
    }

    fn blocking_store(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_store(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_store(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_store(index, val),
        }
    }

    fn store_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.store_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.store_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.store_unmanaged(index, val),
        }
    }

    fn swap<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.swap(index, val),
            AtomicArray::GenericAtomicArray(array) => array.swap(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.swap(index, val),
        }
    }
    fn blocking_swap(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_swap(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_swap(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_swap(index, val),
        }
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for AtomicArray<T> {
    fn add(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.add(index, val),
            AtomicArray::GenericAtomicArray(array) => array.add(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.add(index, val),
        }
    }

    fn blocking_add(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_add(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_add(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_add(index, val),
        }
    }

    fn add_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.add_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.add_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.add_unmanaged(index, val),
        }
    }

    fn fetch_add(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_add(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_add(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_add(index, val),
        }
    }

    fn blocking_fetch_add(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_add(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_add(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_add(index, val),
        }
    }

    fn sub(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.sub(index, val),
            AtomicArray::GenericAtomicArray(array) => array.sub(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.sub(index, val),
        }
    }

    fn blocking_sub(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_sub(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_sub(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_sub(index, val),
        }
    }

    fn sub_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.sub_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.sub_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.sub_unmanaged(index, val),
        }
    }

    fn fetch_sub(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_sub(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_sub(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_sub(index, val),
        }
    }

    fn blocking_fetch_sub(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_sub(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_sub(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_sub(index, val),
        }
    }

    fn mul(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.mul(index, val),
            AtomicArray::GenericAtomicArray(array) => array.mul(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.mul(index, val),
        }
    }

    fn blocking_mul(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_mul(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_mul(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_mul(index, val),
        }
    }

    fn mul_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.mul_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.mul_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.mul_unmanaged(index, val),
        }
    }

    fn fetch_mul(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_mul(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_mul(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_mul(index, val),
        }
    }

    fn blocking_fetch_mul(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_mul(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_mul(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_mul(index, val),
        }
    }

    fn div(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.div(index, val),
            AtomicArray::GenericAtomicArray(array) => array.div(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.div(index, val),
        }
    }

    fn blocking_div(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_div(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_div(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_div(index, val),
        }
    }

    fn div_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.div_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.div_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.div_unmanaged(index, val),
        }
    }

    fn fetch_div(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_div(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_div(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_div(index, val),
        }
    }

    fn blocking_fetch_div(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_div(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_div(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_div(index, val),
        }
    }

    fn rem(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.rem(index, val),
            AtomicArray::GenericAtomicArray(array) => array.rem(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.rem(index, val),
        }
    }

    fn blocking_rem(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_rem(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_rem(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_rem(index, val),
        }
    }

    fn rem_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.rem_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.rem_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.rem_unmanaged(index, val),
        }
    }

    fn fetch_rem(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_rem(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_rem(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_rem(index, val),
        }
    }

    fn blocking_fetch_rem(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_rem(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_rem(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_rem(index, val),
        }
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {
    fn bit_and(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_and(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_and(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.bit_and(index, val),
        }
    }

    fn blocking_bit_and(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_bit_and(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_bit_and(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_bit_and(index, val),
        }
    }

    fn bit_and_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_and_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_and_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.bit_and_unmanaged(index, val),
        }
    }

    fn fetch_bit_and(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_bit_and(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_bit_and(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_bit_and(index, val),
        }
    }

    fn blocking_fetch_bit_and(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_bit_and(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_bit_and(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_bit_and(index, val),
        }
    }

    fn bit_or(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_or(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_or(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.bit_or(index, val),
        }
    }

    fn blocking_bit_or(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_bit_or(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_bit_or(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_bit_or(index, val),
        }
    }

    fn bit_or_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_or_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_or_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.bit_or_unmanaged(index, val),
        }
    }

    fn fetch_bit_or(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_bit_or(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_bit_or(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_bit_or(index, val),
        }
    }

    fn blocking_fetch_bit_or(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_bit_or(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_bit_or(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_bit_or(index, val),
        }
    }

    fn bit_xor(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_xor(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_xor(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.bit_xor(index, val),
        }
    }

    fn blocking_bit_xor(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_bit_xor(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_bit_xor(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_bit_xor(index, val),
        }
    }

    fn bit_xor_unmanaged(&self, index: usize, val: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_xor_unmanaged(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_xor_unmanaged(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.bit_xor_unmanaged(index, val),
        }
    }

    fn fetch_bit_xor(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_bit_xor(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_bit_xor(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.fetch_bit_xor(index, val),
        }
    }

    fn blocking_fetch_bit_xor(&self, index: usize, val: T) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.blocking_fetch_bit_xor(index, val),
            AtomicArray::GenericAtomicArray(array) => array.blocking_fetch_bit_xor(index, val),
            AtomicArray::NetworkAtomicArray(array) => array.blocking_fetch_bit_xor(index, val),
        }
    }
}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for AtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for AtomicArray<T> {
    fn compare_exchange(&self, index: usize, current: T, new: T) -> ArrayResultOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.compare_exchange(index, current, new),
            AtomicArray::GenericAtomicArray(array) => array.compare_exchange(index, current, new),
            AtomicArray::NetworkAtomicArray(array) => {
                array.compare_exchange(index, current, new)
            }
        }
    }

    fn blocking_compare_exchange(&self, index: usize, current: T, new: T) -> Result<T, T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                array.blocking_compare_exchange(index, current, new)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.blocking_compare_exchange(index, current, new)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                array.blocking_compare_exchange(index, current, new)
            }
        }
    }
}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for AtomicArray<T> {
    fn compare_exchange_epsilon(
        &self,
        index: usize,
        current: T,
        new: T,
        eps: T,
    ) -> ArrayResultOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                array.compare_exchange_epsilon(index, current, new, eps)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.compare_exchange_epsilon(index, current, new, eps)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                array.compare_exchange_epsilon(index, current, new, eps)
            }
        }
    }

    fn blocking_compare_exchange_epsilon(
        &self,
        index: usize,
        current: T,
        new: T,
        eps: T,
    ) -> Result<T, T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                array.blocking_compare_exchange_epsilon(index, current, new, eps)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.blocking_compare_exchange_epsilon(index, current, new, eps)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                array.blocking_compare_exchange_epsilon(index, current, new, eps)
            }
        }
    }
}
