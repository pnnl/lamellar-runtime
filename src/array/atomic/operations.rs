use crate::array::atomic::*;
use crate::array::operations::handle::ArrayFetchOpHandle;
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for AtomicArray<T> {
    fn load<'a>(&self, index: usize) -> ArrayFetchOpHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.load(index),
            AtomicArray::GenericAtomicArray(array) => array.load(index),
        }
    }
}

impl<T: ElementOps + 'static> AccessOps<T> for AtomicArray<T> {
    fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.store(index, val),
            AtomicArray::GenericAtomicArray(array) => array.store(index, val),
        }
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for AtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for AtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for AtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for AtomicArray<T> {}
