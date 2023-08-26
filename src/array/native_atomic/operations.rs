use crate::array::native_atomic::*;
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for NativeAtomicArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for NativeAtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for NativeAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for NativeAtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for NativeAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for NativeAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for NativeAtomicArray<T>
{
}
