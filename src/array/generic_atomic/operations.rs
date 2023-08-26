use crate::array::generic_atomic::*;
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for GenericAtomicArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for GenericAtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for GenericAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for GenericAtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for GenericAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for GenericAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for GenericAtomicArray<T>
{
}
