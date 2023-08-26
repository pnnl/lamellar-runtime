use crate::array::atomic::*;
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for AtomicArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for AtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for AtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for AtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for AtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for AtomicArray<T> {}
