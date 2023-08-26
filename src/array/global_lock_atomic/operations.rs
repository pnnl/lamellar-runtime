use crate::array::global_lock_atomic::*;
use crate::array::*;
impl<T: ElementOps + 'static> ReadOnlyOps<T> for GlobalLockArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for GlobalLockArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for GlobalLockArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for GlobalLockArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for GlobalLockArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for GlobalLockArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for GlobalLockArray<T> {}
