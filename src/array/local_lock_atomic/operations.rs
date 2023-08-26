use crate::array::local_lock_atomic::*;
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for LocalLockArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for LocalLockArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for LocalLockArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for LocalLockArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for LocalLockArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for LocalLockArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for LocalLockArray<T> {}
