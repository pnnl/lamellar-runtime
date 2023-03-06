use crate::array::atomic::*;
use crate::array::*;

use std::any::TypeId;
use std::collections::HashMap;

type BufFn = fn(AtomicByteArrayWeak) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<AtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

#[doc(hidden)]
pub struct AtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(AtomicArrayOpBuf);

impl<T: ElementOps + 'static> ReadOnlyOps<T> for AtomicArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for AtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for AtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {}

impl<T: ElementShiftOps<Result = T> + 'static> ShiftOps<T> for AtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for AtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for AtomicArray<T> {}
