use crate::array::native_atomic::*;
use crate::array::*;
use std::any::TypeId;
use std::collections::HashMap;

type BufFn = fn(NativeAtomicByteArrayWeak) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<NativeAtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct NativeAtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(NativeAtomicArrayOpBuf);

impl<T: ElementOps + 'static> ReadOnlyOps<T> for NativeAtomicArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for NativeAtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for NativeAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for NativeAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for NativeAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for NativeAtomicArray<T>
{
}
