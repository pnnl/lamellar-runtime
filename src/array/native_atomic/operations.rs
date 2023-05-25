use crate::array::native_atomic::*;
use crate::array::*;
use std::any::TypeId;
use std::collections::HashMap;

type BufFn = fn(NativeAtomicByteArrayWeak) -> Arc<dyn BufferOp>;
// type MultiMultiFn = fn(NativeAtomicByteArray,ArrayOpCmd2,Vec<u8>) -> LamellarArcAm;
// type MultiSingleFn = fn(NativeAtomicByteArray,ArrayOpCmd2,Vec<u8>,Vec<usize>) -> LamellarArcAm;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<NativeAtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };

    // pub(crate) static ref MULTIMULTIOPS: HashMap<TypeId, MultiMultiFn> = {
    //     let mut map = HashMap::new();
    //     for op in crate::inventory::iter::<NativeAtomicArrayMultiMultiOps> {
    //         map.insert(op.id.clone(), op.op);
    //     }
    //     map
    // };

    // pub(crate) static ref MULTISINGLEOPS: HashMap<TypeId, MultiSingleFn> = {
    //     let mut map = HashMap::new();
    //     for op in crate::inventory::iter::<NativeAtomicArrayMultiSingleOps> {
    //         map.insert(op.id.clone(), op.op);
    //     }
    //     map
    // };
    
}

#[doc(hidden)]
pub struct NativeAtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

// #[doc(hidden)]
// pub struct NativeAtomicArrayMultiMultiOps {
//     pub id: TypeId,
//     pub op: MultiMultiFn,
// }

// #[doc(hidden)]
// pub struct NativeAtomicArrayMultiSingleOps {
//     pub id: TypeId,
//     pub op: MultiSingleFn,
// }

crate::inventory::collect!(NativeAtomicArrayOpBuf);
// crate::inventory::collect!(NativeAtomicArrayMultiMultiOps);
// crate::inventory::collect!(NativeAtomicArrayMultiSingleOps);


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
