use crate::array::atomic::*;
// use crate::array::private::ArrayExecAm;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::memregion::Dist;

// type GetFn = fn(AtomicByteArray, usize, usize) -> LamellarArcAm;
// //#[doc(hidden)]
// pub(crate) struct AtomicArrayGet {
//     pub id: TypeId,
//     pub op: GetFn,
// }
// crate::inventory::collect!(AtomicArrayGet);
// lazy_static! {
//     pub(crate) static ref GET_OPS: HashMap<TypeId, GetFn> = {
//         let mut map = HashMap::new();
//         for get in crate::inventory::iter::<AtomicArrayGet> {
//             map.insert(get.id.clone(),get.op);
//         }
//         map
//         // map.insert(TypeId::of::<f64>(), f64_add::add as AddFn );
//     };
// }

// type PutFn = fn(AtomicByteArray, usize, usize, Vec<u8>) -> LamellarArcAm;
// //#[doc(hidden)]
// pub(crate) struct AtomicArrayPut {
//     pub id: TypeId,
//     pub op: PutFn,
// }
// crate::inventory::collect!(AtomicArrayPut);
// lazy_static! {
//     pub(crate) static ref PUT_OPS: HashMap<TypeId, PutFn> = {
//         let mut map = HashMap::new();
//         for put in crate::inventory::iter::<AtomicArrayPut> {
//             map.insert(put.id.clone(),put.op);
//         }
//         map
//         // map.insert(TypeId::of::<f64>(), f64_add::add as AddFn );
//     };
// }

impl<T: Dist> LamellarArrayGet<T> for AtomicArray<T> {
    unsafe fn get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.get(index, buf),
            AtomicArray::GenericAtomicArray(array) => array.get(index, buf),
        }
    }
    fn at(&self, index: usize) -> ArrayRdmaAtHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.at(index),
            AtomicArray::GenericAtomicArray(array) => array.at(index),
        }
    }
}

impl<T: Dist> LamellarArrayPut<T> for AtomicArray<T> {
    unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.put(index, buf),
            AtomicArray::GenericAtomicArray(array) => array.put(index, buf),
        }
    }
}
