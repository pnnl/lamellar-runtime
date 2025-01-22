// use private::LamellarArrayPrivate;

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
    ) -> ArrayRdmaHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.get(index, buf),
            AtomicArray::GenericAtomicArray(array) => array.get(index, buf),
        }
    }
    fn at(&self, index: usize) -> ArrayAtHandle<T> {
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
    ) -> ArrayRdmaHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.put(index, buf),
            AtomicArray::GenericAtomicArray(array) => array.put(index, buf),
        }
    }
}

// impl<T: Dist> AtomicArray<T> {
//     pub fn atomic_get(&self, index: usize) -> impl Future<Output = T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.atomic_get(index),
//             AtomicArray::GenericAtomicArray(_array) => {
//                 unreachable!("atomic_get not implemented for GenericAtomicArray")
//             }
//         }
//     }

//     pub fn blocking_atomic_get(&self, index: usize) -> T {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.blocking_atomic_get(index),
//             AtomicArray::GenericAtomicArray(array) => array.inner_array().dummy_val(),
//         }
//     }

//     pub fn atomic_put(&self, index: usize, value: T) -> impl Future<Output = ()> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.atomic_put(index, value),
//             AtomicArray::GenericAtomicArray(_array) => {
//                 unreachable!("atomic_put not implemented for GenericAtomicArray")
//             }
//         }
//     }

//     pub fn blocking_atomic_put(&self, index: usize, value: T) {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.blocking_atomic_put(index, value),
//             AtomicArray::GenericAtomicArray(_array) => {
//                 unreachable!("blocking_atomic_put not implemented for GenericAtomicArray")
//             }
//         }
//     }

//     pub fn atomic_swap(&self, index: usize, value: T) -> impl Future<Output = T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.atomic_swap(index, value),
//             AtomicArray::GenericAtomicArray(_array) => {
//                 unreachable!("atomic_swap not implemented for GenericAtomicArray")
//             }
//         }
//     }

//     pub fn blocking_atomic_swap(&self, index: usize, value: T) -> T {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.blocking_atomic_swap(index, value),
//             AtomicArray::GenericAtomicArray(_array) => {
//                 unreachable!("blocking_atomic_swap not implemented for GenericAtomicArray")
//             }
//         }
//     }
// }
