// use private::LamellarArrayPrivate;

use crate::array::atomic::*;
// use crate::array::private::ArrayExecAm;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::memregion::Dist;
use crate::memregion::MemregionRdmaInput;
use crate::memregion::MemregionRdmaInputInner;

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
            AtomicArray::NetworkAtomicArray(array) => array.get(index, buf),
        }
    }
    fn at(&self, index: usize) -> ArrayAtHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.at(index),
            AtomicArray::GenericAtomicArray(array) => array.at(index),
            AtomicArray::NetworkAtomicArray(array) => array.at(index),
        }
    }
}

// impl<T: Dist> LamellarArrayPut<T> for AtomicArray<T> {
//     unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>>>(
//         &self,
//         index: usize,
//         buf: U,
//     ) -> ArrayRdmaHandle<T> {
//         match self {
//             AtomicArray::NativeAtomicArray(array) => array.put(index, buf),
//             AtomicArray::GenericAtomicArray(array) => array.put(index, buf),
//             AtomicArray::NetworkAtomicArray(array) => array.put(index, buf),
//         }
//     }
// }

impl<T: Dist> AtomicArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data) }
    }
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data) }
    }
    pub unsafe fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into())
    }
    pub unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into())
    }
    pub fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe(self, pe, offset, data) }
    }
    pub fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe_unmanaged(self, pe, offset, data) }
    }
    pub unsafe fn put_pe_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        <Self as LamellarRdmaPut<T>>::put_pe_buffer(self, pe, offset, buf.into())
    }
    pub unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(self, pe, offset, buf.into())
    }
    pub fn put_all(&self, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all(self, offset, data) }
    }
    pub fn put_all_unmanaged(&self, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data) }
    }
    pub unsafe fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into())
    }
    pub unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into())
    }
}

impl<T: Dist> LamellarRdmaPut<T> for AtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data)
            }
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(array, index, data)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(array, index, data)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(array, index, data)
            }
        }
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf)
            }
        }
    }
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf,
                )
            }
        }
    }
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(array, pe, offset, data)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(array, pe, offset, data)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(array, pe, offset, data)
            }
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data,
                )
            }
        }
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(array, pe, offset, buf)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(array, pe, offset, buf)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(array, pe, offset, buf)
            }
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf,
                )
            }
        }
    }
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data)
            }
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(array, offset, data)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data,
                )
            }
        }
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(array, offset, buf)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(array, offset, buf)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(array, offset, buf)
            }
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf,
                )
            }
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
