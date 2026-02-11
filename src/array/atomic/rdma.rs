use crate::array::atomic::*;
use crate::array::rdma::private::{LamellarRdmaGet, LamellarRdmaPut, Sealed};
use crate::array::*;
use crate::memregion::AsLamellarBuffer;
use crate::memregion::Dist;
use crate::memregion::LamellarBuffer;
use crate::memregion::MemregionRdmaInput;
use crate::memregion::MemregionRdmaInputInner;

impl<T: Dist> AtomicArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data, Sealed) }
    }
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data, Sealed) }
    }
    pub unsafe fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into(), Sealed)
    }
    pub unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into(), Sealed)
    }
    pub fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe(self, pe, offset, data, Sealed) }
    }
    pub fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe_unmanaged(self, pe, offset, data, Sealed) }
    }
    pub unsafe fn put_pe_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_pe_buffer(self, pe, offset, buf.into(), Sealed)
    }
    pub unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(self, pe, offset, buf.into(), Sealed)
    }
    pub fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all(self, offset, data, Sealed) }
    }
    pub fn put_all_unmanaged(&self, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data, Sealed) }
    }
    pub unsafe fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into(), Sealed)
    }
    pub unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into(), Sealed)
    }

    pub fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get(self, index, Sealed) }
    }
    pub fn blocking_get(&self, index: usize) -> T {
        unsafe { <Self as LamellarRdmaGet<T>>::blocking_get(self, index, Sealed) }
    }

    //we are saying these are unsafe because only elements are atomically updated, not buffers
    pub unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems, Sealed)
    }
    pub unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data, Sealed)
    }
    pub unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(self, index, data, Sealed)
    }

    pub unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset, Sealed)
    }
    pub unsafe fn blocking_get_pe(&self, pe: usize, offset: usize) -> T {
        <Self as LamellarRdmaGet<T>>::blocking_get_pe(self, pe, offset, Sealed)
    }
    pub fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems, Sealed) }
    }
    pub unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data, Sealed)
    }
    pub unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(self, pe, offset, data, Sealed)
    }
}

impl<T: Dist> LamellarRdmaPut<T> for AtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put(array, index, data, Sealed)
            }
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T, _: Sealed) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_unmanaged(
                    array, index, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer(array, index, buf, Sealed)
            }
        }
    }
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_buffer_unmanaged(
                    array, index, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T, _: Sealed) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_unmanaged(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                    array, pe, offset, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(
                    array, pe, offset, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_all(&self, offset: usize, data: T, _: Sealed) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all(array, offset, data, Sealed)
            }
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T, _: Sealed) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_unmanaged(
                    array, offset, data, Sealed,
                )
            }
        }
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) -> ArrayRdmaPutHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                    array, offset, buf, Sealed,
                )
            }
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(
                    array, offset, buf, Sealed,
                )
            }
        }
    }
}

impl<T: Dist> LamellarRdmaGet<T> for AtomicArray<T> {
    unsafe fn get(&self, index: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index, Sealed)
            }
        }
    }

    unsafe fn blocking_get(&self, index: usize, _: Sealed) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get(array, index, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get(array, index, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get(array, index, Sealed)
            }
        }
    }

    unsafe fn get_buffer(
        &self,
        index: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_buffer(&self, index: usize, num_elems: usize, _: Sealed) -> Vec<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer(
                    array, index, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(
                    array, index, data, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer(
                    array, index, data, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data, Sealed,
                )
            }
        }
    }

    unsafe fn get_pe(&self, pe: usize, offset: usize, _: Sealed) -> ArrayRdmaGetHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset, Sealed)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset, Sealed)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset, Sealed)
            }
        }
    }

    unsafe fn blocking_get_pe(&self, pe: usize, offset: usize, _: Sealed) -> T {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(
                    array, pe, offset, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(
                    array, pe, offset, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_pe(
                    array, pe, offset, Sealed,
                )
            }
        }
    }

    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> ArrayRdmaGetBufferHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
        _: Sealed,
    ) -> Vec<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_buffer_pe(
                    array, pe, offset, num_elems, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }

    unsafe fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::blocking_get_into_buffer_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }

    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
        _: Sealed,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data, Sealed,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data, Sealed,
                )
            }
        }
    }
}
