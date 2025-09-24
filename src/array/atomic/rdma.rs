use crate::array::atomic::*;
use crate::array::*;
use crate::memregion::AsLamellarBuffer;
use crate::memregion::Dist;
use crate::memregion::LamellarBuffer;
use crate::memregion::MemregionRdmaInput;
use crate::memregion::MemregionRdmaInputInner;

impl<T: Dist> AtomicArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data) }
    }
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data) }
    }
    pub unsafe fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into())
    }
    pub unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into())
    }
    pub fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
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
    ) -> ArrayRdmaPutHandle<T> {
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
    pub fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all(self, offset, data) }
    }
    pub fn put_all_unmanaged(&self, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data) }
    }
    pub unsafe fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into())
    }
    pub unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into())
    }

    pub fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get(self, index) }
    }
    pub unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems)
    }
    pub unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data)
    }
    pub unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(self, index, data)
    }

    pub unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset)
    }
    pub fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems) }
    }
    pub unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data)
    }
    pub unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(self, pe, offset, data)
    }
}

impl<T: Dist> LamellarRdmaPut<T> for AtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
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
    ) -> ArrayRdmaPutHandle<T> {
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
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
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
    ) -> ArrayRdmaPutHandle<T> {
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
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
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
    ) -> ArrayRdmaPutHandle<T> {
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

impl<T: Dist> LamellarRdmaGet<T> for AtomicArray<T> {
    unsafe fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get(array, index)
            }
        }
    }
    unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(array, index, num_elems)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(array, index, num_elems)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer(array, index, num_elems)
            }
        }
    }
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(array, index, data)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(array, index, data)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer(array, index, data)
            }
        }
    }
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(
                    array, index, data,
                )
            }
        }
    }
    unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset)
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset)
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_pe(array, pe, offset)
            }
        }
    }
    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_buffer_pe(
                    array, pe, offset, num_elems,
                )
            }
        }
    }
    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_pe(
                    array, pe, offset, data,
                )
            }
        }
    }
    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        match self {
            AtomicArray::NativeAtomicArray(array) => {
                <NativeAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data,
                )
            }
            AtomicArray::GenericAtomicArray(array) => {
                <GenericAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data,
                )
            }
            AtomicArray::NetworkAtomicArray(array) => {
                <NetworkAtomicArray<T> as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(
                    array, pe, offset, data,
                )
            }
        }
    }
}
