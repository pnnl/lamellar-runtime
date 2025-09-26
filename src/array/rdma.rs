use crate::{
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    Dist,
};

pub(crate) mod put_handle;
use enum_dispatch::enum_dispatch;
pub use put_handle::ArrayRdmaPutHandle;
pub(crate) use put_handle::ArrayRdmaPutState;
pub(crate) mod get_handle;
pub use get_handle::{ArrayRdmaGetBufferHandle, ArrayRdmaGetHandle, ArrayRdmaGetIntoBufferHandle};
pub(crate) use get_handle::{
    ArrayRdmaGetBufferState, ArrayRdmaGetIntoBufferState, ArrayRdmaGetState,
};
// All functions marked unsafe as it will be up to
// the implementing Array to determine the final saftely
// exposed to the user.

#[enum_dispatch(LamellarWriteArray<T>)]
pub(crate) trait LamellarRdmaPut<T: Dist> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T>;
    unsafe fn put_unmanaged(&self, index: usize, data: T);
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T>;
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    );
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T>;
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T);
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T>;
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    );
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T>;
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T);
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T>;
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    );
}

// All functions marked unsafe as it will be up to
// the implementing Array to determine the final saftely
// exposed to the user.

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub(crate) trait LamellarRdmaGet<T: Dist> {
    unsafe fn get(&self, index: usize) -> ArrayRdmaGetHandle<T>;
    unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T>;
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B>;
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    );

    unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T>;
    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T>;
    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B>;
    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    );
}
