pub(crate) mod put_handle;

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
// we used the sealed trait pattern to prevent external
// implementations of the LamellarRdmaPut and LamellarRdmaGet traits
// but they are needed to be public to be used within some of the iterators and active message implementations
// we prevent users from even calling them directly via the Sealed struct
pub(crate) mod private {
    use crate::{
        array::{
            ArrayRdmaGetBufferHandle, ArrayRdmaGetHandle, ArrayRdmaGetIntoBufferHandle,
            ArrayRdmaPutHandle,
        },
        memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
        Dist,
    };
    use enum_dispatch::enum_dispatch;

    pub struct Sealed;
    #[enum_dispatch(LamellarWriteArray<T>)]
    pub(crate) trait LamellarRdmaPut<T: Dist> {
        unsafe fn put(&self, index: usize, data: T, _marker: Sealed) -> ArrayRdmaPutHandle<T>;
        unsafe fn put_unmanaged(&self, index: usize, data: T, _marker: Sealed);
        unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
            &self,
            index: usize,
            buf: U,
            _marker: Sealed,
        ) -> ArrayRdmaPutHandle<T>;
        unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
            &self,
            index: usize,
            buf: U,
            _marker: Sealed,
        );
        unsafe fn put_pe(
            &self,
            pe: usize,
            offset: usize,
            data: T,
            _marker: Sealed,
        ) -> ArrayRdmaPutHandle<T>;
        unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T, _marker: Sealed);
        unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
            &self,
            pe: usize,
            offset: usize,
            buf: U,
            _marker: Sealed,
        ) -> ArrayRdmaPutHandle<T>;
        unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
            &self,
            pe: usize,
            offset: usize,
            buf: U,
            _marker: Sealed,
        );
        unsafe fn put_all(&self, offset: usize, data: T, _marker: Sealed) -> ArrayRdmaPutHandle<T>;
        unsafe fn put_all_unmanaged(&self, offset: usize, data: T, _marker: Sealed);
        unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
            &self,
            offset: usize,
            buf: U,
            _marker: Sealed,
        ) -> ArrayRdmaPutHandle<T>;
        unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
            &self,
            offset: usize,
            buf: U,
            _marker: Sealed,
        );
    }

    // All functions marked unsafe as it will be up to
    // the implementing Array to determine the final saftely
    // exposed to the user.

    #[doc(hidden)]
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarRdmaGet<T: Dist> {
        unsafe fn get(&self, index: usize, _marker: Sealed) -> ArrayRdmaGetHandle<T>;
        unsafe fn blocking_get(&self, index: usize, _marker: Sealed) -> T;
        unsafe fn get_buffer(
            &self,
            index: usize,
            num_elems: usize,
            _marker: Sealed,
        ) -> ArrayRdmaGetBufferHandle<T>;
        unsafe fn blocking_get_buffer(
            &self,
            index: usize,
            num_elems: usize,
            _marker: Sealed,
        ) -> Vec<T>;
        unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
            &self,
            index: usize,
            data: LamellarBuffer<T, B>,
            _marker: Sealed,
        ) -> ArrayRdmaGetIntoBufferHandle<T, B>;
        unsafe fn blocking_get_into_buffer<B: AsLamellarBuffer<T>>(
            &self,
            index: usize,
            data: LamellarBuffer<T, B>,
            _marker: Sealed,
        );
        unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
            &self,
            index: usize,
            data: LamellarBuffer<T, B>,
            _marker: Sealed,
        );

        unsafe fn get_pe(&self, pe: usize, offset: usize, _marker: Sealed)
            -> ArrayRdmaGetHandle<T>;
        unsafe fn blocking_get_pe(&self, pe: usize, offset: usize, _marker: Sealed) -> T;
        unsafe fn get_buffer_pe(
            &self,
            pe: usize,
            offset: usize,
            num_elems: usize,
            _marker: Sealed,
        ) -> ArrayRdmaGetBufferHandle<T>;
        unsafe fn blocking_get_buffer_pe(
            &self,
            pe: usize,
            offset: usize,
            num_elems: usize,
            _marker: Sealed,
        ) -> Vec<T>;
        unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
            &self,
            pe: usize,
            offset: usize,
            data: LamellarBuffer<T, B>,
            _marker: Sealed,
        ) -> ArrayRdmaGetIntoBufferHandle<T, B>;
        unsafe fn blocking_get_into_buffer_pe<B: AsLamellarBuffer<T>>(
            &self,
            pe: usize,
            offset: usize,
            data: LamellarBuffer<T, B>,
            _marker: Sealed,
        );
        unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
            &self,
            pe: usize,
            offset: usize,
            data: LamellarBuffer<T, B>,
            _marker: Sealed,
        );
    }
}
