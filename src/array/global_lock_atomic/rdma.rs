use parking_lot::Mutex;

use crate::array::global_lock_atomic::GlobalLockArray;
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::{
    ArrayRdmaGetBufferHandle, ArrayRdmaGetBufferState, ArrayRdmaGetHandle,
    ArrayRdmaGetIntoBufferHandle, ArrayRdmaGetIntoBufferState, ArrayRdmaGetState,
    ArrayRdmaPutHandle, ArrayRdmaPutState, LamellarRdmaGet, LamellarRdmaPut,
};
use crate::memregion::{
    AsLamellarBuffer, Dist, LamellarBuffer, MemregionRdmaInput, MemregionRdmaInputInner,
    RTMemoryRegionRDMA, RegisteredMemoryRegion,
};
use crate::{ActiveMessaging, LamellarArray};

impl<T: Dist> GlobalLockArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data) }
    }
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data) }
    }

    pub fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into()) }
    }
    pub fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(&self, index: usize, buf: U) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into()) }
    }
    pub fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe(self, pe, offset, data) }
    }
    pub fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe_unmanaged(self, pe, offset, data) }
    }
    pub fn put_pe_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_pe_buffer(self, pe, offset, buf.into()) }
    }
    pub fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        unsafe {
            <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(self, pe, offset, buf.into())
        }
    }
    pub fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all(self, offset, data) }
    }
    pub fn put_all_unmanaged(&self, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data) }
    }
    pub fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into()) }
    }
    pub fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(&self, offset: usize, buf: U) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into()) }
    }

    pub fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get(self, index) }
    }
    pub fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer(self, index, num_elems) }
    }
    pub fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_into_buffer(self, index, data) }
    }
    pub fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        unsafe { <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged(self, index, data) }
    }

    pub fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset) }
    }
    pub fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems) }
    }
    pub fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_into_buffer_pe(self, pe, offset, data) }
    }
    pub fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        unsafe {
            <Self as LamellarRdmaGet<T>>::get_into_buffer_unmanaged_pe(self, pe, offset, data)
        }
    }
}
impl<T: Dist> LamellarRdmaPut<T> for GlobalLockArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            self.put_pe(pe, offset, data)
        } else {
            panic!("index out of bounds");
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T) {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let _ = self.put_pe(pe, offset, data).spawn();
        } else {
            panic!("index out of bounds");
        }
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local(InitPutBufferAm {
            array: self.clone(),
            index: index,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        let _ = self
            .exec_am_local(InitPutBufferAm {
                array: self.clone(),
                index: index,
                buf: buf.into(),
            })
            .spawn();
    }
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPePutAm {
            array: self.clone(),
            offset,
            pe,
            val: data,
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        let _ = self.exec_am_local_tg(InitPePutAm {
            array: self.clone(),
            offset,
            pe,
            val: data,
        });
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPePutBufferAm {
            array: self.clone(),
            offset,
            pe,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        let _ = self.exec_am_local_tg(InitPePutBufferAm {
            array: self.clone(),
            offset,
            pe: pe,
            buf: buf.into(),
        });
    }
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPutAllAm {
            array: self.clone(),
            offset,
            val: data,
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T) {
        let _ = self.exec_am_local_tg(InitPutAllAm {
            array: self.clone(),
            offset,
            val: data,
        });
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_local_tg(InitPutAllBufferAm {
            array: self.clone(),
            offset,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        let _ = self.exec_am_local_tg(InitPutAllBufferAm {
            array: self.clone(),
            offset,
            buf: buf.into(),
        });
    }
}

impl<T: Dist> LamellarRdmaGet<T> for GlobalLockArray<T> {
    unsafe fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let req = self.exec_am_local_tg(InitGetPeAm {
                array: self.clone(),
                offset,
                pe,
            });
            ArrayRdmaGetHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetState::LocalAmGet(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds");
        }
    }
    unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        let req = self.exec_am_local_tg(InitGetBufferAm {
            array: self.clone(),
            index,
            len: num_elems,
        });
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::LocalAmGet(req),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let req = self.exec_am_local_tg(InitGetIntoBufferAm {
            array: self.clone(),
            index,
            buf: Mutex::new(data),
        });
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::LocalAmGet(req),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        let _ = self
            .exec_am_local_tg(InitGetIntoBufferAm {
                array: self.clone(),
                index,
                buf: Mutex::new(data),
            })
            .spawn();
    }

    unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        let req = self.exec_am_local_tg(InitGetPeAm {
            array: self.clone(),
            offset,
            pe,
        });
        ArrayRdmaGetHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetState::LocalAmGet(req),
            spawned: false,
        }
    }
    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        let req = self.exec_am_local_tg(InitGetBufferPeAm {
            array: self.clone(),
            offset,
            pe,
            len: num_elems,
        });
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::LocalAmGet(req),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let req = self.exec_am_local_tg(InitGetIntoBufferPeAm {
            array: self.clone(),
            offset,
            pe,
            buf: Mutex::new(data),
        });
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::LocalAmGet(req),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        let _ = self
            .exec_am_local_tg(InitGetIntoBufferPeAm {
                array: self.clone(),
                offset,
                pe,
                buf: Mutex::new(data),
            })
            .spawn();
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetPeAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetPeAm<T> {
    async fn exec(self) -> T {
        let _global_lock = self.array.read_lock().await;
        unsafe { self.array.array.get_pe(self.pe, self.offset).await }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    index: usize,
    len: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetBufferAm<T> {
    async fn exec(self) -> Vec<T> {
        let _global_lock = self.array.read_lock().await;
        unsafe { self.array.array.get_buffer(self.index, self.len).await }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetBufferPeAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    len: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetBufferPeAm<T> {
    async fn exec(self) -> Vec<T> {
        let _global_lock = self.array.read_lock().await;
        unsafe {
            self.array
                .array
                .get_buffer_pe(self.pe, self.offset, self.len)
                .await
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetIntoBufferAm<T: Dist, B: AsLamellarBuffer<T>> {
    array: GlobalLockArray<T>,
    index: usize,
    buf: Mutex<LamellarBuffer<T, B>>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static, B: AsLamellarBuffer<T>> LamellarAm for InitGetIntoBufferAm<T, B> {
    async fn exec(self) {
        let _global_lock = self.array.read_lock().await;
        let buf = self.buf.lock().split_off(0);
        unsafe {
            self.array.array.get_into_buffer(self.index, buf).await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitGetIntoBufferPeAm<T: Dist, B: AsLamellarBuffer<T>> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    buf: Mutex<LamellarBuffer<T, B>>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static, B: AsLamellarBuffer<T>> LamellarAm for InitGetIntoBufferPeAm<T, B> {
    async fn exec(self) {
        let _global_lock = self.array.read_lock().await;
        let buf = self.buf.lock().split_off(0);
        unsafe {
            self.array
                .array
                .get_into_buffer_pe(self.pe, self.offset, buf)
                .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    index: usize,
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutBufferAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            <crate::UnsafeArray<T> as LamellarRdmaPut<T>>::put_buffer(
                &self.array.array,
                self.index,
                self.buf.clone(),
            )
            .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPePutAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    val: T,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPePutAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            self.array
                .array
                .put_pe(self.pe, self.offset, self.val)
                .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutAllAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    val: T,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutAllAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            self.array.array.put_all(self.offset, self.val).await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPePutBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    pe: usize,
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPePutBufferAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            <crate::UnsafeArray<T> as LamellarRdmaPut<T>>::put_pe_buffer(
                &self.array.array,
                self.pe,
                self.offset,
                self.buf.clone(),
            )
            .await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutAllBufferAm<T: Dist> {
    array: GlobalLockArray<T>,
    offset: usize,
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutAllBufferAm<T> {
    async fn exec(self) {
        let _global_lock = self.array.write_lock().await;
        unsafe {
            <crate::UnsafeArray<T> as LamellarRdmaPut<T>>::put_all_buffer(
                &self.array.array,
                self.offset,
                self.buf.clone(),
            )
            .await;
        }
    }
}
