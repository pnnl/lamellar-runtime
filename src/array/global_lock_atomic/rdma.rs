use crate::array::global_lock_atomic::{GlobalLockArray, GlobalLockByteArray};
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::{
    ArrayAtHandle, ArrayRdmaHandle, ArrayRdmaHandle2, ArrayRdmaState, Distribution,
    LamellarArrayGet, LamellarArrayInternalGet, LamellarArrayRdmaOutput, LamellarEnv,
    LamellarRdmaPut, LamellarWrite, TeamTryInto,
};
use crate::lamellae::CommSlice;
use crate::memregion::{
    AsBase, Dist, LamellarMemoryRegion, MemregionRdmaInput, MemregionRdmaInputInner,
    RTMemoryRegionRDMA, RegisteredMemoryRegion,
};
use crate::{ActiveMessaging, LamellarArray};

use std::collections::VecDeque;

use super::{ArrayAtHandleState, InnerRdmaHandle};

impl<T: Dist> LamellarArrayInternalGet<T> for GlobalLockArray<T> {
    // fn iget<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
    //     self.iget(index, buf)
    // }
    unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle<T> {
        let req = self.exec_am_local_tg(InitGetAm {
            array: self.clone(),
            index: index,
            buf: buf.into(),
        });
        ArrayRdmaHandle {
            array: self.as_lamellar_byte_array(),
            reqs: InnerRdmaHandle::Am(VecDeque::from([req.into()])),
            spawned: false,
        }
    }
    unsafe fn internal_at(&self, index: usize) -> ArrayAtHandle<T> {
        // let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
        // let req = self.exec_am_local_tg(InitGetAm {
        //     array: self.clone(),
        //     index: index,
        //     buf: buf.clone().into(),
        // });
        let req = self.exec_am_local_tg(GlobalLockAtAm {
            array: self.clone(),
            global_index: index,
        });

        ArrayAtHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayAtHandleState::LocalAm(req),
        }
    }
}

impl<T: Dist> LamellarArrayGet<T> for GlobalLockArray<T> {
    unsafe fn get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle<T> {
        match buf.team_try_into(&self.array.team()) {
            Ok(buf) => self.internal_get(index, buf),
            Err(_) => ArrayRdmaHandle {
                array: self.as_lamellar_byte_array(),
                reqs: InnerRdmaHandle::Am(VecDeque::new()),
                spawned: false,
            },
        }
    }
    fn at(&self, index: usize) -> ArrayAtHandle<T> {
        unsafe { self.internal_at(index) }
    }
}

impl<T: Dist> GlobalLockArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data) }
    }
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data) }
    }

    pub fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_buffer(self, index, buf.into()) }
    }
    pub fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(&self, index: usize, buf: U) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_buffer_unmanaged(self, index, buf.into()) }
    }
    pub fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
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
    ) -> ArrayRdmaHandle2<T> {
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
    pub fn put_all(&self, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all(self, offset, data) }
    }
    pub fn put_all_unmanaged(&self, offset: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data) }
    }
    pub fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_buffer(self, offset, buf.into()) }
    }
    pub fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(&self, offset: usize, buf: U) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_all_buffer_unmanaged(self, offset, buf.into()) }
    }
}
impl<T: Dist> LamellarRdmaPut<T> for GlobalLockArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T> {
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
    ) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_local(InitPutBufferAm {
            array: self.clone(),
            index: index,
            buf: buf.into(),
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::LocalAmPut(req),
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
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_local_tg(InitPePutAm {
            array: self.clone(),
            offset,
            pe,
            val: data,
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::LocalAmPut(req),
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
    ) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_local_tg(InitPePutBufferAm {
            array: self.clone(),
            offset,
            pe,
            buf: buf.into(),
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::LocalAmPut(req),
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
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_local_tg(InitPutAllAm {
            array: self.clone(),
            offset,
            val: data,
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::LocalAmPut(req),
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
    ) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_local_tg(InitPutAllBufferAm {
            array: self.clone(),
            offset,
            buf: buf.into(),
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::LocalAmPut(req),
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

#[lamellar_impl::AmLocalDataRT(Debug)]
struct GlobalLockAtAm<T: Dist> {
    array: GlobalLockArray<T>, //inner of the indices we need to place data into
    global_index: usize,       //local index
}

#[lamellar_impl::rt_am_local]
impl<T: Dist> LamellarAm for GlobalLockAtAm<T> {
    async fn exec(self) -> T {
        let _lock = self.array.read_lock().await;
        unsafe { self.array.array.at(self.global_index).await }
    }
}

#[lamellar_impl::AmLocalDataRT(Debug)]
struct InitGetAm<T: Dist> {
    array: GlobalLockArray<T>, //inner of the indices we need to place data into
    index: usize,              //relative to inner
    buf: LamellarMemoryRegion<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetAm<T> {
    async fn exec(self) {
        // let buf = self.buf.into();
        // let u8_index = self.index * std::mem::size_of::<T>();
        // let u8_len = self.buf.len() * std::mem::size_of::<T>();
        // println!("in InitGetAm {:?} {:?}",u8_index,u8_index + u8_len);
        let mut reqs = vec![];
        for pe in self
            .array
            .array
            .pes_for_range(self.index, self.buf.len())
            .into_iter()
        {
            // println!("pe {:?}",pe);
            let remote_am = GlobalLockRemoteGetAm {
                array: self.array.clone().into(),
                start_index: self.index,
                len: self.buf.len(),
            };
            reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
        }
        unsafe {
            match self.array.array.inner.distribution {
                Distribution::Block => {
                    let u8_buf = self.buf.clone().to_base::<u8>();
                    let mut cur_index = 0;

                    for req in reqs.drain(..) {
                        let data = req.await;
                        // println!("data recv {:?}",data.len());
                        let _ = u8_buf
                            .put_comm_slice(
                                lamellar::current_pe,
                                cur_index,
                                CommSlice::from_slice(&data),
                            )
                            .spawn(); //we can do this conversion because we will spawn the put immediately, upon which the data buffer is free to be dropped
                        cur_index += data.len();
                    }
                }
                Distribution::Cyclic => {
                    let buf_slice = self.buf.as_mut_slice();
                    let num_pes = reqs.len();
                    for (start_index, req) in reqs.drain(..).enumerate() {
                        let data = req.await;
                        let data_t_ptr = data.as_ptr() as *const T;
                        let data_t_len = if data.len() % std::mem::size_of::<T>() == 0 {
                            data.len() / std::mem::size_of::<T>()
                        } else {
                            panic!("memory align error");
                        };
                        let data_t_slice = std::slice::from_raw_parts(data_t_ptr, data_t_len);
                        for (i, val) in data_t_slice.iter().enumerate() {
                            buf_slice[start_index + i * num_pes] = *val;
                        }
                    }
                }
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GlobalLockRemoteGetAm {
    array: GlobalLockByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GlobalLockRemoteGetAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) -> Vec<u8> {
        // println!("in remotegetam {:?} {:?}",self.start_index,self.len);
        let _lock = self.array.lock.read().await;
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _)) => elems.to_vec(),
                None => vec![],
            }
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
