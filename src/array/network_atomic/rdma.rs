use futures_util::future::join_all;
use parking_lot::Mutex;

use crate::array::native_atomic::rdma::{NativeAtomicRemotePePutAm, NativeAtomicRemotePutAm};
use crate::array::network_atomic::*;
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::*;
use crate::lamellae::AtomicOp;
use crate::memregion::{
    AsLamellarBuffer, Dist, LamellarBuffer, MemregionRdmaInput, MemregionRdmaInputInner,
    RemoteMemoryRegion, SubRegion,
};

impl<T: Dist> NetworkAtomicArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        unsafe { <Self as LamellarRdmaPut<T>>::put(self, index, data) }
    }
    pub fn put_unmanaged(&self, index: usize, data: T) {
        unsafe { <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data) }
    }

    //unsafe because the whole buffer is not atomically written, only individual elements are
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

    pub fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        unsafe { <Self as LamellarRdmaGet<T>>::get_pe(self, pe, offset) }
    }
    pub unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        <Self as LamellarRdmaGet<T>>::get_buffer_pe(self, pe, offset, num_elems)
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
impl<T: Dist> LamellarRdmaPut<T> for NetworkAtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let req = self
                .array
                .inner
                .data
                .mem_region
                .atomic_op(pe, offset, AtomicOp::Write(data));
            ArrayRdmaPutHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaPutState::AtomicPut(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds in LamellarArray put");
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T) {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            self.array
                .inner
                .data
                .mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::Write(data));
        } else {
            panic!("index out of bounds in LamellarArray put");
        }
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let am = self.exec_am_local(NetworkAtomicInitPutBufferAm {
            array: self.clone(),
            index: index,
            buf: buf.into(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::LocalAmPut(am),
            spawned: false,
        }
    }

    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        let _ = self
            .exec_am_local(NetworkAtomicInitPutBufferAm {
                array: self.clone(),
                index: index,
                buf: buf.into(),
            })
            .spawn();
    }
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        let req = self
            .array
            .inner
            .data
            .mem_region
            .atomic_op(pe, offset, AtomicOp::Write(data));
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::AtomicPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        let _ = self
            .array
            .inner
            .data
            .mem_region
            .atomic_op(pe, offset, AtomicOp::Write(data));
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_pe_tg(
            pe,
            NativeAtomicRemotePePutAm {
                array: Into::<NetworkAtomicByteArray>::into(self.clone()).into(),
                start_index: offset,
                data: buf.into().to_bytes(),
            },
        );
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RemoteAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        let _ = self.exec_am_pe_tg(
            pe,
            NativeAtomicRemotePePutAm {
                array: Into::<NetworkAtomicByteArray>::into(self.clone()).into(),
                start_index: offset,
                data: buf.into().to_bytes(),
            },
        );
    }
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        let req = self
            .array
            .inner
            .data
            .mem_region
            .atomic_op_all(offset, AtomicOp::Write(data));
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::AtomicPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T) {
        self.array
            .inner
            .data
            .mem_region
            .atomic_op_all_unmanaged(offset, AtomicOp::Write(data));
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self.exec_am_all_tg(NativeAtomicRemotePePutAm {
            array: Into::<NetworkAtomicByteArray>::into(self.clone()).into(),
            start_index: offset,
            data: buf.into().to_bytes(),
        });
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RemoteAmPutAll(req),
            spawned: false,
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        let _ = self
            .exec_am_all_tg(NativeAtomicRemotePePutAm {
                array: Into::<NetworkAtomicByteArray>::into(self.clone()).into(),
                start_index: offset,
                data: buf.into().to_bytes(),
            })
            .spawn();
    }
}

impl<T: Dist> LamellarRdmaGet<T> for NetworkAtomicArray<T> {
    unsafe fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let req = self
                .array
                .inner
                .data
                .mem_region
                .atomic_fetch_op(pe, offset, AtomicOp::Read);
            ArrayRdmaGetHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetState::AtomicGet(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds in LamellarArray get");
        }
    }
    unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        let req = self.exec_am_local(NetworkAtomicInitGetBufferAm {
            array: self.clone(),
            index: index,
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
        let req = self.exec_am_local(NetworkAtomicInitGetIntoBufferAm {
            array: self.clone(),
            index: index,
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
        let _ = self.get_into_buffer(index, data).spawn();
    }

    unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        let req = self
            .array
            .inner
            .data
            .mem_region
            .atomic_fetch_op(pe, offset, AtomicOp::Read);
        ArrayRdmaGetHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetState::AtomicGet(req),
            spawned: false,
        }
    }
    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        let buf = self.array.team_rt().alloc_one_sided_mem_region(num_elems);
        let req = self.exec_am_pe_tg(
            pe,
            NetworkAtomicRemoteGetBufferPeAm {
                array: self.clone().into(),
                offset,
                num_elems,
                buf: unsafe { buf.clone().to_base::<u8>() },
            },
        );
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::RemoteAmGet(req, buf),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let req = self.exec_am_pe_tg(
            pe,
            NetworkAtomicRemoteGetIntoBufferPeAm {
                array: self.clone().into(),
                offset,
                num_elems: data.len(),
            },
        );
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::RemoteAmGet(data, req),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer_unmanaged_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) {
        let _ = self.get_into_buffer_pe(pe, offset, data).spawn();
    }
}

#[lamellar_impl::AmLocalDataRT]
pub(crate) struct NetworkAtomicInitGetBufferAm<T: Dist> {
    array: NetworkAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    len: usize,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for NetworkAtomicInitGetBufferAm<T> {
    async fn exec(self) -> Vec<T> {
        let mut reqs = vec![];
        let mut cur_index = 0;
        let buf = lamellar::team.alloc_one_sided_mem_region::<T>(self.len);
        let mut bufs = vec![];
        for pe in self
            .array
            .array
            .pes_for_range(self.index, self.len)
            .into_iter()
        {
            if let Some(len) = self
                .array
                .array
                .num_elements_on_pe_for_range(pe, self.index, self.len)
            {
                let temp_buf = buf.sub_region(cur_index..cur_index + len);

                let remote_am = NetworkAtomicRemoteGetBufferAm {
                    array: self.array.clone().into(),
                    start_index: self.index,
                    len: self.len,
                    buf: unsafe { temp_buf.clone().to_base::<u8>() },
                };
                bufs.push(temp_buf.clone());
                reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                cur_index += len;
            }
        }
        let num_pes = reqs.len();
        join_all(reqs).await;
        match self.array.array.inner.distribution {
            Distribution::Block => unsafe { buf.as_slice().to_vec() },
            Distribution::Cyclic => {
                let mut data = vec![T::default(); self.len];
                for (k, buf) in bufs.iter().enumerate() {
                    let buf_slice = unsafe { buf.as_slice() };
                    for (i, val) in buf_slice.iter().enumerate() {
                        data[i * num_pes + k] = *val;
                    }
                }
                data
            }
        }
    }
}
#[lamellar_impl::AmDataRT(Debug)]
struct NetworkAtomicRemoteGetBufferAm {
    array: NetworkAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    buf: OneSidedMemoryRegion<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for NetworkAtomicRemoteGetBufferAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) {
        // println!("in NetworkAtomic remotegetam {:?} {:?}",self.start_index,self.len);
        let mut data = vec![0; self.len * self.array.orig_t.size()];
        unsafe {
            if let Some((elems, _indices)) = self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                let src_ptr = elems.as_mut_ptr();
                let dst_ptr = data.as_mut_ptr();
                for offset in (0..data.len()).step_by(self.array.orig_t.size()) {
                    self.array.orig_t.load(
                        src_ptr.offset(offset as isize),
                        dst_ptr.offset(offset as isize),
                    );
                }
            }
            self.buf.put_buffer(0, data).await;
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct NetworkAtomicRemoteGetBufferPeAm {
    array: NetworkAtomicByteArray, //inner of the indices we need to place data into
    offset: usize,
    num_elems: usize,
    buf: OneSidedMemoryRegion<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for NetworkAtomicRemoteGetBufferPeAm {
    async fn exec(self) {
        let local_ptr = unsafe { self.array.array.ptr_for_local_index(self.offset) };
        let orig_t_size = self.array.orig_t.size();
        let mut data = vec![0u8; self.num_elems * orig_t_size];
        for i in 0..self.num_elems {
            let elem_ptr = unsafe { local_ptr.add(i * orig_t_size) };
            self.array
                .orig_t
                .load(elem_ptr, unsafe { data.as_mut_ptr().add(i * orig_t_size) });
        }
        unsafe { self.buf.put_buffer(0, data).await };
    }
}

#[lamellar_impl::AmLocalDataRT]
struct NetworkAtomicInitGetIntoBufferAm<T: Dist, B: AsLamellarBuffer<T>> {
    array: NetworkAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: Mutex<LamellarBuffer<T, B>>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static, B: AsLamellarBuffer<T>> LamellarAm
    for NetworkAtomicInitGetIntoBufferAm<T, B>
{
    async fn exec(self) {
        let mut reqs = vec![];
        let mut buf = self.buf.lock().split_off(0);
        for pe in self
            .array
            .array
            .pes_for_range(self.index, buf.len())
            .into_iter()
        {
            // println!("pe {:?}",pe);
            let remote_am = NetworkAtomicRemoteGetIntoBufferAm {
                array: self.array.clone().into(),
                start_index: self.index,
                len: buf.len(),
            };
            reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
        }

        unsafe {
            match self.array.array.inner.distribution {
                Distribution::Block => {
                    let cur_index = 0;

                    let buf_slice = buf.as_mut_slice();
                    let buf_u8_slice = std::slice::from_raw_parts_mut(
                        buf_slice.as_mut_ptr() as *mut u8,
                        buf_slice.len() * std::mem::size_of::<T>(),
                    );
                    for req in reqs.drain(..) {
                        let data = req.await;
                        buf_u8_slice[cur_index..(cur_index + data.len())].copy_from_slice(&data);
                    }
                }
                Distribution::Cyclic => {
                    let buf_slice = buf.as_mut_slice();
                    let num_pes = reqs.len();
                    for (start_index, req) in reqs.drain(..).enumerate() {
                        let data = req.await;
                        let data_aligned = data.as_ptr() as usize % std::mem::align_of::<T>() == 0;
                        if data_aligned {
                            let data_t_slice = std::slice::from_raw_parts(
                                data.as_ptr() as *const T,
                                data.len() / std::mem::size_of::<T>(),
                            );
                            for (i, val) in data_t_slice.iter().enumerate() {
                                buf_slice[start_index + i * num_pes] = *val;
                            }
                        } else {
                            let data_t_ptr = data.as_ptr() as *mut T;
                            for i in 0..(data.len() / std::mem::size_of::<T>()) {
                                buf_slice[start_index + i * num_pes] =
                                    std::ptr::read_unaligned(data_t_ptr.offset(i as isize));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct NetworkAtomicRemoteGetIntoBufferAm {
    array: NetworkAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for NetworkAtomicRemoteGetIntoBufferAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) -> Vec<u8> {
        // println!("in NetworkAtomic remotegetam {:?} {:?}",self.start_index,self.len);
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _indices)) => {
                    let mut data = elems.to_vec();
                    let src_ptr = elems.as_mut_ptr();
                    let dst_ptr = data.as_mut_ptr();
                    for offset in (0..data.len()).step_by(self.array.orig_t.size()) {
                        self.array.orig_t.load(
                            src_ptr.offset(offset as isize),
                            dst_ptr.offset(offset as isize),
                        );
                    }
                    data
                }
                None => vec![],
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct NetworkAtomicRemoteGetIntoBufferPeAm {
    array: NetworkAtomicByteArray, //inner of the indices we need to place data into
    offset: usize,
    num_elems: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for NetworkAtomicRemoteGetIntoBufferPeAm {
    async fn exec(self) -> Vec<u8> {
        let local_ptr = unsafe { self.array.array.ptr_for_local_index(self.offset) };
        let orig_t_size = self.array.orig_t.size();
        let mut data = vec![0u8; self.num_elems * orig_t_size];
        for i in 0..self.num_elems {
            let elem_ptr = unsafe { local_ptr.add(i * orig_t_size) };
            self.array
                .orig_t
                .load(elem_ptr, unsafe { data.as_mut_ptr().add(i * orig_t_size) });
        }
        data
    }
}
#[lamellar_impl::AmLocalDataRT]
pub(crate) struct NetworkAtomicInitPutBufferAm<T: Dist> {
    array: NetworkAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for NetworkAtomicInitPutBufferAm<T> {
    async fn exec(self) {
        unsafe {
            // let u8_buf = self.buf.clone().to_base::<u8>();
            let mut reqs = vec![];
            match self.array.array.inner.distribution {
                Distribution::Block => {
                    let mut cur_index = 0;
                    for pe in self
                        .array
                        .array
                        .pes_for_range(self.index, self.buf.len())
                        .into_iter()
                    {
                        if let Some(len) = self.array.array.num_elements_on_pe_for_range(
                            pe,
                            self.index,
                            self.buf.len(),
                        ) {
                            // let u8_buf_len = len * std::mem::size_of::<T>();
                            // println!("pe {:?} index: {:?} len {:?} buflen {:?} putting {:?}",pe,self.index,len, self.buf.len(),&u8_buf.as_slice().unwrap()[cur_index..(cur_index+u8_buf_len)]);
                            let remote_am = NativeAtomicRemotePutAm {
                                array: Into::<NetworkAtomicByteArray>::into(self.array.clone())
                                    .into(),
                                start_index: self.index,
                                len: self.buf.len(),
                                data: self.buf.sub_region(cur_index..(cur_index + len)).to_bytes(),
                            };
                            reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                            cur_index += len;
                        } else {
                            panic!("this should not be possible");
                        }
                    }
                }
                Distribution::Cyclic => {
                    let num_pes = ArrayExecAm::team_rt(&self.array).num_pes();
                    let mut pe_u8_vecs: HashMap<usize, Vec<u8>> = HashMap::new();
                    let mut pe_t_slices: HashMap<usize, &mut [T]> = HashMap::new();
                    let buf_slice = self.buf.as_slice();
                    for pe in self
                        .array
                        .array
                        .pes_for_range(self.index, self.buf.len())
                        .into_iter()
                    {
                        if let Some(len) = self.array.array.num_elements_on_pe_for_range(
                            pe,
                            self.index,
                            self.buf.len(),
                        ) {
                            // println!("len {:?} buf len {:?} ",len,self.buf.len());
                            let mut u8_vec = vec![0u8; len * std::mem::size_of::<T>()];
                            let t_slice =
                                std::slice::from_raw_parts_mut(u8_vec.as_mut_ptr() as *mut T, len);
                            pe_u8_vecs.insert(pe, u8_vec);
                            pe_t_slices.insert(pe, t_slice);
                        }
                    }
                    for (buf_index, index) in
                        (self.index..(self.index + self.buf.len())).enumerate()
                    {
                        let pe = match self.array.array.pe_for_dist_index(index) {
                            Some(pe) => pe % num_pes,
                            None => panic!(
                                "Index: {index} is out of bounds for array of length: {:?}",
                                self.array.array.inner.size
                            ),
                        };
                        // println!("pe {:?} tslice index {:?} buf_index {:?}",pe,buf_index/num_pes,buf_index);
                        pe_t_slices.get_mut(&pe).unwrap()[buf_index / num_pes] =
                            buf_slice[buf_index];
                    }
                    for (pe, vec) in pe_u8_vecs.drain() {
                        // println!("pe {:?} vec {:?}",pe,vec);
                        let remote_am = NativeAtomicRemotePutAm {
                            array: Into::<NetworkAtomicByteArray>::into(self.array.clone()).into(), //inner of the indices we need to place data into
                            start_index: self.index,
                            len: self.buf.len(),
                            data: vec,
                        };
                        reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                    }
                }
            }
            for req in reqs.drain(..) {
                req.await;
            }
            // println!("done local put");
        }
    }
}
