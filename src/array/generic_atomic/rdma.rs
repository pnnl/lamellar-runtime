use std::collections::VecDeque;

use crate::array::generic_atomic::*;
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::LamellarWrite;
use crate::array::*;
use crate::lamellae::CommSlice;
use crate::memregion::{
    AsBase, Dist, MemregionRdmaInput, MemregionRdmaInputInner, RTMemoryRegionRDMA,
    RegisteredMemoryRegion,
};

impl<T: Dist> LamellarArrayInternalGet<T> for GenericAtomicArray<T> {
    unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle<T> {
        let req = self.exec_am_local(InitGetAm {
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
        // let req = self.exec_am_local(InitGetAm {
        //     array: self.clone(),
        //     index: index,
        //     buf: buf.clone().into(),
        // });
        let (pe, local_index) = self
            .pe_and_offset_for_global_index(index)
            .expect("Invalid index");
        let req = self.exec_am_pe(
            pe,
            GenericAtomicAtAm {
                array: self.clone().into(),
                local_index,
            },
        );
        ArrayAtHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayAtHandleState::Am(req),
        }
    }
}

impl<T: Dist> LamellarArrayGet<T> for GenericAtomicArray<T> {
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

impl<T: Dist> GenericAtomicArray<T> {
    pub fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T> {
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
        <Self as LamellarRdmaPut<T>>::put_pe_buffer_unmanaged(self, pe, offset, buf.into());
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
impl<T: Dist> LamellarRdmaPut<T> for GenericAtomicArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T> {
        let am = self.store(index, data);
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::StoreOp(am),
            spawned: false,
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T) {
        let _ = self.store(index, data).spawn();
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
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicRemotePePutAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                offset,
                elem_size: std::mem::size_of::<T>(),
                data: unsafe {
                    std::slice::from_raw_parts(
                        (&data as *const T) as *const u8,
                        std::mem::size_of::<T>(),
                    )
                    .to_vec()
                },
            },
        );
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::RemoteAmPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        let _ = self
            .exec_am_pe_tg(
                pe,
                GenericAtomicRemotePePutAm {
                    array: self.clone().into(), //inner of the indices we need to place data into
                    offset,
                    elem_size: std::mem::size_of::<T>(),
                    data: unsafe {
                        std::slice::from_raw_parts(
                            (&data as *const T) as *const u8,
                            std::mem::size_of::<T>(),
                        )
                        .to_vec()
                    },
                },
            )
            .spawn();
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_pe_tg(
            pe,
            GenericAtomicRemotePePutAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                offset,
                elem_size: std::mem::size_of::<T>(),
                data: buf.into().to_bytes(),
            },
        );
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::RemoteAmPut(req),
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
            GenericAtomicRemotePePutAm {
                array: self.clone().into(), //inner of the indices we need to place data into
                offset,
                elem_size: std::mem::size_of::<T>(),
                data: buf.into().to_bytes(),
            },
        );
    }
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: unsafe {
                std::slice::from_raw_parts(
                    (&data as *const T) as *const u8,
                    std::mem::size_of::<T>(),
                )
                .to_vec()
            },
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::RemoteAmPutAll(req),
            spawned: false,
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T) {
        let _ = self.exec_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: unsafe {
                std::slice::from_raw_parts(
                    (&data as *const T) as *const u8,
                    std::mem::size_of::<T>(),
                )
                .to_vec()
            },
        });
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T> {
        let req = self.exec_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: buf.into().to_bytes(),
        });
        ArrayRdmaHandle2 {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaState::RemoteAmPutAll(req),
            spawned: false,
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        let _ = self.exec_am_all_tg(GenericAtomicRemotePePutAm {
            array: self.clone().into(), //inner of the indices we need to place data into
            offset,
            elem_size: std::mem::size_of::<T>(),
            data: buf.into().to_bytes(),
        });
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicAtAm {
    array: GenericAtomicByteArray, //inner of the indices we need to place data into
    local_index: usize,            //local index
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicAtAm<T> {
    async fn exec(self) -> Vec<u8> {
        unsafe {
            let _lock = self.array.lock_index(self.local_index);
            self.array
                .array
                .element_for_local_index(self.local_index)
                .to_vec()
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Debug)]
struct InitGetAm<T: Dist> {
    array: GenericAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: LamellarMemoryRegion<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetAm<T> {
    async fn exec(self) {
        // let buf = self.buf.into();
        // let u8_index = self.index * std::mem::size_of::<T>();
        // let u8_len = self.buf.len() * std::mem::size_of::<T>();
        // println!("in generic_atomic InitGetAm");// {:?} {:?}",u8_index,u8_index + u8_len);
        let mut reqs = vec![];
        for pe in self
            .array
            .array
            .pes_for_range(self.index, self.buf.len())
            .into_iter()
        {
            // println!("pe {:?}",pe);
            let remote_am = GenericAtomicRemoteGetAm {
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
struct GenericAtomicRemoteGetAm {
    array: GenericAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemoteGetAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) -> Vec<u8> {
        // println!("in generic_atomic remotegetam {:?} {:?}",self.start_index,self.len);
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, indices)) => {
                    let mut locks = Vec::new();
                    let mut diff = None;
                    for i in indices {
                        //for simplicity lets lock all the indicies we are concerned about
                        match diff {
                            Some(diff) => {
                                // assert_eq!(i+diff,self.array.array.inner.pe_full_offset_for_local_index(self.array.array.inner.data.my_pe,i).expect("invalid local index"));
                                locks.push(self.array.locks[(i as isize + diff) as usize].lock());
                            }
                            None => {
                                let temp_i = self
                                    .array
                                    .array
                                    .inner
                                    .pe_full_offset_for_local_index(
                                        self.array.array.inner.data.my_pe,
                                        i,
                                    )
                                    .expect("invalid local index");
                                diff = Some(temp_i as isize - i as isize);
                                locks.push(self.array.locks[temp_i].lock());
                            }
                        }
                    }
                    elems.to_vec() //copy the data
                } //locks dropped
                None => vec![],
            }
        }
    }
}
#[lamellar_impl::AmLocalDataRT]
struct InitPutBufferAm<T: Dist> {
    array: GenericAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: MemregionRdmaInputInner<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutBufferAm<T> {
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
                            let remote_am = GenericAtomicRemotePutAm {
                                array: self.array.clone().into(), //inner of the indices we need to place data into
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
                        let remote_am = GenericAtomicRemotePutAm {
                            array: self.array.clone().into(), //inner of the indices we need to place data into
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

#[lamellar_impl::AmDataRT(Debug)]
struct GenericAtomicRemotePutAm {
    array: GenericAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemotePutAm {
    async fn exec(self) {
        // println!("in remote put {:?} {:?} {:?}",self.start_index,self.len,self.data);
        // let _lock = self.array.lock.write();
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, indices)) => {
                    // println!("elems: {:?}",elems);
                    let mut locks = Vec::new();
                    let mut diff = None;
                    for i in indices {
                        //for simplicity lets lock all the indicies we are concerned about
                        match diff {
                            Some(diff) => {
                                // assert_eq!(i+diff,self.array.array.inner.pe_full_offset_for_local_index(self.array.array.inner.data.my_pe,i).expect("invalid local index"));
                                locks.push(self.array.locks[(i as isize + diff) as usize].lock());
                            }
                            None => {
                                let temp_i = self
                                    .array
                                    .array
                                    .inner
                                    .pe_full_offset_for_local_index(
                                        self.array.array.inner.data.my_pe,
                                        i,
                                    )
                                    .expect("invalid local index");
                                diff = Some(temp_i as isize - i as isize);
                                locks.push(self.array.locks[temp_i].lock());
                            }
                        }
                    }
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr(),
                        elems.as_mut_ptr(),
                        elems.len(),
                    )
                }
                None => {}
            }
        }
        // println!("done remote put");
    }
}

#[lamellar_impl::AmDataRT(Debug)]
pub(crate) struct GenericAtomicRemotePePutAm {
    array: GenericAtomicByteArray, //inner of the indices we need to place data into
    offset: usize,
    elem_size: usize,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GenericAtomicRemotePePutAm {
    async fn exec(self) {
        let byte_start_index = self.offset * self.elem_size;
        let u8_slice = &mut self.array.array.mut_local_data::<u8>()
            [byte_start_index..(byte_start_index + self.data.len())];
        let src_ptr = self.data.as_ptr();
        let dst_ptr = u8_slice.as_mut_ptr();
        let mut locks = Vec::new();

        for i in self.offset..(self.offset + self.data.len() / self.elem_size) {
            locks.push(self.array.locks[i].lock());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, self.data.len());
        }
    }
}
