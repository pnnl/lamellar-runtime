use crate::array::global_lock_atomic::{GlobalLockArray, GlobalLockByteArray};
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::{
    ArrayRdmaAtHandle, ArrayRdmaHandle, Distribution, LamellarArrayGet, LamellarArrayInternalGet,
    LamellarArrayInternalPut, LamellarArrayPut, LamellarArrayRdmaInput, LamellarArrayRdmaOutput,
    LamellarEnv, LamellarRead, LamellarWrite, TeamTryInto,
};
use crate::memregion::{
    AsBase, Dist, LamellarMemoryRegion, OneSidedMemoryRegion, RTMemoryRegionRDMA,
    RegisteredMemoryRegion, SubRegion,
};

use std::collections::{HashMap, VecDeque};

impl<T: Dist> LamellarArrayInternalGet<T> for GlobalLockArray<T> {
    // fn iget<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
    //     self.iget(index, buf)
    // }
    unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        let req = self.exec_am_local(InitGetAm {
            array: self.clone(),
            index: index,
            buf: buf.into(),
        });
        ArrayRdmaHandle {
            reqs: VecDeque::from([req.into()]),
        }
    }
    unsafe fn internal_at(&self, index: usize) -> ArrayRdmaAtHandle<T> {
        let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
        let req = self.exec_am_local(InitGetAm {
            array: self.clone(),
            index: index,
            buf: buf.clone().into(),
        });
        ArrayRdmaAtHandle {
            req: Some(req),
            buf: buf,
        }
    }
}

impl<T: Dist> LamellarArrayGet<T> for GlobalLockArray<T> {
    unsafe fn get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        match buf.team_try_into(&self.array.team_rt()) {
            Ok(buf) => self.internal_get(index, buf),
            Err(_) => ArrayRdmaHandle {
                reqs: VecDeque::new(),
            },
        }
    }
    fn at(&self, index: usize) -> ArrayRdmaAtHandle<T> {
        unsafe { self.internal_at(index) }
    }
}

impl<T: Dist> LamellarArrayInternalPut<T> for GlobalLockArray<T> {
    unsafe fn internal_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        let req = self.exec_am_local(InitPutAm {
            array: self.clone(),
            index: index,
            buf: buf.into(),
        });
        ArrayRdmaHandle {
            reqs: VecDeque::from([req.into()]),
        }
    }
}

impl<T: Dist> LamellarArrayPut<T> for GlobalLockArray<T> {
    unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        match buf.team_try_into(&self.array.team_rt()) {
            Ok(buf) => self.internal_put(index, buf),
            Err(_) => ArrayRdmaHandle {
                reqs: VecDeque::new(),
            },
        }
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
            reqs.push(self.array.exec_am_pe(pe, remote_am));
        }
        unsafe {
            match self.array.array.inner.distribution {
                Distribution::Block => {
                    let u8_buf = self.buf.clone().to_base::<u8>();
                    let mut cur_index = 0;
                    for req in reqs.drain(..) {
                        let data = req.await;
                        // println!("data recv {:?}",data.len());
                        u8_buf.put_slice(lamellar::current_pe, cur_index, &data);
                        cur_index += data.len();
                    }
                }
                Distribution::Cyclic => {
                    let buf_slice = self.buf.as_mut_slice().expect("array data should be on PE");
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

#[lamellar_impl::AmLocalDataRT(Debug)]
struct InitPutAm<T: Dist> {
    array: GlobalLockArray<T>, //inner of the indices we need to place data into
    index: usize,              //relative to inner
    buf: LamellarMemoryRegion<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitPutAm<T> {
    async fn exec(self) {
        // let u8_index = self.index * std::mem::size_of::<T>();
        // let u8_len = self.buf.len() * std::mem::size_of::<T>();

        unsafe {
            let u8_buf = self.buf.clone().to_base::<u8>();
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
                            let u8_buf_len = len * std::mem::size_of::<T>();
                            if u8_buf_len > crate::active_messaging::BATCH_AM_SIZE {
                                // println!("pe {:?} index: {:?} len {:?} buflen {:?} putting {:?}",pe,self.index,len, self.buf.len(),&u8_buf.as_slice().unwrap()[cur_index..(cur_index+u8_buf_len)]);
                                let remote_am = GlobalLockRemotePutAm {
                                    array: self.array.clone().into(), //inner of the indices we need to place data into
                                    start_index: self.index,
                                    len: self.buf.len(),
                                    data: u8_buf
                                        .sub_region(cur_index..(cur_index + u8_buf_len))
                                        .into(),
                                    pe: self.array.my_pe(),
                                };
                                reqs.push(self.array.exec_am_pe(pe, remote_am));
                            } else {
                                let remote_am = GlobalLockRemoteSmallPutAm {
                                    array: self.array.clone().into(), //inner of the indices we need to place data into
                                    start_index: self.index,
                                    len: self.buf.len(),
                                    data: u8_buf.as_slice().expect("array data should be on PE")
                                        [cur_index..(cur_index + u8_buf_len)]
                                        .to_vec(),
                                };
                                reqs.push(self.array.exec_am_pe(pe, remote_am));
                            }
                            cur_index += u8_buf_len;
                        } else {
                            panic!("this should not be possible");
                        }
                    }
                }
                Distribution::Cyclic => {
                    //TODO think about optimized put similar to Unsafe
                    let num_pes = ArrayExecAm::team(&self.array).num_pes();
                    let mut pe_u8_vecs: HashMap<usize, Vec<u8>> = HashMap::new();
                    let mut pe_t_slices: HashMap<usize, &mut [T]> = HashMap::new();
                    let buf_slice = self.buf.as_slice().expect("array data should be on PE");
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
                        let remote_am = GlobalLockRemoteSmallPutAm {
                            array: self.array.clone().into(), //inner of the indices we need to place data into
                            start_index: self.index,
                            len: self.buf.len(),
                            data: vec,
                        };
                        reqs.push(self.array.exec_am_pe(pe, remote_am));
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
struct GlobalLockRemotePutAm {
    array: GlobalLockByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    data: LamellarMemoryRegion<u8>,
    pe: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GlobalLockRemotePutAm {
    async fn exec(self) {
        // println!("in remote put {:?} {:?} {:?}",self.start_index,self.len,self.data);
        let _lock = self.array.lock.write().await;
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _)) => {
                    self.data.blocking_get_slice(self.pe, 0, elems);
                }
                None => {}
            }
        }
        // println!("done remote put");
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct GlobalLockRemoteSmallPutAm {
    array: GlobalLockByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for GlobalLockRemoteSmallPutAm {
    async fn exec(self) {
        // println!("in remote put {:?} {:?} {:?}",self.start_index,self.len,self.data);
        let _lock = self.array.lock.write().await;
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _)) => {
                    // println!("elems: {:?}",elems);
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
