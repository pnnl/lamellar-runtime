use std::collections::VecDeque;

use crate::array::network_atomic::*;
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::LamellarWrite;
use crate::array::*;
use crate::lamellae::comm::CommAtomic;
use crate::lamellae::CommSlice;
use crate::memregion::{AsBase, Dist, RTMemoryRegionRDMA, RegisteredMemoryRegion};

impl<T: Dist> LamellarArrayInternalGet<T> for NetworkAtomicArray<T> {
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
        let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
        if self.array.team_rt().lamellae.comm().atomic_avail::<T>() {
            // self.network_atomic_load(index, &buf);

            ArrayAtHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayAtHandleState::NetworkAtomic(ArrayRdmaNetworkAtomicLoadHandle {
                    array: self.clone(),
                    index: index,
                }),
                buf: buf,
            }
        } else {
            let req = self.exec_am_local(InitGetAm {
                array: self.clone(),
                index: index,
                buf: buf.clone().into(),
            });
            ArrayAtHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayAtHandleState::Am(Some(req)),
                buf: buf,
            }
        }
    }
}
impl<T: Dist> LamellarArrayGet<T> for NetworkAtomicArray<T> {
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

impl<T: Dist> LamellarArrayInternalPut<T> for NetworkAtomicArray<T> {
    unsafe fn internal_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle<T> {
        let req = self.exec_am_local(InitPutAm {
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
}

impl<T: Dist> LamellarArrayPut<T> for NetworkAtomicArray<T> {
    unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle<T> {
        match buf.team_try_into(&self.array.team()) {
            Ok(buf) => self.internal_put(index, buf),
            Err(_) => ArrayRdmaHandle {
                array: self.as_lamellar_byte_array(),
                reqs: InnerRdmaHandle::Am(VecDeque::new()),
                spawned: false,
            },
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Debug)]
struct InitGetAm<T: Dist> {
    array: NetworkAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
    buf: LamellarMemoryRegion<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitGetAm<T> {
    async fn exec(self) {
        // let buf = self.buf.into();
        // let u8_index = self.index * std::mem::size_of::<T>();
        // let u8_len = self.buf.len() * std::mem::size_of::<T>();
        // println!("in Network InitGetAm ");//{:?} {:?}",u8_index,u8_index + u8_len);
        let mut reqs = vec![];
        for pe in self
            .array
            .array
            .pes_for_range(self.index, self.buf.len())
            .into_iter()
        {
            // println!("pe {:?}",pe);
            let remote_am = NetworkAtomicRemoteGetAm {
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
struct NetworkAtomicRemoteGetAm {
    array: NetworkAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for NetworkAtomicRemoteGetAm {
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

#[lamellar_impl::AmLocalDataRT(Debug)]
struct InitPutAm<T: Dist> {
    array: NetworkAtomicArray<T>, //inner of the indices we need to place data into
    index: usize,                 //relative to inner
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
                            // println!("pe {:?} index: {:?} len {:?} buflen {:?} putting {:?}",pe,self.index,len, self.buf.len(),&u8_buf.as_slice().unwrap()[cur_index..(cur_index+u8_buf_len)]);
                            let remote_am = NetworkAtomicRemotePutAm {
                                array: self.array.clone().into(), //inner of the indices we need to place data into
                                start_index: self.index,
                                len: self.buf.len(),
                                data: u8_buf.as_slice()[cur_index..(cur_index + u8_buf_len)]
                                    .to_vec(),
                            };
                            reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
                            cur_index += u8_buf_len;
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
                        let remote_am = NetworkAtomicRemotePutAm {
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
struct NetworkAtomicRemotePutAm {
    array: NetworkAtomicByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for NetworkAtomicRemotePutAm {
    async fn exec(self) {
        // println!("in remote put {:?} {:?} {:?}",self.start_index,self.len,self.data);
        // let _lock = self.array.lock.write();
        unsafe {
            match self
                .array
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _indices)) => {
                    // println!("elems: {:?}",elems);
                    let src_ptr = self.data.as_ptr();
                    let dst_ptr = elems.as_mut_ptr();
                    for offset in (0..elems.len()).step_by(self.array.orig_t.size()) {
                        self.array.orig_t.store(
                            src_ptr.offset(offset as isize),
                            dst_ptr.offset(offset as isize),
                        );
                    }
                    // std::ptr::copy_nonoverlapping(
                    //     self.data.as_ptr(),
                    //     elems.as_mut_ptr(),
                    //     elems.len(),
                    // )
                }
                None => {}
            }
        }
        // println!("done remote put");
    }
}

// impl<T: Dist> NetworkAtomicArray<T> {
//     pub(crate) fn network_atomic_store(&self, index: usize, val: T) {
//         if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
//             let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);

//             unsafe {
//                 buf.as_mut_slice()[0] = val;
//                 if pe == self.array.my_pe() {
//                     self.local_data().at(index).store(val);
//                 } else {
//                     self.array
//                         .inner
//                         .data
//                         .mem_region
//                         .atomic_store(pe, offset, buf);
//                 }
//             }
//         } else {
//             panic!("invalid index");
//         }
//     }

//     pub(crate) fn network_iatomic_store(&self, index: usize, val: T) {
//         if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
//             let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);

//             unsafe {
//                 buf.as_mut_slice()[0] = val;
//                 if pe == self.array.my_pe() {
//                     self.local_data().at(index).store(val);
//                 } else {
//                     self.array
//                         .inner
//                         .data
//                         .mem_region
//                         .iatomic_store(pe, offset, buf);
//                 }
//             }
//         } else {
//             panic!("invalid index");
//         }
//     }

//     pub(crate) fn network_atomic_load(&self, index: usize, val: &OneSidedMemoryRegion<T>) {
//         if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
//             self.array
//                 .inner
//                 .data
//                 .mem_region
//                 .atomic_load(pe, offset, val);
//         } else {
//             panic!("invalid index");
//         }
//     }

//     pub(crate) fn network_iatomic_load(&self, index: usize, val: &OneSidedMemoryRegion<T>) {
//         if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
//             self.array
//                 .inner
//                 .data
//                 .mem_region
//                 .iatomic_load(pe, offset, val);
//         } else {
//             panic!("invalid index");
//         }
//     }

//     pub(crate) fn network_atomic_swap(&self, index: usize, val: &OneSidedMemoryRegion<T>) {
//         if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
//             unsafe {
//                 self.array
//                     .inner
//                     .data
//                     .mem_region
//                     .atomic_swap(pe, offset, val, val)
//             }
//         } else {
//             panic!("invalid index");
//         }
//     }

//     pub(crate) fn network_iatomic_swap(&self, index: usize, val: &OneSidedMemoryRegion<T>) {
//         if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
//             unsafe {
//                 self.array
//                     .inner
//                     .data
//                     .mem_region
//                     .iatomic_swap(pe, offset, val, val)
//             }
//         } else {
//             panic!("invalid index");
//         }
//     }

//     pub fn atomic_get(&self, index: usize) -> impl Future<Output = T> {
//         let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
//         self.network_atomic_load(index, &buf);
//         let team = self.team_rt();
//         async move {
//             team.lamellae.comm().wait();
//             unsafe { buf.as_slice()[0] }
//         }
//     }

//     pub fn blocking_atomic_get(&self, index: usize) -> T {
//         let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
//         self.network_iatomic_load(index, &buf);
//         unsafe { buf.as_slice()[0] }
//     }

//     pub fn atomic_put(&self, index: usize, val: T) -> impl Future<Output = ()> {
//         self.network_atomic_store(index, val);
//         let team = self.team_rt();
//         async move {
//             team.lamellae.comm().wait();
//         }
//     }

//     pub fn blocking_atomic_put(&self, index: usize, val: T) {
//         self.network_iatomic_store(index, val);
//     }

//     pub fn atomic_swap(&self, index: usize, val: T) -> impl Future<Output = T> {
//         let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
//         unsafe {
//             buf.as_mut_slice()[0] = val;
//         }
//         self.network_atomic_swap(index, &buf);
//         let team = self.team_rt();
//         async move {
//             team.lamellae.comm().wait();
//             unsafe { buf.as_slice()[0] }
//         }
//     }

//     pub fn blocking_atomic_swap(&self, index: usize, val: T) -> T {
//         let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
//         unsafe {
//             buf.as_mut_slice()[0] = val;
//         }
//         self.network_iatomic_swap(index, &buf);
//         unsafe { buf.as_slice()[0] }
//     }
// }
