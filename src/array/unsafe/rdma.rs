use futures_util::stream::FuturesOrdered;
use tracing::trace;

use crate::{
    array::{private::LamellarArrayPrivate, r#unsafe::*, *},
    lamellae::{CommSlice, RdmaGetBufferHandle, RdmaGetIntoBufferHandle},
    memregion::{
        AsLamellarBuffer, Dist, LamellarBuffer, MemregionRdmaInput, MemregionRdmaInputInner,
    },
    RdmaHandle,
};

// //use tracing::*;

impl<T: Dist> UnsafeArray<T> {
    fn rdma_block_put<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize, //relative to inner
        buf: U,
        unmanaged: bool,
    ) -> Vec<RdmaHandle<T>> {
        let global_index = index + self.inner.offset;
        let buf = buf.into();
        let start_pe = match self.inner.pe_for_dist_index(index) {
            Some(pe) => pe,
            None => panic!("index out of bounds {:?} len {:?}", index, self.len()),
        };
        let end_pe = match self.inner.pe_for_dist_index(index + buf.len() - 1) {
            Some(pe) => pe,
            None => panic!(
                "index out of bounds {:?} len {:?}",
                index + buf.len() - 1,
                self.len()
            ),
        };
        let mut dist_index = global_index;
        let mut buf_index = 0;
        let mut rdma_requests = Vec::new();
        for pe in start_pe..=end_pe {
            let mut full_num_elems_on_pe = self.inner.orig_elem_per_pe;
            if pe < self.inner.orig_remaining_elems {
                full_num_elems_on_pe += 1;
            }
            let pe_full_start_index = self.inner.global_start_index_for_pe(pe);
            let offset = dist_index - pe_full_start_index;
            let len = std::cmp::min(full_num_elems_on_pe - offset, buf.len() - buf_index);
            if len > 0 {
                unsafe {
                    if unmanaged {
                        self.inner
                            .data
                            .mem_region
                            .as_base::<T>()
                            .put_buffer_unmanaged(
                                pe,
                                offset,
                                buf.sub_region(buf_index..(buf_index + len)),
                            );
                    } else {
                        rdma_requests.push(self.inner.data.mem_region.as_base::<T>().put_buffer(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        ))
                    }
                };
                buf_index += len;
                dist_index += len;
            }
        }
        rdma_requests
    }
    fn rdma_block_get_buffer(
        &self,
        index: usize, //relative to inner
        num_elems: usize,
    ) -> FuturesOrdered<RdmaGetBufferHandle<T>> {
        trace!("rdma_block_get_buffer index {index} num_elems {num_elems}");
        let global_index = index + self.inner.offset;
        let start_pe = match self.inner.pe_for_dist_index(index) {
            Some(pe) => pe,
            None => panic!("index out of bounds {:?} len {:?}", index, self.len()),
        };
        let end_pe = match self.inner.pe_for_dist_index(index + num_elems - 1) {
            Some(pe) => pe,
            None => panic!(
                "index out of bounds {:?} len {:?}",
                index + num_elems - 1,
                self.len()
            ),
        };
        let mut dist_index = global_index;
        let mut buf_index = 0;
        let mut rdma_requests = FuturesOrdered::new();
        trace!("rdma_block_get_buffer start pe {start_pe} end pe {end_pe}");
        for pe in start_pe..=end_pe {
            let mut full_num_elems_on_pe = self.inner.orig_elem_per_pe;
            if pe < self.inner.orig_remaining_elems {
                full_num_elems_on_pe += 1;
            }
            let pe_full_start_index = self.inner.global_start_index_for_pe(pe);
            let offset = dist_index - pe_full_start_index;
            let len = std::cmp::min(full_num_elems_on_pe - offset, num_elems - buf_index);
            if len > 0 {
                unsafe {
                    rdma_requests.push_back(
                        self.inner
                            .data
                            .mem_region
                            .as_base::<T>()
                            .get_buffer(pe, offset, len),
                    )
                };
                buf_index += len;
                dist_index += len;
            }
        }
        rdma_requests
    }

    fn rdma_block_get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize, //relative to inner
        mut dst: LamellarBuffer<T, B>,
    ) -> FuturesOrdered<RdmaGetIntoBufferHandle<T, B>> {
        let global_index = index + self.inner.offset;
        let start_pe = match self.inner.pe_for_dist_index(index) {
            Some(pe) => pe,
            None => panic!("index out of bounds {:?} len {:?}", index, self.len()),
        };
        let end_pe = match self.inner.pe_for_dist_index(index + dst.len() - 1) {
            Some(pe) => pe,
            None => panic!(
                "index out of bounds {:?} len {:?}",
                index + dst.len() - 1,
                self.len()
            ),
        };
        let mut dist_index = global_index;
        let mut buf_index = 0;
        let mut rdma_requests = FuturesOrdered::new();
        let orig_len = dst.len();
        for pe in start_pe..=end_pe {
            let mut full_num_elems_on_pe = self.inner.orig_elem_per_pe;
            if pe < self.inner.orig_remaining_elems {
                full_num_elems_on_pe += 1;
            }
            let pe_full_start_index = self.inner.global_start_index_for_pe(pe);
            let offset = dist_index - pe_full_start_index;
            let len = std::cmp::min(full_num_elems_on_pe - offset, orig_len - buf_index);
            if len > 0 {
                let dsts = dst.split(len);
                dst = dsts.1;
                unsafe {
                    rdma_requests.push_back(
                        self.inner
                            .data
                            .mem_region
                            .as_base::<T>()
                            .get_into_buffer(pe, offset, dsts.0),
                    )
                };
                buf_index += len;
                dist_index += len;
            }
        }
        rdma_requests
    }

    fn rdma_cyclic_put<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize, //global_index
        buf: U,
        unmanaged: bool,
    ) -> Vec<RdmaHandle<T>> {
        let global_index = index + self.inner.offset;
        let buf = buf.into();
        let num_pes = self.inner.data.team.num_pes();
        let num_elems_pe = buf.len() / num_pes + 1; //we add plus one to ensure we allocate enough space
        let start_pe = global_index % num_pes;
        let mut rdma_requests = Vec::new();

        let mut pe_data = vec![Vec::with_capacity(num_elems_pe); num_pes];

        let pe_start_index = global_index / num_pes;

        //stripe the data across the PEs starting with start_pe
        for (i, pe) in (0..buf.len()).map(|i| (i, (start_pe + i) % num_pes)) {
            pe_data[pe].push(buf.as_slice()[i]);
        }
        for (pe, data) in pe_data.into_iter().enumerate() {
            if data.len() > 0 {
                let mut pe_index = pe_start_index;
                if pe < start_pe {
                    pe_index += 1;
                }
                if unmanaged {
                    unsafe {
                        self.inner
                            .data
                            .mem_region
                            .as_base::<T>()
                            .put_buffer_unmanaged(pe, pe_index, data);
                    }
                } else {
                    unsafe {
                        rdma_requests.push(
                            self.inner
                                .data
                                .mem_region
                                .as_base::<T>()
                                .put_buffer(pe, pe_index, data),
                        )
                    };
                }
            }
        }

        rdma_requests
    }

    fn rdma_cyclic_get_buffer(
        &self,
        index: usize, //global_index
        num_elems: usize,
    ) -> FuturesOrdered<RdmaGetBufferHandle<T>> {
        let global_index = index + self.inner.offset;
        let num_pes = self.inner.data.team.num_pes();
        let start_pe = global_index % num_pes;
        let num_elems_pe = num_elems / num_pes; //we add plus one to ensure we allocate enough space
        let pe_start_index = global_index / num_pes;
        let mut rdma_requests = FuturesOrdered::new();
        for pe in 0..std::cmp::min(num_elems, num_pes) {
            let pe = (start_pe + pe) % num_pes;
            let mut pe_index = pe_start_index;
            if pe < start_pe {
                pe_index += 1;
            }
            let mut pe_num_elems = num_elems_pe;
            if pe >= start_pe && pe < start_pe + (num_elems % num_pes) {
                pe_num_elems += 1;
            }
            unsafe {
                rdma_requests.push_back(self.inner.data.mem_region.as_base::<T>().get_buffer(
                    pe,
                    pe_index,
                    pe_num_elems,
                ))
            }
        }
        rdma_requests
    }

    pub(crate) fn pes_for_range(
        &self,
        index: usize,
        len: usize,
    ) -> Box<dyn Iterator<Item = usize>> {
        self.inner.pes_for_range(index, len)
    }

    pub(crate) fn num_elements_on_pe_for_range(
        &self,
        pe: usize,
        start_index: usize,
        len: usize,
    ) -> Option<usize> {
        self.inner
            .num_elements_on_pe_for_range(pe, start_index, len)
    }

    // #[doc(alias("One-sided", "onesided"))]
    // /// Performs a raw RDMA "Put" of the data in the specified buffer into this array starting from the provided index
    // ///
    // /// The length of the Put is dictated by the length of the buffer.
    // ///
    // /// The runtime provides no internal mechanism to check for completion when using this call.
    // /// i.e. this means the users themselves will be responsible for determining when the transfer is complete
    // ///
    // /// # Warning
    // /// This is a low-level API, unless you are very confident in low level distributed memory access it is highly recommended
    // /// you use a safe Array type and utilize the LamellarArray load/store operations instead.
    // ///
    // /// # Safety
    // /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    // /// Additionally, when this call returns the data buffer is safe to reuse, but the data may or may not have been delivered to the remote memory
    // ///
    // /// # One-sided Operation
    // /// the calling PE initaites the remote transfer
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// use lamellar::memregion::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block).block();
    // /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    // /// let buf_len = buf.len();
    // /// unsafe {
    // ///     let _ = array.dist_iter_mut().for_each(move |elem| *elem = buf_len).spawn(); //we will used this val as completion detection
    // ///     for (i,elem) in buf.as_mut_slice()
    // ///                          .expect("we just created it so we know its local")
    // ///                          .iter_mut()
    // ///                          .enumerate(){ //initialize mem_region
    // ///         *elem = i;
    // ///     }
    // /// }
    // /// array.wait_all();
    // /// array.barrier();
    // /// println!("PE{my_pe} array data: {:?}",unsafe{array.local_data()});
    // /// if my_pe == 0 { //only perfrom the transfer from one PE
    // ///     unsafe {array.put_unchecked(0,&buf);}
    // ///     println!();
    // /// }
    // /// // wait for the data to show up
    // /// for elem in unsafe{array.local_data()}{
    // ///     while *elem == buf.len(){
    // ///         std::thread::yield_now();
    // ///     }
    // /// }
    // ///
    // /// println!("PE{my_pe} array data: {:?}",unsafe{array.local_data()});
    // ///```
    // /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    // ///```text
    // /// PE0: array data [12,12,12]
    // /// PE1: array data [12,12,12]
    // /// PE2: array data [12,12,12]
    // /// PE3: array data [12,12,12]
    // ///
    // /// PE0: array data [0,1,2]
    // /// PE1: array data [3,4,5]
    // /// PE2: array data [6,7,8]
    // /// PE3: array data [9,10,11]
    // ///```
    // pub unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>> + LamellarRead>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) -> ArrayRdmaHandle<T> {
    //     match buf.team_try_into(&self.inner.data.team.team()) {
    //         Ok(buf) => {
    //             let inner_handle = match self.inner.distribution {
    //                 Distribution::Block => self.block_op(ArrayRdmaCmd::Put, index, buf),
    //                 Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::Put, index, buf),
    //             };
    //             ArrayRdmaHandle {
    //                 array: self.as_lamellar_byte_array(),
    //                 reqs: inner_handle,
    //                 spawned: false,
    //             }
    //         }
    //         Err(_) => ArrayRdmaHandle {
    //             array: self.as_lamellar_byte_array(),
    //             reqs: InnerRdmaHandle::Am(VecDeque::new()),
    //             spawned: false,
    //         },
    //     }
    // }

    // #[doc(alias("One-sided", "onesided"))]
    // /// Performs a raw RDMA "Get" of the data in this array starting at the provided index into the specified buffer
    // ///
    // /// The length of the Get is dictated by the length of the buffer.
    // ///
    // /// The runtime provides no internal mechanism to check for completion when using this call.
    // /// i.e. this means the users themselves will be responsible for determining when the transfer is complete
    // ///
    // /// # Safety
    // /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    // /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    // ///
    // /// # One-sided Operation
    // /// the calling PE initaites the remote transfer
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// use lamellar::memregion::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block).block();
    // /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    // /// unsafe {
    // ///     let _ = array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i).spawn();
    // ///     for elem in buf.as_mut_slice()
    // ///                          .expect("we just created it so we know its local") { //initialize mem_region
    // ///         *elem = buf.len(); //we will used this val as completion detection
    // ///     }
    // /// }
    // /// array.wait_all();
    // /// array.barrier();
    // /// println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    // /// if my_pe == 0 { //only perfrom the transfer from one PE
    // ///     unsafe {array.get(0,&buf)};
    // ///     println!();
    // /// }
    // /// // wait for the data to show up
    // /// for elem in unsafe{buf.as_slice().unwrap()}{
    // ///     while *elem == buf.len(){
    // ///         std::thread::yield_now();
    // ///     }
    // /// }
    // ///
    // /// println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    // ///```
    // /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    // ///```text
    // /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // ///
    // /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE0: buf data [0,1,2,3,4,5,6,7,8,9,10,11] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    // ///```
    // pub unsafe fn get_buffer<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) -> ArrayRdmaHandle<T> {
    //     match buf.team_try_into(&self.inner.data.team.team()) {
    //         Ok(buf) => {
    //             let inner_handle = match self.inner.distribution {
    //                 Distribution::Block => self.block_op(ArrayRdmaCmd::Get(false), index, buf),
    //                 Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::Get(false), index, buf),
    //             };
    //             ArrayRdmaHandle {
    //                 array: self.as_lamellar_byte_array(),
    //                 reqs: inner_handle,
    //                 spawned: false,
    //             }
    //         }
    //         Err(_) => ArrayRdmaHandle {
    //             array: self.as_lamellar_byte_array(),
    //             reqs: InnerRdmaHandle::Am(VecDeque::new()),
    //             spawned: false,
    //         },
    //     }
    // }

    // #[doc(alias("One-sided", "onesided"))]
    // /// Performs an (active message based) "Get" of the data in this array starting at the provided index into the specified buffer
    // ///
    // /// The length of the Get is dictated by the length of the buffer.
    // ///
    // /// This call returns a future that can be awaited to determine when the `put` has finished
    // ///
    // /// # Safety
    // /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    // /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    // ///
    // /// # One-sided Operation
    // /// the calling PE initaites the remote transfer
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// use lamellar::memregion::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block).block();
    // /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    // /// unsafe {
    // ///     let _ = array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i).spawn(); //we will used this val as completion detection
    // ///     for elem in buf.as_mut_slice()
    // ///                          .expect("we just created it so we know its local") { //initialize mem_region
    // ///         *elem = buf.len();
    // ///     }
    // ///     array.wait_all();
    // ///     array.barrier();
    // ///     println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    // ///     if my_pe == 0 { //only perfrom the transfer from one PE
    // ///          println!();
    // ///         let req = array.get(0,&buf);
    // ///         world.block_on(req);
    // ///     }
    // ///     println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    // /// }
    // ///```
    // /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    // ///```text
    // /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // ///
    // /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    // /// PE0: buf data [0,1,2,3,4,5,6,7,8,9,10,11] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    // ///```
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub unsafe fn get<U>(&self, index: usize, buf: U) -> ArrayRdmaHandle<T>
    // where
    //     U: TeamTryInto<LamellarArrayRdmaOutput<T>>,
    // {
    //     trace!("array get {:?}", index);
    //     match buf.team_try_into(&self.inner.data.team.team()) {
    //         Ok(buf) => self.internal_get(index, buf),
    //         Err(_) => ArrayRdmaHandle {
    //             array: self.as_lamellar_byte_array(),
    //             reqs: InnerRdmaHandle::Am(VecDeque::new()),
    //             spawned: false,
    //         },
    //     }
    // }

    // pub(crate) unsafe fn internal_at(&self, index: usize) -> ArrayAtHandle<T> {
    //     // let team = self.team_rt();
    //     // let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(1);
    //     // self.blocking_get(index, &buf);
    //     let (pe, offset) = self
    //         .pe_and_offset_for_global_index(index)
    //         .expect("index out of bounds in LamellarArray at");
    //     ArrayAtHandle {
    //         array: self.as_lamellar_byte_array(),
    //         state: ArrayAtHandleState::Rdma(self.inner.data.mem_region.get(pe, offset)),
    //     }
    // }

    // #[doc(alias("One-sided", "onesided"))]
    // /// Retrieves the element in this array located at the specified `index`
    // ///
    // /// This call returns a future that can be awaited to retrieve to requested element
    // ///
    // /// # Safety
    // /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    // /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    // ///
    // /// # One-sided Operation
    // /// the calling PE initaites the remote transfer
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// use lamellar::memregion::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let num_pes = world.num_pes();
    // /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block).block();
    // /// unsafe {
    // ///     let _ = array.dist_iter_mut().enumerate().for_each(move|(i,elem)| *elem = my_pe).spawn(); //we will used this val as completion detection
    // ///     array.wait_all();
    // ///     array.barrier();
    // ///     println!("PE{my_pe} array data: {:?}",unsafe{array.local_data()});
    // ///     let index = ((my_pe+1)%num_pes) * array.num_elems_local(); // get first index on PE to the right (with wrap arround)
    // ///     let at_req = array.at(index);
    // ///     let val = array.block_on(at_req);
    // ///     println!("PE{my_pe} array[{index}] = {val}");
    // /// }
    // ///```
    // /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    // ///```text
    // /// PE0: buf data [0,0,0]
    // /// PE1: buf data [1,1,1]
    // /// PE2: buf data [2,2,2]
    // /// PE3: buf data [3,3,3]
    // ///
    // /// PE0: array[3] = 1
    // /// PE1: array[6] = 2
    // /// PE2: array[9] = 3
    // /// PE3: array[0] = 0
    // ///```
    // pub unsafe fn at(&self, index: usize) -> ArrayAtHandle<T> {
    //     self.internal_at(index)
    // }
}

// impl<T: Dist> LamellarArrayInternalGet<T> for UnsafeArray<T> {
//     unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
//         &self,
//         index: usize,
//         buf: U,
//     ) -> ArrayRdmaHandle<T> {
//         let buf = buf.into();
//         let reqs = if buf.len() * std::mem::size_of::<T>() > config().am_size_threshold {
//             match self.inner.distribution {
//                 Distribution::Block => self.block_op(ArrayRdmaCmd::GetAm, index, buf),
//                 Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::GetAm, index, buf),
//             }
//         } else {
//             let req = self.exec_am_local(InitSmallGetAm {
//                 array: self.clone(),
//                 index: index,
//                 buf: buf,
//             });
//             let mut reqs = VecDeque::new();
//             reqs.push_back(req.into());
//             InnerRdmaHandle::from_am_reqs(reqs)
//         };
//         ArrayRdmaHandle {
//             array: self.as_lamellar_byte_array(),
//             reqs: reqs,
//             spawned: false,
//         }
//     }

//     unsafe fn internal_at(&self, index: usize) -> ArrayAtHandle<T> {
//         self.internal_at(index)
//     }
// }

impl<T: Dist> UnsafeArray<T> {
    pub unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put(self, index, data)
    }
    pub unsafe fn put_unmanaged(&self, index: usize, data: T) {
        <Self as LamellarRdmaPut<T>>::put_unmanaged(self, index, data)
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
    pub unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_pe(self, pe, offset, data)
    }
    pub unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        <Self as LamellarRdmaPut<T>>::put_pe_unmanaged(self, pe, offset, data)
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
    pub unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        <Self as LamellarRdmaPut<T>>::put_all(self, offset, data)
    }
    pub unsafe fn put_all_unmanaged(&self, offset: usize, data: T) {
        <Self as LamellarRdmaPut<T>>::put_all_unmanaged(self, offset, data)
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

    pub unsafe fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        <Self as LamellarRdmaGet<T>>::get(self, index)
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

impl<T: Dist> LamellarRdmaPut<T> for UnsafeArray<T> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaPutHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let req = self
                .inner
                .data
                .mem_region
                .as_base::<T>()
                .put(pe, offset, data);
            ArrayRdmaPutHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaPutState::RdmaPut(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds in LamellarArray put");
        }
    }
    unsafe fn put_unmanaged(&self, index: usize, data: T) {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .put_unmanaged(pe, offset, data);
        } else {
            panic!("index out of bounds in LamellarArray put");
        }
    }
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let reqs = match self.inner.distribution {
            Distribution::Block => self.rdma_block_put(index, buf, false),
            Distribution::Cyclic => self.rdma_cyclic_put(index, buf, false),
        };
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::MultiRdmaPut(reqs),
            spawned: false,
        }
    }
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        match self.inner.distribution {
            Distribution::Block => {
                self.rdma_block_put(index, buf, true);
            }
            Distribution::Cyclic => {
                self.rdma_cyclic_put(index, buf, true);
            }
        };
    }
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .put(pe, offset, data);
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RdmaPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T) {
        self.inner
            .data
            .mem_region
            .as_base::<T>()
            .put_unmanaged(pe, offset, data);
    }
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .put_buffer(pe, offset, buf);
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RdmaPut(req),
            spawned: false,
        }
    }
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) {
        self.inner
            .data
            .mem_region
            .as_base::<T>()
            .put_buffer_unmanaged(pe, offset, buf);
    }

    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaPutHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .put_all(offset, data);
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RdmaPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T) {
        self.inner
            .data
            .mem_region
            .as_base::<T>()
            .put_all_unmanaged(offset, data);
    }
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaPutHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .put_all_buffer(offset, buf);
        ArrayRdmaPutHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaPutState::RdmaPut(req),
            spawned: false,
        }
    }
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) {
        self.inner
            .data
            .mem_region
            .as_base::<T>()
            .put_all_buffer_unmanaged(offset, buf);
    }
}

impl<T: Dist> LamellarRdmaGet<T> for UnsafeArray<T> {
    unsafe fn get(&self, index: usize) -> ArrayRdmaGetHandle<T> {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let req = self.inner.data.mem_region.as_base::<T>().get(pe, offset);
            ArrayRdmaGetHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetState::RdmaGet(req),
                spawned: false,
            }
        } else {
            panic!("index out of bounds in LamellarArray put");
        }
    }
    unsafe fn get_buffer(&self, index: usize, num_elems: usize) -> ArrayRdmaGetBufferHandle<T> {
        match self.inner.distribution {
            Distribution::Block => ArrayRdmaGetBufferHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetBufferState::MultiRdmaBlockGet(
                    self.rdma_block_get_buffer(index, num_elems).collect(),
                ),
                spawned: false,
            },
            Distribution::Cyclic => ArrayRdmaGetBufferHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetBufferState::MultiRdmaCyclicGet(
                    self.rdma_cyclic_get_buffer(index, num_elems).collect(),
                ),
                spawned: false,
            },
        }
    }
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        mut data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let num_elems = data.len();
        match self.inner.distribution {
            Distribution::Block => ArrayRdmaGetIntoBufferHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetIntoBufferState::MultiRdmaBlockGet(
                    self.rdma_block_get_into_buffer(index, data).collect(),
                ),
                spawned: false,
            },
            Distribution::Cyclic => ArrayRdmaGetIntoBufferHandle {
                array: self.as_lamellar_byte_array(),
                state: ArrayRdmaGetIntoBufferState::MultiRdmaCyclicGet(
                    data,
                    self.rdma_cyclic_get_buffer(index, num_elems).collect(),
                ),
                spawned: false,
            },
        }
    }
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        if let Some((pe, offset)) = self.pe_and_offset_for_global_index(index) {
            let _ = self
                .inner
                .data
                .mem_region
                .as_base::<T>()
                .get_into_buffer(pe, offset, data)
                .spawn();
        } else {
            panic!("index out of bounds in LamellarArray put");
        }
    }

    unsafe fn get_pe(&self, pe: usize, offset: usize) -> ArrayRdmaGetHandle<T> {
        let req = self.inner.data.mem_region.as_base::<T>().get(pe, offset);
        ArrayRdmaGetHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetState::RdmaGet(req),
            spawned: false,
        }
    }
    unsafe fn get_buffer_pe(
        &self,
        pe: usize,
        offset: usize,
        num_elems: usize,
    ) -> ArrayRdmaGetBufferHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .get_buffer(pe, offset, num_elems);
        ArrayRdmaGetBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetBufferState::RdmaGet(req),
            spawned: false,
        }
    }
    unsafe fn get_into_buffer_pe<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        data: LamellarBuffer<T, B>,
    ) -> ArrayRdmaGetIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .get_into_buffer(pe, offset, data);
        ArrayRdmaGetIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayRdmaGetIntoBufferState::RdmaGet(req),
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
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .get_into_buffer(pe, offset, data)
            .spawn();
    }
}

impl UnsafeByteArray {
    pub(crate) unsafe fn local_elements_for_range(
        &self,
        index: usize,
        len: usize,
    ) -> Option<(&mut [u8], Box<dyn Iterator<Item = usize>>)> {
        self.inner.local_elements_for_range(index, len)
    }

    pub(crate) unsafe fn element_for_local_index(&self, index: usize) -> &mut [u8] {
        self.inner.element_for_local_index(index)
    }

    pub(crate) unsafe fn ptr_for_local_index(&self, index: usize) -> *mut u8 {
        self.inner.ptr_for_local_index(index)
    }

    // pub(crate) fn comm_slice_for_range(&self, index: usize, len: usize) -> CommSlice<u8> {
    //     self.inner.comm_slice_for_range(index, len)
    // }
}

impl UnsafeArrayInner {
    pub(crate) fn pes_for_range(
        &self,
        index: usize,
        len: usize,
    ) -> Box<dyn Iterator<Item = usize>> {
        match self.distribution {
            Distribution::Block => {
                if let Some(start_pe) = self.pe_for_dist_index(index) {
                    //((global_start+1) as f64 / self.elem_per_pe).round() as usize;
                    if let Some(end_pe) = self.pe_for_dist_index(index + len - 1) {
                        //((global_end+1) as f64 / self.elem_per_pe).round() as usize;
                        return Box::new(start_pe..=end_pe);
                    }
                }
                return Box::new(0..0);
            }
            Distribution::Cyclic => {
                let global_start = self.offset + index;
                let global_end = global_start + len - 1; //inclusive
                let num_pes = self.data.num_pes;
                let mut pes = vec![];
                for index in global_start..=global_end {
                    pes.push(index % num_pes);
                    if pes.len() == num_pes {
                        break;
                    }
                }
                return Box::new(pes.into_iter());
            }
        }
    }

    pub(crate) unsafe fn element_for_local_index(&self, index: usize) -> &mut [u8] {
        &mut self.local_as_mut_slice()[index * self.elem_size..(index + 1) * self.elem_size]
    }

    pub(crate) unsafe fn ptr_for_local_index(&self, index: usize) -> *mut u8 {
        self.local_as_mut_ptr().add(index * self.elem_size)
    }

    //index with respect to inner
    pub(crate) unsafe fn local_elements_for_range(
        &self,
        index: usize,
        len: usize,
    ) -> Option<(&mut [u8], Box<dyn Iterator<Item = usize>>)> {
        let my_pe = self.data.my_pe;
        let start_pe = match self.pe_for_dist_index(index) {
            Some(pe) => pe,
            None => panic!(
                "Index: {index} out of bounds for array of len {:?}",
                self.size
            ),
        };
        let end_pe = match self.pe_for_dist_index(index + len - 1) {
            Some(pe) => pe,
            None => panic!(
                "Index: {:?} out of bounds for array of len {:?}",
                index + len - 1,
                self.size
            ),
        };

        // println!(
        //     "i {:?} len {:?} spe {:?} epe {:?}  ",
        //     index, len, start_pe, end_pe
        // );
        match self.distribution {
            Distribution::Block => {
                let num_elems_local = self.num_elems_local();
                if my_pe > end_pe || my_pe < start_pe {
                    //starts and ends before this pe, or starts (and ends) after this pe... no local elements
                    return None;
                }
                let subarray_start_index = self
                    .start_index_for_pe(my_pe)
                    .expect("array data exists on PE"); //passed the above if statement;
                let (start_index, rem_elem) = if my_pe == start_pe {
                    (index - subarray_start_index, len)
                } else {
                    (0, len - (subarray_start_index - index))
                };
                let end_index = if my_pe == end_pe {
                    start_index + rem_elem
                } else {
                    num_elems_local
                };
                // println!(
                //     "ssi {:?} si {:?} ei {:?} nel {:?} es {:?}",
                //     subarray_start_index, start_index, end_index, num_elems_local, self.elem_size
                // );
                Some((
                    &mut self.local_as_mut_slice()
                        [start_index * self.elem_size..end_index * self.elem_size],
                    Box::new(start_index..end_index),
                ))
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                let mut local_index = index; // + if my_pe < start_pe {1} else {0});
                if my_pe >= start_pe {
                    local_index += my_pe - start_pe;
                } else {
                    local_index += (num_pes - start_pe) + my_pe;
                }
                let start_index = local_index / num_pes;
                let mut num_elems = len / num_pes;
                if len % num_pes != 0 {
                    //we have left over elements
                    if start_pe <= end_pe {
                        //no wrap around occurs
                        if my_pe >= start_pe && my_pe <= end_pe {
                            num_elems += 1
                        }
                    } else {
                        //wrap arround occurs
                        if my_pe >= start_pe || my_pe <= end_pe {
                            num_elems += 1
                        }
                    }
                }
                if num_elems > 0 {
                    let end_index = start_index + num_elems;
                    // println!("li {:?} si {:?} ei {:?}",local_index,start_index,end_index);
                    Some((
                        &mut self.local_as_mut_slice()
                            [start_index * self.elem_size..end_index * self.elem_size],
                        Box::new(start_index..end_index),
                    ))
                } else {
                    None
                }
            }
        }
    }

    pub(crate) fn comm_slice_for_range(&self, index: usize, len: usize) -> CommSlice<u8> {
        unsafe {
            //safe as these elements are local (and in registered memory) to the calling pe,
            match self.local_elements_for_range(index, len) {
                Some((slice, _)) => CommSlice::from_slice(slice),
                None => CommSlice::from_slice(&self.local_as_mut_slice()[0..0]),
            }
        }
    }

    //index with respect to subarray
    pub(crate) fn num_elements_on_pe_for_range(
        &self,
        pe: usize,
        index: usize,
        len: usize,
    ) -> Option<usize> {
        let start_pe = match self.pe_for_dist_index(index) {
            Some(pe) => pe,
            None => panic!(
                "Index: {index} out of bounds for array of len {:?}",
                self.size
            ),
        };
        let end_pe = match self.pe_for_dist_index(index + len - 1) {
            Some(pe) => pe,
            None => panic!(
                "Index: {:?} out of bounds for array of len {:?}",
                index + len - 1,
                self.size
            ),
        };

        // println!("i {:?} len {:?} pe {:?} spe {:?} epe {:?}  ",index,len,pe,start_pe,end_pe);
        match self.distribution {
            Distribution::Block => {
                let num_elems_pe = self.num_elems_pe(pe);
                if pe > end_pe || pe < start_pe {
                    //starts and ends before this pe, or starts (and ends) after this pe... no local elements
                    return None;
                }
                let subarray_start_index = self
                    .start_index_for_pe(pe)
                    .expect("array data exists on PE"); //passed the above if statement;
                let (start_index, rem_elem) = if pe == start_pe {
                    (index - subarray_start_index, len)
                } else {
                    (0, len - (subarray_start_index - index))
                };
                let end_index = if pe == end_pe {
                    start_index + rem_elem
                } else {
                    num_elems_pe
                };
                // println!("si {:?} ei {:?} nel {:?}",start_index,end_index,num_elems_pe);
                Some(end_index - start_index)
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                let mut num_elems = len / num_pes;
                if len % num_pes != 0 {
                    //we have left over elements
                    if start_pe <= end_pe {
                        //no wrap around occurs
                        if pe >= start_pe && pe <= end_pe {
                            num_elems += 1
                        }
                    } else {
                        //wrap arround occurs
                        if pe >= start_pe || pe <= end_pe {
                            num_elems += 1
                        }
                    }
                }
                if num_elems > 0 {
                    // println!("si {:?} ei {:?}",start_index,end_index);
                    Some(num_elems)
                } else {
                    None
                }
            }
        }
    }
}

// #[lamellar_impl::AmLocalDataRT(Debug)]
// struct UnsafeBlockGetAm {
//     array: UnsafeByteArray, //inner of the indices we need to place data into
//     offset: usize,
//     data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
//     pe: usize,
// }

// #[lamellar_impl::rt_am_local]
// impl LamellarAm for UnsafeBlockGetAm {
//     async fn exec(self) {
//         unsafe {
//             self.array
//                 .inner
//                 .data
//                 .mem_region
//                 .get_into_buffer(
//                     self.pe,
//                     self.offset * self.array.inner.elem_size,
//                     LamellarBuffer::<u8, LamellarMemoryRegion<u8>>::from_lamellar_memory_region(
//                         self.data.clone(),
//                     ),
//                 )
//                 .await;
//         };
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Debug)]
// struct UnsafeCyclicGetAm {
//     array: UnsafeByteArray, //inner of the indices we need to place data into
//     data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
//     // temp_data: LamellarMemoryRegion<u8>,
//     num_elems: usize,
//     i: usize,
//     pe: usize,
//     my_pe: usize,
//     num_pes: usize,
//     offset: usize,
// }

// #[lamellar_impl::rt_am_local]
// impl LamellarAm for UnsafeCyclicGetAm {
//     async fn exec(self) {
//         let data = unsafe {
//             self.array
//                 .inner
//                 .data
//                 .mem_region
//                 .get_buffer(
//                     self.pe,
//                     self.offset * self.array.inner.elem_size,
//                     self.num_elems * self.array.inner.elem_size,
//                 )
//                 .await
//         };
//         let team_rt = self.array.team_rt();
//         let temp_data: MemregionRdmaInputInner<u8> = data.into();
//         for (k, j) in (self.i..self.data.len() / self.array.inner.elem_size)
//             .step_by(self.num_pes)
//             .enumerate()
//             .map(|(k, j)| {
//                 (
//                     k * self.array.inner.elem_size,
//                     j * self.array.inner.elem_size,
//                 )
//             })
//         {
//             unsafe {
//                 let _ = self.data.put_buffer_unmanaged(
//                     self.my_pe,
//                     j,
//                     temp_data.sub_region(k..(k + self.array.inner.elem_size)),
//                 );
//             }
//         }
//         team_rt.lamellae.comm().wait();
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Debug)]
// struct InitSmallGetAm<T: Dist> {
//     array: UnsafeArray<T>, //inner of the indices we need to place data into
//     index: usize,          //relative to inner
//     buf: LamellarMemoryRegion<T>,
// }

// #[lamellar_impl::rt_am_local]
// impl<T: Dist + 'static> LamellarAm for InitSmallGetAm<T> {
//     #[tracing::instrument(skip_all, level = "debug")]
//     async fn exec(self) {
//         let mut reqs = vec![];
//         for pe in self
//             .array
//             .pes_for_range(self.index, self.buf.len())
//             .into_iter()
//         {
//             trace!(
//                 "InitSmallGetAm pe {:?} index {:?} len {:?}",
//                 pe,
//                 self.index,
//                 self.buf.len()
//             );
//             let remote_am = UnsafeRemoteSmallGetAm {
//                 array: self.array.clone().into(),
//                 start_index: self.index,
//                 len: self.buf.len(),
//             };
//             reqs.push(self.array.spawn_am_pe_tg(pe, remote_am));
//         }
//         unsafe {
//             match self.array.inner.distribution {
//                 Distribution::Block => {
//                     let u8_buf = self.buf.clone().to_base::<u8>();
//                     let mut cur_index = 0;

//                     for req in reqs.drain(..) {
//                         let data = req.await;
//                         // println!("data recv {:?}", data.len());
//                         trace!("InitSmallGetAm put_comm_slice: {:?}", data.as_ptr());

//                         let _ = u8_buf
//                             .put_comm_slice(
//                                 lamellar::current_pe,
//                                 cur_index,
//                                 CommSlice::from_slice(&data),
//                             )
//                             .spawn(); //we can do this conversion because we will spawn the put immediately, upon which the data buffer is free to be dropped
//                         cur_index += data.len();
//                         trace!("InitSmallGetAm put_comm_slice after: {:?}", data.as_ptr());
//                     }
//                 }
//                 Distribution::Cyclic => {
//                     let buf_slice = self.buf.as_mut_slice(); //.expect("array data exists on PE");
//                     let num_pes = reqs.len();
//                     for (start_index, req) in reqs.drain(..).enumerate() {
//                         let data = req.await;
//                         let data_t_ptr = data.as_ptr() as *const T;
//                         let data_t_len = if data.len() % std::mem::size_of::<T>() == 0 {
//                             data.len() / std::mem::size_of::<T>()
//                         } else {
//                             panic!("memory align error");
//                         };
//                         let data_t_slice = std::slice::from_raw_parts(data_t_ptr, data_t_len);
//                         for (i, val) in data_t_slice.iter().enumerate() {
//                             buf_slice[start_index + i * num_pes] = *val;
//                         }
//                     }
//                 }
//             }
//         }
//     }
// }

// #[lamellar_impl::AmDataRT(Debug)]
// struct UnsafeRemoteSmallGetAm {
//     array: UnsafeByteArray, //inner of the indices we need to place data into
//     start_index: usize,
//     len: usize,
// }

// #[lamellar_impl::rt_am]
// impl LamellarAm for UnsafeRemoteSmallGetAm {
//     //we cant directly do a put from the array in to the data buf
//     //because we need to guarantee the put operation is atomic (maybe iput would work?)
//     async fn exec(self) -> Vec<u8> {
//         trace!(
//             "in remotegetam index {:?} len {:?}",
//             self.start_index,
//             self.len
//         );
//         // let _lock = self.array.lock.read();
//         let vals = unsafe {
//             match self
//                 .array
//                 .local_elements_for_range(self.start_index, self.len)
//             {
//                 Some((elems, _)) => elems.to_vec(),
//                 None => vec![],
//             }
//         };
//         // println!("done remotegetam len {:?}", vals.len());
//         vals
//     }
// }

// #[lamellar_impl::AmDataRT(Debug)]
// struct UnsafePutAm {
//     array: UnsafeByteArray,         //byte representation of the array
//     start_index: usize,             //index with respect to inner (of type T)
//     len: usize,                     //len of buf (with respect to original type T)
//     data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
//     pe: usize,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAm for UnsafePutAm {
//     async fn exec(self) {
//         unsafe {
//             let comm_slice = self
//                 .array
//                 .inner
//                 .comm_slice_for_range(self.start_index, self.len);
//             self.data.get_comm_slice(self.pe, 0, comm_slice).await;
//         };
//     }
// }

// #[lamellar_impl::AmDataRT(Debug)]
// struct UnsafeSmallPutAm {
//     array: UnsafeByteArray, //byte representation of the array
//     start_index: usize,     //index with respect to inner (of type T)
//     len: usize,             //len of buf (with respect to original type T)
//     #[serde(with = "serde_bytes")]
//     data: Vec<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
//                    // pe: usize,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAm for UnsafeSmallPutAm {
//     async fn exec(self) {
//         unsafe {
//             // println!("unsafe put am: pe {:?} si {:?} len {:?}",self.pe,self.start_index,self.len);
//             match self
//                 .array
//                 .inner
//                 .local_elements_for_range(self.start_index, self.len)
//             {
//                 Some((elems, _)) => std::ptr::copy_nonoverlapping(
//                     self.data.as_ptr(),
//                     elems.as_mut_ptr(),
//                     elems.len(),
//                 ),
//                 None => {}
//             }
//         };
//     }
// }
