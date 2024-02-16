use crate::array::private::ArrayExecAm;
use crate::array::r#unsafe::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};

// //use tracing::*;

impl<T: Dist> UnsafeArray<T> {
    fn block_op<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        op: ArrayRdmaCmd,
        index: usize, //relative to inner
        buf: U,
    ) -> Vec<Box<dyn LamellarRequest<Output = ()>>> {
        let global_index = index + self.inner.offset;
        // let buf = buf.team_into(&self.inner.data.team);
        let buf = buf.into();
        let start_pe = match self.inner.pe_for_dist_index(index) {
            Some(pe) => pe,
            None => panic!("index out of bounds {:?} len {:?}", index, self.len()),
        };
        // .expect("index out of bounds"); //((index+1) as f64 / self.elem_per_pe).round() as usize;
        let end_pe = match self.inner.pe_for_dist_index(index + buf.len() - 1) {
            Some(pe) => pe,
            None => panic!(
                "index out of bounds {:?} len {:?}",
                index + buf.len() - 1,
                self.len()
            ),
        };
        // .expect("index out of bounds"); //(((index + buf.len()) as f64) / self.elem_per_pe).round() as usize;
        // println!("block_op {:?} {:?}",start_pe,end_pe);
        let mut dist_index = global_index;
        // let mut subarray_index = index;
        let mut buf_index = 0;
        let mut reqs = vec![];
        for pe in start_pe..=end_pe {
            let num_elems_on_pe = (self.inner.orig_elem_per_pe * (pe + 1) as f64).round() as usize
                - (self.inner.orig_elem_per_pe * pe as f64).round() as usize;
            let pe_start_index = (self.inner.orig_elem_per_pe * pe as f64).round() as usize;
            let offset = dist_index - pe_start_index;
            let len = std::cmp::min(num_elems_on_pe - offset, buf.len() - buf_index);
            if len > 0 {
                // println!("pe {:?} offset {:?} range: {:?}-{:?} dist_index {:?} pe_start_index {:?} num_elems {:?} len {:?}", pe, offset, buf_index, buf_index+len, dist_index, pe_start_index, num_elems_on_pe, len);
                match op {
                    ArrayRdmaCmd::Put => unsafe {
                        self.inner.data.mem_region.blocking_put(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        )
                    },
                    ArrayRdmaCmd::Get(immediate) => unsafe {
                        if immediate {
                            self.inner.data.mem_region.blocking_get(
                                pe,
                                offset,
                                buf.sub_region(buf_index..(buf_index + len)),
                            )
                        } else {
                            self.inner.data.mem_region.get_unchecked(
                                pe,
                                offset,
                                buf.sub_region(buf_index..(buf_index + len)),
                            )
                        }
                    },
                    ArrayRdmaCmd::PutAm => {
                        // println!("di ({:?}-{:?}), bi ({:?}-{:?})",dist_index,(dist_index + len),buf_index,(buf_index + len));
                        // unsafe{
                        //     println!("{:?} {:?},",buf.clone().to_base::<u8>().as_slice(), buf.sub_region(buf_index..(buf_index + len)).to_base::<u8>().as_slice());
                        // }
                        if buf.len() * std::mem::size_of::<T>()
                            > crate::active_messaging::BATCH_AM_SIZE
                        {
                            let am = UnsafePutAm {
                                array: self.clone().into(),
                                start_index: index,
                                len: buf.len(),
                                data: unsafe {
                                    buf.sub_region(buf_index..(buf_index + len))
                                        .to_base::<u8>()
                                        .into()
                                },
                                pe: self.inner.data.my_pe,
                            };
                            reqs.push(self.exec_am_pe(pe, am));
                        } else {
                            let am = UnsafeSmallPutAm {
                                array: self.clone().into(),
                                start_index: index,
                                len: buf.len(),
                                data: unsafe {
                                    buf.sub_region(buf_index..(buf_index + len))
                                        .to_base::<u8>()
                                        .as_slice()
                                        .expect("array data to exist on PE")
                                        .to_vec()
                                },
                            };
                            reqs.push(self.exec_am_pe(pe, am));
                        }
                    }
                    ArrayRdmaCmd::GetAm => {
                        // if buf.len()*std::mem::size_of::<T>() > crate::active_messaging::BATCH_AM_SIZE{
                        let am = UnsafeBlockGetAm {
                            array: self.clone().into(),
                            offset: offset,
                            data: unsafe {
                                buf.sub_region(buf_index..(buf_index + len))
                                    .to_base::<u8>()
                                    .into()
                            },
                            pe: pe,
                        };
                        reqs.push(self.exec_am_local(am));
                        // }
                        // else {
                        //     let am = UnsafeSmallBlockGetAm {
                        //         array: self.clone().into(),
                        //         offset: offset,
                        //         data: unsafe {
                        //             buf.sub_region(buf_index..(buf_index + len))
                        //                 .to_base::<u8>()
                        //                 .into()
                        //         },
                        //         pe: pe,
                        //     };
                        //     reqs.push(self.exec_am_local(am));
                        // }
                    }
                }
                buf_index += len;
                dist_index += len;
            }
        }
        reqs
    }
    fn cyclic_op<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        op: ArrayRdmaCmd,
        index: usize, //global_index
        buf: U,
    ) -> Vec<Box<dyn LamellarRequest<Output = ()>>> {
        let global_index = index + self.inner.offset;
        // let buf = buf.team_into(&self.inner.data.team);
        let buf = buf.into();
        let my_pe = self.inner.data.my_pe;
        let num_pes = self.inner.data.team.num_pes();
        let num_elems_pe = buf.len() / num_pes + 1; //we add plus one to ensure we allocate enough space
        let mut overflow = 0;
        let start_pe = global_index % num_pes;
        let mut reqs = vec![];
        // println!("start_pe {:?} num_elems_pe {:?} buf len {:?}",start_pe,num_elems_pe,buf.len());
        match op {
            ArrayRdmaCmd::Put => {
                let temp_memreg = self
                    .inner
                    .data
                    .team
                    .alloc_one_sided_mem_region::<T>(num_elems_pe);
                unsafe {
                    for i in 0..std::cmp::min(buf.len(), num_pes) {
                        let mut k = 0;
                        let pe = (start_pe + i) % num_pes;
                        let offset = global_index / num_pes + overflow;
                        for j in (i..buf.len()).step_by(num_pes) {
                            temp_memreg.put(k, buf.sub_region(j..=j));
                            k += 1;
                        }
                        self.inner.data.mem_region.blocking_put(
                            pe,
                            offset,
                            temp_memreg.sub_region(0..k),
                        );
                        if pe + 1 == num_pes {
                            overflow += 1;
                        }
                    }
                }
            }
            ArrayRdmaCmd::PutAm => {
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    let temp_memreg = self
                        .inner
                        .data
                        .team
                        .alloc_one_sided_mem_region::<T>(num_elems_pe);
                    let mut k = 0;
                    let pe = (start_pe + i) % num_pes;
                    // let offset = global_index / num_pes + overflow;
                    for j in (i..buf.len()).step_by(num_pes) {
                        unsafe { temp_memreg.put(k, buf.sub_region(j..=j)) };
                        k += 1;
                    }
                    // println!("{:?}",temp_memreg.clone().to_base::<u8>().as_slice());
                    // println!("si: {:?} ei {:?}",offset,offset+k);

                    if buf.len() * std::mem::size_of::<T>() > crate::active_messaging::BATCH_AM_SIZE
                    {
                        let am = UnsafePutAm {
                            array: self.clone().into(),
                            start_index: index,
                            len: buf.len(),
                            data: unsafe { temp_memreg.to_base::<u8>().into() },
                            pe: self.inner.data.my_pe,
                        };
                        reqs.push(self.exec_am_pe(pe, am));
                    } else {
                        let am = UnsafeSmallPutAm {
                            array: self.clone().into(),
                            start_index: index,
                            len: buf.len(),
                            data: unsafe {
                                temp_memreg
                                    .to_base::<u8>()
                                    .to_base::<u8>()
                                    .as_slice()
                                    .expect("array data to exist on PE")
                                    .to_vec()
                            },
                        };
                        reqs.push(self.exec_am_pe(pe, am));
                    }
                    if pe + 1 == num_pes {
                        overflow += 1;
                    }
                }
            }
            ArrayRdmaCmd::Get(immediate) => {
                unsafe {
                    if immediate {
                        let temp_memreg = self
                            .inner
                            .data
                            .team
                            .alloc_one_sided_mem_region::<T>(num_elems_pe);
                        let rem = buf.len() % num_pes;
                        // let temp_buf: LamellarMemoryRegion<T> = buf.team_into(&self.inner.data.team);
                        for i in 0..std::cmp::min(buf.len(), num_pes) {
                            let pe = (start_pe + i) % num_pes;
                            let offset = global_index / num_pes + overflow;
                            let num_elems = (num_elems_pe - 1) + if i < rem { 1 } else { 0 };
                            // println!("i {:?} pe {:?} num_elems {:?} offset {:?} rem {:?}",i,pe,num_elems,offset,rem);
                            self.inner.data.mem_region.blocking_get(
                                pe,
                                offset,
                                temp_memreg.sub_region(0..num_elems),
                            );
                            // let mut k = 0;
                            // println!("{:?}",temp_memreg.clone().to_base::<u8>().as_slice());

                            for (k, j) in (i..buf.len()).step_by(num_pes).enumerate() {
                                buf.put(my_pe, j, temp_memreg.sub_region(k..=k));
                            }
                            if pe + 1 == num_pes {
                                overflow += 1;
                            }
                        }
                    } else {
                        for i in 0..buf.len() {
                            self.inner.data.mem_region.get_unchecked(
                                (index + i) % num_pes,
                                (index + i) / num_pes,
                                buf.sub_region(i..=i),
                            )
                        }
                    }
                }
            }
            ArrayRdmaCmd::GetAm => {
                // if buf.len()*std::mem::size_of::<T>() > crate::active_messaging::BATCH_AM_SIZE{
                let rem = buf.len() % num_pes;
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    let temp_memreg = self
                        .inner
                        .data
                        .team
                        .alloc_one_sided_mem_region::<T>(num_elems_pe);
                    let pe = (start_pe + i) % num_pes;
                    let offset = global_index / num_pes + overflow;
                    let num_elems = (num_elems_pe - 1) + if i < rem { 1 } else { 0 };
                    // println!("i {:?} pe {:?} k {:?} offset {:?}",i,pe,k,offset);
                    let am = UnsafeCyclicGetAm {
                        array: self.clone().into(),
                        data: unsafe { buf.clone().to_base::<u8>() },
                        temp_data: unsafe {
                            temp_memreg.sub_region(0..num_elems).to_base::<u8>().into()
                        },
                        i: i,
                        pe: pe,
                        my_pe: self.inner.data.my_pe,
                        num_pes: num_pes,
                        offset: offset,
                    };
                    reqs.push(self.exec_am_local(am));
                    if pe + 1 == num_pes {
                        overflow += 1;
                    }
                }
                // }
                // else {
                //     let am = UnsafeSmallGetAm {
                //         array: self.clone().into(),
                //         offset: index,
                //         data: unsafe { buf.clone().to_base::<u8>() },
                //         pe: pe,
                //     };
                //     reqs.push(self.exec_am_local(am));
                // }
            }
        }
        reqs
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

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a raw RDMA "Put" of the data in the specified buffer into this array starting from the provided index
    ///
    /// The length of the Put is dictated by the length of the buffer.
    ///
    /// The runtime provides no internal mechanism to check for completion when using this call.
    /// i.e. this means the users themselves will be responsible for determining when the transfer is complete
    ///
    /// # Warning
    /// This is a low-level API, unless you are very confident in low level distributed memory access it is highly recommended
    /// you use a safe Array type and utilize the LamellarArray load/store operations instead.
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the data buffer is safe to reuse, but the data may or may not have been delivered to the remote memory
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block);
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// let buf_len = buf.len();
    /// unsafe {
    ///     array.dist_iter_mut().for_each(move |elem| *elem = buf_len); //we will used this val as completion detection
    ///     for (i,elem) in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local")
    ///                          .iter_mut()
    ///                          .enumerate(){ //initialize mem_region
    ///         *elem = i;
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",unsafe{array.local_data()});
    /// if my_pe == 0 { //only perfrom the transfer from one PE
    ///     unsafe {array.put_unchecked(0,&buf);}
    ///     println!();
    /// }
    /// // wait for the data to show up
    /// for elem in unsafe{array.local_data()}{
    ///     while *elem == buf.len(){
    ///         std::thread::yield_now();    
    ///     }
    /// }
    ///    
    /// println!("PE{my_pe} array data: {:?}",unsafe{array.local_data()});
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: array data [12,12,12]
    /// PE1: array data [12,12,12]
    /// PE2: array data [12,12,12]
    /// PE3: array data [12,12,12]
    ///
    /// PE0: array data [0,1,2]
    /// PE1: array data [3,4,5]
    /// PE2: array data [6,7,8]
    /// PE3: array data [9,10,11]
    ///```
    pub unsafe fn put_unchecked<U: TeamTryInto<LamellarArrayRdmaInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) {
        match buf.team_try_into(&self.inner.data.team) {
            Ok(buf) => match self.inner.distribution {
                Distribution::Block => {
                    self.block_op(ArrayRdmaCmd::Put, index, buf);
                }
                Distribution::Cyclic => {
                    self.cyclic_op(ArrayRdmaCmd::Put, index, buf);
                }
            },
            Err(_) => {}
        };
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a raw RDMA "Get" of the data in this array starting at the provided index into the specified buffer
    ///
    /// The length of the Get is dictated by the length of the buffer.
    ///
    /// The runtime provides no internal mechanism to check for completion when using this call.
    /// i.e. this means the users themselves will be responsible for determining when the transfer is complete
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block);
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// unsafe {
    ///     array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i);
    ///     for elem in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local") { //initialize mem_region
    ///         *elem = buf.len(); //we will used this val as completion detection
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    /// if my_pe == 0 { //only perfrom the transfer from one PE
    ///     unsafe {array.get_unchecked(0,&buf)};
    ///     println!();
    /// }
    /// // wait for the data to show up
    /// for elem in unsafe{buf.as_slice().unwrap()}{
    ///     while *elem == buf.len(){
    ///         std::thread::yield_now();    
    ///     }
    /// }
    ///    
    /// println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    ///
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE0: buf data [0,1,2,3,4,5,6,7,8,9,10,11] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    ///```
    pub unsafe fn get_unchecked<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        match buf.team_try_into(&self.inner.data.team) {
            Ok(buf) => match self.inner.distribution {
                Distribution::Block => {
                    self.block_op(ArrayRdmaCmd::Get(false), index, buf);
                }
                Distribution::Cyclic => {
                    self.cyclic_op(ArrayRdmaCmd::Get(false), index, buf);
                }
            },
            Err(_) => {}
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs a blocking (active message based) "Get" of the data in this array starting at the provided index into the specified buffer
    ///
    /// The length of the Get is dictated by the length of the buffer.
    ///
    /// When this function returns, `buf` will have been populated with the results of the `get`
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block);
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// unsafe {
    ///     array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i); //we will used this val as completion detection
    ///     for elem in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local") { //initialize mem_region
    ///         *elem = buf.len();
    ///     }
    ///     array.wait_all();
    ///     array.barrier();
    ///     println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    ///     if my_pe == 0 { //only perfrom the transfer from one PE
    ///          println!();
    ///         array.blocking_get(0,&buf);
    ///         
    ///     }
    ///     println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    /// }
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    ///
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE0: buf data [0,1,2,3,4,5,6,7,8,9,10,11] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    ///```
    pub unsafe fn blocking_get<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) {
        // println!("unsafe iget {:?}",index);
        if let Ok(buf) = buf.team_try_into(&self.inner.data.team) {
            match self.inner.distribution {
                Distribution::Block => self.block_op(ArrayRdmaCmd::Get(true), index, buf),
                Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::Get(true), index, buf),
            };
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Performs an (active message based) "Get" of the data in this array starting at the provided index into the specified buffer
    ///
    /// The length of the Get is dictated by the length of the buffer.
    ///
    /// This call returns a future that can be awaited to determine when the `put` has finished
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block);
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// unsafe {
    ///     array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i); //we will used this val as completion detection
    ///     for elem in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local") { //initialize mem_region
    ///         *elem = buf.len();
    ///     }
    ///     array.wait_all();
    ///     array.barrier();
    ///     println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    ///     if my_pe == 0 { //only perfrom the transfer from one PE
    ///          println!();
    ///         let req = array.get(0,&buf);
    ///         world.block_on(req);
    ///     }
    ///     println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    /// }
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    ///
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE0: buf data [0,1,2,3,4,5,6,7,8,9,10,11] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    ///```
    pub unsafe fn get<U>(&self, index: usize, buf: U) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        U: TeamTryInto<LamellarArrayRdmaOutput<T>>,
    {
        match buf.team_try_into(&self.team_rt()) {
            Ok(buf) => self.internal_get(index, buf).into_future(),
            Err(_) => Box::pin(async move { () }),
        }
    }

    pub(crate) unsafe fn internal_at(
        &self,
        index: usize,
    ) -> Box<dyn LamellarArrayRequest<Output = T>> {
        let buf: OneSidedMemoryRegion<T> = self.team_rt().alloc_one_sided_mem_region(1);
        self.blocking_get(index, &buf);
        Box::new(ArrayRdmaAtHandle {
            reqs: vec![],
            buf: buf,
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Retrieves the element in this array located at the specified `index`
    ///
    /// This call returns a future that can be awaited to retrieve to requested element
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,12,Distribution::Block);
    /// unsafe {
    ///     array.dist_iter_mut().enumerate().for_each(move|(i,elem)| *elem = my_pe); //we will used this val as completion detection
    ///     array.wait_all();
    ///     array.barrier();
    ///     println!("PE{my_pe} array data: {:?}",unsafe{array.local_data()});
    ///     let index = ((my_pe+1)%num_pes) * array.num_elems_local(); // get first index on PE to the right (with wrap arround)
    ///     let at_req = array.at(index);
    ///     let val = array.block_on(at_req);
    ///     println!("PE{my_pe} array[{index}] = {val}");
    /// }
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [0,0,0]
    /// PE1: buf data [1,1,1]
    /// PE2: buf data [2,2,2]
    /// PE3: buf data [3,3,3]
    ///
    /// PE0: array[3] = 1
    /// PE1: array[6] = 2
    /// PE2: array[9] = 3
    /// PE3: array[0] = 0
    ///```
    pub unsafe fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.internal_at(index).into_future()
    }
}

/// We dont implement this because "at" is actually same for all but UnsafeArray so we just implement those directly
// impl<T: Dist > LamellarArrayGet<T> for UnsafeArray<T> {
//     fn get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
//         &self,
//         index: usize,
//         dst: U,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
//         self.internal_get(index, dst).into_future()
//     }

//     fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
//         self.internal_at(index).into_future()
//     }
// }

impl<T: Dist> LamellarArrayInternalGet<T> for UnsafeArray<T> {
    unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        let buf = buf.into();
        let reqs = if buf.len() * std::mem::size_of::<T>() > crate::active_messaging::BATCH_AM_SIZE
        {
            match self.inner.distribution {
                Distribution::Block => self.block_op(ArrayRdmaCmd::GetAm, index, buf),
                Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::GetAm, index, buf),
            }
        } else {
            let req = self.exec_am_local(InitSmallGetAm {
                array: self.clone(),
                index: index,
                buf: buf,
            });
            vec![req]
        };
        Box::new(ArrayRdmaHandle { reqs: reqs })
    }

    unsafe fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>> {
        self.internal_at(index)
    }
}

impl<T: Dist> LamellarArrayInternalPut<T> for UnsafeArray<T> {
    unsafe fn internal_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        let reqs = match self.inner.distribution {
            Distribution::Block => self.block_op(ArrayRdmaCmd::PutAm, index, buf.into()),
            Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::PutAm, index, buf.into()),
        };
        Box::new(ArrayRdmaHandle { reqs: reqs })
    }
}

impl<T: Dist> LamellarArrayPut<T> for UnsafeArray<T> {
    unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        match buf.team_try_into(&self.team_rt()) {
            Ok(buf) => self.internal_put(index, buf).into_future(),
            Err(_) => Box::pin(async move { () }),
        }
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

        // println!("i {:?} len {:?} spe {:?} epe {:?}  ",index,len,start_pe,end_pe);
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
                // println!("ssi {:?} si {:?} ei {:?} nel {:?} es {:?}",subarray_start_index,start_index,end_index,num_elems_local,self.elem_size);
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

#[lamellar_impl::AmLocalDataRT(Debug)]
struct UnsafeBlockGetAm {
    array: UnsafeByteArray, //inner of the indices we need to place data into
    offset: usize,
    data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
    pe: usize,
}

#[lamellar_impl::rt_am_local]
impl LamellarAm for UnsafeBlockGetAm {
    async fn exec(self) {
        unsafe {
            self.array.inner.data.mem_region.blocking_get(
                self.pe,
                self.offset * self.array.inner.elem_size,
                self.data.clone(),
            )
        };
    }
}

#[lamellar_impl::AmLocalDataRT(Debug)]
struct UnsafeCyclicGetAm {
    array: UnsafeByteArray, //inner of the indices we need to place data into
    data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
    temp_data: LamellarMemoryRegion<u8>,
    i: usize,
    pe: usize,
    my_pe: usize,
    num_pes: usize,
    offset: usize,
}

#[lamellar_impl::rt_am_local]
impl LamellarAm for UnsafeCyclicGetAm {
    async fn exec(self) {
        unsafe {
            self.array.inner.data.mem_region.blocking_get(
                self.pe,
                self.offset * self.array.inner.elem_size,
                self.temp_data.clone(),
            );
        }
        for (k, j) in (self.i..self.data.len() / self.array.inner.elem_size)
            .step_by(self.num_pes)
            .enumerate()
            .map(|(k, j)| {
                (
                    k * self.array.inner.elem_size,
                    j * self.array.inner.elem_size,
                )
            })
        {
            unsafe {
                self.data.put(
                    self.my_pe,
                    j,
                    self.temp_data
                        .sub_region(k..(k + self.array.inner.elem_size)),
                )
            };
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Debug)]
struct InitSmallGetAm<T: Dist> {
    array: UnsafeArray<T>, //inner of the indices we need to place data into
    index: usize,          //relative to inner
    buf: LamellarMemoryRegion<T>,
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static> LamellarAm for InitSmallGetAm<T> {
    async fn exec(self) {
        let mut reqs = vec![];
        for pe in self
            .array
            .pes_for_range(self.index, self.buf.len())
            .into_iter()
        {
            // println!("pe {:?}",pe);
            let remote_am = UnsafeRemoteSmallGetAm {
                array: self.array.clone().into(),
                start_index: self.index,
                len: self.buf.len(),
            };
            reqs.push(self.array.exec_am_pe(pe, remote_am).into_future());
        }
        unsafe {
            match self.array.inner.distribution {
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
                    let buf_slice = self.buf.as_mut_slice().expect("array data exists on PE");
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
struct UnsafeRemoteSmallGetAm {
    array: UnsafeByteArray, //inner of the indices we need to place data into
    start_index: usize,
    len: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAm for UnsafeRemoteSmallGetAm {
    //we cant directly do a put from the array in to the data buf
    //because we need to guarantee the put operation is atomic (maybe iput would work?)
    async fn exec(self) -> Vec<u8> {
        // println!("in remotegetam {:?} {:?}",self.start_index,self.len);
        // let _lock = self.array.lock.read();
        unsafe {
            match self
                .array
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _)) => elems.to_vec(),
                None => vec![],
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct UnsafePutAm {
    array: UnsafeByteArray,         //byte representation of the array
    start_index: usize,             //index with respect to inner (of type T)
    len: usize,                     //len of buf (with respect to original type T)
    data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
    pe: usize,
}
#[lamellar_impl::rt_am]
impl LamellarAm for UnsafePutAm {
    async fn exec(self) {
        unsafe {
            // println!("unsafe put am: pe {:?} si {:?} len {:?}",self.pe,self.start_index,self.len);
            match self
                .array
                .inner
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _)) => {
                    self.data.blocking_get_slice(self.pe, 0, elems);
                }
                None => {}
            }
        };
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct UnsafeSmallPutAm {
    array: UnsafeByteArray, //byte representation of the array
    start_index: usize,     //index with respect to inner (of type T)
    len: usize,             //len of buf (with respect to original type T)
    data: Vec<u8>, //change this to an enum which is a vector or OneSidedMemoryRegion depending on data size
                   // pe: usize,
}
#[lamellar_impl::rt_am]
impl LamellarAm for UnsafeSmallPutAm {
    async fn exec(self) {
        unsafe {
            // println!("unsafe put am: pe {:?} si {:?} len {:?}",self.pe,self.start_index,self.len);
            match self
                .array
                .inner
                .local_elements_for_range(self.start_index, self.len)
            {
                Some((elems, _)) => std::ptr::copy_nonoverlapping(
                    self.data.as_ptr(),
                    elems.as_mut_ptr(),
                    elems.len(),
                ),
                None => {}
            }
        };
    }
}
