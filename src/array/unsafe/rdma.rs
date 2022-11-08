use crate::array::private::ArrayExecAm;
use crate::array::r#unsafe::*;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};

// use tracing::*;

impl<T: Dist> UnsafeArray<T> {
    fn block_op<U: MyInto<LamellarArrayInput<T>>>(
        &self,
        op: ArrayRdmaCmd,
        index: usize, //relative to inner
        buf: U,
    ) -> Vec<Box<dyn LamellarRequest<Output = ()>>> {
        let global_index = index + self.inner.offset;
        let buf = buf.my_into(&self.inner.data.team);
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
                        self.inner.data.mem_region.put(
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
                    }
                    ArrayRdmaCmd::GetAm => {
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
                    }
                }
                buf_index += len;
                dist_index += len;
            }
        }
        reqs
    }
    fn cyclic_op<U: MyInto<LamellarArrayInput<T>>>(
        &self,
        op: ArrayRdmaCmd,
        index: usize, //global_index
        buf: U,
    ) -> Vec<Box<dyn LamellarRequest<Output = ()>>> {
        let global_index = index + self.inner.offset;
        let buf = buf.my_into(&self.inner.data.team);
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
                        self.inner
                            .data
                            .mem_region
                            .blocking_put(pe, offset, temp_memreg.sub_region(0..k));
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
                    let am = UnsafePutAm {
                        array: self.clone().into(),
                        start_index: index,
                        len: buf.len(),
                        data: unsafe {temp_memreg.to_base::<u8>().into()},
                        pe: self.inner.data.my_pe,
                    };
                    reqs.push(self.exec_am_pe(pe, am));
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
                        // let temp_buf: LamellarMemoryRegion<T> = buf.my_into(&self.inner.data.team);
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
                        temp_data: unsafe{temp_memreg.sub_region(0..num_elems).to_base::<u8>().into()},
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

    // pub(crate) unsafe fn local_elements_for_range(
    //     &self,
    //     index: usize,
    //     len: usize,
    // ) -> Option<(&mut [u8], Box<dyn Iterator<Item = usize>>)> {
    //     self.inner.local_elements_for_range(index, len)
    // }

    pub(crate) fn num_elements_on_pe_for_range(
        &self,
        pe: usize,
        start_index: usize,
        len: usize,
    ) -> Option<usize> {
        self.inner
            .num_elements_on_pe_for_range(pe, start_index, len)
    }

    pub unsafe fn put_unchecked<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.inner.distribution {
            Distribution::Block => self.block_op(ArrayRdmaCmd::Put, index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::Put, index, buf),
        };
    }

    // pub fn iput<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
    //     let reqs = match self.inner.distribution {
    //         Distribution::Block => self.block_op(ArrayRdmaCmd::PutAm, index, buf),
    //         Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::PutAm, index, buf),
    //     };
    //     for req in reqs {
    //         // println!("waiting for req");
    //         req.get();
    //     }
    // }

    pub(crate) fn internal_put<U: MyInto<LamellarArrayInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        let reqs = match self.inner.distribution {
            Distribution::Block => self.block_op(ArrayRdmaCmd::PutAm, index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::PutAm, index, buf),
        };
        Box::new(ArrayRdmaHandle { reqs: reqs })
    }

    pub fn put<U: MyInto<LamellarArrayInput<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.internal_put(index, buf).into_future()
    }

    pub unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.inner.distribution {
            Distribution::Block => self.block_op(ArrayRdmaCmd::Get(false), index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::Get(false), index, buf),
        };
    }
    pub fn iget<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        // println!("unsafe iget {:?}",index);
        match self.inner.distribution {
            Distribution::Block => self.block_op(ArrayRdmaCmd::Get(true), index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::Get(true), index, buf),
        };
    }

    pub(crate) fn internal_get<U>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>>
    where
        U: MyInto<LamellarArrayInput<T>>,
    {
        let reqs = match self.inner.distribution {
            Distribution::Block => self.block_op(ArrayRdmaCmd::GetAm, index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayRdmaCmd::GetAm, index, buf),
        };
        Box::new(ArrayRdmaHandle { reqs: reqs })
    }

    pub fn get<U>(&self, index: usize, buf: U) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        U: MyInto<LamellarArrayInput<T>>,
    {
        self.internal_get(index, buf).into_future()
    }

    pub(crate) fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>> {
        let buf: OneSidedMemoryRegion<T> = self.team().alloc_one_sided_mem_region(1);
        self.iget(index, &buf);
        Box::new(ArrayRdmaAtHandle {
            reqs: vec![],
            buf: buf,
        })
    }
    pub fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.internal_at(index).into_future()
    }
}

impl<T: Dist + 'static> LamellarArrayGet<T> for UnsafeArray<T> {
    // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) {
    //     self.get_unchecked(index, buf)
    // }
    // fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
    //     self.iget(index, buf)
    // }
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.get(index, buf)
    }
    fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.at(index)
    }
}

impl<T: Dist + 'static> LamellarArrayInternalGet<T> for UnsafeArray<T> {
    fn internal_get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        self.internal_get(index, buf)
    }
    fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>> {
        self.internal_at(index)
    }
}

impl<T: Dist> LamellarArrayPut<T> for UnsafeArray<T> {
    // unsafe fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.put_unchecked(index, buf)
    // }
    fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.put(index, buf)
    }
}

impl<T: Dist> LamellarArrayInternalPut<T> for UnsafeArray<T> {
    // unsafe fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.put_unchecked(index, buf)
    // }
    fn internal_put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        self.internal_put(index, buf)
    }
}

impl UnsafeByteArray {
    // pub(crate) fn pes_for_range(&self,index: usize, len: usize) ->  Box<dyn Iterator<Item=usize>>{
    //     self.inner.pes_for_range(index,len)
    // }

    pub(crate) unsafe fn local_elements_for_range(
        &self,
        index: usize,
        len: usize,
    ) -> Option<(&mut [u8], Box<dyn Iterator<Item = usize>>)> {
        self.inner.local_elements_for_range(index, len)
    }

    // pub(crate) fn num_elements_on_pe_for_range(&self,pe: usize, start_index: usize, len: usize) -> Option<usize> {
    //     self.inner.num_elements_on_pe_for_range(pe,start_index,len)
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

    //index with respect to inner
    pub(crate) unsafe fn local_elements_for_range(
        &self,
        index: usize,
        len: usize,
    ) -> Option<(&mut [u8], Box<dyn Iterator<Item = usize>>)> {
        let my_pe = self.data.my_pe;
        let start_pe = self.pe_for_dist_index(index).unwrap();
        let end_pe = self.pe_for_dist_index(index + len - 1).unwrap();

        // println!("i {:?} len {:?} spe {:?} epe {:?}  ",index,len,start_pe,end_pe);
        match self.distribution {
            Distribution::Block => {
                let num_elems_local = self.num_elems_local();
                if my_pe > end_pe || my_pe < start_pe {
                    //starts and ends before this pe, or starts (and ends) after this pe... no local elements
                    return None;
                }
                let subarray_start_index = self.start_index_for_pe(my_pe).unwrap(); //passed the above if statement;
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
        let start_pe = self.pe_for_dist_index(index).unwrap();
        let end_pe = self.pe_for_dist_index(index + len - 1).unwrap();

        // println!("i {:?} len {:?} pe {:?} spe {:?} epe {:?}  ",index,len,pe,start_pe,end_pe);
        match self.distribution {
            Distribution::Block => {
                let num_elems_pe = self.num_elems_pe(pe);
                if pe > end_pe || pe < start_pe {
                    //starts and ends before this pe, or starts (and ends) after this pe... no local elements
                    return None;
                }
                let subarray_start_index = self.start_index_for_pe(pe).unwrap(); //passed the above if statement;
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
        unsafe {self.array.inner.data.mem_region.blocking_get(
            self.pe,
            self.offset * self.array.inner.elem_size,
            self.data.clone(),
        )};
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
