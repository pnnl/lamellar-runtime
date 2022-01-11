use crate::array::r#unsafe::*;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::array::private::ArrayExecAm;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};

impl<T: Dist> UnsafeArray<T> {
    fn block_op<U: MyInto<LamellarArrayInput<T>>>(
        &self,
        op: ArrayRdmaCmd,
        index: usize,
        buf: U,
    ) -> Vec<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let buf = buf.my_into(&self.inner.team);
        let start_pe = (index as f64 / self.elem_per_pe).floor() as usize;
        let end_pe = (((index + buf.len()) as f64) / self.elem_per_pe).ceil() as usize;
        let mut dist_index = index;
        let mut buf_index = 0;
        let mut reqs = vec![];
        for pe in start_pe..end_pe {
            let num_elems_on_pe = (self.elem_per_pe * (pe + 1) as f64).round() as usize
                - (self.elem_per_pe * pe as f64).round() as usize;
            let pe_start_index = (self.elem_per_pe * pe as f64).round() as usize;
            let offset = dist_index - pe_start_index;
            let len = std::cmp::min(num_elems_on_pe - offset, buf.len() - buf_index);
            if len > 0 {
                // println!("pe {:?} offset {:?} range: {:?}-{:?} dist_index {:?} pe_start_index {:?} num_elems {:?} len {:?}", pe, offset, buf_index, buf_index+len, dist_index, pe_start_index, num_elems_on_pe, len);
                match op {
                    ArrayRdmaCmd::Put => unsafe {
                        self.inner.mem_region.put(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        )
                    },
                    ArrayRdmaCmd::Get(immediate) => unsafe {
                        if immediate {
                            self.inner.mem_region.iget(
                                pe,
                                offset,
                                buf.sub_region(buf_index..(buf_index + len)),
                            )
                        } else {
                            self.inner.mem_region.get_unchecked(
                                pe,
                                offset,
                                buf.sub_region(buf_index..(buf_index + len)),
                            )
                        }
                    },
                    ArrayRdmaCmd::PutAm => {
                        let am = UnsafePutAm {
                            array: unsafe {
                                self.sub_array(dist_index..(dist_index + len))
                                    .as_bytes()
                            },
                            data: unsafe {
                                buf.sub_region(buf_index..(buf_index + len))
                                    .to_base::<u8>()
                                    .into()
                            },
                            pe: self.inner.my_pe,
                        };
                        reqs.push(self.exec_am_pe(pe, am));
                    }
                    ArrayRdmaCmd::GetAm => {
                        let am = UnsafeBlockGetAm {
                            array: unsafe {
                                self.sub_array(dist_index..(dist_index + len))
                                    .as_bytes()
                            },
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
        index: usize,
        buf: U,
    ) -> Vec<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let buf = buf.my_into(&self.inner.team);
        let my_pe = self.inner.my_pe;
        let num_pes = self.inner.team.num_pes();
        let num_elems_pe = buf.len() / num_pes + 1; //we add plus one to ensure we allocate enough space
        let mut overflow = 0;
        let start_pe = index % num_pes;
        let mut reqs = vec![];
        match op {
            ArrayRdmaCmd::Put => {
                let temp_memreg = self.inner.team.alloc_local_mem_region::<T>(num_elems_pe);
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    let mut k = 0;
                    let pe = (start_pe + i) % num_pes;
                    let offset = index / num_pes + overflow;
                    for j in (i..buf.len()).step_by(num_pes) {
                        unsafe { temp_memreg.put(my_pe, k, buf.sub_region(j..=j)) };
                        k += 1;
                    }
                    self.inner
                        .mem_region
                        .iput(pe, offset, temp_memreg.sub_region(0..k));
                    if pe + 1 == num_pes {
                        overflow += 1;
                    }
                }
            }
            ArrayRdmaCmd::PutAm => {
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    let temp_memreg = self.inner.team.alloc_local_mem_region::<T>(num_elems_pe);
                    let mut k = 0;
                    let pe = (start_pe + i) % num_pes;
                    let offset = index / num_pes + overflow;
                    for j in (i..buf.len()).step_by(num_pes) {
                        unsafe { temp_memreg.put(my_pe, k, buf.sub_region(j..=j)) };
                        k += 1;
                    }
                    let am = UnsafePutAm {
                        array: unsafe {
                            self.sub_array(offset..(offset + k)).as_bytes()
                        },
                        data: temp_memreg.to_base::<u8>().into(),
                        pe: self.inner.my_pe,
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
                        let temp_memreg = self.inner.team.alloc_local_mem_region::<T>(num_elems_pe);
                        // let temp_buf: LamellarMemoryRegion<T> = buf.my_into(&self.inner.team);
                        for i in 0..std::cmp::min(buf.len(), num_pes) {
                            let pe = (start_pe + i) % num_pes;
                            let offset = index / num_pes + overflow;
                            let k = (buf.len() - i) / num_pes;
                            self.inner
                                .mem_region
                                .iget(pe, offset, temp_memreg.sub_region(0..k));
                            // let mut k = 0;

                            for (k, j) in (i..buf.len()).step_by(num_pes).enumerate() {
                                buf.put(my_pe, j, temp_memreg.sub_region(k..=k));
                            }
                            if pe + 1 == num_pes {
                                overflow += 1;
                            }
                        }
                    } else {
                        for i in 0..buf.len() {
                            self.inner.mem_region.get_unchecked(
                                (index + i) % num_pes,
                                (index + i) / num_pes,
                                buf.sub_region(i..=i),
                            )
                        }
                    }
                }
            }
            ArrayRdmaCmd::GetAm => {
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    let temp_memreg = self.inner.team.alloc_local_mem_region::<T>(num_elems_pe);
                    let pe = (start_pe + i) % num_pes;
                    let offset = index / num_pes + overflow;
                    let k = (buf.len() - i) / num_pes;
                    let am = UnsafeCyclicGetAm {
                        array: unsafe { self.clone().as_bytes() },
                        data: unsafe { buf.clone().to_base::<u8>() },
                        temp_data: temp_memreg.sub_region(0..k).to_base::<u8>().into(),
                        i: i,
                        pe: pe,
                        my_pe: self.inner.my_pe,
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

    pub(crate) fn pes_for_range(&self, start_index: usize, end_index: usize) -> Box<dyn Iterator<Item=usize>>{
        let global_start = self.sub_array_offset + start_index;
        let global_end = self.sub_array_offset + end_index;
        match self.distribution {
            Distribution::Block => {
                let start_pe = (global_start as f64 / self.elem_per_pe).floor() as usize;
                let end_pe = (global_end as f64 / self.elem_per_pe).ceil() as usize;
                return Box::new(start_pe..end_pe);
            }
            Distribution::Cyclic => {
                let num_pes =self.inner.team.num_pes();
                let len = global_end-global_start;
                if num_pes < len {
                    return Box::new(0..num_pes);
                }
                else{
                    let mut pes = vec![];
                    let start_pe = global_start % num_pes;
                    for index in global_start..global_end{
                        pes.push(index % num_pes)
                    }
                    return Box::new(pes.into_iter());
                }
            }
        }
            
    }

    // pub(crate) fn elems_per_pes_for_range(&self, start_index: usize, end_index: usize) -> Box<dyn Iterator<Item=(usize,usize)>>{
    //     let global_start = self.sub_array_offset + start_index;
    //     let global_end = self.sub_array_offset + end_index;
    //     let num_pes = self.inner.team.num_pes();
    //     match self.distribution {
    //         Distribution::Block => {
    //             let start_pe = (global_start as f64 / self.elem_per_pe).floor() as usize;
    //             let end_pe = (global_end as f64 / self.elem_per_pe).ceil() as usize;
    //             let elem_per_pe = self.elem_per_pe;
    //             // let elems_per_pe = (start_pe..end_pe);
    //             return Box::new((start_pe..end_pe).map( move |pe| {
    //                 (pe,(elem_per_pe * (pe + 1) as f64).round() as usize - (elem_per_pe * (pe) as f64).round() as usize)
    //             }));
    //         }
    //         Distribution::Cyclic => {
    //             let len = global_end - global_start;
    //             let start_pe = global_start % num_pes; 
    //             let end_pe = global_end % num_pes;
    //             let num_elems_pe = len / num_pes;
    //             let rem_elems = len % num_pes; 
                
    //             if rem_elems == 0{
    //                 return Box::new((0..num_pes).map(move |pe| {
    //                     (pe,num_elems_pe)
    //                 }));
    //             }
    //             else {
    //                 return Box::new((0..num_pes).map(move |pe| {
    //                     if pe >= start_pe && pe < start_pe + rem_elems{
    //                         (pe,num_elems_pe +1)
    //                     }
    //                     else{
    //                         (pe,num_elems_pe)
    //                     }
    //                 }));
    //             }
    //         }
    //     }
            
    // }


    pub(crate) unsafe fn local_elements_for_range(&self, start_index: usize, end_index: usize) -> Option<(&mut [T],Box<dyn Iterator<Item=usize>>)> {
        let global_start = self.sub_array_offset + start_index;
        let global_end = self.sub_array_offset + end_index;
        let my_pe = self.inner.my_pe;
        let num_pes = self.inner.team.num_pes();
        match self.distribution {
            Distribution::Block => {
                let my_global_start_index = (self.elem_per_pe * my_pe as f64).round() as usize;
                let num_elems_on_pe = (self.elem_per_pe * (my_pe + 1) as f64).round() as usize
                - my_global_start_index;
                let my_global_end_index = my_global_start_index + num_elems_on_pe;

                if global_end < my_global_start_index || my_global_end_index < global_start{ //starts and ends before this pe, or starts (and ends) after this pe... no local elements
                    return None;
                }                

                let (my_local_start,_my_global_start) = if global_start < my_global_start_index{ //starts on pe less than me
                    (0,my_global_start_index)
                }
                else{ //starts on my pe
                    (global_start - my_global_start_index,global_start)
                };

                let (my_local_end,_my_global_end) = if global_end > my_global_end_index{ //ends on pe greater than me
                    (num_elems_on_pe - 1 , my_global_end_index)
                }
                else {
                    (global_end-my_global_start_index, global_end)
                };
                Some((&mut self.local_as_mut_slice()[my_local_start..my_local_end],Box::new(my_local_start..my_local_end)))
            }
            Distribution::Cyclic => {
                let len = global_end - global_start;
                let start_pe = global_start % num_pes; 
                let end_pe = global_end % num_pes;
                let num_elems_pe = len / num_pes;
                let rem_elems = len % num_pes; 
                let mut my_local_start = global_start/num_pes;
                let mut _my_global_start = global_start;
                if my_pe < start_pe { 
                    my_local_start += 1;
                    _my_global_start += (num_pes-start_pe) + my_pe
                }
                else{
                    _my_global_start += my_pe - start_pe
                }
                let mut my_local_end  = (global_end/num_pes) as isize;
                let mut _my_global_end = global_end;
                if my_pe > end_pe {
                    my_local_end -=1;
                    _my_global_end -= (num_pes - my_pe) + end_pe;
                }else{
                    _my_global_end -= end_pe - my_pe;
                }
                if my_local_end >= my_local_start as isize{
                    Some((&mut self.local_as_mut_slice()[my_local_start..my_local_end as usize],Box::new(my_local_start..my_local_end as usize)))
                }
                else{
                    None
                }
            }

        }
    }

    pub(crate) unsafe fn elements_on_pe_for_range(&self,pe: usize, start_index: usize, end_index: usize) -> Option<usize> {
        let global_start = self.sub_array_offset + start_index;
        let global_end = self.sub_array_offset + end_index;
        let num_pes = self.inner.team.num_pes();
        match self.distribution {
            Distribution::Block => {
                let my_global_start_index = (self.elem_per_pe * pe as f64).round() as usize;
                let num_elems_on_pe = (self.elem_per_pe * (pe + 1) as f64).round() as usize
                - my_global_start_index;
                let my_global_end_index = my_global_start_index + num_elems_on_pe;

                if global_end < my_global_start_index || my_global_end_index < global_start{ //starts and ends before this pe, or starts (and ends) after this pe... no local elements
                    return None;
                }                

                let (my_local_start,_my_global_start) = if global_start < my_global_start_index{ //starts on pe less than me
                    (0,my_global_start_index)
                }
                else{ //starts on my pe
                    (global_start - my_global_start_index,global_start)
                };

                let (my_local_end,_my_global_end) = if global_end > my_global_end_index{ //ends on pe greater than me
                    (num_elems_on_pe - 1 , my_global_end_index)
                }
                else {
                    (global_end-my_global_start_index, global_end)
                };
                Some(my_local_end-my_local_start)
            }
            Distribution::Cyclic => {
                let len = global_end - global_start;
                let start_pe = global_start % num_pes; 
                let end_pe = global_end % num_pes;
                let num_elems_pe = len / num_pes;
                let rem_elems = len % num_pes; 
                let mut my_local_start = global_start/num_pes;
                let mut _my_global_start = global_start;
                if pe < start_pe { 
                    my_local_start += 1;
                    _my_global_start += (num_pes-start_pe) + pe
                }
                else{
                    _my_global_start += pe - start_pe
                }
                let mut my_local_end  = (global_end/num_pes) as isize;
                let mut _my_global_end = global_end;
                if pe > end_pe {
                    my_local_end -=1;
                    _my_global_end -= (num_pes - pe) + end_pe;
                }else{
                    _my_global_end -= end_pe - pe;
                }
                if my_local_end >= my_local_start as isize{
                    Some( my_local_end as usize -my_local_start)
                }
                else{
                    None
                }
            }

        }
    }

    pub unsafe fn put_unchecked<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => {
                self.block_op(ArrayRdmaCmd::Put, self.sub_array_offset + index, buf)
            }
            Distribution::Cyclic => {
                self.cyclic_op(ArrayRdmaCmd::Put, self.sub_array_offset + index, buf)
            }
        };
    }

    pub fn iput<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        let reqs = match self.distribution {
            Distribution::Block => {
                self.block_op(ArrayRdmaCmd::PutAm, self.sub_array_offset + index, buf)
            }
            Distribution::Cyclic => {
                self.cyclic_op(ArrayRdmaCmd::PutAm, self.sub_array_offset + index, buf)
            }
        };
        for req in reqs {
            req.get();
        }
    }
    pub fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => {
                self.block_op(ArrayRdmaCmd::PutAm, self.sub_array_offset + index, buf)
            }
            Distribution::Cyclic => {
                self.cyclic_op(ArrayRdmaCmd::Put, self.sub_array_offset + index, buf)
            }
        };
    }

    pub unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => {
                self.block_op(ArrayRdmaCmd::Get(false), self.sub_array_offset + index, buf)
            }
            Distribution::Cyclic => {
                self.cyclic_op(ArrayRdmaCmd::Get(false), self.sub_array_offset + index, buf)
            }
        };
    }
    pub fn iget<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        let reqs = match self.distribution {
            Distribution::Block => {
                self.block_op(ArrayRdmaCmd::Get(true), self.sub_array_offset + index, buf)
            }
            Distribution::Cyclic => {
                self.cyclic_op(ArrayRdmaCmd::Get(true), self.sub_array_offset + index, buf)
            }
        };
        for req in reqs {
            req.get();
        }
    }

    pub fn get<U>(&self, index: usize, buf: U)
    where
        U: MyInto<LamellarArrayInput<T>>,
    {
        match self.distribution {
            Distribution::Block => {
                self.block_op(ArrayRdmaCmd::GetAm, self.sub_array_offset + index, buf)
            }
            Distribution::Cyclic => {
                self.cyclic_op(ArrayRdmaCmd::GetAm, self.sub_array_offset + index, buf)
            }
        };
    }

    pub fn iat(&self, index: usize) -> T {
        let buf: LocalMemoryRegion<T> = self.team().alloc_local_mem_region(1);
        self.iget(index, &buf);
        buf.as_slice().unwrap()[0]
    }
}

impl<T: Dist + 'static> LamellarArrayRead<T> for UnsafeArray<T> {
    // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) {
    //     self.get_unchecked(index, buf)
    // }
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.iget(index, buf)
    }
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.get(index, buf)
    }
    fn iat(&self, index: usize) -> T {
        self.iat(index)
    }
}

impl<T: Dist> LamellarArrayWrite<T> for UnsafeArray<T> {
    // unsafe fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.put_unchecked(index, buf)
    // }
}

#[lamellar_impl::AmLocalDataRT]
struct UnsafeBlockGetAm {
    array: UnsafeArray<u8>, //subarray of the indices we need to place data into
    data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or localmemoryregion depending on data size
    pe: usize,
}

#[lamellar_impl::rt_am_local]
impl LamellarAm for UnsafeBlockGetAm {
    fn exec(self) {
        self.array
            .inner
            .mem_region
            .iget(self.pe, 0, self.data.clone());
    }
}
#[lamellar_impl::AmLocalDataRT]
struct UnsafeCyclicGetAm {
    array: UnsafeArray<u8>, //subarray of the indices we need to place data into
    data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or localmemoryregion depending on data size
    temp_data: LamellarMemoryRegion<u8>,
    i: usize,
    pe: usize,
    my_pe: usize,
    num_pes: usize,
    offset: usize,
}

#[lamellar_impl::rt_am_local]
impl LamellarAm for UnsafeCyclicGetAm {
    fn exec(self) {
        self.array
            .inner
            .mem_region
            .iget(self.pe, self.offset, self.temp_data.clone());
        for (k, j) in (self.i..self.temp_data.len())
            .step_by(self.num_pes)
            .enumerate()
        {
            unsafe {
                self.data
                    .put(self.my_pe, j, self.temp_data.sub_region(k..=k))
            };
        }
    }
}

#[lamellar_impl::AmDataRT]
struct UnsafePutAm {
    array: UnsafeArray<u8>, //subarray of the indices we need to place data into
    data: LamellarMemoryRegion<u8>, //change this to an enum which is a vector or localmemoryregion depending on data size
    pe: usize,
}
#[lamellar_impl::rt_am]
impl LamellarAm for UnsafePutAm {
    fn exec(self) {
        unsafe {
            self.data
                .iget_slice(self.pe, 0, self.array.local_as_mut_slice())
        };
    }
}
