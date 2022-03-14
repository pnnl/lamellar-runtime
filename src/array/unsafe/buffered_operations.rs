use crate::active_messaging::*;
use crate::array::r#unsafe::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
// use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

type OpFn = fn(*const u8, UnsafeByteArray, usize) -> LamellarArcAm;

lazy_static! {
    static ref OPS: HashMap<(ArrayOpCmd, TypeId), OpFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<UnsafeArrayOp> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct UnsafeArrayOp {
    pub id: (ArrayOpCmd, TypeId),
    pub op: OpFn,
}

crate::inventory::collect!(UnsafeArrayOp);

type BufFn = fn(UnsafeByteArray) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<UnsafeArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct UnsafeArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(UnsafeArrayOpBuf);

impl<T: AmDist + Dist + 'static> UnsafeArray<T> {
    pub(crate) fn dummy_val(&self) -> T {
        let slice = self.inner.data.mem_region.as_slice().unwrap();
        unsafe {
            std::slice::from_raw_parts(
                slice.as_ptr() as *const T,
                slice.len() / std::mem::size_of::<T>(),
            )[0]
        }
    }

    pub(crate) fn initiate_op(
        &self,
        pe: usize,
        val: T,
        local_index: &[usize],
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        let mut stall_mark = self.inner.data.req_cnt.fetch_add(1, Ordering::SeqCst);
        let buf_op = self.inner.data.op_buffers.read()[pe].clone();
        let (first, complete) = buf_op.add_ops(op, local_index, &val as *const T as *const u8);
        if first {
            let array = self.clone();
            self.inner
                .data
                .team
                .team_counters
                .outstanding_reqs
                .fetch_add(1, Ordering::SeqCst); // we need to tell the world we have a request pending
            self.inner.data.team.scheduler.submit_task(async move {
                // println!("starting");
                let mut wait_cnt = 0;
                while wait_cnt < 1000
                    && array.inner.data.req_cnt.load(Ordering::Relaxed)
                        * std::mem::size_of::<(ArrayOpCmd, usize, T)>()
                        < 100000000
                {
                    while stall_mark != array.inner.data.req_cnt.load(Ordering::Relaxed) {
                        stall_mark = array.inner.data.req_cnt.load(Ordering::Relaxed);
                        async_std::task::yield_now().await;
                    }
                    wait_cnt += 1;
                    async_std::task::yield_now().await;
                }

                let (ams, len, complete, results) = buf_op.into_arc_am(array.sub_array_range());
                if len > 0 {
                    let mut res = Vec::new();
                    for am in ams{
                        res.push(array.inner.data.team.exec_arc_am_pe::<Vec<u8>>(
                            pe,
                            am,
                            Some(array.inner.data.array_counters.clone()),
                        ));
                    }

                    let mut full_results: Vec<u8> = Vec::new();
                    for r in res{
                    // println!("submitted indirectly {:?} ",len);
                        let results_u8: Vec<u8> = r.into_future().await.unwrap();
                        full_results.extend(results_u8);
                    }
                    let mut results = results.write();
                    std::mem::swap(&mut full_results, &mut results);
                    complete.store(true, Ordering::Relaxed);
                    // println!("indirectly done!");
                    let _cnt1 = array
                        .inner
                        .data
                        .team
                        .team_counters
                        .outstanding_reqs
                        .fetch_sub(1, Ordering::SeqCst); //remove our pending req now that it has actually been submitted;
                    let _cnt2 = array.inner.data.req_cnt.fetch_sub(len, Ordering::SeqCst);
                    
                    // println!("indirect cnts {:?}->{:?} {:?}->{:?} -- {:?}",cnt1,cnt1-1,cnt2,cnt2-len,len);
                } else {
                    array
                        .inner
                        .data
                        .team
                        .team_counters
                        .outstanding_reqs
                        .fetch_sub(1, Ordering::SeqCst);
                }
            });
        } 
        Box::new(ArrayOpHandle { complete: complete })
    }

    pub(crate) fn initiate_fetch_op(
        &self,
        pe: usize,
        val: T,
        local_index: usize,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let mut stall_mark = self.inner.data.req_cnt.fetch_add(1, Ordering::SeqCst);
        let buf_op = self.inner.data.op_buffers.read()[pe].clone();
        let (first, complete, res_index, results) =
            buf_op.add_fetch_op(op, local_index, &val as *const T as *const u8);
        if first {
            // println!("pending buf for pe {:?}",pe);
            let array = self.clone();
            self.inner
                .data
                .team
                .team_counters
                .outstanding_reqs
                .fetch_add(1, Ordering::SeqCst); // we need to tell the world we have a request pending
            self.inner.data.team.scheduler.submit_task(async move {
                let mut wait_cnt = 0;
                while wait_cnt < 1000 {
                    while stall_mark != array.inner.data.req_cnt.load(Ordering::Relaxed) {
                        stall_mark = array.inner.data.req_cnt.load(Ordering::Relaxed);
                        async_std::task::yield_now().await;
                    }
                    wait_cnt += 1;
                    async_std::task::yield_now().await;
                }

                let (ams, len, complete, results) = buf_op.into_arc_am(array.sub_array_range());
                
                let mut res = Vec::new();
                for am in ams{
                    res.push(array.inner.data.team.exec_arc_am_pe::<Vec<u8>>(
                        pe,
                        am,
                        Some(array.inner.data.array_counters.clone()),
                    ));
                }

                let mut full_results: Vec<u8> = Vec::new();
                for r in res{
                // println!("submitted indirectly {:?} ",len);
                    let results_u8: Vec<u8> = r.into_future().await.unwrap();
                    full_results.extend(results_u8);
                }
                let mut results = results.write();
                std::mem::swap(&mut full_results, &mut results);
                complete.store(true, Ordering::Relaxed);
                array
                    .inner
                    .data
                    .team
                    .team_counters
                    .outstanding_reqs
                    .fetch_sub(1, Ordering::SeqCst); //remove our pending req now that it has actually been executed;
                array.inner.data.req_cnt.fetch_sub(len, Ordering::SeqCst);
                // println!("done!");
            });
        }
        Box::new(ArrayOpFetchHandle {
            index: res_index,
            complete: complete,
            results: results,
            _phantom: PhantomData,
        })
    }

    // pub fn add_indices(&self, indices: &[usize], val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>{
    //     let mut pe_offsets: HashMap<usize,Vec<usize>> = HashMap::new();
    //     for i in indices{
    //         let pe = self
    //         .inner
    //         .pe_for_dist_index(*i)
    //         .expect("index out of bounds");
    //         let local_index = self.inner.pe_offset_for_dist_index(pe, *i).unwrap(); //calculated pe above
    //         pe_offsets.entry(pe).or_insert(Vec::new()).push(local_index);
    //     }
    //     for (pe,vals) in pe_offsets{
    //     //     if vals.len() * std::mem::size_of::<(ArrayOpCmd, usize, T)>() > 10000000{
    //     //         let n = (vals.len() as f32/10000000.0).ceil();
    //     //         let num = vals.len() as f32 / n as f32;
    //     //         for i in 0..n as usize{
    //     //             self.initiate_op(pe, val, &vals[(i as f32*num).round() as usize..((1.0 + i as f32)*num).round() as usize], ArrayOpCmd::Add);
                
    //     //         }
    //     //     }
    //     //     else{
    //             self.initiate_op(pe, val, &vals, ArrayOpCmd::Add);
    //         // }
    //     }
    //     None
    // }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for UnsafeArray<T> {
    fn add(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let mut pe_offsets: HashMap<usize,Vec<usize>> = HashMap::new();
        let indices = index.as_op_input();
        // println!("indices {:?}",indices);
        for i in indices{
            let pe = self
            .inner
            .pe_for_dist_index(i)
            .expect("index out of bounds");
            let local_index = self.inner.pe_offset_for_dist_index(pe, i).unwrap(); //calculated pe above
            pe_offsets.entry(pe).or_insert(Vec::new()).push(local_index);
        }
        pe_offsets.iter().map(|(pe,vals)| self.initiate_op(*pe, val, &vals, ArrayOpCmd::Add)).last()
       
    }
    fn fetch_add(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchAdd)
    }
    fn sub(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, std::slice::from_ref(&local_index), ArrayOpCmd::Sub))
    }
    fn fetch_sub(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchSub)
    }
    fn mul(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, std::slice::from_ref(&local_index), ArrayOpCmd::Mul))
    }
    fn fetch_mul(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchMul)
    }
    fn div(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, std::slice::from_ref(&local_index), ArrayOpCmd::Div))
    }
    fn fetch_div(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchDiv)
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for UnsafeArray<T> {
    fn bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, std::slice::from_ref(&local_index), ArrayOpCmd::And))
    }
    fn fetch_bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchAnd)
    }

    fn bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, std::slice::from_ref(&local_index), ArrayOpCmd::Or))
    }
    fn fetch_bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchOr)
    }
}

// impl<T: Dist + std::ops::AddAssign> UnsafeArray<T> {
// impl<T: ElementArithmeticOps> LocalArithmeticOps<T> for UnsafeArray<T> {
//     fn local_fetch_add(&self, index: usize, val: T) -> T {
//         // println!("local_add LocalArithmeticOps<T> for UnsafeArray<T> ");
//         unsafe {
//             let orig = self.local_as_mut_slice()[index];
//             self.local_as_mut_slice()[index] += val;
//             orig
//         }
//     }
//     fn local_fetch_sub(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
//         unsafe {
//             let orig = self.local_as_mut_slice()[index];
//             self.local_as_mut_slice()[index] -= val;
//             orig
//         }
//     }
//     fn local_fetch_mul(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
//         unsafe {
//             let orig = self.local_as_mut_slice()[index];
//             self.local_as_mut_slice()[index] *= val;
//             // println!("orig: {:?} new {:?} va; {:?}",orig,self.local_as_mut_slice()[index] ,val);
//             orig
//         }
//     }
//     fn local_fetch_div(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
//         unsafe {
//             let orig = self.local_as_mut_slice()[index];
//             self.local_as_mut_slice()[index] /= val;
//             // println!("div i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//             orig
//         }
//     }
// }
// impl<T: ElementBitWiseOps> LocalBitWiseOps<T> for UnsafeArray<T> {
//     fn local_fetch_bit_and(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
//         unsafe {
//             let orig = self.local_as_mut_slice()[index];
//             self.local_as_mut_slice()[index] &= val;
//             orig
//         }
//     }
//     fn local_fetch_bit_or(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
//         unsafe {
//             let orig = self.local_as_mut_slice()[index];
//             self.local_as_mut_slice()[index] |= val;
//             orig
//         }
//     }
// }

// #[macro_export]
// macro_rules! UnsafeArray_create_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::unsafearray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}
//         }
//     }
// }

// #[macro_export]
// macro_rules! UnsafeArray_create_bitwise_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::unsafearray_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
//             $crate::unsafearray_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
//         }
//     }
// }
// #[macro_export]
// macro_rules! unsafearray_register {
//     ($id:ident, $optype:path, $op:ident, $local:ident) => {
//         inventory::submit! {
//             #![crate =$crate]
//             $crate::array::UnsafeArrayOp{
//                 id: ($optype,std::any::TypeId::of::<$id>()),
//                 op: $op,
//             }
//         }
//     };
// }
