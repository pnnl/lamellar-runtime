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
        val: T,
        index: impl OpInput<usize>,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        let mut pe_offsets: HashMap<usize,Vec<usize>> = HashMap::new();
        let indices = index.as_op_input();
        for i in indices{
            let pe = self
            .inner
            .pe_for_dist_index(i)
            .expect("index out of bounds");
            let local_index = self.inner.pe_offset_for_dist_index(pe, i).unwrap(); //calculated pe above
            pe_offsets.entry(pe).or_insert(Vec::new()).push(local_index);
        }
        pe_offsets.iter().map(|(pe,indices)|{
            let pe = *pe;
            let mut stall_mark = self.inner.data.req_cnt.fetch_add(1, Ordering::SeqCst);
            let buf_op = self.inner.data.op_buffers.read()[pe].clone();
            let (first, complete) = buf_op.add_ops(op, indices, &val as *const T as *const u8);
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

                    let (ams, len, complete, results) = buf_op.into_arc_am(array.sub_array_range(),self.inner.wait.clone());
                    // println!("pe{:?} ams: {:?} len{:?}",pe,ams.len(),len);
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
                        println!("here {:?} {:?} ",ams.len(),len);
                        complete.store(true, Ordering::Relaxed);
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
        }).last().unwrap()
        
    }

    pub(crate) fn initiate_fetch_op(
        &self,
        val: T,
        index: impl OpInput<usize>,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        let mut pe_offsets: HashMap<usize,Vec<usize>> = HashMap::new();
        let indices = index.as_op_input();
        let  res_indices = Arc::new(Mutex::new(vec![]));
        for i in indices{
            let pe = self
            .inner
            .pe_for_dist_index(i)
            .expect("index out of bounds");
            let local_index = self.inner.pe_offset_for_dist_index(pe, i).unwrap(); //calculated pe above
            // println!("i: {:?} pe: {:?} local_index{:?}",i,pe,local_index);
            pe_offsets.entry(pe).or_insert(Vec::new()).push(local_index);
        }
        
        pe_offsets.iter().map(|(pe,indices)|{
            let pe = *pe;
            let mut stall_mark = self.inner.data.req_cnt.fetch_add(1, Ordering::SeqCst);
            let buf_op = self.inner.data.op_buffers.read()[pe].clone();
            let (first, complete, results) =
                buf_op.add_fetch_ops(op, indices, &val as *const T as *const u8, res_indices.clone());
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

                    let (ams, len, complete, results) = buf_op.into_arc_am(array.sub_array_range(),self.inner.wait.clone());
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
                        array
                            .inner
                            .data
                            .team
                            .team_counters
                            .outstanding_reqs
                            .fetch_sub(1, Ordering::SeqCst); //remove our pending req now that it has actually been executed;
                        array.inner.data.req_cnt.fetch_sub(len, Ordering::SeqCst);
                    }else{
                        complete.store(true, Ordering::Relaxed);
                        array
                            .inner
                            .data
                            .team
                            .team_counters
                            .outstanding_reqs
                            .fetch_sub(1, Ordering::SeqCst); //remove our pending req now that it has actually been executed;
                    }
                    // println!("done!");
                });
            }
            Box::new(ArrayOpFetchHandle {
                indices: res_indices.clone(),
                complete: complete,
                results: results,
                _phantom: PhantomData,
            })
        }).last().unwrap()
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for UnsafeArray<T> {
    fn add(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Add)       
    }
    fn fetch_add(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchAdd)
    }
    fn sub(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>{
        self.initiate_op(val, index, ArrayOpCmd::Sub)
    }
    fn fetch_sub(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchSub)
    }
    fn mul(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Mul)
    }
    fn fetch_mul(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchMul)
    }
    fn div(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Div)
    }
    fn fetch_div(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchDiv)
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for UnsafeArray<T> {
    fn bit_and(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::And)
    }
    fn fetch_bit_and(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchAnd)
    }

    fn bit_or(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Or)
    }
    fn fetch_bit_or(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchOr)
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
