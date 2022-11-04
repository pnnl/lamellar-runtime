use crate::active_messaging::*;
use crate::array::operations::*;
use crate::array::r#unsafe::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
// use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

// type OpFn = fn(*const u8, UnsafeByteArray, usize) -> LamellarArcAm;

// lazy_static! {
//     static ref OPS: HashMap<(ArrayOpCmd, TypeId), OpFn> = {
//         let mut map = HashMap::new();
//         for op in crate::inventory::iter::<UnsafeArrayOp> {
//             map.insert(op.id.clone(), op.op);
//         }
//         map
//     };
// }

// pub struct UnsafeArrayOp<T: Dist> {
//     pub id: (ArrayOpCmd, TypeId),
//     pub op: OpFn,
// }

// crate::inventory::collect!(UnsafeArrayOp);

type BufFn = fn(UnsafeByteArrayWeak) -> Arc<dyn BufferOp>;

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

#[derive(Debug)]
pub(crate) enum BufOpsRequest<T: Dist> {
    // NoFetch(Box<dyn LamellarRequest<Output = ()>  >),
    // Fetch(Box<dyn LamellarRequest<Output = Vec<T>>  >),
    // Result(Box<dyn LamellarRequest<Output = Vec<Result<T,T>>>  >),
    NoFetch(Box<ArrayOpHandleInner>),
    Fetch(Box<ArrayOpFetchHandleInner<T>>),
    Result(Box<ArrayOpResultHandleInner<T>>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum OpReturnType {
    None,
    Fetch,
    Result,
}

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

    #[tracing::instrument(skip_all)]
    fn initiate_op_task<'a>(
        &self,
        ret_type: OpReturnType,
        op: ArrayOpCmd<T>,
        input: InputToValue<T>,
        submit_cnt: usize,
        req_handles: Arc<Mutex<HashMap<usize, BufOpsRequest<T>>>>,
    ) {
        // println!("initiate_op_task");
        let (pe_offsets, req_ids, req_cnt) = input.to_pe_offsets(self); //HashMap<usize, InputToValue<'a,T>>

        let res_offsets_map = OpReqOffsets::new();
        let res_map = OpResults::new();
        let mut complete_cnt = Vec::new();
        // println!("pe_offsets size {:?}",pe_offsets.len());

        // println!("req_cnt: {:?}",req_cnt);
        let req = pe_offsets
            .iter()
            .map(|(pe, op_data)| {
                // println!("pe: {:?} op_len {:?}",pe,op_data.len());
                let pe = *pe;
                let mut stall_mark = self
                    .inner
                    .data
                    .req_cnt
                    .fetch_add(op_data.len(), Ordering::SeqCst);
                // println!("added to req_cnt: {} {}",stall_mark+op_data.len(),op_data.len());
                let buf_op = self.inner.data.op_buffers.read()[pe].clone();

                let (first, complete, res_offsets) = match ret_type {
                    OpReturnType::Fetch | OpReturnType::Result => buf_op.add_fetch_ops(
                        pe,
                        &op as *const ArrayOpCmd<T> as *const u8,
                        op_data as *const InputToValue<T> as *const u8,
                        &req_ids[&pe],
                        res_map.clone(),
                        self.inner.data.team.clone(),
                    ),
                    OpReturnType::None => {
                        let (first, complete) = buf_op.add_ops(
                            &op as *const ArrayOpCmd<T> as *const u8,
                            op_data as *const InputToValue<T> as *const u8,
                            self.inner.data.team.clone(),
                        );
                        (first, complete, None)
                    }
                };
                if let Some(res_offsets) = res_offsets {
                    res_offsets_map.insert(pe, res_offsets);
                }
                if first {
                    // let res_map = res_map.clone();
                    let array = self.clone();
                    let _team_cnt = self
                        .inner
                        .data
                        .team
                        .team_counters
                        .outstanding_reqs
                        .fetch_add(1, Ordering::SeqCst); // we need to tell the world we have a request pending
                                                         // println!("updated team cnt: {}",_team_cnt +1);
                    self.inner.data.team.scheduler.submit_task(async move {
                        // println!("starting");
                        let mut wait_cnt = 0;
                        while wait_cnt < 1000
                            && array.inner.data.req_cnt.load(Ordering::Relaxed)
                                * std::mem::size_of::<(usize, T)>()
                                < 100000000
                        {
                            while stall_mark != array.inner.data.req_cnt.load(Ordering::Relaxed) {
                                stall_mark = array.inner.data.req_cnt.load(Ordering::Relaxed);
                                async_std::task::yield_now().await;
                            }
                            wait_cnt += 1;
                            async_std::task::yield_now().await;
                        }

                        let (ams, len, complete, results) =
                            buf_op.into_arc_am(pe, array.sub_array_range());
                        // println!("pe{:?} ams: {:?} len{:?}",pe,ams.len(),len);
                        if len > 0 {
                            let mut res = Vec::new();
                            for am in ams {
                                res.push(array.inner.data.team.exec_arc_am_pe::<Vec<u8>>(
                                    pe,
                                    am,
                                    Some(array.inner.data.array_counters.clone()),
                                ));
                            }

                            let mut full_results: Vec<u8> = Vec::new();
                            for r in res {
                                // println!("submitted indirectly {:?} ",len);
                                let results_u8: Vec<u8> = r.into_future().await;
                                // println!("returned_u8 {:?}",results_u8);

                                full_results.extend(results_u8);
                            }

                            // println!("{:?} {:?}",full_results.len(),full_results);
                            std::mem::swap(&mut full_results, &mut results.lock());
                            // println!("inserted results {:}",pe);
                            // res_map.insert(pe,full_results);
                            // println!("results {:?}",res_map);
                            // println!("done!");
                            let _cnt1 = array
                                .inner
                                .data
                                .team
                                .team_counters
                                .outstanding_reqs
                                .fetch_sub(1, Ordering::SeqCst); //remove our pending req now that it has actually been submitted;
                                                                 // println!("updated team cnt: {}",_cnt1 -1);
                            let _cnt2 = array.inner.data.req_cnt.fetch_sub(len, Ordering::SeqCst);
                            // println!("removed frm req_cnt: {} {}",_cnt2-len,len);
                            complete.store(true, Ordering::Relaxed);

                            // println!("indirect cnts {:?}->{:?} {:?}->{:?} -- {:?}",cnt1,cnt1-1,cnt2,cnt2-len,len);
                        } else {
                            // println!("here {:?} {:?} ",ams.len(),len);
                            // complete.store(true, Ordering::Relaxed);
                            let _team_cnt = array
                                .inner
                                .data
                                .team
                                .team_counters
                                .outstanding_reqs
                                .fetch_sub(1, Ordering::SeqCst);
                            // println!("updated team cnt: {} but not sure I should be here",_team_cnt -1);
                            complete.store(true, Ordering::Relaxed);
                        }
                    });
                }
                complete_cnt.push(complete.clone());
                // match results{
                //     Some(results) =>{
                match ret_type {
                    OpReturnType::Fetch => {
                        BufOpsRequest::Fetch(Box::new(ArrayOpFetchHandleInner {
                            indices: res_offsets_map.clone(),
                            complete: complete_cnt.clone(),
                            results: res_map.clone(),
                            req_cnt: req_cnt,
                            _phantom: PhantomData,
                        }))
                    }
                    OpReturnType::Result => {
                        BufOpsRequest::Result(Box::new(ArrayOpResultHandleInner {
                            indices: res_offsets_map.clone(),
                            complete: complete_cnt.clone(),
                            results: res_map.clone(),
                            req_cnt: req_cnt,
                            _phantom: PhantomData,
                        }))
                    }
                    OpReturnType::None => BufOpsRequest::NoFetch(Box::new(ArrayOpHandleInner {
                        complete: complete_cnt.clone(),
                    })),
                }
                // }
            })
            .last()
            .unwrap();
        // submit_cnt.fetch_sub(1, Ordering::SeqCst);
        // req
        req_handles.lock().insert(submit_cnt, req);
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn inner_initiate_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        ret_type: OpReturnType,
    ) -> Arc<Mutex<HashMap<usize, BufOpsRequest<T>>>> {
        let (mut indices, i_len) = index.as_op_input(); //(Vec<OpInputEnum<'a, usize>>, usize);
        let (mut vals, v_len) = val.as_op_input();
        let mut i_v_iters = vec![];
        let req_handles = Arc::new(Mutex::new(HashMap::new()));
        // println!("i_len {i_len} indices len {} v_len {v_len} vals len {}",indices.len(),vals.len());
        if v_len > 0 && i_len > 0 {
            if v_len == 1 && i_len > 1 {
                // println!("here 0");
                let val = vals[0].first();
                for i in indices.drain(..) {
                    i_v_iters.push(InputToValue::ManyToOne(i, val));
                }
            } else if v_len > 1 && i_len == 1 {
                // println!("here 1");
                let idx = indices[0].first();
                for v in vals.drain(..) {
                    i_v_iters.push(InputToValue::OneToMany(idx, v));
                }
            } else if i_len == v_len {
                if i_len == 1 {
                    // println!("here 2");
                    i_v_iters.push(InputToValue::OneToOne(indices[0].first(), vals[0].first()));
                } else {
                    // println!("here 3");
                    for (i, v) in indices.iter().zip(vals.iter()) {
                        i_v_iters.push(InputToValue::ManyToMany(i.clone(), v.clone()));
                    }
                }
            } else {
                panic!(
                    "not sure this case can exist!! indices len {:?} vals len {:?}",
                    i_len, v_len
                );
            }

            // println!("i_v_iters len {:?}", i_v_iters.len());

            let num_sub_reqs = i_v_iters.len(); //Arc::new(AtomicUsize::new(i_v_iters.len()));
            let mut submit_cnt = num_sub_reqs - 1;

            while i_v_iters.len() > 1 {
                // let submit_cnt = submit_cnt.clone();
                let req_handles = req_handles.clone();
                let array = self.clone();
                let input = i_v_iters.pop().unwrap();
                self.inner.data.team.scheduler.submit_task(async move {
                    array.initiate_op_task(ret_type, op, input, submit_cnt, req_handles);
                });
                submit_cnt -= 1;
            }
            self.initiate_op_task(
                ret_type,
                op,
                i_v_iters.pop().unwrap(),
                submit_cnt,
                req_handles.clone(),
            );
            // req_handles.lock().insert(0,req);
            while req_handles.lock().len() < num_sub_reqs {
                std::thread::yield_now();
            }
            // println!("submit_cnt {:?} {:?}",req_handles.lock().len(),num_sub_reqs);
            req_handles
        } else {
            // println!("im not sure i can ever be here");
            req_handles.lock().insert(
                0,
                match ret_type {
                    OpReturnType::Fetch => {
                        BufOpsRequest::Fetch(Box::new(ArrayOpFetchHandleInner {
                            indices: OpReqOffsets::new(),
                            complete: Vec::new(),
                            results: OpResults::new(),
                            req_cnt: 0,
                            _phantom: PhantomData,
                        }))
                    }
                    OpReturnType::Result => {
                        BufOpsRequest::Result(Box::new(ArrayOpResultHandleInner {
                            indices: OpReqOffsets::new(),
                            complete: Vec::new(),
                            results: OpResults::new(),
                            req_cnt: 0,
                            _phantom: PhantomData,
                        }))
                    }
                    OpReturnType::None => BufOpsRequest::NoFetch(Box::new(ArrayOpHandleInner {
                        complete: Vec::new(),
                    })),
                },
            );
            req_handles
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let req_handles_mutex = self.inner_initiate_op(val, index, op, OpReturnType::None);
        let mut req_handles = req_handles_mutex.lock();
        let mut reqs = vec![];
        for i in 0..req_handles.len() {
            match req_handles.remove(&i).unwrap() {
                BufOpsRequest::NoFetch(req) => reqs.push(req),
                BufOpsRequest::Fetch(_) => {
                    panic!("trying to return a fetch request for not fetch operations")
                }
                BufOpsRequest::Result(_) => {
                    panic!("trying to return a result request for not fetch operations")
                }
            }
        }
        Box::new(ArrayOpHandle { reqs }).into_future()
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_fetch_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Pin<Box<dyn Future<Output = T> + Send>> {
        let req_handles_mutex = self.inner_initiate_op(val, index, op, OpReturnType::Fetch);
        let mut req_handles = req_handles_mutex.lock();
        match req_handles.remove(&0).unwrap() {
            BufOpsRequest::NoFetch(_) => {
                panic!("trying to return a non fetch request for fetch operations")
            }
            BufOpsRequest::Fetch(req) => Box::new(ArrayOpFetchHandle { req }).into_future(),
            BufOpsRequest::Result(_) => {
                panic!("trying to return a result request for fetch operations")
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_batch_fetch_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        let req_handles_mutex = self.inner_initiate_op(val, index, op, OpReturnType::Fetch);
        let mut req_handles = req_handles_mutex.lock();
        let mut reqs = vec![];
        // println!("req_handles len {:?} {:?}",req_handles.len(),req_handles);
        for i in 0..req_handles.len() {
            match req_handles.remove(&i).unwrap() {
                BufOpsRequest::NoFetch(_) => {
                    panic!("trying to return a non fetch request for fetch operations")
                }
                BufOpsRequest::Fetch(req) => reqs.push(req),
                BufOpsRequest::Result(_) => {
                    panic!("trying to return a result request for fetch operations")
                }
            }
        }
        Box::new(ArrayOpBatchFetchHandle { reqs }).into_future()
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_result_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Pin<Box<dyn Future<Output = Result<T, T>> + Send>> {
        let req_handles_mutex = self.inner_initiate_op(val, index, op, OpReturnType::Result);
        let mut req_handles = req_handles_mutex.lock();
        // println!("req_handles len {:?}",req_handles.len());

        match req_handles.remove(&0).unwrap() {
            BufOpsRequest::NoFetch(_) => {
                panic!("trying to return a non fetch request for result operations")
            }
            BufOpsRequest::Fetch(_) => {
                panic!("trying to return a fetch request for result operations")
            }
            BufOpsRequest::Result(req) => Box::new(ArrayOpResultHandle { req }).into_future(),
        }
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_batch_result_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>> + Send>> {
        let req_handles_mutex = self.inner_initiate_op(val, index, op, OpReturnType::Result);
        let mut req_handles = req_handles_mutex.lock();
        let mut reqs = vec![];
        // println!("req_handles len {:?}",req_handles.len());
        for i in 0..req_handles.len() {
            match req_handles.remove(&i).unwrap() {
                BufOpsRequest::NoFetch(_) => {
                    panic!("trying to return a non fetch request for result operations")
                }
                BufOpsRequest::Fetch(_) => {
                    panic!("trying to return a fetch request for result operations")
                }
                BufOpsRequest::Result(req) => reqs.push(req),
            }
        }
        Box::new(ArrayOpBatchResultHandle { reqs }).into_future()
    }

    // pub fn op_put(
    //     &self,
    //     index: impl OpInput<'a,usize>,
    //     val: impl OpInput<'a,T>,
    // ) -> Box<dyn LamellarRequest<Output = ()>  > {
    //     self.initiate_op(val,index,ArrayOpCmd::Put)
    // }
}

impl<T: ElementOps + 'static> ReadOnlyOps<T> for UnsafeArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for UnsafeArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for UnsafeArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for UnsafeArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for UnsafeArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for UnsafeArray<T> {}

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
