use crate::active_messaging::*;
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

impl<'a, T: Dist> InputToValue<'a, T> {
    fn len(&self) -> usize {
        match self {
            InputToValue::OneToOne(_, _) => 1,
            InputToValue::OneToMany(_, vals) => vals.len(),
            InputToValue::ManyToOne(indices, _) => indices.len(),
            InputToValue::ManyToMany(indices, _) => indices.len(),
        }
    }
    // fn num_bytes(&self) -> usize{
    //     match self{
    //         InputToValue::OneToOne(_,_) => std::mem::size_of::<(usize,T)>(),
    //         InputToValue::OneToMany(_,vals) => std::mem::size_of::<usize>()+ vals.len() * std::mem::size_of::<T>(),
    //         InputToValue::ManyToOne(indices,_) => indices.len() * std::mem::size_of::<usize>() + std::mem::size_of::<T>(),
    //         InputToValue::ManyToMany(indices,vals) => indices.len() * std::mem::size_of::<usize>() +  vals.len() * std::mem::size_of::<T>(),
    //     }
    // }
    fn to_pe_offsets(
        self,
        array: &UnsafeArray<T>,
    ) -> (
        HashMap<usize, InputToValue<'a, T>>,
        HashMap<usize, Vec<usize>>,
        usize,
    ) {
        let mut pe_offsets = HashMap::new();
        let mut req_ids = HashMap::new();
        match self {
            InputToValue::OneToOne(index, value) => {
                let (pe, local_index) = array.calc_pe_and_offset(index);
                pe_offsets.insert(pe, InputToValue::OneToOne(local_index, value));
                req_ids.insert(pe, vec![0]);
                (pe_offsets, req_ids, 1)
            }
            InputToValue::OneToMany(index, values) => {
                let (pe, local_index) = array.calc_pe_and_offset(index);
                let vals_len = values.len();
                req_ids.insert(pe, (0..vals_len).collect());
                pe_offsets.insert(pe, InputToValue::OneToMany(local_index, values));

                (pe_offsets, req_ids, vals_len)
            }
            InputToValue::ManyToOne(indices, value) => {
                let mut temp_pe_offsets = HashMap::new();
                let mut req_cnt = 0;
                for index in indices.iter() {
                    let (pe, local_index) = array.calc_pe_and_offset(index);
                    temp_pe_offsets
                        .entry(pe)
                        .or_insert(vec![])
                        .push(local_index);
                    req_ids.entry(pe).or_insert(vec![]).push(req_cnt);
                    req_cnt += 1;
                }

                for (pe, local_indices) in temp_pe_offsets {
                    pe_offsets.insert(
                        pe,
                        InputToValue::ManyToOne(OpInputEnum::Vec(local_indices), value),
                    );
                }

                (pe_offsets, req_ids, indices.len())
            }
            InputToValue::ManyToMany(indices, values) => {
                let mut temp_pe_offsets = HashMap::new();
                let mut req_cnt = 0;
                for (index, val) in indices.iter().zip(values.iter()) {
                    let (pe, local_index) = array.calc_pe_and_offset(index);
                    let data = temp_pe_offsets.entry(pe).or_insert((vec![], vec![]));
                    data.0.push(local_index);
                    data.1.push(val);
                    req_ids.entry(pe).or_insert(vec![]).push(req_cnt);
                    req_cnt += 1;
                }
                for (pe, (local_indices, vals)) in temp_pe_offsets {
                    pe_offsets.insert(
                        pe,
                        InputToValue::ManyToMany(
                            OpInputEnum::Vec(local_indices),
                            OpInputEnum::Vec(vals),
                        ),
                    );
                }
                (pe_offsets, req_ids, indices.len())
            }
        }
    }
}
impl<'a, T: Dist + serde::Serialize + serde::de::DeserializeOwned> InputToValue<'a, T> {
    pub fn as_op_am_input(&self) -> OpAmInputToValue<T> {
        match self {
            InputToValue::OneToOne(index, value) => OpAmInputToValue::OneToOne(*index, *value),
            InputToValue::OneToMany(index, values) => {
                OpAmInputToValue::OneToMany(*index, values.iter().collect())
            }
            InputToValue::ManyToOne(indices, value) => {
                OpAmInputToValue::ManyToOne(indices.iter().collect(), *value)
            }
            InputToValue::ManyToMany(indices, values) => {
                OpAmInputToValue::ManyToMany(indices.iter().collect(), values.iter().collect())
            }
        }
    }
}

impl<T: Dist> OpAmInputToValue<T> {
    pub fn len(&self) -> usize {
        match self {
            OpAmInputToValue::OneToOne(_, _) => 1,
            OpAmInputToValue::OneToMany(_, vals) => vals.len(),
            OpAmInputToValue::ManyToOne(indices, _) => indices.len(),
            OpAmInputToValue::ManyToMany(indices, _) => indices.len(),
        }
    }
    pub fn num_bytes(&self) -> usize {
        match self {
            OpAmInputToValue::OneToOne(_, _) => std::mem::size_of::<(usize, T)>(),
            OpAmInputToValue::OneToMany(_, vals) => {
                std::mem::size_of::<usize>() + vals.len() * std::mem::size_of::<T>()
            }
            OpAmInputToValue::ManyToOne(indices, _) => {
                indices.len() * std::mem::size_of::<usize>() + std::mem::size_of::<T>()
            }
            OpAmInputToValue::ManyToMany(indices, vals) => {
                indices.len() * std::mem::size_of::<usize>() + vals.len() * std::mem::size_of::<T>()
            }
        }
    }
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

    fn initiate_op_task<'a>(
        &self,
        ret_type: OpReturnType,
        op: ArrayOpCmd<T>,
        input: InputToValue<T>,
        submit_cnt: usize,
        req_handles: Arc<Mutex<HashMap<usize, BufOpsRequest<T>>>>,
    ) {
        let (pe_offsets, req_ids, req_cnt) = input.to_pe_offsets(self); //HashMap<usize, InputToValue<'a,T>>

        let res_offsets_map = OpReqOffsets::new();
        let res_map = OpResults::new();
        let mut complete_cnt = Vec::new();
        // println!("pe_offsets size {:?}",pe_offsets);

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
                let buf_op = self.inner.data.op_buffers.read()[pe].clone();

                let (first, complete, res_offsets) = match ret_type {
                    OpReturnType::Fetch | OpReturnType::Result => buf_op.add_fetch_ops(
                        pe,
                        &op as *const ArrayOpCmd<T> as *const u8,
                        op_data as *const InputToValue<T> as *const u8,
                        &req_ids[&pe],
                        res_map.clone(),
                    ),
                    OpReturnType::None => {
                        let (first, complete) = buf_op.add_ops(
                            &op as *const ArrayOpCmd<T> as *const u8,
                            op_data as *const InputToValue<T> as *const u8,
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
                            buf_op.into_arc_am(array.sub_array_range());
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
                            let _cnt2 = array.inner.data.req_cnt.fetch_sub(len, Ordering::SeqCst);
                            complete.store(true, Ordering::Relaxed);

                            // println!("indirect cnts {:?}->{:?} {:?}->{:?} -- {:?}",cnt1,cnt1-1,cnt2,cnt2-len,len);
                        } else {
                            // println!("here {:?} {:?} ",ams.len(),len);
                            // complete.store(true, Ordering::Relaxed);
                            array
                                .inner
                                .data
                                .team
                                .team_counters
                                .outstanding_reqs
                                .fetch_sub(1, Ordering::SeqCst);
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
        // println!("i_len {i_len} v_len {v_len}");
        if v_len > 0 && i_len > 0 {
            if v_len == 1 && i_len > 1 {
                let val = vals[0].first();
                for i in indices.drain(..) {
                    i_v_iters.push(InputToValue::ManyToOne(i, val));
                }
            } else if v_len > 1 && i_len == 1 {
                let idx = indices[0].first();
                for v in vals.drain(..) {
                    i_v_iters.push(InputToValue::OneToMany(idx, v));
                }
            } else if i_len == v_len {
                if i_len == 1 {
                    i_v_iters.push(InputToValue::OneToOne(indices[0].first(), vals[0].first()));
                } else {
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

            // println!("i_v_iters len {:?}",i_v_iters.len());

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

    pub(crate) fn initiate_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
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
        Box::new(ArrayOpHandle { reqs })
    }

    pub(crate) fn initiate_fetch_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
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
        Box::new(ArrayOpFetchHandle { reqs })
    }
    pub(crate) fn initiate_result_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
    ) -> Box<dyn LamellarRequest<Output = Vec<Result<T, T>>>> {
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
        Box::new(ArrayOpResultHandle { reqs })
    }

    // pub fn op_put(
    //     &self,
    //     index: impl OpInput<'a,usize>,
    //     val: impl OpInput<'a,T>,
    // ) -> Box<dyn LamellarRequest<Output = ()>  > {
    //     self.initiate_op(val,index,ArrayOpCmd::Put)
    // }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for UnsafeArray<T> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
        self.initiate_op(val, index, ArrayOpCmd::Add)
    }
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchAdd)
    }
    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
        self.initiate_op(val, index, ArrayOpCmd::Sub)
    }
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchSub)
    }
    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
        self.initiate_op(val, index, ArrayOpCmd::Mul)
    }
    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchMul)
    }
    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
        self.initiate_op(val, index, ArrayOpCmd::Div)
    }
    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchDiv)
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for UnsafeArray<T> {
    fn bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
        self.initiate_op(val, index, ArrayOpCmd::And)
    }
    fn fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchAnd)
    }

    fn bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>> {
        self.initiate_op(val, index, ArrayOpCmd::Or)
    }
    fn fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>> {
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
