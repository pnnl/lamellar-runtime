use crate::active_messaging::*;
use crate::array::operations::*;
use crate::array::r#unsafe::*;
use crate::array::*;
// use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;
// use itertools::Itertools;

type MultiValMultiIdxFn = fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, u8) -> LamellarArcAm;
type SingleValMultiIdxFn =
    fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, Vec<u8>, u8) -> LamellarArcAm;
type MultiValSingleIdxFn =
    fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, usize) -> LamellarArcAm;

lazy_static! {


    pub(crate) static ref MULTI_VAL_MULTI_IDX_OPS: HashMap<(TypeId,TypeId,BatchReturnType), MultiValMultiIdxFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<MultiValMultiIdxOps> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
    pub(crate) static ref SINGLE_VAL_MULTI_IDX_OPS: HashMap<(TypeId,TypeId,BatchReturnType), SingleValMultiIdxFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<SingleValMultiIdxOps> {
            // println!("{:?}",op.id.clone());
            map.insert(op.id.clone(), op.op);
        }
        map
    };
    pub(crate) static ref MULTI_VAL_SINGLE_IDX_OPS: HashMap<(TypeId,TypeId,BatchReturnType), MultiValSingleIdxFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<MultiValSingleIdxOps> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

#[derive(Debug, Copy, Clone)]
enum IndexSize {
    U8,
    U16,
    U32,
    U64,
    Usize,
}

impl From<usize> for IndexSize {
    fn from(size: usize) -> Self {
        if size <= u8::MAX as usize {
            IndexSize::U8
        } else if size <= u16::MAX as usize {
            IndexSize::U16
        } else if size <= u32::MAX as usize {
            IndexSize::U32
        } else if size <= u64::MAX as usize {
            IndexSize::U64
        } else {
            IndexSize::Usize
        }
    }
}

impl IndexSize {
    fn len(&self) -> usize {
        match self {
            IndexSize::U8 => 1,
            IndexSize::U16 => 2,
            IndexSize::U32 => 4,
            IndexSize::U64 => 8,
            IndexSize::Usize => 8,
        }
    }
    fn as_bytes(&self, val: &usize) -> &[u8] {
        match self {
            IndexSize::U8 => unsafe {
                std::slice::from_raw_parts(val as *const usize as *const u8, 1)
            },
            IndexSize::U16 => unsafe {
                std::slice::from_raw_parts(val as *const usize as *const u8, 2)
            },
            IndexSize::U32 => unsafe {
                std::slice::from_raw_parts(val as *const usize as *const u8, 4)
            },
            IndexSize::U64 => unsafe {
                std::slice::from_raw_parts(val as *const usize as *const u8, 8)
            },
            IndexSize::Usize => unsafe {
                std::slice::from_raw_parts(val as *const usize as *const u8, 8)
            },
        }
    }
}
#[doc(hidden)]
pub struct MultiValMultiIdxOps {
    pub id: (TypeId, TypeId, BatchReturnType),
    pub op: MultiValMultiIdxFn,
}

#[doc(hidden)]
pub struct SingleValMultiIdxOps {
    pub id: (TypeId, TypeId, BatchReturnType),
    pub op: SingleValMultiIdxFn,
}

#[doc(hidden)]
pub struct MultiValSingleIdxOps {
    pub id: (TypeId, TypeId, BatchReturnType),
    pub op: MultiValSingleIdxFn,
}

crate::inventory::collect!(MultiValMultiIdxOps);
crate::inventory::collect!(SingleValMultiIdxOps);
crate::inventory::collect!(MultiValSingleIdxOps);

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
    pub(crate) fn initiate_batch_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();

        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        let index_size = IndexSize::from(max_local_size);

        let res: Pin<Box<dyn Future<Output = Vec<((), Vec<usize>)>> + Send>> =
            if v_len == 1 && i_len == 1 {
                //one to one
                self.single_val_single_index::<()>(
                    byte_array,
                    vals[0].first(),
                    indices[0].first(),
                    op,
                    BatchReturnType::None,
                )
            } else if v_len > 1 && i_len == 1 {
                //many vals one index
                self.multi_val_one_index::<()>(
                    byte_array,
                    vals,
                    indices[0].first(),
                    op,
                    BatchReturnType::None,
                    index_size,
                )
            } else if v_len == 1 && i_len > 1 {
                //one val many indices
                self.one_val_multi_indices::<()>(
                    byte_array,
                    vals[0].first(),
                    indices,
                    op,
                    BatchReturnType::None,
                    index_size,
                )
            } else if v_len > 1 && i_len > 1 {
                //many vals many indices
                self.multi_val_multi_index::<()>(
                    byte_array,
                    vals,
                    indices,
                    op,
                    BatchReturnType::None,
                    index_size,
                )
            } else {
                //no vals no indices
                Box::pin(async { Vec::new() })
            };
        Box::pin(async {
            res.await;
            ()
        })
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_batch_fetch_op_2<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        // println!("here in batch fetch op 2");
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();
        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        let index_size = IndexSize::from(max_local_size);
        // println!("i_len {:?} v_len {:?}",i_len,v_len );
        let res: Pin<Box<dyn Future<Output = Vec<(Vec<T>, Vec<usize>)>> + Send>> =
            if v_len == 1 && i_len == 1 {
                //one to one
                self.single_val_single_index::<Vec<T>>(
                    byte_array,
                    vals[0].first(),
                    indices[0].first(),
                    op,
                    BatchReturnType::Vals,
                )
            } else if v_len > 1 && i_len == 1 {
                //many vals one index
                self.multi_val_one_index::<Vec<T>>(
                    byte_array,
                    vals,
                    indices[0].first(),
                    op,
                    BatchReturnType::Vals,
                    index_size,
                )
            } else if v_len == 1 && i_len > 1 {
                //one val many indices
                self.one_val_multi_indices::<Vec<T>>(
                    byte_array,
                    vals[0].first(),
                    indices,
                    op,
                    BatchReturnType::Vals,
                    index_size,
                )
            } else if v_len > 1 && i_len > 1 {
                //many vals many indices
                self.multi_val_multi_index::<Vec<T>>(
                    byte_array,
                    vals,
                    indices,
                    op,
                    BatchReturnType::Vals,
                    index_size,
                )
            } else {
                //no vals no indices
                panic!("should not be here");
                // Box::pin(async { Vec::new() })
            };
        Box::pin(async {
            let mut results = Vec::with_capacity(std::cmp::max(i_len, v_len));
            unsafe {
                results.set_len(std::cmp::max(i_len, v_len));
            }
            for (mut vals, mut idxs) in res.await.into_iter() {
                // println!("vals {:?} idx {:?}",vals.len(),idxs);
                for (v, i) in vals.drain(..).zip(idxs.drain(..)) {
                    results[i] = v;
                }
            }
            results
            // res.await.into_iter().flatten().collect::<(Vec<Result<T,T>>, Vec<usize)>()
        })
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn initiate_batch_result_op_2<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>> + Send>> {
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();
        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        let index_size = IndexSize::from(max_local_size);

        let res: Pin<Box<dyn Future<Output = Vec<(Vec<Result<T, T>>, Vec<usize>)>> + Send>> =
            if v_len == 1 && i_len == 1 {
                //one to one
                self.single_val_single_index::<Vec<Result<T, T>>>(
                    byte_array,
                    vals[0].first(),
                    indices[0].first(),
                    op,
                    BatchReturnType::Result,
                )
            } else if v_len > 1 && i_len == 1 {
                //many vals one index
                self.multi_val_one_index::<Vec<Result<T, T>>>(
                    byte_array,
                    vals,
                    indices[0].first(),
                    op,
                    BatchReturnType::Result,
                    index_size,
                )
            } else if v_len == 1 && i_len > 1 {
                //one val many indices
                self.one_val_multi_indices::<Vec<Result<T, T>>>(
                    byte_array,
                    vals[0].first(),
                    indices,
                    op,
                    BatchReturnType::Result,
                    index_size,
                )
            } else if v_len > 1 && i_len > 1 {
                //many vals many indices
                self.multi_val_multi_index::<Vec<Result<T, T>>>(
                    byte_array,
                    vals,
                    indices,
                    op,
                    BatchReturnType::Result,
                    index_size,
                )
            } else {
                //no vals no indices
                Box::pin(async { Vec::new() })
            };
        Box::pin(async {
            let mut results = Vec::with_capacity(std::cmp::max(i_len, v_len));
            unsafe {
                results.set_len(std::cmp::max(i_len, v_len));
            }
            for (mut vals, mut idxs) in res.await.into_iter() {
                for (v, i) in vals.drain(..).zip(idxs.drain(..)) {
                    results[i] = v;
                }
            }
            results
            // res.await.into_iter().flatten().collect::<(Vec<Result<T,T>>, Vec<usize)>()
        })
    }

    fn one_val_multi_indices<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        val: T,
        mut indices: Vec<OpInputEnum<usize>>,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: IndexSize,
    ) -> Pin<Box<dyn Future<Output = Vec<(R, Vec<usize>)>> + Send>> {
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 10000,
        };
        let num_pes = self.inner.data.team.num_pes();
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(Vec::new()));
        let num_reqs = indices.len();
        let mut start_i = 0;

        // println!("single_val_multi_index");

        for (_i, index) in indices.drain(..).enumerate() {
            let cnt2 = cnt.clone();
            let futures2 = futures.clone();
            let byte_array2 = byte_array.clone();
            let len = index.len();
            // println!("num_reqs {:?}",num_reqs);
            self.inner
                .data
                .team
                .scheduler
                .submit_immediate_task2(async move {
                    let mut buffs =
                        vec![Vec::with_capacity(num_per_batch * index_size.len()); num_pes];
                    let mut res_buffs = vec![Vec::with_capacity(num_per_batch); num_pes];
                    let mut reqs: Vec<Pin<Box<dyn Future<Output = (R, Vec<usize>)> + Send>>> =
                        Vec::new();
                    // let mut res_index = 0;
                    for (ii, idx) in index.iter().enumerate() {
                        let j = ii + start_i;
                        let (pe, local_index) = self.pe_and_offset_for_global_index(idx).unwrap();
                        buffs[pe].extend_from_slice(index_size.as_bytes(&local_index));
                        res_buffs[pe].push(j);
                        if buffs[pe].len() >= num_per_batch {
                            let mut new_buffer =
                                Vec::with_capacity(num_per_batch * index_size.len());
                            std::mem::swap(&mut buffs[pe], &mut new_buffer);
                            let mut new_res_buffer = Vec::with_capacity(num_per_batch);
                            std::mem::swap(&mut res_buffs[pe], &mut new_res_buffer);

                            let am = SingleValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                new_buffer,
                                val,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = self
                                .inner
                                .data
                                .team
                                .exec_arc_am_pe::<R>(
                                    pe,
                                    am,
                                    Some(self.inner.data.array_counters.clone()),
                                )
                                .into_future();
                            reqs.push(Box::pin(async move { (req.await, new_res_buffer) }));
                        }
                    }
                    for (pe, (buff, res_buff)) in
                        buffs.into_iter().zip(res_buffs.into_iter()).enumerate()
                    {
                        if buff.len() > 0 {
                            let am = SingleValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                buff,
                                val,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = self
                                .inner
                                .data
                                .team
                                .exec_arc_am_pe::<R>(
                                    pe,
                                    am,
                                    Some(self.inner.data.array_counters.clone()),
                                )
                                .into_future();
                            reqs.push(Box::pin(async move { (req.await, res_buff) }));
                        }
                    }
                    // println!("reqs len {:?}",reqs.len());
                    futures2.lock().extend(reqs);
                    cnt2.fetch_add(1, Ordering::SeqCst);
                });
            start_i += len;
        }

        // println!("futures len {:?}",futures.lock().len());
        Box::pin(async move {
            while cnt.load(Ordering::SeqCst) < num_reqs {
                // self.inner.data.team.scheduler.exec_task();
                async_std::task::yield_now().await;
            }
            // println!("futures len {:?}",futures.lock().len());
            futures::future::join_all(futures.lock().drain(..)).await
        })
    }

    // in general this type of operation will likely incur terrible cache performance, the obvious optimization is to apply the updates locally then send it over,
    // this clearly works for ops like add and mul, does it hold for sub (i think so? given that it is always array[i] - val), need to think about other ops as well...
    fn multi_val_one_index<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        mut vals: Vec<OpInputEnum<T>>,
        index: usize,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: IndexSize,
    ) -> Pin<Box<dyn Future<Output = Vec<(R, Vec<usize>)>> + Send>> {
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
            Err(_) => 10000,                      //+ 1 to account for main thread
        };
        // println!("multi_val_one_index");
        // let num_pes = self.inner.data.team.num_pes();
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(Vec::new()));
        let (pe, local_index) = self.pe_and_offset_for_global_index(index).unwrap();
        let num_reqs = vals.len();
        // println!("num_reqs {:?}",num_reqs);
        let mut start_i = 0;
        for val in vals.drain(..) {
            let cnt2 = cnt.clone();
            let futures2 = futures.clone();
            let byte_array2 = byte_array.clone();
            let len = val.len();
            self.inner
                .data
                .team
                .scheduler
                .submit_immediate_task2(async move {
                    // let mut buffs = vec![Vec::with_capacity(num_per_batch); num_pes];
                    // let val_slice = val.as_slice();
                    let mut inner_start_i = start_i;
                    let mut reqs: Vec<Pin<Box<dyn Future<Output = (R, Vec<usize>)> + Send>>> =
                        Vec::new();
                    val.as_vec_chunks(num_per_batch)
                        .into_iter()
                        .for_each(|val| {
                            let val_len = val.len();
                            let am = MultiValSingleIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                local_index,
                                val,
                            )
                            .into_am::<T>(ret);
                            let req = self
                                .inner
                                .data
                                .team
                                .exec_arc_am_pe::<R>(
                                    pe,
                                    am,
                                    Some(self.inner.data.array_counters.clone()),
                                )
                                .into_future();
                            // println!("start_i: {:?} inner_start_i {:?} val_len: {:?}",start_i,inner_start_i,val_len);
                            let res_buffer =
                                (inner_start_i..inner_start_i + val_len).collect::<Vec<usize>>();
                            reqs.push(Box::pin(async move { (req.await, res_buffer) }));
                            inner_start_i += val_len;
                        });
                    // println!("reqs len {:?}",reqs.len());
                    futures2.lock().extend(reqs);
                    cnt2.fetch_add(1, Ordering::SeqCst);
                });
            start_i += len;
        }
        while cnt.load(Ordering::SeqCst) < num_reqs {
            self.inner.data.team.scheduler.exec_task();
        }
        // println!("futures len {:?}",futures.lock().len());
        Box::pin(async move {
            // println!("futures len {:?}",futures.lock().len());
            futures::future::join_all(futures.lock().drain(..)).await
        })
    }

    fn multi_val_multi_index<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        mut vals: Vec<OpInputEnum<T>>,
        mut indices: Vec<OpInputEnum<usize>>,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: IndexSize,
    ) -> Pin<Box<dyn Future<Output = Vec<(R, Vec<usize>)>> + Send>> {
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
            Err(_) => 10000,                      //+ 1 to account for main thread
        };
        let bytes_per_batch = match index_size {
            IndexSize::U8 => num_per_batch * std::mem::size_of::<IdxVal<u8, T>>(),
            IndexSize::U16 => num_per_batch * std::mem::size_of::<IdxVal<u16, T>>(),
            IndexSize::U32 => num_per_batch * std::mem::size_of::<IdxVal<u32, T>>(),
            IndexSize::U64 => num_per_batch * std::mem::size_of::<IdxVal<u64, T>>(),
            IndexSize::Usize => num_per_batch * std::mem::size_of::<IdxVal<usize, T>>(),
        };

        let num_pes = self.inner.data.team.num_pes();
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(Vec::new()));
        let num_reqs = vals.len();

        // println!("num_reqs {:?}",num_reqs);
        let mut start_i = 0;

        for (_i, (index, val)) in indices.drain(..).zip(vals.drain(..)).enumerate() {
            let cnt2 = cnt.clone();
            let futures2 = futures.clone();
            let byte_array2 = byte_array.clone();
            let len = index.len();
            self.inner
                .data
                .team
                .scheduler
                .submit_immediate_task2(async move {
                    let mut buffs = vec![Vec::with_capacity(bytes_per_batch); num_pes];
                    let mut res_buffs = vec![Vec::with_capacity(num_per_batch); num_pes];
                    let mut reqs: Vec<Pin<Box<dyn Future<Output = (R, Vec<usize>)> + Send>>> =
                        Vec::new();
                    // let mut res_index = 0;
                    for (ii, (idx, val)) in index.iter().zip(val.iter()).enumerate() {
                        let j = ii + start_i;
                        let (pe, local_index) = self.pe_and_offset_for_global_index(idx).unwrap();
                        match index_size {
                            IndexSize::U8 => buffs[pe].extend_from_slice(
                                IdxVal {
                                    index: local_index as u8,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::U16 => buffs[pe].extend_from_slice(
                                IdxVal {
                                    index: local_index as u16,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::U32 => buffs[pe].extend_from_slice(
                                IdxVal {
                                    index: local_index as u32,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::U64 => buffs[pe].extend_from_slice(
                                IdxVal {
                                    index: local_index as u64,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::Usize => buffs[pe].extend_from_slice(
                                IdxVal {
                                    index: local_index as usize,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                        }
                        res_buffs[pe].push(j);
                        if buffs[pe].len() >= bytes_per_batch {
                            let mut new_buffer = Vec::with_capacity(bytes_per_batch);
                            std::mem::swap(&mut buffs[pe], &mut new_buffer);
                            let mut new_res_buffer = Vec::with_capacity(num_per_batch);
                            std::mem::swap(&mut res_buffs[pe], &mut new_res_buffer);

                            // println!("buff len {}",new_buffer.len());
                            let am = MultiValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                new_buffer,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = self
                                .inner
                                .data
                                .team
                                .exec_arc_am_pe::<R>(
                                    pe,
                                    am,
                                    Some(self.inner.data.array_counters.clone()),
                                )
                                .into_future();
                            reqs.push(Box::pin(async move { (req.await, new_res_buffer) }));
                        }
                    }
                    for (pe, (buff, res_buff)) in
                        buffs.into_iter().zip(res_buffs.into_iter()).enumerate()
                    {
                        if buff.len() > 0 {
                            // println!("buff len {}",buff.len());
                            let am = MultiValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                buff,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = self
                                .inner
                                .data
                                .team
                                .exec_arc_am_pe::<R>(
                                    pe,
                                    am,
                                    Some(self.inner.data.array_counters.clone()),
                                )
                                .into_future();
                            reqs.push(Box::pin(async move { (req.await, res_buff) }));
                        }
                    }
                    futures2.lock().extend(reqs);
                    cnt2.fetch_add(1, Ordering::SeqCst);
                });
            start_i += len;
        }
        while cnt.load(Ordering::SeqCst) < num_reqs {
            self.inner.data.team.scheduler.exec_task();
        }
        Box::pin(async move {
            // println!("futures len: {:?}",futures.lock().len());
            futures::future::join_all(futures.lock().drain(..)).await
        })
    }

    fn single_val_single_index<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        val: T,
        index: usize,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
    ) -> Pin<Box<dyn Future<Output = Vec<(R, Vec<usize>)>> + Send>> {
        let (pe, local_index) = self.pe_and_offset_for_global_index(index).unwrap();
        let mut buff = Vec::new();
        buff.extend_from_slice(
            IdxVal {
                index: local_index,
                val: val,
            }
            .as_bytes(),
        );
        let res_buff = vec![0];
        let am = MultiValMultiIndex::new_with_vec(byte_array.clone(), op, buff, IndexSize::Usize)
            .into_am::<T>(ret);
        let req = self
            .inner
            .data
            .team
            .exec_arc_am_pe::<R>(pe, am, Some(self.inner.data.array_counters.clone()))
            .into_future();
        let mut reqs = vec![Box::pin(async move { (req.await, res_buff) })];

        Box::pin(async move { futures::future::join_all(reqs.drain(..)).await })
    }
}

#[doc(hidden)]
#[derive(Copy, Clone, Debug, Hash, std::cmp::Eq, std::cmp::PartialEq)]
pub enum BatchReturnType {
    None,
    Vals,
    Result,
}

struct SingleValMultiIndex {
    array: LamellarByteArray,
    idx: Vec<u8>,
    val: Vec<u8>,
    op: ArrayOpCmd<Vec<u8>>,
    index_size: IndexSize,
}

impl SingleValMultiIndex {
    fn new_with_vec<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        indices: Vec<u8>,
        val: T,
        index_size: IndexSize,
    ) -> Self {
        let val_u8 = &val as *const T as *const u8;
        Self {
            array: array.into(),
            idx: indices,
            val: unsafe { std::slice::from_raw_parts(val_u8, std::mem::size_of::<T>()) }.to_vec(),
            op: op.into(),
            index_size: index_size,
        } //, type_id: TypeId::of::<T>() }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        // println!("{:?} {:?} {:?}",self.array.type_id(),TypeId::of::<T>(),ret);
        SINGLE_VAL_MULTI_IDX_OPS
            .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
            .unwrap()(
            self.array,
            self.op,
            self.val,
            self.idx,
            self.index_size.len() as u8,
        )
    }
}

struct MultiValSingleIndex {
    array: LamellarByteArray,
    idx: usize,
    val: Vec<u8>,
    op: ArrayOpCmd<Vec<u8>>,
}

impl MultiValSingleIndex {
    fn new_with_vec<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        index: usize,
        val: Vec<T>,
    ) -> Self {
        let val_u8 = val.as_ptr() as *const u8;

        Self {
            array: array.into(),
            idx: index,
            val: unsafe {
                std::slice::from_raw_parts(val_u8, std::mem::size_of::<T>() * val.len())
            }
            .to_vec(),
            op: op.into(),
        } //, type_id: TypeId::of::<T>() }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        MULTI_VAL_SINGLE_IDX_OPS
            .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
            .unwrap()(self.array, self.op, self.val, self.idx)
    }
}

struct MultiValMultiIndex {
    array: LamellarByteArray,
    idxs_vals: Vec<u8>,
    op: ArrayOpCmd<Vec<u8>>,
    index_size: IndexSize,
}

impl MultiValMultiIndex {
    fn new_with_vec<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        idxs_vals: Vec<u8>,
        index_size: IndexSize,
    ) -> Self {
        Self {
            array: array.into(),
            idxs_vals: idxs_vals,
            op: op.into(),
            index_size: index_size,
        } //, type_id: TypeId::of::<T>() }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        MULTI_VAL_MULTI_IDX_OPS
            .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
            .unwrap()(
            self.array,
            self.op,
            self.idxs_vals,
            self.index_size.len() as u8,
        )
    }
}

impl<T: ElementOps + 'static> ReadOnlyOps<T> for UnsafeArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for UnsafeArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for UnsafeArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for UnsafeArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for UnsafeArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for UnsafeArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T> for UnsafeArray<T> {}
