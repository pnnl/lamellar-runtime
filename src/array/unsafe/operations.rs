use crate::active_messaging::LamellarArcAm;
use crate::array::operations::handle::*;
use crate::array::operations::*;
use crate::array::r#unsafe::UnsafeArray;
use crate::array::{AmDist, Dist, LamellarArray, LamellarByteArray, LamellarEnv};
use crate::env_var::{config, IndexType};
use crate::AmHandle;
use parking_lot::Mutex;
use std::any::TypeId;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type MultiValMultiIdxFnNew =
    fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, u8, BatchReturnType) -> LamellarArcAm;
type SingleValMultiIdxFnNew = fn(
    LamellarByteArray,
    ArrayOpCmd<Vec<u8>>,
    Vec<u8>,
    Vec<u8>,
    u8,
    BatchReturnType,
) -> LamellarArcAm;
type MultiValSingleIdxFnNew = fn(
    LamellarByteArray,
    ArrayOpCmd<Vec<u8>>,
    (*const u8, usize, usize),
    usize,
    BatchReturnType,
) -> LamellarArcAm;

type MultiValMultiIdxFn = fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, u8) -> LamellarArcAm;
type SingleValMultiIdxFn =
    fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, Vec<u8>, u8) -> LamellarArcAm;
type MultiValSingleIdxFn =
    fn(LamellarByteArray, ArrayOpCmd<Vec<u8>>, Vec<u8>, usize) -> LamellarArcAm;

lazy_static! {

    pub(crate) static ref MULTI_VAL_MULTI_IDX_OPS_NEW: HashMap<TypeId, MultiValMultiIdxFnNew> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<multi_val_multi_idx_ops_new> {
            map.insert((op.id)(), op.op);
        }
        map
    };

    pub(crate) static ref SINGLE_VAL_MULTI_IDX_OPS_NEW: HashMap<TypeId, SingleValMultiIdxFnNew> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<single_val_multi_idx_ops_new> {
            map.insert((op.id)(), op.op);
        }
        map
    };
    pub(crate) static ref MULTI_VAL_SINGLE_IDX_OPS_NEW: HashMap<TypeId, MultiValSingleIdxFnNew> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<multi_val_single_idx_ops_new> {
            map.insert((op.id)(), op.op);
        }
        map
    };

    pub(crate) static ref MULTI_VAL_MULTI_IDX_OPS: HashMap<(TypeId,TypeId,BatchReturnType), MultiValMultiIdxFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<multi_val_multi_idx_ops> {
            // println!("{:?} {:?} {:?} -- {:?} ",op.id, op.batch_type, op.op,(op.id)(op.batch_type));
            // println!("{:?}",);
            map.insert((op.id)(op.batch_type), op.op);
        }
        map
    };
    pub(crate) static ref SINGLE_VAL_MULTI_IDX_OPS: HashMap<(TypeId,TypeId,BatchReturnType), SingleValMultiIdxFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<single_val_multi_idx_ops> {
            // println!("{:?}",op.id.clone());
            map.insert((op.id)(op.batch_type), op.op);
        }
        map
    };
    pub(crate) static ref MULTI_VAL_SINGLE_IDX_OPS: HashMap<(TypeId,TypeId,BatchReturnType), MultiValSingleIdxFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<multi_val_single_idx_ops> {
            map.insert((op.id)(op.batch_type), op.op);
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
        match config().index_size {
            IndexType::Dynamic => {
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
            IndexType::Static => IndexSize::Usize,
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
    #[allow(dead_code)]
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

    fn create_buf(&self, num_elems: usize) -> IndexBuf {
        let num_bytes = num_elems * self.len();
        match self {
            IndexSize::U8 => {
                let mut vec = Vec::with_capacity(num_bytes);
                unsafe {
                    vec.set_len(num_bytes);
                }
                IndexBuf::U8(0, vec)
            }
            IndexSize::U16 => {
                let mut vec = Vec::with_capacity(num_bytes);
                unsafe {
                    vec.set_len(num_bytes);
                }
                IndexBuf::U16(0, vec)
            }
            IndexSize::U32 => {
                let mut vec = Vec::with_capacity(num_bytes);
                unsafe {
                    vec.set_len(num_bytes);
                }
                IndexBuf::U32(0, vec)
            }
            IndexSize::U64 => {
                let mut vec = Vec::with_capacity(num_bytes);
                unsafe {
                    vec.set_len(num_bytes);
                }
                IndexBuf::U64(0, vec)
            }
            IndexSize::Usize => {
                let mut vec = Vec::with_capacity(num_bytes);
                unsafe {
                    vec.set_len(num_bytes);
                }
                IndexBuf::Usize(0, vec)
            }
        }
    }
}

#[derive(Debug, Clone)]
enum IndexBuf {
    U8(usize, Vec<u8>),
    U16(usize, Vec<u8>),
    U32(usize, Vec<u8>),
    U64(usize, Vec<u8>),
    Usize(usize, Vec<u8>),
}

impl IndexBuf {
    fn push(&mut self, val: usize) {
        match self {
            IndexBuf::U8(i, vec) => {
                let vec_ptr = vec.as_mut_ptr() as *mut u8;
                unsafe {
                    std::ptr::write(vec_ptr.offset(*i as isize), val as u8);
                }
                *i += 1;
            }
            IndexBuf::U16(i, vec) => {
                let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut u16;
                unsafe {
                    std::ptr::write(vec_ptr.offset(*i as isize), val as u16);
                }
                *i += 1;
            }
            IndexBuf::U32(i, vec) => {
                let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut u32;
                unsafe {
                    std::ptr::write(vec_ptr.offset(*i as isize), val as u32);
                }
                *i += 1;
            }
            IndexBuf::U64(i, vec) => {
                let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut u64;
                unsafe {
                    std::ptr::write(vec_ptr.offset(*i as isize), val as u64);
                }
                *i += 1;
            }
            IndexBuf::Usize(i, vec) => {
                let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut usize;
                unsafe {
                    std::ptr::write(vec_ptr.offset(*i as isize), val as usize);
                }
                *i += 1;
            }
        }
    }
    fn len(&self) -> usize {
        match self {
            IndexBuf::U8(i, _) => *i,
            IndexBuf::U16(i, _) => *i,
            IndexBuf::U32(i, _) => *i,
            IndexBuf::U64(i, _) => *i,
            IndexBuf::Usize(i, _) => *i,
        }
    }
    fn to_vec(self) -> Vec<u8> {
        match self {
            IndexBuf::U8(i, mut vec) => {
                unsafe {
                    vec.set_len(i);
                }
                vec
            }
            IndexBuf::U16(i, mut vec) => {
                unsafe {
                    vec.set_len(i * std::mem::size_of::<u16>());
                }
                vec
            }
            IndexBuf::U32(i, mut vec) => {
                unsafe {
                    vec.set_len(i * std::mem::size_of::<u32>());
                }
                vec
            }
            IndexBuf::U64(i, mut vec) => {
                unsafe {
                    vec.set_len(i * std::mem::size_of::<u64>());
                }
                vec
            }
            IndexBuf::Usize(i, mut vec) => {
                unsafe {
                    vec.set_len(i * std::mem::size_of::<usize>());
                }
                vec
            }
        }
    }
}

type IdGenNew = fn() -> TypeId;
#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct multi_val_multi_idx_ops_new {
    pub id: IdGenNew,
    pub op: MultiValMultiIdxFnNew,
}
#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct single_val_multi_idx_ops_new {
    pub id: IdGenNew,
    pub op: SingleValMultiIdxFnNew,
}
#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct multi_val_single_idx_ops_new {
    pub id: IdGenNew,
    pub op: MultiValSingleIdxFnNew,
}

type IdGen = fn(BatchReturnType) -> (TypeId, TypeId, BatchReturnType);
#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct multi_val_multi_idx_ops {
    pub id: IdGen,
    pub batch_type: BatchReturnType,
    pub op: MultiValMultiIdxFn,
}

#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct single_val_multi_idx_ops {
    pub id: IdGen,
    pub batch_type: BatchReturnType,
    pub op: SingleValMultiIdxFn,
}

#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct multi_val_single_idx_ops {
    pub id: IdGen,
    pub batch_type: BatchReturnType,
    pub op: MultiValSingleIdxFn,
}

crate::inventory::collect!(multi_val_multi_idx_ops_new);
crate::inventory::collect!(single_val_multi_idx_ops_new);
crate::inventory::collect!(multi_val_single_idx_ops_new);
crate::inventory::collect!(multi_val_multi_idx_ops);
crate::inventory::collect!(single_val_multi_idx_ops);
crate::inventory::collect!(multi_val_single_idx_ops);

impl<T: AmDist + Dist + 'static> UnsafeArray<T> {
    pub(crate) fn dummy_val(&self) -> T {
        let slice = self.inner.data.mem_region.as_slice();
        assert!(slice.len() > 0 && slice.len() % std::mem::size_of::<T>() == 0);
        unsafe {
            std::slice::from_raw_parts(
                slice.as_ptr() as *const T,
                slice.len() / std::mem::size_of::<T>(),
            )[0]
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn initiate_batch_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> ArrayBatchOpHandle {
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();

        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        let index_size = IndexSize::from(max_local_size);
        let res = if v_len == 1 && i_len == 1 {
            //one to one
            self.single_val_single_index::<()>(
                byte_array.clone(),
                vals[0].first(),
                indices[0].first(),
                op,
                BatchReturnType::None,
            )
        } else if v_len > 1 && i_len == 1 {
            //many vals one index
            self.multi_val_one_index::<()>(
                byte_array.clone(),
                vals,
                indices[0].first(),
                op,
                BatchReturnType::None,
                index_size,
            )
        } else if v_len == 1 && i_len > 1 {
            //one val many indices
            self.one_val_multi_indices::<()>(
                byte_array.clone(),
                vals[0].first(),
                indices,
                op,
                BatchReturnType::None,
                index_size,
            )
            .into()
        } else if v_len > 1 && i_len > 1 {
            //many vals many indices
            self.multi_val_multi_index::<()>(
                byte_array.clone(),
                vals,
                indices,
                op,
                BatchReturnType::None,
                index_size,
            )
        } else {
            //no vals no indices
            VecDeque::new()
        };
        ArrayBatchOpHandle {
            array: byte_array,
            state: BatchOpState::Reqs(res),
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn initiate_batch_fetch_op_2<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> ArrayFetchBatchOpHandle<T> {
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();
        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        let index_size = IndexSize::from(max_local_size);
        let res = if v_len == 1 && i_len == 1 {
            //one to one
            self.single_val_single_index::<Vec<T>>(
                byte_array.clone(),
                vals[0].first(),
                indices[0].first(),
                op,
                BatchReturnType::Vals,
            )
        } else if v_len > 1 && i_len == 1 {
            //many vals one index
            self.multi_val_one_index::<Vec<T>>(
                byte_array.clone(),
                vals,
                indices[0].first(),
                op,
                BatchReturnType::Vals,
                index_size,
            )
        } else if v_len == 1 && i_len > 1 {
            //one val many indices
            self.one_val_multi_indices::<Vec<T>>(
                byte_array.clone(),
                vals[0].first(),
                indices,
                op,
                BatchReturnType::Vals,
                index_size,
            )
        } else if v_len > 1 && i_len > 1 {
            //many vals many indices
            self.multi_val_multi_index::<Vec<T>>(
                byte_array.clone(),
                vals,
                indices,
                op,
                BatchReturnType::Vals,
                index_size,
            )
        } else {
            VecDeque::new()
        };
        if res.len() == 0 {
            return ArrayFetchBatchOpHandle::new(byte_array, res, 0);
        }
        ArrayFetchBatchOpHandle::new(byte_array, res, std::cmp::max(i_len, v_len))
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn initiate_batch_result_op_2<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> ArrayResultBatchOpHandle<T> {
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();
        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        let index_size = IndexSize::from(max_local_size);
        let res = if v_len == 1 && i_len == 1 {
            //one to one
            self.single_val_single_index::<Vec<Result<T, T>>>(
                byte_array.clone(),
                vals[0].first(),
                indices[0].first(),
                op,
                BatchReturnType::Result,
            )
        } else if v_len > 1 && i_len == 1 {
            //many vals one index
            self.multi_val_one_index::<Vec<Result<T, T>>>(
                byte_array.clone(),
                vals,
                indices[0].first(),
                op,
                BatchReturnType::Result,
                index_size,
            )
        } else if v_len == 1 && i_len > 1 {
            //one val many indices
            self.one_val_multi_indices::<Vec<Result<T, T>>>(
                byte_array.clone(),
                vals[0].first(),
                indices,
                op,
                BatchReturnType::Result,
                index_size,
            )
        } else if v_len > 1 && i_len > 1 {
            //many vals many indices
            self.multi_val_multi_index::<Vec<Result<T, T>>>(
                byte_array.clone(),
                vals,
                indices,
                op,
                BatchReturnType::Result,
                index_size,
            )
        } else {
            //no vals no indices
            VecDeque::new()
        };
        ArrayResultBatchOpHandle::new(byte_array, res, std::cmp::max(i_len, v_len))
    }

    fn one_val_multi_indices<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        val: T,
        mut indices: Vec<OpInputEnum<usize>>,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: IndexSize,
    ) -> VecDeque<(AmHandle<R>, Vec<usize>)> {
        let num_per_batch =
            (config().am_size_threshold as f32 / index_size.len() as f32).ceil() as usize;

        let num_pes = self.inner.data.team.num_pes();
        // let my_pe = self.inner.data.team.my_pe();
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(VecDeque::new()));
        let num_reqs = indices.len();
        let mut start_i = 0;

        // println!("single_val_multi_index");

        for (_i, index) in indices.drain(..).enumerate() {
            let cnt2 = cnt.clone();
            let futures2 = futures.clone();
            let byte_array2 = byte_array.clone();
            let len = index.len();
            self.inner.data.array_counters.inc_outstanding(1);
            self.inner.data.team.inc_outstanding(1);
            let index_vec = index.to_vec();
            let the_array: UnsafeArray<T> = self.clone();
            self.inner
                .data
                .team
                .scheduler
                .submit_immediate_task(async move {
                    let mut buffs = vec![index_size.create_buf(num_per_batch); num_pes];
                    let mut res_buffs = vec![Vec::with_capacity(num_per_batch); num_pes];
                    let mut reqs: Vec<(AmHandle<R>, Vec<usize>)> = Vec::new();

                    for (ii, idx) in index_vec.iter().enumerate() {
                        let j = ii + start_i;
                        let (pe, local_index) = match the_array.pe_and_offset_for_global_index(*idx)
                        {
                            Some((pe, local_index)) => (pe, local_index),
                            None => panic!(
                                "Index: {idx} out of bounds for array of len: {:?}",
                                the_array.inner.size
                            ),
                        };
                        buffs[pe].push(local_index);
                        res_buffs[pe].push(j);
                        if buffs[pe].len() >= num_per_batch {
                            let mut new_buffer = index_size.create_buf(num_per_batch);
                            std::mem::swap(&mut buffs[pe], &mut new_buffer);
                            let mut new_res_buffer = Vec::with_capacity(num_per_batch);
                            std::mem::swap(&mut res_buffs[pe], &mut new_res_buffer);

                            let am = SingleValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                new_buffer.to_vec(),
                                val,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                                pe,
                                am,
                                Some(the_array.inner.data.array_counters.clone()),
                            );

                            reqs.push((req, new_res_buffer));
                        }
                    }
                    for (pe, (buff, res_buff)) in
                        buffs.into_iter().zip(res_buffs.into_iter()).enumerate()
                    {
                        if buff.len() > 0 {
                            let am = SingleValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                buff.to_vec(),
                                val,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                                pe,
                                am,
                                Some(the_array.inner.data.array_counters.clone()),
                            );

                            reqs.push((req, res_buff));
                        }
                    }
                    futures2.lock().extend(reqs);
                    cnt2.fetch_add(1, Ordering::SeqCst);
                    the_array.inner.data.array_counters.dec_outstanding(1);
                    the_array.inner.data.team.dec_outstanding(1);
                });
            start_i += len;
        }
        // We need this loop so that we ensure all the internal AMs have launched so calls like wait_all work properly
        while cnt.load(Ordering::SeqCst) < num_reqs {
            self.inner.data.team.scheduler.exec_task();
        }
        let res = std::mem::take(&mut *futures.lock());
        res
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
        _index_size: IndexSize,
    ) -> VecDeque<(AmHandle<R>, Vec<usize>)> {
        let num_per_batch =
            (config().am_size_threshold as f32 / std::mem::size_of::<T>() as f32).ceil() as usize;

        // println!("multi_val_one_index");
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(VecDeque::new()));
        let (pe, local_index) = match self.pe_and_offset_for_global_index(index) {
            Some((pe, local_index)) => (pe, local_index),
            None => panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            ),
        };
        let num_reqs = vals.len();
        let mut start_i = 0;
        let scheduler = self.inner.data.team.scheduler.clone();
        for val in vals.drain(..) {
            let cnt2 = cnt.clone();
            let futures2 = futures.clone();
            let byte_array2 = byte_array.clone();
            let len = val.len();
            self.inner.data.array_counters.inc_outstanding(1);
            self.inner.data.team.inc_outstanding(1);
            let the_array: UnsafeArray<T> = self.clone();
            let val_chunks = val.into_vec_chunks(num_per_batch);
            scheduler.submit_immediate_task(async move {
                let mut inner_start_i = start_i;
                let mut reqs: Vec<(AmHandle<R>, Vec<usize>)> = Vec::new();
                val_chunks.into_iter().for_each(|val| {
                    let val_len = val.len();
                    let am = MultiValSingleIndex::new_with_vec(
                        byte_array2.clone(),
                        op,
                        local_index,
                        val,
                    )
                    .into_am::<T>(ret);
                    let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                        pe,
                        am,
                        Some(the_array.inner.data.array_counters.clone()),
                    );
                    let res_buffer =
                        (inner_start_i..inner_start_i + val_len).collect::<Vec<usize>>();
                    reqs.push((req, res_buffer));
                    inner_start_i += val_len;
                });
                futures2.lock().extend(reqs);
                cnt2.fetch_add(1, Ordering::SeqCst);
                the_array.inner.data.array_counters.dec_outstanding(1);
                the_array.inner.data.team.dec_outstanding(1);
            });
            start_i += len;
        }

        // We need this loop so that we ensure all the internal AMs have launched so calls like wait_all work properly
        while cnt.load(Ordering::SeqCst) < num_reqs {
            self.inner.data.team.scheduler.exec_task();
        }
        let res = std::mem::take(&mut *futures.lock());
        res
    }

    fn multi_val_multi_index<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        mut vals: Vec<OpInputEnum<T>>,
        mut indices: Vec<OpInputEnum<usize>>,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: IndexSize,
    ) -> VecDeque<(AmHandle<R>, Vec<usize>)> {
        let idx_val_bytes = match index_size {
            IndexSize::U8 => std::mem::size_of::<IdxVal<u8, T>>(),
            IndexSize::U16 => std::mem::size_of::<IdxVal<u16, T>>(),
            IndexSize::U32 => std::mem::size_of::<IdxVal<u32, T>>(),
            IndexSize::U64 => std::mem::size_of::<IdxVal<u64, T>>(),
            IndexSize::Usize => std::mem::size_of::<IdxVal<usize, T>>(),
        };
        let num_per_batch =
            (config().am_size_threshold as f32 / idx_val_bytes as f32).ceil() as usize;
        let bytes_per_batch = num_per_batch * idx_val_bytes;

        let num_pes = self.inner.data.team.num_pes();
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(VecDeque::new()));
        let num_reqs = vals.len();

        // println!("num_reqs {:?}", num_reqs);
        let mut start_i = 0;

        for (_i, (index, val)) in indices.drain(..).zip(vals.drain(..)).enumerate() {
            let cnt2 = cnt.clone();
            let futures2 = futures.clone();
            let byte_array2 = byte_array.clone();
            let len = index.len();
            self.inner.data.array_counters.inc_outstanding(1);
            self.inner.data.team.inc_outstanding(1);
            let index_vec = index.to_vec();
            let vals_vec = val.to_vec();
            let the_array: UnsafeArray<T> = self.clone();
            self.inner
                .data
                .team
                .scheduler
                .submit_immediate_task(async move {
                    let mut buffs = vec![Vec::with_capacity(bytes_per_batch); num_pes];
                    let mut res_buffs = vec![Vec::with_capacity(num_per_batch); num_pes];
                    let mut reqs: Vec<(AmHandle<R>, Vec<usize>)> = Vec::new();
                    for (ii, (idx, val)) in
                        index_vec.into_iter().zip(vals_vec.into_iter()).enumerate()
                    {
                        let j = ii + start_i;
                        let (pe, local_index) = match the_array.pe_and_offset_for_global_index(idx)
                        {
                            Some((pe, local_index)) => (pe, local_index),
                            None => panic!(
                                "Index: {idx} out of bounds for array of len: {:?}",
                                the_array.inner.size
                            ),
                        };
                        match index_size {
                            IndexSize::U8 => buffs[pe].extend_from_slice(
                                IdxVal::<u8, T> {
                                    index: local_index as u8,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::U16 => buffs[pe].extend_from_slice(
                                IdxVal::<u16, T> {
                                    index: local_index as u16,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::U32 => buffs[pe].extend_from_slice(
                                IdxVal::<u32, T> {
                                    index: local_index as u32,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::U64 => buffs[pe].extend_from_slice(
                                IdxVal::<u64, T> {
                                    index: local_index as u64,
                                    val: val,
                                }
                                .as_bytes(),
                            ),
                            IndexSize::Usize => buffs[pe].extend_from_slice(
                                IdxVal::<usize, T> {
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
                            let am = MultiValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                new_buffer,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                                pe,
                                am,
                                Some(the_array.inner.data.array_counters.clone()),
                            );
                            reqs.push((req, new_res_buffer));
                        }
                    }
                    for (pe, (buff, res_buff)) in
                        buffs.into_iter().zip(res_buffs.into_iter()).enumerate()
                    {
                        if buff.len() > 0 {
                            let am = MultiValMultiIndex::new_with_vec(
                                byte_array2.clone(),
                                op,
                                buff,
                                index_size,
                            )
                            .into_am::<T>(ret);
                            let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                                pe,
                                am,
                                Some(the_array.inner.data.array_counters.clone()),
                            );
                            reqs.push((req, res_buff));
                        }
                    }
                    futures2.lock().extend(reqs);
                    cnt2.fetch_add(1, Ordering::SeqCst);
                    the_array.inner.data.array_counters.dec_outstanding(1);
                    the_array.inner.data.team.dec_outstanding(1);
                });
            start_i += len;
        }
        // We need this loop so that we ensure all the internal AMs have launched so calls like wait_all work properly
        while cnt.load(Ordering::SeqCst) < num_reqs {
            self.inner.data.team.scheduler.exec_task();
        }
        let res = std::mem::take(&mut *futures.lock());
        res
    }

    fn single_val_single_index<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        val: T,
        index: usize,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
    ) -> VecDeque<(AmHandle<R>, Vec<usize>)> {
        let (pe, local_index) = match self.pe_and_offset_for_global_index(index) {
            Some((pe, local_index)) => (pe, local_index),
            None => panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            ),
        };
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
        let req = self.inner.data.team.exec_arc_am_pe::<R>(
            pe,
            am,
            Some(self.inner.data.array_counters.clone()),
        );
        VecDeque::from(vec![(req, res_buff)])
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
        }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        // println!("{:?} {:?} {:?}",self.array.type_id(),TypeId::of::<T>(),ret);
        match SINGLE_VAL_MULTI_IDX_OPS_NEW.get(&TypeId::of::<T>()) {
            Some(op) => op(
                self.array,
                self.op,
                self.val,
                self.idx,
                self.index_size.len() as u8,
                ret,
            ),
            None => SINGLE_VAL_MULTI_IDX_OPS
                .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
                .unwrap()(
                self.array,
                self.op,
                self.val,
                self.idx,
                self.index_size.len() as u8,
            ),
        }
        // match std::env::var("TEST_OPS") {
        //     Ok(_) => SINGLE_VAL_MULTI_IDX_OPS_NEW
        //         .get(&TypeId::of::<T>())
        //         .unwrap()(
        //         self.array,
        //         self.op,
        //         self.val,
        //         self.idx,
        //         self.index_size.len() as u8,
        //         ret,
        //     ),
        //     Err(_) => SINGLE_VAL_MULTI_IDX_OPS
        //         .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
        //         .unwrap()(
        //         self.array,
        //         self.op,
        //         self.val,
        //         self.idx,
        //         self.index_size.len() as u8,
        //     ),
        // }
    }
}

struct MultiValSingleIndex {
    array: LamellarByteArray,
    idx: usize,
    val: (*const u8, usize, usize),
    op: ArrayOpCmd<Vec<u8>>,
}

impl MultiValSingleIndex {
    fn new_with_vec<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        index: usize,
        val: Vec<T>,
    ) -> Self {
        // let val_u8 = val.as_ptr() as *const u8;

        // Prevent running `val`'s destructor so we are in complete control
        // of the allocation.
        let mut val = std::mem::ManuallyDrop::new(val);

        // Pull out the various important pieces of information about `v`
        let p = val.as_mut_ptr() as *const u8;
        let len = val.len();
        let cap = val.capacity();

        Self {
            array: array.into(),
            idx: index,
            val: (p, len, cap),
            op: op.into(),
        }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        match MULTI_VAL_SINGLE_IDX_OPS_NEW.get(&TypeId::of::<T>()) {
            Some(op) => op(self.array, self.op, self.val, self.idx, ret),
            None => {
                let val =
                    unsafe { Vec::from_raw_parts(self.val.0 as *mut T, self.val.1, self.val.2) };
                let val_u8 = val.as_ptr() as *const u8;

                MULTI_VAL_SINGLE_IDX_OPS
                    .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
                    .unwrap()(
                    self.array,
                    self.op,
                    unsafe {
                        std::slice::from_raw_parts(val_u8, std::mem::size_of::<T>() * val.len())
                    }
                    .to_vec(),
                    self.idx,
                )
            }
        }
        // match std::env::var("TEST_OPS") {
        //     Ok(_) => MULTI_VAL_SINGLE_IDX_OPS_NEW
        //         .get(&TypeId::of::<T>())
        //         .unwrap()(self.array, self.op, self.val, self.idx, ret),
        //     Err(_) => {
        //         let val =
        //             unsafe { Vec::from_raw_parts(self.val.0 as *mut T, self.val.1, self.val.2) };
        //         let val_u8 = val.as_ptr() as *const u8;

        //         MULTI_VAL_SINGLE_IDX_OPS
        //             .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
        //             .unwrap()(
        //             self.array,
        //             self.op,
        //             unsafe {
        //                 std::slice::from_raw_parts(val_u8, std::mem::size_of::<T>() * val.len())
        //             }
        //             .to_vec(),
        //             self.idx,
        //         )
        //     }
        // }
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
        // println!(
        //     "{:?} {:?} {:?} {:?}",
        //     self.array.type_id(),
        //     TypeId::of::<T>(),
        //     ret,
        //     std::any::type_name::<T>(),
        // );
        match MULTI_VAL_MULTI_IDX_OPS_NEW.get(&TypeId::of::<T>()) {
            Some(op) => op(
                self.array,
                self.op,
                self.idxs_vals,
                self.index_size.len() as u8,
                ret,
            ),
            None => MULTI_VAL_MULTI_IDX_OPS
                .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
                .unwrap()(
                self.array,
                self.op,
                self.idxs_vals,
                self.index_size.len() as u8,
            ),
        }
        // match std::env::var("TEST_OPS") {
        //     Ok(_) => MULTI_VAL_MULTI_IDX_OPS_NEW.get(&TypeId::of::<T>()).unwrap()(
        //         self.array,
        //         self.op,
        //         self.idxs_vals,
        //         self.index_size.len() as u8,
        //         ret,
        //     ),
        //     Err(_) => MULTI_VAL_MULTI_IDX_OPS
        //         .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
        //         .unwrap()(
        //         self.array,
        //         self.op,
        //         self.idxs_vals,
        //         self.index_size.len() as u8,
        //     ),
        // }
    }
}

impl<T: ElementOps + 'static> UnsafeReadOnlyOps<T> for UnsafeArray<T> {}

impl<T: ElementOps + 'static> UnsafeAccessOps<T> for UnsafeArray<T> {}

impl<T: ElementArithmeticOps + 'static> UnsafeArithmeticOps<T> for UnsafeArray<T> {}

impl<T: ElementBitWiseOps + 'static> UnsafeBitWiseOps<T> for UnsafeArray<T> {}

impl<T: ElementShiftOps + 'static> UnsafeShiftOps<T> for UnsafeArray<T> {}

impl<T: ElementCompareEqOps + 'static> UnsafeCompareExchangeOps<T> for UnsafeArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> UnsafeCompareExchangeEpsilonOps<T>
    for UnsafeArray<T>
{
}
