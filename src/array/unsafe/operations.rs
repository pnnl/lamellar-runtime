use crate::active_messaging::LamellarArcAm;
use crate::memregion::{OneSidedMemoryRegion};
use crate::array::operations::handle::*;
use crate::array::operations::*;
use crate::array::r#unsafe::UnsafeArray;
use crate::array::{AmDist, Dist, LamellarArray, LamellarByteArray, LamellarEnv,ScalarType};
 use crate::array::scalar_impls::{PackedIndicies,PackedIdxVal,ScalarMultiIdxSingleValAm, ScalarMultiIdxMultiValAm, ScalarSingleIdxMultiValAm,ScalarMultiIdxSingleValAmReturn, ScalarMultiIdxMultiValAmReturn, ScalarSingleIdxMultiValAmReturn};
use crate::env_var::{config, IndexType};
use crate::lamellae::AtomicOp;
use crate::AmHandle;
use core::panic;
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

// #[derive(Debug, Copy, Clone)]
// enum IndexSize {
//     U8,
//     U16,
//     U32,
//     U64,
//     Usize,
// }

// impl From<usize> for IndexSize {
//     fn from(size: usize) -> Self {
//         match config().array_index_size {
//             IndexType::Dynamic => {
//                 if size <= u8::MAX as usize {
//                     IndexSize::U8
//                 } else if size <= u16::MAX as usize {
//                     IndexSize::U16
//                 } else if size <= u32::MAX as usize {
//                     IndexSize::U32
//                 } else if size <= u64::MAX as usize {
//                     IndexSize::U64
//                 } else {
//                     IndexSize::Usize
//                 }
//             }
//             IndexType::Static => IndexSize::Usize,
//         }
//     }
// }

// impl IndexSize {
//     fn len(&self) -> usize {
//         match self {
//             IndexSize::U8 => 1,
//             IndexSize::U16 => 2,
//             IndexSize::U32 => 4,
//             IndexSize::U64 => 8,
//             IndexSize::Usize => 8,
//         }
//     }
//     #[allow(dead_code)]
//     fn as_bytes(&self, val: &usize) -> &[u8] {
//         match self {
//             IndexSize::U8 => unsafe {
//                 std::slice::from_raw_parts(val as *const usize as *const u8, 1)
//             },
//             IndexSize::U16 => unsafe {
//                 std::slice::from_raw_parts(val as *const usize as *const u8, 2)
//             },
//             IndexSize::U32 => unsafe {
//                 std::slice::from_raw_parts(val as *const usize as *const u8, 4)
//             },
//             IndexSize::U64 => unsafe {
//                 std::slice::from_raw_parts(val as *const usize as *const u8, 8)
//             },
//             IndexSize::Usize => unsafe {
//                 std::slice::from_raw_parts(val as *const usize as *const u8, 8)
//             },
//         }
//     }

//     fn create_buf(&self, num_elems: usize) -> IndexBuf {
//         let num_bytes = num_elems * self.len();
//         match self {
//             IndexSize::U8 => {
//                 let mut vec = Vec::with_capacity(num_bytes);
//                 unsafe {
//                     vec.set_len(num_bytes);
//                 }
//                 IndexBuf::U8(0, vec)
//             }
//             IndexSize::U16 => {
//                 let mut vec = Vec::with_capacity(num_bytes);
//                 unsafe {
//                     vec.set_len(num_bytes);
//                 }
//                 IndexBuf::U16(0, vec)
//             }
//             IndexSize::U32 => {
//                 let mut vec = Vec::with_capacity(num_bytes);
//                 unsafe {
//                     vec.set_len(num_bytes);
//                 }
//                 IndexBuf::U32(0, vec)
//             }
//             IndexSize::U64 => {
//                 let mut vec = Vec::with_capacity(num_bytes);
//                 unsafe {
//                     vec.set_len(num_bytes);
//                 }
//                 IndexBuf::U64(0, vec)
//             }
//             IndexSize::Usize => {
//                 let mut vec = Vec::with_capacity(num_bytes);
//                 unsafe {
//                     vec.set_len(num_bytes);
//                 }
//                 IndexBuf::Usize(0, vec)
//             }
//         }
//     }
// }

// #[derive(Debug, Clone)]
// enum IndexBuf {
//     U8(usize, Vec<u8>),
//     U16(usize, Vec<u8>),
//     U32(usize, Vec<u8>),
//     U64(usize, Vec<u8>),
//     Usize(usize, Vec<u8>),
// }

// impl IndexBuf {
//     fn push(&mut self, val: usize) {
//         match self {
//             IndexBuf::U8(i, vec) => {
//                 let vec_ptr = vec.as_mut_ptr() as *mut u8;
//                 unsafe {
//                     std::ptr::write(vec_ptr.offset(*i as isize), val as u8);
//                 }
//                 *i += 1;
//             }
//             IndexBuf::U16(i, vec) => {
//                 let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut u16;
//                 unsafe {
//                     std::ptr::write(vec_ptr.offset(*i as isize), val as u16);
//                 }
//                 *i += 1;
//             }
//             IndexBuf::U32(i, vec) => {
//                 let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut u32;
//                 unsafe {
//                     std::ptr::write(vec_ptr.offset(*i as isize), val as u32);
//                 }
//                 *i += 1;
//             }
//             IndexBuf::U64(i, vec) => {
//                 let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut u64;
//                 unsafe {
//                     std::ptr::write(vec_ptr.offset(*i as isize), val as u64);
//                 }
//                 *i += 1;
//             }
//             IndexBuf::Usize(i, vec) => {
//                 let vec_ptr = vec.as_mut_ptr() as *mut u8 as *mut usize;
//                 unsafe {
//                     std::ptr::write(vec_ptr.offset(*i as isize), val as usize);
//                 }
//                 *i += 1;
//             }
//         }
//     }
//     fn len(&self) -> usize {
//         match self {
//             IndexBuf::U8(i, _) => *i,
//             IndexBuf::U16(i, _) => *i,
//             IndexBuf::U32(i, _) => *i,
//             IndexBuf::U64(i, _) => *i,
//             IndexBuf::Usize(i, _) => *i,
//         }
//     }
//     fn to_vec(self) -> Vec<u8> {
//         match self {
//             IndexBuf::U8(i, mut vec) => {
//                 unsafe {
//                     vec.set_len(i);
//                 }
//                 vec
//             }
//             IndexBuf::U16(i, mut vec) => {
//                 unsafe {
//                     vec.set_len(i * std::mem::size_of::<u16>());
//                 }
//                 vec
//             }
//             IndexBuf::U32(i, mut vec) => {
//                 unsafe {
//                     vec.set_len(i * std::mem::size_of::<u32>());
//                 }
//                 vec
//             }
//             IndexBuf::U64(i, mut vec) => {
//                 unsafe {
//                     vec.set_len(i * std::mem::size_of::<u64>());
//                 }
//                 vec
//             }
//             IndexBuf::Usize(i, mut vec) => {
//                 unsafe {
//                     vec.set_len(i * std::mem::size_of::<usize>());
//                 }
//                 vec
//             }
//         }
//     }
// }

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
        self.mem_region.as_slice()[0]
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn initiate_op<'a>(
        &self,
        val: T,
        index: usize,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> ArrayOpHandle<T> {
        let (am, _) = self
            .initiate_batch_op_inner(val, index, op, byte_array.clone())
            .pop_front()
            .unwrap();
        ArrayOpHandle {
            array: byte_array,
            state: OpState::Am(am),
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn initiate_batch_op<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> ArrayBatchOpHandle {
        let res = self.initiate_batch_op_inner(val, index, op, byte_array.clone());
        ArrayBatchOpHandle {
            array: byte_array,
            state: BatchOpState::Reqs(res),
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn initiate_batch_op_inner<'a>(
        &self,
        val: impl OpInput<'a, T>,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd<T>,
        byte_array: LamellarByteArray,
    ) -> VecDeque<(AmHandle<()>, Vec<usize>)> {
        let (indices, i_len) = index.as_op_input();
        let (vals, v_len) = val.as_op_input();

        let max_local_size = (0..self.num_pes())
            .map(|pe| self.inner.num_elems_pe(pe))
            .max()
            .unwrap();
        // let index_size = IndexSize::from(max_local_size);
        let index_size = match config().array_index_size {
            IndexType::Dynamic => {
                let bits_needed = usize::BITS as usize - max_local_size.leading_zeros() as usize;
                bits_needed.div_ceil(8).next_power_of_two().min(8)
            },
            IndexType::Static => std::mem::size_of::<usize>(),
        };

        if self.inner.data.team.my_pe() == 0 {
            println!("max_local_size: {:?} index_size: {:?}", max_local_size, index_size);
        }
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
        res
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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
        // let index_size = IndexSize::from(max_local_size);
        let index_size = match config().array_index_size {
            IndexType::Dynamic => {
                let bits_needed = usize::BITS as usize - max_local_size.leading_zeros() as usize;
                bits_needed.div_ceil(8).next_power_of_two().min(8)
            },
            IndexType::Static => std::mem::size_of::<usize>(),
        };
        let res: VecDeque<(AmHandle<OneSidedMemoryRegion<u8>>, Vec<usize>)> = if v_len == 1 && i_len == 1 {
            //one to one
            self.single_val_single_index::<OneSidedMemoryRegion<u8>>(
                byte_array.clone(),
                vals[0].first(),
                indices[0].first(),
                op,
                BatchReturnType::Vals,
            )
        } else if v_len > 1 && i_len == 1 {
            //many vals one index
            self.multi_val_one_index::<OneSidedMemoryRegion<u8>>(
                byte_array.clone(),
                vals,
                indices[0].first(),
                op,
                BatchReturnType::Vals,
                index_size,
            )
        } else if v_len == 1 && i_len > 1 {
            //one val many indices
            self.one_val_multi_indices::<OneSidedMemoryRegion<u8>>(
                byte_array.clone(),
                vals[0].first(),
                indices,
                op,
                BatchReturnType::Vals,
                index_size,
            )
        } else if v_len > 1 && i_len > 1 {
            //many vals many indices
            self.multi_val_multi_index::<OneSidedMemoryRegion<u8>>(
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
            return ArrayFetchBatchOpHandle::new_one_sided_memory_region(byte_array, res, 0);
        }
        ArrayFetchBatchOpHandle::new_one_sided_memory_region(byte_array, res, std::cmp::max(i_len, v_len))
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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
        // let index_size = IndexSize::from(max_local_size);
        let index_size = match config().array_index_size {
            IndexType::Dynamic => {
                let bits_needed = usize::BITS as usize - max_local_size.leading_zeros() as usize;
                bits_needed.div_ceil(8).next_power_of_two().min(8)
            },
            IndexType::Static => std::mem::size_of::<usize>(),
        };


        let res: VecDeque<(AmHandle<OneSidedMemoryRegion<u8>>, Vec<usize>)> = if v_len == 1 && i_len == 1 {
            //one to one
            self.single_val_single_index::<OneSidedMemoryRegion<u8>>(
                byte_array.clone(),
                vals[0].first(),
                indices[0].first(),
                op,
                BatchReturnType::Result,
            )
        } else if v_len > 1 && i_len == 1 {
            //many vals one index
            self.multi_val_one_index::<OneSidedMemoryRegion<u8>>(
                byte_array.clone(),
                vals,
                indices[0].first(),
                op,
                BatchReturnType::Result,
                index_size,
            )
        } else if v_len == 1 && i_len > 1 {
            //one val many indices
            self.one_val_multi_indices::<OneSidedMemoryRegion<u8>>(
                byte_array.clone(),
                vals[0].first(),
                indices,
                op,
                BatchReturnType::Result,
                index_size,
            )
        } else if v_len > 1 && i_len > 1 {
            //many vals many indices
            self.multi_val_multi_index::<OneSidedMemoryRegion<u8>>(
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
        ArrayResultBatchOpHandle::new_one_sided_memory_region(byte_array, res, std::cmp::max(i_len, v_len))
    }

    fn one_val_multi_indices<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        val: T,
        mut indices: Vec<OpInputEnum<usize>>,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: usize,
    ) -> VecDeque<(AmHandle<R>, Vec<usize>)> {
        let num_per_batch =
            (config().am_size_threshold as f32 / index_size as f32).ceil() as usize;

        let num_pes = self.inner.data.team.num_pes();
        // let my_pe = self.inner.data.team.my_pe();
        let cnt = Arc::new(AtomicUsize::new(0));
        let futures = Arc::new(Mutex::new(VecDeque::new()));
        let num_reqs = indices.len();
        let mut start_i = 0;

        let val_bytes_slice = unsafe{
                                std::slice::from_raw_parts(&val as *const T as *const u8, std::mem::size_of::<T>())
                            };

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
                    // let mut buffs = vec![index_size.create_buf(num_per_batch); num_pes];
                    let mut buffs = vec![PackedIndicies::new_with_capacity(index_size, num_per_batch); num_pes];
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
                            // let mut new_buffer = PackedIndicies::new_with_capacity(index_size, num_per_batch);
                            // std::mem::swap(&mut buffs[pe], &mut new_buffer);
                            let mut new_res_buffer = Vec::with_capacity(num_per_batch);
                            std::mem::swap(&mut res_buffs[pe], &mut new_res_buffer);
                            

                            
                            let am = SingleValMultiIndex::new(
                                byte_array2.clone(),
                                op,
                                &mut  buffs[pe],
                                val_bytes_slice.to_vec(),
                                index_size,
                            ).await
                            .into_am::<T>(ret);
                            let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                                pe,
                                am,
                                Some(the_array.inner.data.array_counters.clone()),
                            );

                            reqs.push((req, new_res_buffer));
                        
                        }
                    }
                    for (pe, (mut buff, res_buff)) in
                        buffs.into_iter().zip(res_buffs.into_iter()).enumerate()
                    {
                        if buff.len() > 0 {
                            
                            let am = SingleValMultiIndex::new(
                                byte_array2.clone(),
                                op,
                                &mut buff,
                                val_bytes_slice.to_vec(),
                                index_size,
                            ).await
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
        _index_size: usize,
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
                for val_chunk in val_chunks.into_iter() {
                // val_chunks.into_iter().for_each(|val| {
                    let val_len = val_chunk.len();
                    let am = MultiValSingleIndex::new(
                        byte_array2.clone(),
                        op,
                        local_index,
                        val_chunk,
                    ).await
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

    fn multi_val_multi_index<R: AmDist>(
        &self,
        byte_array: LamellarByteArray,
        mut vals: Vec<OpInputEnum<T>>,
        mut indices: Vec<OpInputEnum<usize>>,
        op: ArrayOpCmd<T>,
        ret: BatchReturnType,
        index_size: usize,
    ) -> VecDeque<(AmHandle<R>, Vec<usize>)> {
        let idx_val_bytes = index_size + std::mem::size_of::<T>();
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
                    let mut buffs = vec![PackedIdxVal::new_with_capacity::<T>(index_size, num_per_batch); num_pes];
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
                        buffs[pe].push(local_index, val);
                        res_buffs[pe].push(j);
                        if buffs[pe].len() >= bytes_per_batch {
                            // let mut new_buffer = PackedIdxVal::<T>::new_with_capacity(index_size, num_per_batch);
                            // std::mem::swap(&mut buffs[pe], &mut new_buffer);
                            let mut new_res_buffer = Vec::with_capacity(num_per_batch);
                            std::mem::swap(&mut res_buffs[pe], &mut new_res_buffer);
                            let am = MultiValMultiIndex::new(
                                byte_array2.clone(),
                                op,
                                &mut buffs[pe],
                                index_size,
                            ).await
                            .into_am::<T>(ret);
                            let req = the_array.inner.data.team.exec_arc_am_pe::<R>(
                                pe,
                                am,
                                Some(the_array.inner.data.array_counters.clone()),
                            );
                            reqs.push((req, new_res_buffer));
                        }
                    }
                    for (pe, (mut buff, res_buff)) in
                        buffs.into_iter().zip(res_buffs.into_iter()).enumerate()
                    {
                        if buff.len() > 0 {
                            let am = MultiValMultiIndex::new(
                                byte_array2.clone(),
                                op,
                                &mut buff,
                                index_size,
                            ).await
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
        let index_size = std::mem::size_of::<usize>();
        let mut idx_val = PackedIdxVal::new_with_capacity::<T>(index_size,1);
        idx_val.push(local_index, val);
        let res_buff = vec![0];
        let am = self.inner
                .data
                .team
                .scheduler
                .block_on(
                    async move {
                        MultiValMultiIndex::new(byte_array.clone(), op, &mut idx_val, index_size).await
                    .into_am::<T>(ret)
                    }
                );
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
    indices: Option<OneSidedMemoryRegion<u8>>,
    indices_u8: Option<Vec<u8>>,
    val: Vec<u8>,
    op: ArrayOpCmd<Vec<u8>>,
    index_size: usize,
    scalar_type: Option<(ScalarType,bool)>, // type and whether it is wrapped in an option
}

impl SingleValMultiIndex {
    async fn new<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        indices: &mut PackedIndicies,
        val: Vec<u8>,
        index_size: usize,
    ) -> Self {
        let scalar_type = ScalarType::get_type::<T>();
        let (indices_mr, indices_u8) = if scalar_type.is_some() {
            let mut mem_region = array.team().try_alloc_one_sided_mem_region(indices.len());
            while let None = mem_region {
                // println!("Failed to allocate mem region, retrying...");
                // async_std::task::sleep(std::time::Duration::from_millis(10)).await;
                async_std::task::yield_now().await;
                mem_region = array.team().try_alloc_one_sided_mem_region(indices.len());
            }
            let mem_region = mem_region.unwrap();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    indices.as_ptr(),
                    mem_region.as_mut_ptr().unwrap(),
                    indices.len(),
                );
            }(Some(mem_region), None)
        }
        else{
            let indices_u8 = unsafe{
                std::slice::from_raw_parts(indices.as_ptr(), indices.len()).to_vec()
            };
            (None, Some(indices_u8))
        };
        indices.clear();
        Self {
            array: array.into(),
            indices: indices_mr,
            indices_u8,
            val,
            op: op.into(),
            index_size,
            scalar_type: ScalarType::get_type::<T>(),
        }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        if let Some((scalar_type,opt)) = self.scalar_type { //built in Scalar support 
            match ret {
                BatchReturnType::None => 
                    Arc::new(ScalarMultiIdxSingleValAm{
                        array: self.array,
                        val: self.val,
                        indices: self.indices.unwrap(),
                        idx_size: self.index_size,
                        scalar_type,
                        opt,
                        op: self.op,
                    }),
                BatchReturnType::Vals | BatchReturnType::Result => 
                    Arc::new(ScalarMultiIdxSingleValAmReturn{
                        array: self.array,
                        val: self.val,
                        indices: self.indices.unwrap(),
                        idx_size: self.index_size,
                        scalar_type,
                        opt,
                        op: self.op,
                    }),
            }
        }
        else{ // user defined type support
            // panic!("User defined types not currently supported for single val multi index ops");
            SINGLE_VAL_MULTI_IDX_OPS
                .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
                .unwrap()(
                self.array,
                self.op,
                self.val,
                self.indices_u8.unwrap(),
                self.index_size as u8,
            )
        }
    }
}

struct MultiValSingleIndex {
    array: LamellarByteArray,
    idx: usize,
    vals: Option<OneSidedMemoryRegion<u8>>,
    val_u8: Option<Vec<u8>>,
    op: ArrayOpCmd<Vec<u8>>,
    scalar_type: Option<(ScalarType,bool)>, // type and whether it is wrapped in an option
}

impl MultiValSingleIndex {
    async fn new<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        index: usize,
        val: Vec<T>,
    ) -> Self {
        let scalar_type = ScalarType::get_type::<T>();
            let(vals,val_u8) = if scalar_type.is_some() {
            let mut mem_region = array.team().try_alloc_one_sided_mem_region::<T>(val.len());
            while let None = mem_region {
                // println!("Failed to allocate mem region, retrying...");
                // async_std::task::sleep(std::time::Duration::from_millis(10)).await;
                async_std::task::yield_now().await;
                mem_region = array.team().try_alloc_one_sided_mem_region::<T>(val.len());
            }
            let mem_region = mem_region.unwrap();
            let mem_region = unsafe {
                mem_region.as_mut_slice().copy_from_slice(&val);
                mem_region.to_base::<u8>()
            };
            (Some(mem_region), None)
        }
        else{
            let val_u8 = unsafe{
                std::slice::from_raw_parts(val.as_ptr() as *const u8, val.len() * std::mem::size_of::<T>()).to_vec()
            };
            (None, Some(val_u8))
        };



        Self {
            array: array.into(),
            idx: index,
            vals,
            val_u8,
            op: op.into(),
            scalar_type,
        }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
         if let Some((scalar_type,opt)) = self.scalar_type { //built in Scalar support 
            match ret {
                BatchReturnType::None => 
                    Arc::new(ScalarSingleIdxMultiValAm{
                        array: self.array,
                        index: self.idx,
                        vals: self.vals.unwrap(),
                        scalar_type,
                        opt,
                        op: self.op,
                    }),
                BatchReturnType::Vals | BatchReturnType::Result => 
                    Arc::new(ScalarSingleIdxMultiValAmReturn{
                        array: self.array,
                        index: self.idx,
                        vals: self.vals.unwrap(),
                        scalar_type,
                        opt,
                        op: self.op,
                    }),
            }
        }
        else{ // user defined type support
            // panic!("User defined types not currently supported for single val multi index ops");
            MULTI_VAL_SINGLE_IDX_OPS
                .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
                .unwrap()(
                self.array,
                self.op,
                self.val_u8.unwrap(),
                self.idx,
            )
        }
    }
}

struct MultiValMultiIndex {
    array: LamellarByteArray,
    idxs_vals: Option<OneSidedMemoryRegion<u8>>,
    idx_vals_u8: Option<Vec<u8>>,
    op: ArrayOpCmd<Vec<u8>>,
    index_size: usize,
    scalar_type: Option<(ScalarType,bool)>, // type and whether it is wrapped in an option
}

impl MultiValMultiIndex {
    async fn new<T: Dist>(
        array: LamellarByteArray,
        op: ArrayOpCmd<T>,
        idxs_vals: &mut PackedIdxVal,
        index_size: usize,
    ) -> Self {
        let scalar_type = ScalarType::get_type::<T>();
        let (idx_vals_mr, idx_vals_u8) = if let Some(_) = &scalar_type {
            let mut mem_region = array.team().try_alloc_one_sided_mem_region(idxs_vals.len());
            while let None = mem_region {
                // println!("Failed to allocate mem region, retrying...");
                // async_std::task::sleep(std::time::Duration::from_millis(10)).await;
                async_std::task::yield_now().await;
                mem_region = array.team().try_alloc_one_sided_mem_region(idxs_vals.len());
            }
            let mem_region = mem_region.unwrap();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    idxs_vals.as_ptr(),
                    mem_region.as_mut_ptr().unwrap(),
                    idxs_vals.len(),
                );
            }
            (Some(mem_region), None)
        }
        else{
            let idx_vals_u8 = unsafe{
                std::slice::from_raw_parts(idxs_vals.as_ptr(), idxs_vals.len()).to_vec()
            };
            (None, Some(idx_vals_u8))
        };
        idxs_vals.clear(); // ensure the buffer is empty and can be reused by the caller if they want
        Self {
            array: array.into(),
            idxs_vals: idx_vals_mr,
            idx_vals_u8,
            op: op.into(),
            index_size,
            scalar_type,
        } //, type_id: TypeId::of::<T>() }
    }

    fn into_am<T: Dist>(self, ret: BatchReturnType) -> LamellarArcAm {
        if let Some((scalar_type,opt)) = self.scalar_type { //built in Scalar support 
            match ret {
                BatchReturnType::None => 
                    Arc::new(ScalarMultiIdxMultiValAm{
                        array: self.array,
                        idx_val: self.idxs_vals.unwrap(),
                        idx_size: self.index_size,
                        scalar_type,
                        opt,
                        op: self.op,
                    }),
                BatchReturnType::Vals | BatchReturnType::Result => 
                    Arc::new(ScalarMultiIdxMultiValAmReturn{
                        array: self.array,
                        idx_val: self.idxs_vals.unwrap(),
                        idx_size: self.index_size,
                        scalar_type,
                        opt,
                        op: self.op,
                    }),
            }
        }else{
            // panic!("MultiValMultiIndex AM does not support user defined types yet");
            MULTI_VAL_MULTI_IDX_OPS
                .get(&(self.array.type_id(), TypeId::of::<T>(), ret))
                .unwrap()(
                self.array,
                self.op,
                self.idx_vals_u8.unwrap(),
                self.index_size as u8,
            )
        }
    }
}

impl<T: ElementOps + 'static> UnsafeReadOnlyOps<T> for UnsafeArray<T> {
    unsafe fn load<'a>(&self, index: usize) -> ArrayFetchOpHandle<T> {
        // println!("in Network atomic store");
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            unsafe {
                let handle = self.mem_region.get(pe, offset);
                ArrayFetchOpHandle {
                    array: self.clone().into(),
                    state: FetchOpState::Rdma(handle, None),
                }
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }
}

impl<T: ElementOps + 'static> UnsafeAccessOps<T> for UnsafeArray<T> {
    unsafe fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        // println!("in Network atomic store");
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            // let mut buf: OneSidedMemoryRegion<T> =
            //     self.array.team_rt().alloc_one_sided_mem_region(1);
            unsafe {
                // buf.as_mut_slice()[0] = val;
                let handle = self.mem_region.put(pe, offset, val);
                ArrayOpHandle {
                    array: self.clone().into(),
                    state: OpState::Rdma(handle),
                }
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_store(&self, index: usize, val: T) {
        // println!("in Network atomic blocking store");
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            unsafe {
                self.mem_region.put_blocking(pe, offset, val);
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn store_unmanaged(&self, index: usize, val: T) {
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            unsafe {
                self.mem_region.put_unmanaged(pe, offset, val);
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn swap<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        // println!("in Network atomic swap");
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            if self.atomic_support.swap {
                let handle =
                    self.mem_region
                        .atomic_fetch_op(pe, offset, AtomicOp::Write(Box::pin(val)));
                ArrayFetchOpHandle {
                    array: self.clone().into(),
                    state: FetchOpState::Network(handle),
                }
            } else {
                self.initiate_batch_fetch_op_2(val, index, ArrayOpCmd::Swap, self.clone().into())
                    .into()
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }
    unsafe fn blocking_swap(&self, index: usize, val: T) -> T {
        // println!("in Network atomic blocking swap");
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            if self.atomic_support.swap {
                self.mem_region
                    .atomic_fetch_op_blocking(pe, offset, AtomicOp::Write(Box::pin(val)))
            } else {
                self.initiate_batch_fetch_op_2(val, index, ArrayOpCmd::Swap, self.clone().into())
                    .block()[0]
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }
}

impl<T: ElementArithmeticOps + 'static> UnsafeArithmeticOps<T> for UnsafeArray<T> {
    unsafe fn add<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        if !self.atomic_support.add {
            return self.initiate_op(val, index, ArrayOpCmd::Add, self.clone().into());
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_op(pe, offset, AtomicOp::Sum(Box::pin(val)));
            ArrayOpHandle {
                array: self.clone().into(),
                state: OpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_add(&self, index: usize, val: T) {
        if !self.atomic_support.add {
            self.initiate_op(val, index, ArrayOpCmd::Add, self.clone().into())
                .block();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_blocking(pe, offset, AtomicOp::Sum(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn add_unmanaged(&self, index: usize, val: T) {
        if !self.atomic_support.add {
            let _ = self
                .initiate_op(val, index, ArrayOpCmd::Add, self.clone().into())
                .spawn();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::Sum(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn sub<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        if !self.atomic_support.add {
            return self.initiate_op(val, index, ArrayOpCmd::Sub, self.clone().into());
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_op(pe, offset, AtomicOp::Sub(Box::pin(val)));
            ArrayOpHandle {
                array: self.clone().into(),
                state: OpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn sub_unmanaged(&self, index: usize, val: T) {
        if !self.atomic_support.add {
            let _ = self
                .initiate_op(val, index, ArrayOpCmd::Sub, self.clone().into())
                .spawn();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::Sub(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_sub(&self, index: usize, val: T) {
        if !self.atomic_support.add {
            self.initiate_op(val, index, ArrayOpCmd::Sub, self.clone().into())
                .block();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_blocking(pe, offset, AtomicOp::Sub(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn fetch_add<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        if !self.atomic_support.fetch_add {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchAdd, self.clone().into())
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle =
                self.mem_region
                    .atomic_fetch_op(pe, offset, AtomicOp::FetchSum(Box::pin(val)));
            ArrayFetchOpHandle {
                array: self.clone().into(),
                state: FetchOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_fetch_add(&self, index: usize, val: T) -> T {
        if !self.atomic_support.fetch_add {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchAdd, self.clone().into())
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_fetch_op_blocking(pe, offset, AtomicOp::FetchSum(Box::pin(val)))
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn fetch_sub<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        if !self.atomic_support.fetch_add {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchSub, self.clone().into())
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle =
                self.mem_region
                    .atomic_fetch_op(pe, offset, AtomicOp::FetchSub(Box::pin(val)));
            ArrayFetchOpHandle {
                array: self.clone().into(),
                state: FetchOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_fetch_sub(&self, index: usize, val: T) -> T {
        if !self.atomic_support.fetch_add {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchSub, self.clone().into())
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_fetch_op_blocking(pe, offset, AtomicOp::FetchSub(Box::pin(val)))
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn mul<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        if !self.atomic_support.prod {
            return self.initiate_op(val, index, ArrayOpCmd::Mul, self.clone().into());
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_op(pe, offset, AtomicOp::Prod(Box::pin(val)));
            ArrayOpHandle {
                array: self.clone().into(),
                state: OpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_mul(&self, index: usize, val: T) {
        if !self.atomic_support.prod {
            self.initiate_op(val, index, ArrayOpCmd::Mul, self.clone().into())
                .block();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_blocking(pe, offset, AtomicOp::Prod(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn mul_unmanaged(&self, index: usize, val: T) {
        if !self.atomic_support.prod {
            let _ = self
                .initiate_op(val, index, ArrayOpCmd::Mul, self.clone().into())
                .spawn();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::Prod(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn fetch_mul<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        if !self.atomic_support.fetch_prod {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchMul, self.clone().into())
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle =
                self.mem_region
                    .atomic_fetch_op(pe, offset, AtomicOp::FetchProd(Box::pin(val)));
            ArrayFetchOpHandle {
                array: self.clone().into(),
                state: FetchOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_fetch_mul(&self, index: usize, val: T) -> T {
        if !self.atomic_support.fetch_prod {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchMul, self.clone().into())
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_fetch_op_blocking(pe, offset, AtomicOp::FetchProd(Box::pin(val)))
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }
}

impl<T: ElementBitWiseOps + 'static> UnsafeBitWiseOps<T> for UnsafeArray<T> {
    unsafe fn bit_and<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        if !self.atomic_support.bit_and {
            return self.initiate_op(val, index, ArrayOpCmd::And, self.clone().into());
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_op(pe, offset, AtomicOp::BitAnd(Box::pin(val)));
            ArrayOpHandle {
                array: self.clone().into(),
                state: OpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_bit_and(&self, index: usize, val: T) {
        if !self.atomic_support.bit_and {
            self.initiate_op(val, index, ArrayOpCmd::And, self.clone().into())
                .block();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_blocking(pe, offset, AtomicOp::BitAnd(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn bit_and_unmanaged(&self, index: usize, val: T) {
        if !self.atomic_support.bit_and {
            let _ = self
                .initiate_op(val, index, ArrayOpCmd::And, self.clone().into())
                .spawn();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::BitAnd(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_fetch_bit_and(&self, index: usize, val: T) -> T {
        if !self.atomic_support.fetch_bit_and {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchAnd, self.clone().into())
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region.atomic_fetch_op_blocking(
                pe,
                offset,
                AtomicOp::FetchBitAnd(Box::pin(val)),
            )
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn fetch_bit_and<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        if !self.atomic_support.fetch_bit_and {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchAnd, self.clone().into())
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle =
                self.mem_region
                    .atomic_fetch_op(pe, offset, AtomicOp::FetchBitAnd(Box::pin(val)));
            ArrayFetchOpHandle {
                array: self.clone().into(),
                state: FetchOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn bit_or<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        if !self.atomic_support.bit_or {
            return self.initiate_op(val, index, ArrayOpCmd::Or, self.clone().into());
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_op(pe, offset, AtomicOp::BitOr(Box::pin(val)));
            ArrayOpHandle {
                array: self.clone().into(),
                state: OpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_bit_or(&self, index: usize, val: T) {
        if !self.atomic_support.bit_or {
            self.initiate_op(val, index, ArrayOpCmd::Or, self.clone().into())
                .block();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_blocking(pe, offset, AtomicOp::BitOr(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn bit_or_unmanaged(&self, index: usize, val: T) {
        if !self.atomic_support.bit_or {
            let _ = self
                .initiate_op(val, index, ArrayOpCmd::Or, self.clone().into())
                .spawn();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::BitOr(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_fetch_bit_or(&self, index: usize, val: T) -> T {
        if !self.atomic_support.fetch_bit_or {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchOr, self.clone().into())
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region.atomic_fetch_op_blocking(
                pe,
                offset,
                AtomicOp::FetchBitOr(Box::pin(val)),
            )
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn fetch_bit_or<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        if !self.atomic_support.fetch_bit_or {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchOr, self.clone().into())
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle =
                self.mem_region
                    .atomic_fetch_op(pe, offset, AtomicOp::FetchBitOr(Box::pin(val)));
            ArrayFetchOpHandle {
                array: self.clone().into(),
                state: FetchOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn bit_xor<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        if !self.atomic_support.bit_xor {
            return self.initiate_op(val, index, ArrayOpCmd::Xor, self.clone().into());
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_op(pe, offset, AtomicOp::BitXor(Box::pin(val)));
            ArrayOpHandle {
                array: self.clone().into(),
                state: OpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_bit_xor(&self, index: usize, val: T) {
        if !self.atomic_support.bit_xor {
            self.initiate_op(val, index, ArrayOpCmd::Xor, self.clone().into())
                .block();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_blocking(pe, offset, AtomicOp::BitXor(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn bit_xor_unmanaged(&self, index: usize, val: T) {
        if !self.atomic_support.bit_xor {
            let _ = self
                .initiate_op(val, index, ArrayOpCmd::Xor, self.clone().into())
                .spawn();
            return;
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_op_unmanaged(pe, offset, AtomicOp::BitXor(Box::pin(val)));
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn fetch_bit_xor<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        if !self.atomic_support.fetch_bit_xor {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchXor, self.clone().into())
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle =
                self.mem_region
                    .atomic_fetch_op(pe, offset, AtomicOp::FetchBitXor(Box::pin(val)));
            ArrayFetchOpHandle {
                array: self.clone().into(),
                state: FetchOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_fetch_bit_xor(&self, index: usize, val: T) -> T {
        if !self.atomic_support.fetch_bit_xor {
            return self
                .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::FetchXor, self.clone().into())
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region.atomic_fetch_op_blocking(
                pe,
                offset,
                AtomicOp::FetchBitXor(Box::pin(val)),
            )
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }
}

impl<T: ElementShiftOps + 'static> UnsafeShiftOps<T> for UnsafeArray<T> {}

impl<T: ElementCompareEqOps + 'static> UnsafeCompareExchangeOps<T> for UnsafeArray<T> {
    unsafe fn compare_exchange<'a>(
        &self,
        index: usize,
        current: T,
        new: T,
    ) -> ArrayResultOpHandle<T> {
        if !self.atomic_support.cas {
            return self
                .initiate_batch_result_op_2(
                    new,
                    index,
                    ArrayOpCmd::CompareExchange(current),
                    self.clone().into(),
                )
                .into();
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            let handle = self
                .mem_region
                .atomic_compare_exchange(pe, offset, current, new);
            ArrayResultOpHandle {
                array: self.clone().into(),
                state: ResultOpState::Network(handle),
            }
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }

    unsafe fn blocking_compare_exchange(&self, index: usize, current: T, new: T) -> Result<T, T> {
        if !self.atomic_support.cas {
            return self
                .initiate_batch_result_op_2(
                    new,
                    index,
                    ArrayOpCmd::CompareExchange(current),
                    self.clone().into(),
                )
                .block()[0];
        }
        if let Some((pe, offset)) = self.pe_and_rdma_offset_for_global_index(index) {
            self.mem_region
                .atomic_compare_exchange_blocking(pe, offset, current, new)
        } else {
            panic!(
                "Index: {index} out of bounds for array of len: {:?}",
                self.inner.size
            );
        }
    }
}

impl<T: ElementComparePartialEqOps + 'static> UnsafeCompareExchangeEpsilonOps<T>
    for UnsafeArray<T>
{
}
