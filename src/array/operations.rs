// use crate::active_messaging::LamellarArcAm;
use crate::array::atomic::*;
use crate::array::generic_atomic::*;
use crate::array::global_lock_atomic::*;
use crate::array::local_lock_atomic::*;
use crate::array::native_atomic::*;
use crate::array::{AmDist, Dist, LamellarEnv, LamellarWriteArray};
use crate::config;

// use crate::lamellar_request::LamellarRequest;
// use crate::scheduler::Scheduler;
// use crate::LamellarTeamRT;

pub(crate) mod handle;
pub use handle::{
    ArrayBatchOpHandle, ArrayFetchBatchOpHandle, ArrayOpHandle, ArrayResultBatchOpHandle,
};
pub(crate) mod access;
pub use access::{AccessOps, LocalAtomicOps, UnsafeAccessOps};
pub(crate) mod arithmetic;
pub use arithmetic::{
    ArithmeticOps, ElementArithmeticOps, LocalArithmeticOps, UnsafeArithmeticOps,
};
pub(crate) mod bitwise;
pub use bitwise::{BitWiseOps, ElementBitWiseOps, LocalBitWiseOps, UnsafeBitWiseOps};
pub(crate) mod compare_exchange;
pub use compare_exchange::{
    CompareExchangeEpsilonOps, CompareExchangeOps, ElementCompareEqOps, ElementComparePartialEqOps,
    UnsafeCompareExchangeEpsilonOps, UnsafeCompareExchangeOps,
};
pub(crate) mod read_only;
pub use read_only::{ReadOnlyOps, UnsafeReadOnlyOps};
pub(crate) mod shift;
pub use shift::{ElementShiftOps, LocalShiftOps, ShiftOps, UnsafeShiftOps};

// use async_trait::async_trait;
// use parking_lot::Mutex;
// use std::collections::HashMap;
// use std::marker::PhantomData;
// use std::pin::Pin;
// use std::sync::atomic::{AtomicBool, Ordering};
// use std::sync::Arc;
use std::u8;

/// A marker trait for types that can be used as an array
/// Users should not implement this directly, rather they should use the [trait@ArrayOps] derive macro
/// by passing it as an argument to the [macro@crate::active_messaging::AmData] attribute macro to automatically derive this trait.
///
/// # Examples
///
/// ```
/// // this import includes everything we need
/// use lamellar::array::prelude::*;
///
///
/// #[lamellar::AmData(
///     // Lamellar traits
///     ArrayOps, // needed to derive the ArrayType trait (and additional traits required by the runtime)
///     Default,       // needed to be able to initialize a LamellarArray
///     //  Notice we use `lamellar::AmData` instead of `derive`
///     //  for common traits, e.g. Debug, Clone.    
///     PartialEq,     // needed for CompareExchangeEpsilonOps
///     PartialOrd,    // needed for CompareExchangeEpsilonOps
///     Debug,         // any addition traits you want derived
///     Clone,
/// )]
/// struct Custom {
///     int: usize,
///     float: f32,
/// }
pub trait ArrayOps {}

#[doc(hidden)]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Hash,
    std::cmp::PartialEq,
    std::cmp::Eq,
    Clone,
    Debug,
    Copy,
)]
#[serde(bound = "T: AmDist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum ArrayOpCmd<T: AmDist> {
    Add,
    FetchAdd,
    Sub,
    FetchSub,
    Mul,
    FetchMul,
    Div,
    FetchDiv,
    Rem,
    FetchRem,
    And,
    FetchAnd,
    Or,
    FetchOr,
    Xor,
    FetchXor,
    Store,
    Load,
    Swap,
    Put,
    Get,
    CompareExchange(T),
    CompareExchangeEps(T, T),
    Shl,
    FetchShl,
    Shr,
    FetchShr,
}

impl<T: Dist> From<ArrayOpCmd<T>> for ArrayOpCmd<Vec<u8>> {
    fn from(cmd: ArrayOpCmd<T>) -> Self {
        match cmd {
            ArrayOpCmd::Add => ArrayOpCmd::Add,
            ArrayOpCmd::FetchAdd => ArrayOpCmd::FetchAdd,
            ArrayOpCmd::Sub => ArrayOpCmd::Sub,
            ArrayOpCmd::FetchSub => ArrayOpCmd::FetchSub,
            ArrayOpCmd::Mul => ArrayOpCmd::Mul,
            ArrayOpCmd::FetchMul => ArrayOpCmd::FetchMul,
            ArrayOpCmd::Div => ArrayOpCmd::Div,
            ArrayOpCmd::FetchDiv => ArrayOpCmd::FetchDiv,
            ArrayOpCmd::Rem => ArrayOpCmd::Rem,
            ArrayOpCmd::FetchRem => ArrayOpCmd::FetchRem,
            ArrayOpCmd::And => ArrayOpCmd::And,
            ArrayOpCmd::FetchAnd => ArrayOpCmd::FetchAnd,
            ArrayOpCmd::Or => ArrayOpCmd::Or,
            ArrayOpCmd::FetchOr => ArrayOpCmd::FetchOr,
            ArrayOpCmd::Xor => ArrayOpCmd::Xor,
            ArrayOpCmd::FetchXor => ArrayOpCmd::FetchXor,
            ArrayOpCmd::Store => ArrayOpCmd::Store,
            ArrayOpCmd::Load => ArrayOpCmd::Load,
            ArrayOpCmd::Swap => ArrayOpCmd::Swap,
            ArrayOpCmd::Put => ArrayOpCmd::Put,
            ArrayOpCmd::Get => ArrayOpCmd::Get,
            ArrayOpCmd::CompareExchange(old) => {
                let old_u8 = &old as *const T as *const u8;
                let old_u8_vec = unsafe {
                    std::slice::from_raw_parts(old_u8, std::mem::size_of::<T>()).to_vec()
                };
                ArrayOpCmd::CompareExchange(old_u8_vec)
            }
            ArrayOpCmd::CompareExchangeEps(old, eps) => {
                let old_u8 = &old as *const T as *const u8;
                let old_u8_vec = unsafe {
                    std::slice::from_raw_parts(old_u8, std::mem::size_of::<T>()).to_vec()
                };
                let eps_u8 = &eps as *const T as *const u8;
                let eps_u8_vec = unsafe {
                    std::slice::from_raw_parts(eps_u8, std::mem::size_of::<T>()).to_vec()
                };
                ArrayOpCmd::CompareExchangeEps(old_u8_vec, eps_u8_vec)
            }
            ArrayOpCmd::Shl => ArrayOpCmd::Shl,
            ArrayOpCmd::FetchShl => ArrayOpCmd::FetchShl,
            ArrayOpCmd::Shr => ArrayOpCmd::Shr,
            ArrayOpCmd::FetchShr => ArrayOpCmd::FetchShr,
        }
    }
}

impl<T: Dist> From<ArrayOpCmd<Vec<u8>>> for ArrayOpCmd<T> {
    fn from(cmd: ArrayOpCmd<Vec<u8>>) -> Self {
        match cmd {
            ArrayOpCmd::Add => ArrayOpCmd::Add,
            ArrayOpCmd::FetchAdd => ArrayOpCmd::FetchAdd,
            ArrayOpCmd::Sub => ArrayOpCmd::Sub,
            ArrayOpCmd::FetchSub => ArrayOpCmd::FetchSub,
            ArrayOpCmd::Mul => ArrayOpCmd::Mul,
            ArrayOpCmd::FetchMul => ArrayOpCmd::FetchMul,
            ArrayOpCmd::Div => ArrayOpCmd::Div,
            ArrayOpCmd::FetchDiv => ArrayOpCmd::FetchDiv,
            ArrayOpCmd::Rem => ArrayOpCmd::Rem,
            ArrayOpCmd::FetchRem => ArrayOpCmd::FetchRem,
            ArrayOpCmd::And => ArrayOpCmd::And,
            ArrayOpCmd::FetchAnd => ArrayOpCmd::FetchAnd,
            ArrayOpCmd::Or => ArrayOpCmd::Or,
            ArrayOpCmd::FetchOr => ArrayOpCmd::FetchOr,
            ArrayOpCmd::Xor => ArrayOpCmd::Xor,
            ArrayOpCmd::FetchXor => ArrayOpCmd::FetchXor,
            ArrayOpCmd::Store => ArrayOpCmd::Store,
            ArrayOpCmd::Load => ArrayOpCmd::Load,
            ArrayOpCmd::Swap => ArrayOpCmd::Swap,
            ArrayOpCmd::Put => ArrayOpCmd::Put,
            ArrayOpCmd::Get => ArrayOpCmd::Get,
            ArrayOpCmd::CompareExchange(old) => {
                let old_t = unsafe {
                    std::slice::from_raw_parts(old.as_ptr() as *const T, std::mem::size_of::<T>())
                };
                ArrayOpCmd::CompareExchange(old_t[0])
            }
            ArrayOpCmd::CompareExchangeEps(old, eps) => {
                let old_t = unsafe {
                    std::slice::from_raw_parts(old.as_ptr() as *const T, std::mem::size_of::<T>())
                };
                let eps_t = unsafe {
                    std::slice::from_raw_parts(eps.as_ptr() as *const T, std::mem::size_of::<T>())
                };
                ArrayOpCmd::CompareExchangeEps(old_t[0], eps_t[0])
            }
            ArrayOpCmd::Shl => ArrayOpCmd::Shl,
            ArrayOpCmd::FetchShl => ArrayOpCmd::FetchShl,
            ArrayOpCmd::Shr => ArrayOpCmd::Shr,
            ArrayOpCmd::FetchShr => ArrayOpCmd::FetchShr,
        }
    }
}

#[doc(hidden)]
#[repr(C)] //required as we reinterpret as bytes
#[lamellar_impl::AmLocalDataRT]
pub struct IdxVal<I, T> {
    pub index: I,
    pub val: T,
}

impl<I, T> IdxVal<I, T> {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }
}

#[doc(hidden)]
#[derive(Clone, serde::Serialize, Debug)]
pub enum OpInputEnum<'a, T: Dist> {
    Val(T),
    Slice(&'a [T]),
    Vec(Vec<T>),
    NativeAtomicLocalData(NativeAtomicLocalData<T>),
    GenericAtomicLocalData(GenericAtomicLocalData<T>),
    LocalLockLocalData(LocalLockLocalData<T>),
    GlobalLockLocalData(GlobalLockLocalData<T>),
    // Iter(Box<dyn Iterator<Item = T> + 'a>),

    // while it would be convienient to directly use the following, doing so
    // is ambiguous with respect to both safety (Memregions and UnsafeArray)
    // but also it hides the fact we are only operating on the local segment of an array or memory region
    // #[serde(serialize_with = "OneSidedMemoryRegion::serialize_local_data")]
    // MemoryRegion(OneSidedMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
    // ReadOnlyArray(ReadOnlyArray<T>),
    // AtomicArray(AtomicArray<T>),
}

impl<'a, T: Dist> OpInputEnum<'a, T> {
    // #[tracing::instrument(skip_all)]
    // pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
    //     match self {
    //         OpInputEnum::Val(v) => Box::new(std::iter::repeat(v).map(|elem| *elem)),
    //         OpInputEnum::Slice(s) => Box::new(s.iter().map(|elem| *elem)),
    //         OpInputEnum::Vec(v) => Box::new(v.iter().map(|elem| *elem)),
    //         OpInputEnum::NativeAtomicLocalData(a) => Box::new(a.iter().map(|elem| elem.load())),
    //         OpInputEnum::GenericAtomicLocalData(a) => Box::new(a.iter().map(|elem| elem.load())),
    //         OpInputEnum::LocalLockLocalData(a) => Box::new(a.iter().map(|elem| *elem)),
    //         OpInputEnum::GlobalLockLocalData(a) => Box::new(a.iter().map(|elem| *elem)),
    //         // OpInputEnum::MemoryRegion(mr) => Box::new(
    //         //     unsafe { mr.as_slice() }
    //         //         .expect("memregion not local")
    //         //         .iter()
    //         //         .map(|elem| *elem),
    //         // ),
    //         // OpInputEnum::UnsafeArray(a) => Box::new(unsafe{a.local_data()}.iter().map(|elem| *elem)),
    //         // OpInputEnum::ReadOnlyArray(a) => Box::new(a.local_data().iter().map(|elem| *elem)),
    //         // OpInputEnum::AtomicArray(a) => Box::new(a.local_data().iter().map(|elem| elem.load())),
    //     }
    // }
    // #[tracing::instrument(skip_all)]
    pub(crate) fn len(&self) -> usize {
        match self {
            OpInputEnum::Val(_) => 1,
            OpInputEnum::Slice(s) => s.len(),
            OpInputEnum::Vec(v) => v.len(),
            OpInputEnum::NativeAtomicLocalData(a) => a.len(),
            OpInputEnum::GenericAtomicLocalData(a) => a.len(),
            OpInputEnum::LocalLockLocalData(a) => a.len(),
            OpInputEnum::GlobalLockLocalData(a) => a.len(),
            // OpInputEnum::MemoryRegion(mr) => {
            //     unsafe { mr.as_slice() }.expect("memregion not local").len()
            // }
            // OpInputEnum::UnsafeArray(a) => unsafe{a.local_data()}.len(),
            // OpInputEnum::ReadOnlyArray(a) => a.local_data().len(),
            // OpInputEnum::AtomicArray(a) => unsafe{a.__local_as_slice().len()},
        }
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) fn first(&self) -> T {
        match self {
            OpInputEnum::Val(v) => *v,
            OpInputEnum::Slice(s) => *s.first().expect("slice is empty"),
            OpInputEnum::Vec(v) => *v.first().expect("vec is empty"),
            OpInputEnum::NativeAtomicLocalData(a) => a.at(0).load(),
            OpInputEnum::GenericAtomicLocalData(a) => a.at(0).load(),
            OpInputEnum::LocalLockLocalData(a) => *a.first().expect("array is empty"),
            OpInputEnum::GlobalLockLocalData(a) => *a.first().expect("array is empty"),
            // OpInputEnum::MemoryRegion(mr) => *unsafe { mr.as_slice() }
            //     .expect("memregion not local")
            //     .first()
            //     .expect("memregion is empty"),
        }
    }

    // //#[tracing::instrument(skip_all)]
    pub(crate) fn into_vec_chunks(self, chunk_size: usize) -> Vec<Vec<T>> {
        match self {
            OpInputEnum::Val(v) => vec![vec![v]],
            OpInputEnum::Slice(s) => s.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect(),
            OpInputEnum::Vec(v) => v.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect(),
            OpInputEnum::NativeAtomicLocalData(a) => {
                let mut data = Vec::with_capacity(chunk_size);

                a.iter()
                    .enumerate()
                    .filter_map(move |(i, elem)| {
                        data.push(elem.load());
                        if data.len() == chunk_size || i == a.len() - 1 {
                            let mut new_data = Vec::with_capacity(chunk_size);
                            std::mem::swap(&mut data, &mut new_data);
                            Some(new_data)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            OpInputEnum::GenericAtomicLocalData(a) => {
                let mut data = Vec::with_capacity(chunk_size);

                a.iter()
                    .enumerate()
                    .filter_map(move |(i, elem)| {
                        data.push(elem.load());
                        if data.len() == chunk_size || i == a.len() - 1 {
                            let mut new_data = Vec::with_capacity(chunk_size);
                            std::mem::swap(&mut data, &mut new_data);
                            Some(new_data)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            OpInputEnum::LocalLockLocalData(a) => {
                a.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
            }
            OpInputEnum::GlobalLockLocalData(a) => {
                a.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
            } // OpInputEnum::MemoryRegion(mr) => *unsafe { mr.as_slice() }
              //     .expect("memregion not local")
              //     .first()
              //     .expect("memregion is empty"),
        }
    }

    pub(crate) fn to_vec(self) -> Vec<T> {
        match self {
            OpInputEnum::Val(v) => vec![v],
            OpInputEnum::Slice(s) => s.to_vec(),
            OpInputEnum::Vec(v) => v,
            OpInputEnum::NativeAtomicLocalData(a) => a.iter().map(|elem| elem.load()).collect(),
            OpInputEnum::GenericAtomicLocalData(a) => a.iter().map(|elem| elem.load()).collect(),
            OpInputEnum::LocalLockLocalData(a) => a.to_vec(),
            OpInputEnum::GlobalLockLocalData(a) => a.to_vec(),
        }
    }
}

// impl<'a, T: Dist> From<&T> for OpInputEnum<'a, T> {
//     fn from(v: T) -> Self {
//         OpInputEnum::Val(v)
//     }
// }

// impl<'a, T: Dist, I: Iterator<Item=T>> From<I> for OpInputEnum<'a, T> {
//     fn from(v: I) -> Self {
//         OpInputEnum::Iterator(Box::new(v))
//     }
// }

/// This trait is used to represent the input to a batched LamellarArray element-wise operation.
///
/// # Contents
/// - [Overview](#overview)
/// - [Batch-compatible input types and type conversion methods](#batch-compatible-input-types-and-type-conversion-methods)
///
/// # Overview
///
/// Valid inputs to batched operations are essentially "safe" list-like data structures, such as `Vec<T>` and slice `&[T]`.
/// We have also provided functions on the LamellarArray types that allow you to access a PE's local data. These functions
/// appropriately protect the local data so the the safety guarantees of the given array type are maintained.
/// See a list of [batch-compatible input types and type conversion methods](#batch-compatible-input-types-and-type-conversion-methods)
/// below.
///
/// Currently it is not recommended to try to implement this for your types. Rather you should try to convert to a slice if possible.
///
/// # Batch-compatible input types and type conversion methods
///
/// Methods that return a batch-compatible input type (and the array type that provides it):
/// - `local_data`- ([AtomicArray][crate::array::AtomicArray], [ReadOnlyArray][crate::array::ReadOnlyArray])
/// - `mut_local_data` - ([AtomicArray][crate::array::AtomicArray], [ReadOnlyArray][crate::array::ReadOnlyArray])
/// - `read_local_data` - ([LocalLockArray][crate::array::LocalLockArray],[GlobalLockArray][crate::array::GlobalLockArray])
/// - `write_local_data` -([LocalLockArray][crate::array::LocalLockArray],[GlobalLockArray][crate::array::GlobalLockArray])
///
/// Batch-compatible input types:
/// - T and &T
/// - &\[T\]
///     - the local data of a [ReadOnlyArray](crate::array::ReadOnlyArray) is directly the underlying slice
///     - ```read_only_array.local_data();```
/// - Vec\[T\] and &Vec\[T\]
/// - [AtomicLocalData][crate::array::atomic::AtomicLocalData]
///     - ```atomic_array.local_data();```
/// - [LocalLockLocalData][crate::array::local_lock_atomic::LocalLockLocalData]
///     - ```local_lock_array.read_local_data();```
///     - ```local_lock_array.write_local_data();```
/// - [GlobalLockLocalData][crate::array::global_lock_atomic::GlobalLockLocalData]
///     - ```global_lock_array.read_local_data();```
///     - ```global_lock_array.write_local_data();```
///
/// It is possible to use a LamellarMemoryRegion or UnsafeArray as the parent source, but these will require unsafe calls to retrieve the underlying slices.
/// The retrieved slices can then be used for the batched operation
///```ignore
/// unsafe { onesided_mem_region.as_slice().expect("not on allocating PE") };
/// unsafe { shared_mem_region.as_slice() };
/// unsafe { unsafe_array.local_data() };
///```
pub trait OpInput<'a, T: Dist> {
    #[doc(hidden)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize); //(Vec<(Box<dyn Iterator<Item = T>    + '_>,usize)>,usize);
}

impl<'a, T: Dist> OpInput<'a, T> for T {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("val as op input");
        (vec![OpInputEnum::Val(self)], 1)
    }
}
impl<'a, T: Dist> OpInput<'a, T> for &T {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("ref as op input");
        (vec![OpInputEnum::Val(*self)], 1)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a [T] {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("slice as op input");
        let len = self.len();
        let mut iters = vec![];
        if len == 0 {
            return (iters, len);
        }
        let num = if len < 1000 {
            1
        } else {
            match config().batch_op_threads {
                Some(n) => n,
                None => std::cmp::max(1, config().threads / 4),
            }
        };
        let num_per_batch = len / num;
        for i in 0..num {
            let temp = &self[(i * num_per_batch)..((i + 1) * num_per_batch)];
            iters.push(OpInputEnum::Slice(temp));
        }
        let rem = len % num_per_batch;
        if rem > 0 {
            let temp = &self[(num * num_per_batch)..];
            iters.push(OpInputEnum::Slice(temp));
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a mut (dyn Iterator<Item = T> + 'a) {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("iter as op input");
        self.collect::<Vec<_>>().as_op_input()
    }
}

// impl<'a, T: Dist, I: Iterator<Item = T>> OpInput<'a, T> for &'a I  {
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         self.collect::<Vec<_>>().as_op_input()
//     }
// }

// impl<'a, T: Dist, I: Iterator<Item = &'a T>> OpInput<'a, T> for I  {
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         self.map(|e| *e).collect::<Vec<_>>().as_op_input()
//     }
// }

// impl<'a, T: Dist, I: Iterator<Item = &'a T>> OpInput<'a, T> for &'a I  {
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         self.map(|e| *e).collect::<Vec<_>>().as_op_input()
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for &'a mut [T] {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("slice as mut op input");
        let len = self.len();
        let mut iters = vec![];
        if len == 0 {
            return (iters, len);
        }

        if len == 0 {
            return (iters, len);
        }
        let num = if len < 1000 {
            1
        } else {
            match config().batch_op_threads {
                Some(n) => n,
                None => std::cmp::max(1, config().threads / 4),
            }
        };
        let num_per_batch = len / num;
        for i in 0..num {
            let temp = &self[(i * num_per_batch)..((i + 1) * num_per_batch)];
            iters.push(OpInputEnum::Slice(temp));
        }
        let rem = len % num_per_batch;
        if rem > 0 {
            let temp = &self[(num * num_per_batch)..];
            iters.push(OpInputEnum::Slice(temp));
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a Vec<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("vec ref as op input");
        (&self[..]).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a mut Vec<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("vec ref mut as op input");
        (&self[..]).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for Vec<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("vec as op input");
        let len = self.len();
        if len == 0 {
            return (vec![], len);
        }
        let num = if len < 1000 {
            1
        } else {
            match config().batch_op_threads {
                Some(n) => n,
                None => std::cmp::max(1, config().threads / 4),
            }
        };
        let num_per_batch = len / num;
        let iters = self
            .chunks(num_per_batch)
            .map(|c| OpInputEnum::Vec(c.to_vec()))
            .collect::<_>();

        // iters.reverse(); //the indice slices get pushed in from the back, but we want to return in order
        (iters, len)
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for &OneSidedMemoryRegion<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         let slice = unsafe { self.as_slice() }.expect("mem region not local");
//         let len = slice.len();
//         let mut iters = vec![];
//         let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
//             Ok(n) => n.parse::<usize>().unwrap(),
//             Err(_) => 10000,
//         };
//         let num = len / num_per_batch;
//         for i in 0..num {
//             let sub_region = self.sub_region((i * num_per_batch)..((i + 1) * num_per_batch));
//             iters.push(OpInputEnum::MemoryRegion(sub_region));
//         }
//         let rem = len % num_per_batch;
//         if rem > 0 {
//             let sub_region = self.sub_region((num * num_per_batch)..(num * num_per_batch) + rem);
//             iters.push(OpInputEnum::MemoryRegion(sub_region));
//         }
//         (iters, len)
//     }
// }

// // impl<'a, T: Dist> OpInput<'a, T> for &OneSidedMemoryRegion<T> {
// //     //#[tracing::instrument(skip_all)]
// //     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
// //         LamellarMemoryRegion::from(self).as_op_input()
// //     }
// // }

// impl<'a, T: Dist> OpInput<'a, T> for OneSidedMemoryRegion<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         // LamellarMemoryRegion::from(self).as_op_input()
//         (&self).as_op_input()
//     }
// }

// impl<'a, T: Dist> OpInput<'a, T> for &SharedMemoryRegion<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         LamellarMemoryRegion::from(self).as_op_input()
//     }
// }

// impl<'a, T: Dist> OpInput<'a, T> for SharedMemoryRegion<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         LamellarMemoryRegion::from(self).as_op_input()
//     }
// }

// impl<'a, T: Dist> OpInput<'a, T> for &'a UnsafeArray<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         let slice = unsafe { self.local_as_slice() };
//         // let slice = unsafe { std::mem::transmute::<&'_ [T], &'a [T]>(slice) }; //this is safe in the context of buffered_ops because we know we wait for all the requests to submit before we return
//         slice.as_op_input()
//     }
// }

// impl<'a, T: Dist> OpInput<'a, T> for UnsafeArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         unsafe{self.clone().local_as_slice().as_op_input()}
//     }
// }

// impl<'a, T: Dist> OpInput<'a, T> for &'a ReadOnlyArray<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         let slice = self.local_as_slice();
//         // let slice = unsafe { std::mem::transmute::<&'_ [T], &'a [T]>(slice) }; //this is safe in the context of buffered_ops because we know we wait for all the requests to submit before we return
//         slice.as_op_input()
//     }
// }

// impl<'a, T: Dist> OpInput<'a, T> for ReadOnlyArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for &'a LocalLockLocalData<T> {
    // #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("LocalLockLocalData as_op_input {:?}", self.deref());
        let len = self.len();
        let mut iters = vec![];
        if len == 0 {
            return (iters, len);
        }
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match config().batch_op_threads {
                    Some(n) => n,
                    None => std::cmp::max(1, config().threads / 4),
                }
            };
            let num_per_batch = len / num;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = self
                    .clone()
                    .into_sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                // println!("sub_data: {:?}", sub_data.deref());
                iters.push(OpInputEnum::LocalLockLocalData(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data = self.clone().into_sub_data(num * num_per_batch, len);
                iters.push(OpInputEnum::LocalLockLocalData(sub_data));
            }
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a GlobalLockLocalData<T> {
    // #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // println!("GlobalLockLocalData as_op_input");
        let len = self.len();
        let mut iters = vec![];
        if len == 0 {
            return (iters, len);
        }
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match config().batch_op_threads {
                    Some(n) => n,
                    None => std::cmp::max(1, config().threads / 4),
                }
            };
            let num_per_batch = len / num;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = self
                    .clone()
                    .into_sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                iters.push(OpInputEnum::GlobalLockLocalData(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data = self.clone().into_sub_data(num * num_per_batch, len);
                iters.push(OpInputEnum::GlobalLockLocalData(sub_data));
            }
        }

        (iters, len)
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for LocalLockArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &AtomicLocalData<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        match self.array.clone() {
            AtomicArray::GenericAtomicArray(a) => a.local_data().as_op_input(),
            AtomicArray::NativeAtomicArray(a) => a.local_data().as_op_input(),
        }
    }
}

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for AtomicLocalData<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        match self.array {
            AtomicArray::GenericAtomicArray(a) => a.local_data().as_op_input(),
            AtomicArray::NativeAtomicArray(a) => a.local_data().as_op_input(),
        }
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for AtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &GenericAtomicLocalData<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // let slice = unsafe { self.__local_as_slice() };

        let local_data = self.clone();
        let len = local_data.len();
        let mut iters = vec![];
        if len == 0 {
            return (iters, len);
        }
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match config().batch_op_threads {
                    Some(n) => n,
                    None => std::cmp::max(1, config().threads / 4),
                }
            };
            let num_per_batch = len / num;
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = local_data.sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                iters.push(OpInputEnum::GenericAtomicLocalData(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data = local_data.sub_data(num * num_per_batch, len);
                iters.push(OpInputEnum::GenericAtomicLocalData(sub_data));
            }
        }
        (iters, len)
    }
}

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for GenericAtomicLocalData<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (&self).as_op_input()
    }
}

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &NativeAtomicLocalData<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // let slice = unsafe { self.__local_as_slice() };
        // let len = slice.len();
        // let local_data = self.local_data();
        let local_data = self.clone();
        let len = local_data.len();
        let mut iters = vec![];
        if len == 0 {
            return (iters, len);
        }
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match config().batch_op_threads {
                    Some(n) => n,
                    None => std::cmp::max(1, config().threads / 4),
                }
            };
            let num_per_batch = len / num;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = local_data.sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                // for j in sub_data.clone().into_iter() {
                //     println!("{:?} {:?}",i, j);
                // }
                iters.push(OpInputEnum::NativeAtomicLocalData(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data = local_data.sub_data(num * num_per_batch, len);
                iters.push(OpInputEnum::NativeAtomicLocalData(sub_data));
            }
        }
        (iters, len)
    }
}

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for NativeAtomicLocalData<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (&self).as_op_input()
    }
}

/// Supertrait specifying that array elements must be [Sized] and must be able to be used in remote operations [Dist].
pub trait ElementOps: Dist + Sized {}
impl<T> ElementOps for T where T: Dist {}

impl<T: ElementArithmeticOps> ArithmeticOps<T> for LamellarWriteArray<T> {}
