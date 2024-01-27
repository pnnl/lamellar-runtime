use crate::active_messaging::LamellarArcAm;
use crate::array::atomic::*;
use crate::array::generic_atomic::*;
use crate::array::global_lock_atomic::*;
use crate::array::local_lock_atomic::*;
use crate::array::native_atomic::*;
use crate::array::{AmDist, Dist, LamellarArrayRequest, LamellarEnv, LamellarWriteArray};
use crate::lamellar_request::LamellarRequest;
use crate::scheduler::Scheduler;
use crate::LamellarTeamRT;

pub(crate) mod access;
pub use access::{AccessOps, LocalAtomicOps};
pub(crate) mod arithmetic;
pub use arithmetic::{ArithmeticOps, ElementArithmeticOps, LocalArithmeticOps};
pub(crate) mod bitwise;
pub use bitwise::{BitWiseOps, ElementBitWiseOps, LocalBitWiseOps};
pub(crate) mod compare_exchange;
pub use compare_exchange::{
    CompareExchangeEpsilonOps, CompareExchangeOps, ElementCompareEqOps, ElementComparePartialEqOps,
};
pub(crate) mod read_only;
pub use read_only::ReadOnlyOps;
pub(crate) mod shift;
pub use shift::{ElementShiftOps, LocalShiftOps, ShiftOps};

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::u8;

#[doc(hidden)]
pub static OPS_BUFFER_SIZE: usize = 10_000_000;

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
    #[tracing::instrument(skip_all)]
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        match self {
            OpInputEnum::Val(v) => Box::new(std::iter::repeat(v).map(|elem| *elem)),
            OpInputEnum::Slice(s) => Box::new(s.iter().map(|elem| *elem)),
            OpInputEnum::Vec(v) => Box::new(v.iter().map(|elem| *elem)),
            OpInputEnum::NativeAtomicLocalData(a) => Box::new(a.iter().map(|elem| elem.load())),
            OpInputEnum::GenericAtomicLocalData(a) => Box::new(a.iter().map(|elem| elem.load())),
            OpInputEnum::LocalLockLocalData(a) => Box::new(a.iter().map(|elem| *elem)),
            OpInputEnum::GlobalLockLocalData(a) => Box::new(a.iter().map(|elem| *elem)),
            // OpInputEnum::MemoryRegion(mr) => Box::new(
            //     unsafe { mr.as_slice() }
            //         .expect("memregion not local")
            //         .iter()
            //         .map(|elem| *elem),
            // ),
            // OpInputEnum::UnsafeArray(a) => Box::new(unsafe{a.local_data()}.iter().map(|elem| *elem)),
            // OpInputEnum::ReadOnlyArray(a) => Box::new(a.local_data().iter().map(|elem| *elem)),
            // OpInputEnum::AtomicArray(a) => Box::new(a.local_data().iter().map(|elem| elem.load())),
        }
    }
    #[tracing::instrument(skip_all)]
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
            OpInputEnum::Val(v) =>vec![vec![v]],
            OpInputEnum::Slice(s) => s.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect(),
            OpInputEnum::Vec(v) => v.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect(),
            OpInputEnum::NativeAtomicLocalData(a) => {
                let mut data = Vec::with_capacity(chunk_size);

                a.iter().enumerate().filter_map(move |(i, elem)| {
                    data.push(elem.load());
                    if data.len() == chunk_size || i == a.len() - 1 {
                        let mut new_data = Vec::with_capacity(chunk_size);
                        std::mem::swap(&mut data, &mut new_data);
                        Some(new_data)
                    } else {
                        None
                    }
                }).collect()
            }
            OpInputEnum::GenericAtomicLocalData(a) => {
                let mut data = Vec::with_capacity(chunk_size);

                a.iter().enumerate().filter_map(move |(i, elem)| {
                    data.push(elem.load());
                    if data.len() == chunk_size || i == a.len() - 1 {
                        let mut new_data = Vec::with_capacity(chunk_size);
                        std::mem::swap(&mut data, &mut new_data);
                        Some(new_data)
                    } else {
                        None
                    }
                }).collect()
            }
            OpInputEnum::LocalLockLocalData(a) => {
                a.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
            }
            OpInputEnum::GlobalLockLocalData(a) => {
                a.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
            }
            // OpInputEnum::MemoryRegion(mr) => *unsafe { mr.as_slice() }
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
        (vec![OpInputEnum::Val(self)], 1)
    }
}
impl<'a, T: Dist> OpInput<'a, T> for &T {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (vec![OpInputEnum::Val(*self)], 1)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a [T] {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let len = self.len();
        let mut iters = vec![];
        let num = if len < 1000 {
            1
        } else {
            match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => match std::env::var("LAMELLAR_THREADS") {
                    Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4),
                    Err(_) => 4,
                },
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
        let len = self.len();
        let mut iters = vec![];

        let num = if len < 1000 {
            1
        } else {
            match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => {
                    match std::env::var("LAMELLAR_THREADS") {
                        Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4),
                        Err(_) => 4, //+ 1 to account for main thread
                    }
                }
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
        (&self[..]).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a mut Vec<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (&self[..]).as_op_input()
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for Vec<T> {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(mut self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         let len = self.len();
//         let mut iters = vec![];
//         let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
//             Ok(n) => n.parse::<usize>().unwrap(),
//             Err(_) => 10000,
//         };
//         let num = (len as f32 / num_per_batch as f32).ceil() as usize;
//         println!("num: {}", num);
//         for i in (1..num).rev() {
//             let temp = self.split_off(i * num_per_batch);
//             // println!("temp: {:?} {:?} {:?}", temp,i ,i * num_per_batch);
//             iters.push(OpInputEnum::Vec(temp));
//         }
//         let rem = len % num_per_batch;
//         // println!("rem: {} {:?}", rem,self);
//         // if rem > 0 || num == 1 {
//         if self.len() > 0 {
//             iters.push(OpInputEnum::Vec(self));
//         }
//         iters.reverse(); //the indice slices get pushed in from the back, but we want to return in order
//         (iters, len)
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for Vec<T> {
    //#[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let len = self.len();
        let num = if len < 1000 {
            1
        } else {
            match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => {
                    match std::env::var("LAMELLAR_THREADS") {
                        Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4),
                        Err(_) => 4, //+ 1 to account for main thread
                    }
                }
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

// impl<'a, T: Dist, I: Iterator<Item=T>> OpInput<'a, T> for I {
//     //#[tracing::instrument(skip_all)]
//     fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
//         self.collect::<Vec<T>>().as_op_input()
//     }
// }

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
        let len = self.len();
        let mut iters = vec![];
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                    Ok(n) => n.parse::<usize>().unwrap(),
                    Err(_) => {
                        match std::env::var("LAMELLAR_THREADS") {
                            Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4), //+ 1 to account for main thread
                            Err(_) => 4, //+ 1 to account for main thread
                        }
                    }
                }
            };
            let num_per_batch = len / num;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = self
                    .clone()
                    .into_sub_data(i * num_per_batch, (i + 1) * num_per_batch);
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
        let len = self.len();
        let mut iters = vec![];
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                    Ok(n) => n.parse::<usize>().unwrap(),
                    Err(_) => {
                        match std::env::var("LAMELLAR_THREADS") {
                            Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4), //+ 1 to account for main thread
                            Err(_) => 4, //+ 1 to account for main thread
                        }
                    }
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
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                    Ok(n) => n.parse::<usize>().unwrap(),
                    Err(_) => {
                        match std::env::var("LAMELLAR_THREADS") {
                            Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4),
                            Err(_) => 4, //+ 1 to account for main thread
                        }
                    }
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
        let my_pe = self.array.my_pe();
        if let Some(_start_index) = self.array.array.inner.start_index_for_pe(my_pe) {
            let num = if len < 1000 {
                1
            } else {
                match std::env::var("LAMELLAR_BATCH_OP_THREADS") {
                    Ok(n) => n.parse::<usize>().unwrap(),
                    Err(_) => {
                        match std::env::var("LAMELLAR_THREADS") {
                            Ok(n) => std::cmp::max(1, (n.parse::<usize>().unwrap()) / 4),
                            Err(_) => 4, //+ 1 to account for main thread
                        }
                    }
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

#[doc(hidden)]
pub trait BufferOp: Sync + Send {
    fn add_ops(
        &self,
        op: *const u8,
        op_data: *const u8,
        team: Pin<Arc<LamellarTeamRT>>,
    ) -> (bool, Arc<AtomicBool>);
    fn add_fetch_ops(
        &self,
        pe: usize,
        op: *const u8,
        op_data: *const u8,
        req_ids: &Vec<usize>,
        res_map: OpResults,
        team: Pin<Arc<LamellarTeamRT>>,
    ) -> (bool, Arc<AtomicBool>, Option<OpResultOffsets>);

    fn into_arc_am(
        &self,
        pe: usize,
        sub_array: std::ops::Range<usize>,
    ) -> (
        Vec<LamellarArcAm>,
        usize,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<u8>>>,
    );
}

#[doc(hidden)]
pub type OpResultOffsets = Vec<(usize, usize, usize)>; //reqid,offset,len

#[doc(hidden)]
pub struct OpReqOffsets(Arc<Mutex<HashMap<usize, OpResultOffsets>>>); //pe
impl OpReqOffsets {
    //#[tracing::instrument(skip_all)]
    // pub(crate) fn new() -> Self {
    //     OpReqOffsets(Arc::new(Mutex::new(HashMap::new())))
    // }
    //#[tracing::instrument(skip_all)]
    pub fn insert(&self, index: usize, indices: OpResultOffsets) {
        let mut map = self.0.lock();
        map.insert(index, indices);
    }
    //#[tracing::instrument(skip_all)]
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<HashMap<usize, OpResultOffsets>> {
        self.0.lock()
    }
}

impl Clone for OpReqOffsets {
    fn clone(&self) -> Self {
        OpReqOffsets(self.0.clone())
    }
}

impl std::fmt::Debug for OpReqOffsets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let map = self.0.lock();
        write!(f, "{:?} {:?}", map.len(), map)
    }
}

#[doc(hidden)]
pub type PeOpResults = Arc<Mutex<Vec<u8>>>;

#[doc(hidden)]
pub struct OpResults(Arc<Mutex<HashMap<usize, PeOpResults>>>);
impl OpResults {
    //#[tracing::instrument(skip_all)]
    // pub(crate) fn new() -> Self {
    //     OpResults(Arc::new(Mutex::new(HashMap::new())))
    // }
    //#[tracing::instrument(skip_all)]
    pub fn insert(&self, index: usize, val: PeOpResults) {
        let mut map = self.0.lock();
        map.insert(index, val);
    }
    //#[tracing::instrument(skip_all)]
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<HashMap<usize, PeOpResults>> {
        self.0.lock()
    }
}

impl Clone for OpResults {
    fn clone(&self) -> Self {
        OpResults(self.0.clone())
    }
}

impl std::fmt::Debug for OpResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let map = self.0.lock();
        write!(f, "{:?} {:?}", map.len(), map)
    }
}

pub(crate) struct ArrayOpHandle {
    pub(crate) reqs: Vec<Box<ArrayOpHandleInner>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpHandleInner {
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) scheduler: Arc<Scheduler>,
}

pub(crate) struct ArrayOpFetchHandle<T: Dist> {
    pub(crate) req: Box<ArrayOpFetchHandleInner<T>>,
}

pub(crate) struct ArrayOpBatchFetchHandle<T: Dist> {
    pub(crate) reqs: Vec<Box<ArrayOpFetchHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpFetchHandleInner<T: Dist> {
    pub(crate) indices: OpReqOffsets,
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) results: OpResults,
    pub(crate) req_cnt: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) _phantom: PhantomData<T>,
}

pub(crate) struct ArrayOpResultHandle<T: Dist> {
    pub(crate) req: Box<ArrayOpResultHandleInner<T>>,
}
pub(crate) struct ArrayOpBatchResultHandle<T: Dist> {
    pub(crate) reqs: Vec<Box<ArrayOpResultHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpResultHandleInner<T> {
    pub(crate) indices: OpReqOffsets,
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) results: OpResults,
    pub(crate) req_cnt: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) _phantom: PhantomData<T>,
}

#[async_trait]
impl LamellarRequest for ArrayOpHandle {
    type Output = ();
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.into_future().await;
        }
        ()
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for req in &self.reqs {
            req.get();
        }
        ()
    }
}

#[async_trait]
impl LamellarRequest for ArrayOpHandleInner {
    type Output = ();
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        ()
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                // std::thread::yield_now();
                self.scheduler.exec_task();
            }
        }
        ()
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpFetchHandle<T> {
    type Output = T;
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req
            .into_future()
            .await
            .pop()
            .expect("should have a single request")
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        self.req.get().pop().expect("should have a single request")
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpBatchFetchHandle<T> {
    type Output = Vec<T>;
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await);
        }
        res
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get());
        }
        // println!("res: {:?}",res);
        res
    }
}

impl<T: Dist> ArrayOpFetchHandleInner<T> {
    //#[tracing::instrument(skip_all)]
    fn get_result(&self) -> Vec<T> {
        if self.req_cnt > 0 {
            let mut res_vec = Vec::with_capacity(self.req_cnt);
            unsafe {
                res_vec.set_len(self.req_cnt);
            }
            // println!("req_cnt: {:?}", self.req_cnt);

            for (pe, res) in self.results.lock().iter() {
                let res = res.lock();
                for (rid, offset, len) in self.indices.lock().get(pe).unwrap().iter() {
                    let len = *len;
                    if len == std::mem::size_of::<T>() + 1 {
                        panic!(
                            "unexpected results len {:?} {:?}",
                            len,
                            std::mem::size_of::<T>() + 1
                        );
                    }
                    let res_t = unsafe {
                        std::slice::from_raw_parts(
                            res.as_ptr().offset(*offset as isize) as *const T,
                            len / std::mem::size_of::<T>(),
                        )
                    };
                    // println!("rid {:?} offset {:?} len {:?} {:?}",rid,offset,len,res.len());
                    // println!("res {:?} {:?}",res.len(),&res[offset..offset+len]);
                    // println!("res {:?} {:?}",res_t,res_t.len());
                    res_vec[*rid] = res_t[0];
                }
            }
            res_vec
        } else {
            vec![]
        }
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpFetchHandleInner<T> {
    type Output = Vec<T>;
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        self.get_result()
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                // std::thread::yield_now();
                self.scheduler.exec_task();
            }
        }
        self.get_result()
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpResultHandle<T> {
    type Output = Result<T, T>;
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req
            .into_future()
            .await
            .pop()
            .expect("should have a single request")
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        self.req.get().pop().expect("should have a single request")
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpBatchResultHandle<T> {
    type Output = Vec<Result<T, T>>;
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        // println!("num_reqs: {}",self.reqs.len());
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await);
        }
        res
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get());
        }
        res
    }
}

impl<T: Dist> ArrayOpResultHandleInner<T> {
    //#[tracing::instrument(skip_all)]
    fn get_result(&self) -> Vec<Result<T, T>> {
        // println!("req_cnt: {:?}", self.req_cnt);
        if self.req_cnt > 0 {
            let mut res_vec = Vec::with_capacity(self.req_cnt);
            unsafe {
                res_vec.set_len(self.req_cnt);
            }

            for (pe, res) in self.results.lock().iter() {
                let res = res.lock();
                // println!("{pe} {:?}",res.len());
                // let mut rids = std::collections::HashSet::new();
                let res_offsets_lock = self.indices.lock();
                let res_offsets = res_offsets_lock.get(pe).unwrap();
                // println!("{pe} {:?} {:?}",res_offsets[0],res_offsets.last());
                for (rid, offset, len) in res_offsets.iter() {
                    // if rids.contains(rid){
                    //     println!("uhhh ohhhhh not sure this should be possible {:?}",rid);
                    // }
                    // else{
                    //     rids.insert(rid);
                    // }
                    let ok: bool;
                    let mut offset = *offset;
                    let mut len = *len;
                    if len == std::mem::size_of::<T>() + 1 {
                        ok = res[offset] == 0;
                        offset += 1;
                        len -= 1;
                    } else {
                        panic!(
                            "unexpected results len {:?} {:?}",
                            len,
                            std::mem::size_of::<T>() + 1
                        );
                    };
                    let res_t = unsafe {
                        std::slice::from_raw_parts(
                            res.as_ptr().offset(offset as isize) as *const T,
                            len / std::mem::size_of::<T>(),
                        )
                    };

                    if ok {
                        res_vec[*rid] = Ok(res_t[0]);
                    } else {
                        res_vec[*rid] = Err(res_t[0]);
                    }
                }
            }
            res_vec
        } else {
            vec![]
        }
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpResultHandleInner<T> {
    type Output = Vec<Result<T, T>>;
    //#[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        // println!("comp size: {}",self.complete.len());
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        self.get_result()
    }
    //#[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                // std::thread::yield_now();
                self.scheduler.exec_task();
            }
        }
        self.get_result()
    }
}

/// Supertrait specifying that array elements must be [Sized] and must be able to be used in remote operations [Dist].
pub trait ElementOps: Dist + Sized {}
impl<T> ElementOps for T where T: Dist {}

#[doc(hidden)]
pub struct LocalOpResult<T: Dist> {
    val: T,
}

#[async_trait]
impl<T: Dist> LamellarArrayRequest for LocalOpResult<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.val
    }
    fn wait(self: Box<Self>) -> Self::Output {
        self.val
    }
}

impl<T: ElementArithmeticOps> ArithmeticOps<T> for LamellarWriteArray<T> {}
