mod handle;
pub(crate) use handle::NetworkAtomicArrayHandle;

pub(crate) mod iteration;
pub(crate) mod operations;
pub(crate) mod collective;
mod rdma;
use crate::array::atomic::AtomicElement;
use crate::array::native_atomic::NativeAtomicType;
use crate::array::private::ArrayExecAm;
use crate::array::r#unsafe::UnsafeAtomicOpSupport;
use crate::array::r#unsafe::__UnsafeByteArray;
use crate::array::r#unsafe::UnsafeCollectiveSupport;
use crate::array::*;
use crate::barrier::BarrierHandle;
use crate::darc::DarcMode;
use crate::lamellae::{AtomicOp, CommInfo};
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;
use crate::{Darc, Remote};

use serde::ser::SerializeSeq;
use std::any::TypeId;
use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};
use std::sync::atomic::{AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};

// macro_rules! impl_atomic_ops{
//     { $A:ty, $B:ty , $C:ident} => {
//         // #[allow(dead_code)]
//         pub(crate) struct $C<'a>(pub(crate) &'a $B);
//         impl AddAssign<$A> for $C<'_>{
//             fn add_assign(&mut self, val: $A) {
//                self.0.fetch_add(val,Ordering::SeqCst);
//             }
//         }
//         impl SubAssign<$A> for $C<'_>{
//             fn sub_assign(&mut self, val: $A) {
//                self.0.fetch_sub(val,Ordering::SeqCst);
//             }
//         }
//         impl MulAssign<$A> for $C<'_>{
//             fn mul_assign(&mut self, val: $A) {
//                 let mut cur = self.0.load(Ordering::SeqCst);
//                 let mut new = cur*val;
//                 while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
//                     std::thread::yield_now();
//                     cur = self.0.load(Ordering::SeqCst);
//                     new = cur*val;
//                 }
//             }
//         }
//         impl DivAssign<$A> for $C<'_>{
//             fn div_assign(&mut self, val: $A) {
//                 let mut cur = self.0.load(Ordering::SeqCst);
//                 let mut new = cur/val;
//                 while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
//                     std::thread::yield_now();
//                     cur = self.0.load(Ordering::SeqCst);
//                     new = cur/val;
//                 }
//             }
//         }
//         impl RemAssign<$A> for $C<'_>{
//             fn rem_assign(&mut self, val: $A) {
//                 let mut cur = self.0.load(Ordering::SeqCst);
//                 let mut new = cur%val;
//                 while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
//                     std::thread::yield_now();
//                     cur = self.0.load(Ordering::SeqCst);
//                     new = cur%val;
//                 }
//             }
//         }
//         impl BitAndAssign<$A> for $C<'_>{
//             fn bitand_assign(&mut self, val: $A) {
//                 self.0.fetch_and(val,Ordering::SeqCst);
//             }
//         }
//         impl BitOrAssign<$A> for $C<'_>{
//             fn bitor_assign(&mut self, val: $A) {
//                 self.0.fetch_or(val,Ordering::SeqCst);
//             }
//         }
//         impl BitXorAssign<$A> for $C<'_>{
//             fn bitxor_assign(&mut self, val: $A) {
//                 self.0.fetch_xor(val,Ordering::SeqCst);
//             }
//         }
//         impl ShlAssign<$A> for $C<'_> {
//             fn shl_assign(&mut self, val: $A ) {
//                 let mut cur = self.0.load(Ordering::SeqCst);
//                 let mut new = cur<<val;
//                 while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
//                     std::thread::yield_now();
//                     cur = self.0.load(Ordering::SeqCst);
//                     new = cur>>val;
//                 }
//             }
//         }
//         impl ShrAssign<$A> for $C<'_> {
//             fn shr_assign(&mut self, val: $A )  {
//                 let mut cur = self.0.load(Ordering::SeqCst);
//                 let mut new = cur<<val;
//                 while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
//                     std::thread::yield_now();
//                     cur = self.0.load(Ordering::SeqCst);
//                     new = cur>>val;
//                 }
//             }
//         }
//     }
// }

// use std::sync::atomic::AtomicI8;
// impl_atomic_ops! {i8,AtomicI8,MyAtomicI8}
// use std::sync::atomic::AtomicI16;
// impl_atomic_ops! {i16,AtomicI16,MyAtomicI16}
// use std::sync::atomic::AtomicI32;
// impl_atomic_ops! {i32,AtomicI32,MyAtomicI32}
// use std::sync::atomic::AtomicI64;
// impl_atomic_ops! {i64,AtomicI64,MyAtomicI64}
// use std::sync::atomic::AtomicIsize;
// impl_atomic_ops! {isize,AtomicIsize,MyAtomicIsize}
// use std::sync::atomic::AtomicU8;
// impl_atomic_ops! {u8,AtomicU8,MyAtomicU8}
// use std::sync::atomic::AtomicU16;
// impl_atomic_ops! {u16,AtomicU16,MyAtomicU16}
// use std::sync::atomic::AtomicU32;
// impl_atomic_ops! {u32,AtomicU32,MyAtomicU32}
// use std::sync::atomic::AtomicU64;
// impl_atomic_ops! {u64,AtomicU64,MyAtomicU64}
// use std::sync::atomic::AtomicUsize;
// impl_atomic_ops! {usize,AtomicUsize,MyAtomicUsize}
// use std::sync::atomic::AtomicBool;
// impl_atomic_ops! {bool,AtomicBool,MyAtomicBool}

macro_rules! slice_as_atomic{
    { $A:ty, $B:ty, $slice:ident } => {
        {
            use std::mem::align_of;
            let [] = [(); align_of::<$B>() - align_of::<$A>()];
            // SAFETY:
            //  - the mutable reference guarantees unique ownership.
            //  - the alignment of `$int_type` and `Self` is the
            //    same, as promised by $cfg_align and verified above.
            let slice_ptr = $slice.as_mut_ptr() as *mut $B;
            std::slice::from_raw_parts_mut(slice_ptr,$slice.len())
        }
    }
}

macro_rules! as_type{
    {  $val:ident,$A:ty  } => {
        {
            *(&$val as *const T as *const $A)
        }
    }
}

macro_rules! compare_exchange_op{
    ($A:ty, $B:ty, $self:ident, $val:ident) => { //used for swap -- can't fail
        {
            let slice = $self.array.__local_as_mut_slice();
            let slice = slice_as_atomic!($A,$B,slice);
            let val = as_type!($val,$A);
            let mut cur = slice[$self.local_index].load(Ordering::SeqCst);
            while slice[$self.local_index].compare_exchange(cur,val,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                std::thread::yield_now();
                cur = slice[$self.local_index].load(Ordering::SeqCst);
            }
            cur
        }
    };
    ($A:ty, $B:ty, $self:ident, $old:ident, $val:ident) => { //used for compare_exchange -- can fail
        {
            let slice = $self.array.__local_as_mut_slice();
            let slice = slice_as_atomic!($A,$B,slice);
            let val = as_type!($val,$A);
            let old = as_type!($old,$A);
            // match slice[$self.local_index].compare_exchange(old,val,Ordering::SeqCst,Ordering::SeqCst){
            //     Ok(old) => old,
            //     Err(old) => old,
            // }
            slice[$self.local_index].compare_exchange(old,val,Ordering::SeqCst,Ordering::SeqCst)
        }
    };
    ($A:ty, $B:ty, $self:ident, $old:ident, $val:ident, $eps:ident ) => { //used for compare_exchange epsilon
        {
            let slice = $self.array.__local_as_mut_slice();
            let slice = slice_as_atomic!($A,$B,slice);
            let val = as_type!($val,$A);
            let old = as_type!($old,$A);
            let eps = as_type!($eps,$A);
            let mut cur = slice[$self.local_index].load(Ordering::SeqCst);
            let mut done = false;
            while cur.abs_diff(old) as $A < eps  && !done{
                cur = match slice[$self.local_index].compare_exchange(old,val,Ordering::SeqCst,Ordering::SeqCst){
                    Ok(cur) => {
                        done = true;
                        cur
                    },
                    Err(cur) => {
                        std::thread::yield_now();
                        cur
                    }
                }
            }
            if done{
                Ok(cur)
            }
            else {
                Err(cur)
            }
        }
    };
    // ($A:ty, $B:ty, $C:ty, $self:ident, $val:ident, $op:tt ) => { //used for shift --can't fail
    //     {
    //         let slice = $self.array.__local_as_mut_slice();
    //         let slice = slice_as_atomic!($A,$B,slice);
    //         let val = $val;
    //         let mut cur = slice[$self.local_index].load(Ordering::SeqCst);
    //         let mut new = cur $op val;
    //         while slice[$self.local_index].compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
    //             std::thread::yield_now();
    //             cur = slice[$self.local_index].load(Ordering::SeqCst);
    //             new = cur $op val;
    //         }
    //         (cur,new)
    //     }
    // };
    ($A:ty, $B:ty, $self:ident, $val:ident, $op:tt ) => { //used for everything else --can't fail
        {
            let slice = $self.array.__local_as_mut_slice();
            let slice = slice_as_atomic!($A,$B,slice);
            let val = as_type!($val,$A);
            let mut cur = slice[$self.local_index].load(Ordering::SeqCst);
            let mut new = cur $op val;
            while slice[$self.local_index].compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                std::thread::yield_now();
                cur = slice[$self.local_index].load(Ordering::SeqCst);
                new = cur $op val;
            }
            cur
        }
    };
}

macro_rules! impl_shift {
    ($self:ident,$op:tt,$val:ident) => {
        // mul, div
        unsafe {
            match $self.array.orig_t {
                //deref to the original type
                NetworkAtomicType::I8 => {
                    *(&compare_exchange_op!(i8, AtomicI8, $self, $val, $op) as *const i8 as *mut T)
                }
                NetworkAtomicType::I16 => {
                    *(&compare_exchange_op!(i16, AtomicI16, $self, $val, $op) as *const i16
                        as *mut T)
                }
                NetworkAtomicType::I32 => {
                    *(&compare_exchange_op!(i32, AtomicI32, $self, $val, $op) as *const i32
                        as *mut T)
                }
                NetworkAtomicType::I64 => {
                    *(&compare_exchange_op!(i64, AtomicI64, $self, $val, $op) as *const i64
                        as *mut T)
                }
                NetworkAtomicType::Isize => {
                    *(&compare_exchange_op!(isize, AtomicIsize, $self, $val, $op) as *const isize
                        as *mut T)
                }
                NetworkAtomicType::U8 => {
                    *(&compare_exchange_op!(u8, AtomicU8, $self, $val, $op) as *const u8 as *mut T)
                }
                NetworkAtomicType::U16 => {
                    *(&compare_exchange_op!(u16, AtomicU16, $self, $val, $op) as *const u16
                        as *mut T)
                }
                NetworkAtomicType::U32 => {
                    *(&compare_exchange_op!(u32, AtomicU32, $self, $val, $op) as *const u32
                        as *mut T)
                }
                NetworkAtomicType::U64 => {
                    *(&compare_exchange_op!(u64, AtomicU64, $self, $val, $op) as *const u64
                        as *mut T)
                }
                NetworkAtomicType::Usize => {
                    *(&compare_exchange_op!(usize, AtomicUsize, $self, $val, $op) as *const usize
                        as *mut T)
                }
            }
        }
    };
}
macro_rules! impl_mul_div {
    ($self:ident,$op:tt,$val:ident) => {
        // mul, div
        unsafe {
            match $self.array.orig_t {
                //deref to the original type
                NetworkAtomicType::I8 => {
                    *(&compare_exchange_op!(i8, AtomicI8, $self, $val, $op) as *const i8 as *mut T)
                }
                NetworkAtomicType::I16 => {
                    *(&compare_exchange_op!(i16, AtomicI16, $self, $val, $op) as *const i16
                        as *mut T)
                }
                NetworkAtomicType::I32 => {
                    *(&compare_exchange_op!(i32, AtomicI32, $self, $val, $op) as *const i32
                        as *mut T)
                }
                NetworkAtomicType::I64 => {
                    *(&compare_exchange_op!(i64, AtomicI64, $self, $val, $op) as *const i64
                        as *mut T)
                }
                NetworkAtomicType::Isize => {
                    *(&compare_exchange_op!(isize, AtomicIsize, $self, $val, $op) as *const isize
                        as *mut T)
                }
                NetworkAtomicType::U8 => {
                    *(&compare_exchange_op!(u8, AtomicU8, $self, $val, $op) as *const u8 as *mut T)
                }
                NetworkAtomicType::U16 => {
                    *(&compare_exchange_op!(u16, AtomicU16, $self, $val, $op) as *const u16
                        as *mut T)
                }
                NetworkAtomicType::U32 => {
                    *(&compare_exchange_op!(u32, AtomicU32, $self, $val, $op) as *const u32
                        as *mut T)
                }
                NetworkAtomicType::U64 => {
                    *(&compare_exchange_op!(u64, AtomicU64, $self, $val, $op) as *const u64
                        as *mut T)
                }
                NetworkAtomicType::Usize => {
                    *(&compare_exchange_op!(usize, AtomicUsize, $self, $val, $op) as *const usize
                        as *mut T)
                }
            }
        }
    };
}
macro_rules! impl_add_sub_and_or_xor {
    ($self:ident,$op:ident,$val:ident) => {
        //add,sub,and,or (returns value)
        unsafe {
            let slice = $self.array.__local_as_mut_slice();
            match $self.array.orig_t {
                //deref to the original type
                NetworkAtomicType::I8 => {
                    *(&slice_as_atomic!(i8, AtomicI8, slice)[$self.local_index]
                        .$op(as_type!($val, i8), Ordering::SeqCst) as *const i8
                        as *mut T)
                }
                NetworkAtomicType::I16 => {
                    *(&slice_as_atomic!(i16, AtomicI16, slice)[$self.local_index]
                        .$op(as_type!($val, i16), Ordering::SeqCst) as *const i16
                        as *mut T)
                }
                NetworkAtomicType::I32 => {
                    *(&slice_as_atomic!(i32, AtomicI32, slice)[$self.local_index]
                        .$op(as_type!($val, i32), Ordering::SeqCst) as *const i32
                        as *mut T)
                }
                NetworkAtomicType::I64 => {
                    *(&slice_as_atomic!(i64, AtomicI64, slice)[$self.local_index]
                        .$op(as_type!($val, i64), Ordering::SeqCst) as *const i64
                        as *mut T)
                }
                NetworkAtomicType::Isize => {
                    *(&slice_as_atomic!(isize, AtomicIsize, slice)[$self.local_index]
                        .$op(as_type!($val, isize), Ordering::SeqCst)
                        as *const isize as *mut T)
                }
                NetworkAtomicType::U8 => {
                    *(&slice_as_atomic!(u8, AtomicU8, slice)[$self.local_index]
                        .$op(as_type!($val, u8), Ordering::SeqCst) as *const u8
                        as *mut T)
                }
                NetworkAtomicType::U16 => {
                    *(&slice_as_atomic!(u16, AtomicU16, slice)[$self.local_index]
                        .$op(as_type!($val, u16), Ordering::SeqCst) as *const u16
                        as *mut T)
                }
                NetworkAtomicType::U32 => {
                    *(&slice_as_atomic!(u32, AtomicU32, slice)[$self.local_index]
                        .$op(as_type!($val, u32), Ordering::SeqCst) as *const u32
                        as *mut T)
                }
                NetworkAtomicType::U64 => {
                    *(&slice_as_atomic!(u64, AtomicU64, slice)[$self.local_index]
                        .$op(as_type!($val, u64), Ordering::SeqCst) as *const u64
                        as *mut T)
                }
                NetworkAtomicType::Usize => {
                    *(&slice_as_atomic!(usize, AtomicUsize, slice)[$self.local_index]
                        .$op(as_type!($val, usize), Ordering::SeqCst)
                        as *const usize as *mut T)
                }
            }
        }
    };
}
macro_rules! impl_store {
    ($self:ident,$val:ident) => {
        //store
        unsafe {
            let slice = $self.array.__local_as_mut_slice();
            match $self.array.orig_t {
                NetworkAtomicType::I8 => {
                    slice_as_atomic!(i8, AtomicI8, slice)[$self.local_index]
                        .store(as_type!($val, i8), Ordering::SeqCst);
                }
                NetworkAtomicType::I16 => {
                    slice_as_atomic!(i16, AtomicI16, slice)[$self.local_index]
                        .store(as_type!($val, i16), Ordering::SeqCst);
                }
                NetworkAtomicType::I32 => {
                    slice_as_atomic!(i32, AtomicI32, slice)[$self.local_index]
                        .store(as_type!($val, i32), Ordering::SeqCst);
                }
                NetworkAtomicType::I64 => {
                    slice_as_atomic!(i64, AtomicI64, slice)[$self.local_index]
                        .store(as_type!($val, i64), Ordering::SeqCst);
                }
                NetworkAtomicType::Isize => {
                    slice_as_atomic!(isize, AtomicIsize, slice)[$self.local_index]
                        .store(as_type!($val, isize), Ordering::SeqCst);
                }
                NetworkAtomicType::U8 => {
                    slice_as_atomic!(u8, AtomicU8, slice)[$self.local_index]
                        .store(as_type!($val, u8), Ordering::SeqCst);
                }
                NetworkAtomicType::U16 => {
                    slice_as_atomic!(u16, AtomicU16, slice)[$self.local_index]
                        .store(as_type!($val, u16), Ordering::SeqCst);
                }
                NetworkAtomicType::U32 => {
                    slice_as_atomic!(u32, AtomicU32, slice)[$self.local_index]
                        .store(as_type!($val, u32), Ordering::SeqCst);
                }
                NetworkAtomicType::U64 => {
                    slice_as_atomic!(u64, AtomicU64, slice)[$self.local_index]
                        .store(as_type!($val, u64), Ordering::SeqCst);
                }
                NetworkAtomicType::Usize => {
                    slice_as_atomic!(usize, AtomicUsize, slice)[$self.local_index]
                        .store(as_type!($val, usize), Ordering::SeqCst);
                }
            }
        }
    };
}

macro_rules! impl_load {
    ($self:ident) => {
        //load
        unsafe {
            let slice = $self.array.__local_as_mut_slice();
            match $self.array.orig_t {
                NetworkAtomicType::I8 => {
                    *(&(slice_as_atomic!(i8, AtomicI8, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const i8 as *const T)
                }
                NetworkAtomicType::I16 => {
                    *(&(slice_as_atomic!(i16, AtomicI16, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const i16 as *const T)
                }
                NetworkAtomicType::I32 => {
                    *(&(slice_as_atomic!(i32, AtomicI32, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const i32 as *const T)
                }
                NetworkAtomicType::I64 => {
                    *(&(slice_as_atomic!(i64, AtomicI64, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const i64 as *const T)
                }
                NetworkAtomicType::Isize => {
                    *(&(slice_as_atomic!(isize, AtomicIsize, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const isize as *const T)
                }
                NetworkAtomicType::U8 => {
                    *(&(slice_as_atomic!(u8, AtomicU8, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const u8 as *const T)
                }
                NetworkAtomicType::U16 => {
                    *(&(slice_as_atomic!(u16, AtomicU16, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const u16 as *const T)
                }
                NetworkAtomicType::U32 => {
                    *(&(slice_as_atomic!(u32, AtomicU32, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const u32 as *const T)
                }
                NetworkAtomicType::U64 => {
                    *(&(slice_as_atomic!(u64, AtomicU64, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const u64 as *const T)
                }
                NetworkAtomicType::Usize => {
                    *(&(slice_as_atomic!(usize, AtomicUsize, slice)[$self.local_index]
                        .load(Ordering::SeqCst)) as *const usize as *const T)
                }
            }
        }
    };
}

macro_rules! impl_swap {
    ($self:ident,$val:ident) => {
        //swap

        unsafe {
            match $self.array.orig_t {
                //deref to the original type
                NetworkAtomicType::I8 => {
                    *(&compare_exchange_op!(i8, AtomicI8, $self, $val) as *const i8 as *mut T)
                }
                NetworkAtomicType::I16 => {
                    *(&compare_exchange_op!(i16, AtomicI16, $self, $val) as *const i16 as *mut T)
                }
                NetworkAtomicType::I32 => {
                    *(&compare_exchange_op!(i32, AtomicI32, $self, $val) as *const i32 as *mut T)
                }
                NetworkAtomicType::I64 => {
                    *(&compare_exchange_op!(i64, AtomicI64, $self, $val) as *const i64 as *mut T)
                }
                NetworkAtomicType::Isize => {
                    *(&compare_exchange_op!(isize, AtomicIsize, $self, $val) as *const isize
                        as *mut T)
                }
                NetworkAtomicType::U8 => {
                    *(&compare_exchange_op!(u8, AtomicU8, $self, $val) as *const u8 as *mut T)
                }
                NetworkAtomicType::U16 => {
                    *(&compare_exchange_op!(u16, AtomicU16, $self, $val) as *const u16 as *mut T)
                }
                NetworkAtomicType::U32 => {
                    *(&compare_exchange_op!(u32, AtomicU32, $self, $val) as *const u32 as *mut T)
                }
                NetworkAtomicType::U64 => {
                    *(&compare_exchange_op!(u64, AtomicU64, $self, $val) as *const u64 as *mut T)
                }
                NetworkAtomicType::Usize => {
                    *(&compare_exchange_op!(usize, AtomicUsize, $self, $val) as *const usize
                        as *mut T)
                }
            }
        }
    };
}

macro_rules! impl_compare_exchange {
    ($self:ident,$old:ident,$val:ident) => {
        unsafe {
            match $self.array.orig_t {
                //deref to the original type
                NetworkAtomicType::I8 => {
                    *(&compare_exchange_op!(i8, AtomicI8, $self, $old, $val)
                        as *const Result<i8, i8> as *mut Result<T, T>)
                }
                NetworkAtomicType::I16 => {
                    *(&compare_exchange_op!(i16, AtomicI16, $self, $old, $val)
                        as *const Result<i16, i16> as *mut Result<T, T>)
                }
                NetworkAtomicType::I32 => {
                    *(&compare_exchange_op!(i32, AtomicI32, $self, $old, $val)
                        as *const Result<i32, i32> as *mut Result<T, T>)
                }
                NetworkAtomicType::I64 => {
                    *(&compare_exchange_op!(i64, AtomicI64, $self, $old, $val)
                        as *const Result<i64, i64> as *mut Result<T, T>)
                }
                NetworkAtomicType::Isize => {
                    *(&compare_exchange_op!(isize, AtomicIsize, $self, $old, $val)
                        as *const Result<isize, isize> as *mut Result<T, T>)
                }
                NetworkAtomicType::U8 => {
                    *(&compare_exchange_op!(u8, AtomicU8, $self, $old, $val)
                        as *const Result<u8, u8> as *mut Result<T, T>)
                }
                NetworkAtomicType::U16 => {
                    *(&compare_exchange_op!(u16, AtomicU16, $self, $old, $val)
                        as *const Result<u16, u16> as *mut Result<T, T>)
                }
                NetworkAtomicType::U32 => {
                    *(&compare_exchange_op!(u32, AtomicU32, $self, $old, $val)
                        as *const Result<u32, u32> as *mut Result<T, T>)
                }
                NetworkAtomicType::U64 => {
                    *(&compare_exchange_op!(u64, AtomicU64, $self, $old, $val)
                        as *const Result<u64, u64> as *mut Result<T, T>)
                }
                NetworkAtomicType::Usize => {
                    *(&compare_exchange_op!(usize, AtomicUsize, $self, $old, $val)
                        as *const Result<usize, usize> as *mut Result<T, T>)
                }
            }
        }
    };
}

macro_rules! impl_compare_exchange_eps {
    ($self:ident,$old:ident,$val:ident,$eps:ident) => {
        unsafe {
            match $self.array.orig_t {
                //deref to the original type
                NetworkAtomicType::I8 => {
                    *(&compare_exchange_op!(i8, AtomicI8, $self, $old, $val, $eps)
                        as *const Result<i8, i8> as *mut Result<T, T>)
                }
                NetworkAtomicType::I16 => {
                    *(&compare_exchange_op!(i16, AtomicI16, $self, $old, $val, $eps)
                        as *const Result<i16, i16> as *mut Result<T, T>)
                }
                NetworkAtomicType::I32 => {
                    *(&compare_exchange_op!(i32, AtomicI32, $self, $old, $val, $eps)
                        as *const Result<i32, i32> as *mut Result<T, T>)
                }
                NetworkAtomicType::I64 => {
                    *(&compare_exchange_op!(i64, AtomicI64, $self, $old, $val, $eps)
                        as *const Result<i64, i64> as *mut Result<T, T>)
                }
                NetworkAtomicType::Isize => {
                    *(&compare_exchange_op!(isize, AtomicIsize, $self, $old, $val, $eps)
                        as *const Result<isize, isize> as *mut Result<T, T>)
                }
                NetworkAtomicType::U8 => {
                    *(&compare_exchange_op!(u8, AtomicU8, $self, $old, $val, $eps)
                        as *const Result<u8, u8> as *mut Result<T, T>)
                }
                NetworkAtomicType::U16 => {
                    *(&compare_exchange_op!(u16, AtomicU16, $self, $old, $val, $eps)
                        as *const Result<u16, u16> as *mut Result<T, T>)
                }
                NetworkAtomicType::U32 => {
                    *(&compare_exchange_op!(u32, AtomicU32, $self, $old, $val, $eps)
                        as *const Result<u32, u32> as *mut Result<T, T>)
                }
                NetworkAtomicType::U64 => {
                    *(&compare_exchange_op!(u64, AtomicU64, $self, $old, $val, $eps)
                        as *const Result<u64, u64> as *mut Result<T, T>)
                }
                NetworkAtomicType::Usize => {
                    *(&compare_exchange_op!(usize, AtomicUsize, $self, $old, $val, $eps)
                        as *const Result<usize, usize> as *mut Result<T, T>)
                }
            }
        }
    };
}

#[doc(hidden)]
pub struct NetworkAtomicElement<T: Remote> {
    array: NetworkAtomicArray<T>,
    local_index: usize,
}

impl<T: Dist> From<NetworkAtomicElement<T>> for AtomicElement<T> {
    fn from(element: NetworkAtomicElement<T>) -> AtomicElement<T> {
        AtomicElement::NetworkAtomicElement(element)
    }
}

impl<T: Dist> NetworkAtomicElement<T> {
    /// Returns the value of the atomic element.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let val = elem.load();
    /// }
    ///```
    pub fn load(&self) -> T {
        impl_load!(self)
    }
    /// Stores a value into the atomic element.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     elem.store(42);
    /// }
    ///```
    pub fn store(&self, val: T) {
        impl_store!(self, val);
    }
    /// Atomically replaces the current value with `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.swap(42);
    /// }
    ///```
    pub fn swap(&self, val: T) -> T {
        impl_swap!(self, val)
    }
    /// Performs an atomic compare and exchange operation.
    /// If the current value equals `old`, replaces it with `new` and returns `Ok(old)`.
    /// Otherwise returns `Err(current)`.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let result = elem.compare_exchange(0, 42);
    /// }
    ///```
    pub fn compare_exchange(&self, old: T, new: T) -> Result<T, T> {
        impl_compare_exchange!(self, old, new)
    }
    /// Performs an atomic compare and exchange operation with an epsilon tolerance.
    /// Succeeds if the absolute difference between the current value and `old` is less than `eps`.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let result = elem.compare_exchange_epsilon(0, 42, 1);
    /// }
    ///```
    pub fn compare_exchange_epsilon(&self, old: T, new: T, eps: T) -> Result<T, T> {
        impl_compare_exchange_eps!(self, old, new, eps)
    }
    /// Atomically adds `val` to the current value and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_add(5);
    /// }
    ///```
    pub fn fetch_add(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_add, val)
    }
    /// Atomically subtracts `val` from the current value and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_sub(5);
    /// }
    ///```
    pub fn fetch_sub(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_sub, val)
    }
    /// Atomically multiplies the current value by `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_mul(2);
    /// }
    ///```
    pub fn fetch_mul(&self, val: T) -> T {
        impl_mul_div!(self, * , val)
    }
    /// Atomically divides the current value by `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_div(2);
    /// }
    ///```
    pub fn fetch_div(&self, val: T) -> T {
        impl_mul_div!(self, /, val)
    }
    /// Atomically computes the remainder of the current value divided by `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_rem(3);
    /// }
    ///```
    pub fn fetch_rem(&self, val: T) -> T {
        impl_mul_div!(self, %, val)
    }
    /// Atomically left-shifts the current value by `val` bits and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_shl(2);
    /// }
    ///```
    pub fn fetch_shl(&self, val: T) -> T {
        //result.0 is old value, result.1 is new value
        impl_shift!(self, <<, val)
    }
    /// Atomically right-shifts the current value by `val` bits and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_shr(2);
    /// }
    ///```
    pub fn fetch_shr(&self, val: T) -> T {
        //result.0 is old value, result.1 is new value
        impl_shift!(self, >>, val)
    }
}

impl<T: ElementBitWiseOps + 'static> NetworkAtomicElement<T> {
    /// Atomically performs a bitwise AND of the current value with `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_and(0xFF);
    /// }
    ///```
    pub fn fetch_and(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_and, val)
    }
    /// Atomically performs a bitwise OR of the current value with `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_or(0x01);
    /// }
    ///```
    pub fn fetch_or(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_or, val)
    }
    /// Atomically performs a bitwise XOR of the current value with `val` and returns the old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// if let AtomicArray::NetworkAtomicArray(ref array) = array {
    ///     let local_data = array.local_data();
    ///     let elem = local_data.at(0);
    ///     let old = elem.fetch_xor(0xFF);
    /// }
    ///```
    pub fn fetch_xor(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_xor, val)
    }
}

impl<T: Dist + ElementArithmeticOps> AddAssign<T> for NetworkAtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        self.fetch_add(val);
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for NetworkAtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        self.fetch_sub(val);
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for NetworkAtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        self.fetch_mul(val);
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for NetworkAtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        self.fetch_div(val);
    }
}

impl<T: Dist + ElementArithmeticOps> RemAssign<T> for NetworkAtomicElement<T> {
    fn rem_assign(&mut self, val: T) {
        self.fetch_rem(val);
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for NetworkAtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        self.fetch_and(val);
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for NetworkAtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        self.fetch_or(val);
    }
}

impl<T: Dist + ElementBitWiseOps> BitXorAssign<T> for NetworkAtomicElement<T> {
    fn bitxor_assign(&mut self, val: T) {
        self.fetch_xor(val);
    }
}

impl<T: Dist + ElementShiftOps> ShlAssign<T> for NetworkAtomicElement<T> {
    fn shl_assign(&mut self, val: T) {
        self.fetch_shl(val);
    }
}

impl<T: Dist + ElementShiftOps> ShrAssign<T> for NetworkAtomicElement<T> {
    fn shr_assign(&mut self, val: T) {
        self.fetch_shr(val);
    }
}

impl<T: Dist + std::fmt::Debug> std::fmt::Debug for NetworkAtomicElement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.load())
    }
}

/// Borrowed reference to a single element of a [NetworkAtomicArray].
/// Zero Darc clone/drop cost — holds a plain `&'a NetworkAtomicArray<T>`.
#[doc(hidden)]
pub struct NetworkAtomicElementRef<'a, T: Remote> {
    pub(crate) array: &'a NetworkAtomicArray<T>,
    pub(crate) local_index: usize,
}

impl<'a, T: Dist> NetworkAtomicElementRef<'a, T> {
    pub fn load(&self) -> T {
        impl_load!(self)
    }
    pub fn store(&self, val: T) {
        impl_store!(self, val);
    }
    pub fn swap(&self, val: T) -> T {
        impl_swap!(self, val)
    }
    pub fn compare_exchange(&self, old: T, new: T) -> Result<T, T> {
        impl_compare_exchange!(self, old, new)
    }
    pub fn compare_exchange_epsilon(&self, old: T, new: T, eps: T) -> Result<T, T> {
        impl_compare_exchange_eps!(self, old, new, eps)
    }
    pub fn fetch_add(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_add, val)
    }
    pub fn fetch_sub(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_sub, val)
    }
    pub fn fetch_mul(&self, val: T) -> T {
        impl_mul_div!(self, *, val)
    }
    pub fn fetch_div(&self, val: T) -> T {
        impl_mul_div!(self, /, val)
    }
    pub fn fetch_rem(&self, val: T) -> T {
        impl_mul_div!(self, %, val)
    }
    pub fn fetch_shl(&self, val: T) -> T {
        impl_shift!(self, <<, val)
    }
    pub fn fetch_shr(&self, val: T) -> T {
        impl_shift!(self, >>, val)
    }
}

impl<'a, T: ElementBitWiseOps + 'static> NetworkAtomicElementRef<'a, T> {
    pub fn fetch_and(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_and, val)
    }
    pub fn fetch_or(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_or, val)
    }
    pub fn fetch_xor(&self, val: T) -> T {
        impl_add_sub_and_or_xor!(self, fetch_xor, val)
    }
}

impl<'a, T: Dist + ElementArithmeticOps> AddAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn add_assign(&mut self, val: T) { self.fetch_add(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> SubAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn sub_assign(&mut self, val: T) { self.fetch_sub(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> MulAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn mul_assign(&mut self, val: T) { self.fetch_mul(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> DivAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn div_assign(&mut self, val: T) { self.fetch_div(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> RemAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn rem_assign(&mut self, val: T) { self.fetch_rem(val); }
}
impl<'a, T: Dist + ElementBitWiseOps> BitAndAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn bitand_assign(&mut self, val: T) { self.fetch_and(val); }
}
impl<'a, T: Dist + ElementBitWiseOps> BitOrAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn bitor_assign(&mut self, val: T) { self.fetch_or(val); }
}
impl<'a, T: Dist + ElementBitWiseOps> BitXorAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn bitxor_assign(&mut self, val: T) { self.fetch_xor(val); }
}
impl<'a, T: Dist + ElementShiftOps> ShlAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn shl_assign(&mut self, val: T) { self.fetch_shl(val); }
}
impl<'a, T: Dist + ElementShiftOps> ShrAssign<T> for NetworkAtomicElementRef<'a, T> {
    fn shr_assign(&mut self, val: T) { self.fetch_shr(val); }
}

impl<'a, T: Dist + std::fmt::Debug> std::fmt::Debug for NetworkAtomicElementRef<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.load())
    }
}

/// A variant of an [AtomicArray] providing atomic access for any integer type that has a corresponding Rust supported Atomic type (e.g. usize -> AtomicUsize)
///
/// Generally any operation on this array type will be performed via an internal runtime Active Message, i.e. direct RDMA operations are not allowed
///
/// You should not be directly interacting with this type, rather you should be operating on an [AtomicArray][crate::array::AtomicArray].
// #[derive(Debug)]
#[derive(crate::Deserialize, crate::Serialize, Clone, Debug)]
#[serde(bound = "T: Dist")]
pub struct NetworkAtomicArray<T: Remote> {
    pub(crate) array: UnsafeArray<T>,
    pub(crate) orig_t: NetworkAtomicType,
    pub(crate) op_support: UnsafeAtomicOpSupport,
    pub(crate) collective_support: UnsafeCollectiveSupport,
}

impl<T: Remote> crate::active_messaging::DarcSerde for NetworkAtomicArray<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        self.array.ser(num_pes, darcs);
    }
}

/// Internal runtime data struct used by the Lamellar runtime.
/// Not intended for direct use by library users.
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct __NetworkAtomicByteArray {
    pub(crate) array: __UnsafeByteArray,
    pub(crate) orig_t: NetworkAtomicType,
}
impl __NetworkAtomicByteArray {}

/// Internal runtime local-data wrapper for NetworkAtomic arrays.
/// Not intended for direct use by library users.
/// Users should interact with the public `AtomicLocalData` API instead;
/// see [AtomicLocalData][crate::array::atomic::AtomicLocalData].
#[derive(Clone, Debug)]
pub struct __NetworkAtomicLocalData<T: Remote> {
    // + NetworkAtomicOps> {
    pub(crate) array: NetworkAtomicArray<T>,
    start_index: usize,
    end_index: usize,
}

/// Internal iterator for `__NetworkAtomicLocalData`.
/// Holds a reference to the array — no Darc operations occur per element.
#[derive(Debug)]
pub struct __NetworkAtomicLocalDataIter<'a, T: Dist> {
    array: &'a NetworkAtomicArray<T>,
    index: usize,
    end_index: usize,
}

impl<T: Dist> __NetworkAtomicLocalData<T> {
    #[doc(hidden)]
    pub fn at(&self, index: usize) -> NetworkAtomicElementRef<'_, T> {
        NetworkAtomicElementRef {
            array: &self.array,
            local_index: index,
        }
    }

    #[doc(hidden)]
    pub fn get_mut(&self, index: usize) -> Option<NetworkAtomicElementRef<'_, T>> {
        Some(NetworkAtomicElementRef {
            array: &self.array,
            local_index: index,
        })
    }

    #[doc(hidden)]
    pub fn len(&self) -> usize {
        self.end_index - self.start_index
    }

    #[doc(hidden)]
    pub fn iter(&self) -> __NetworkAtomicLocalDataIter<'_, T> {
        __NetworkAtomicLocalDataIter {
            array: &self.array,
            index: self.start_index,
            end_index: self.end_index,
        }
    }

    #[doc(hidden)]
    pub fn sub_data(&self, start_index: usize, end_index: usize) -> __NetworkAtomicLocalData<T> {
        __NetworkAtomicLocalData {
            array: self.array.clone(),
            start_index: start_index,
            end_index: std::cmp::min(end_index, self.array.num_elems_local()),
        }
    }

    #[doc(hidden)]
    pub fn as_slice<A>(&self) -> Option<&[A]> {
        unsafe {
            let slice = self.array.__local_as_slice();
            match self.array.orig_t {
                NetworkAtomicType::U8 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<u8>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::U16 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<u16>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::U32 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<u32>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::U64 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<u64>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::Usize => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<usize>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::I8 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<i8>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::I16 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<i16>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::I32 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<i32>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::I64 => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<i64>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
                NetworkAtomicType::Isize => {
                    if std::mem::align_of::<A>() - std::mem::align_of::<isize>() != 0 {
                        return None;
                    }
                    let slice_ptr = slice.as_ptr() as *mut A;
                    Some(std::slice::from_raw_parts(slice_ptr, slice.len()))
                }
            }
        }
    }

    // pub fn load_iter(&self) -> NetworkAtomicLocalDataIter<T> {
    //     NetworkAtomicLocalDataIter {
    //         array: self.array.clone(),
    //         index: 0,
    //     }
    // }
}

impl<T: Dist + serde::Serialize> serde::Serialize for __NetworkAtomicLocalData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for i in 0..self.len() {
            seq.serialize_element(&self.at(i).load())?;
        }
        seq.end()
    }
}

impl<'a, T: Dist> IntoIterator for &'a __NetworkAtomicLocalData<T> {
    type Item = NetworkAtomicElementRef<'a, T>;
    type IntoIter = __NetworkAtomicLocalDataIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        __NetworkAtomicLocalDataIter {
            array: &self.array,
            index: self.start_index,
            end_index: self.end_index,
        }
    }
}

impl<'a, T: Dist> Iterator for __NetworkAtomicLocalDataIter<'a, T> {
    type Item = NetworkAtomicElementRef<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end_index {
            let index = self.index;
            self.index += 1;
            Some(NetworkAtomicElementRef {
                array: self.array,
                local_index: index,
            })
        } else {
            None
        }
    }
}

impl<T: Dist + ArrayOps + std::default::Default> NetworkAtomicArray<T> {
    // Send + Copy  == Dist
    pub(crate) fn new_internal<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> NetworkAtomicArrayHandle<T> {
        // println!("new Network atomic array 1");
        // let array = UnsafeArray::new(team.clone(), array_size, distribution);
        // array.block_on_outstanding(DarcMode::NetworkAtomicArray);

        // NetworkAtomicArray {
        //     array: array,
        //     orig_t: NetworkAtomicType::of::<T>(),
        // }
        let team = team.into().team.clone();
        NetworkAtomicArrayHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(UnsafeArray::async_new(
                team,
                array_size,
                distribution,
                DarcMode::NetworkAtomicArray,
            )),
        }
    }
}

#[doc(hidden)]
impl<T: Dist> NetworkAtomicArray<T> {
    pub(crate) fn detect_op_support(array: &UnsafeArray<T>) -> UnsafeAtomicOpSupport {
        let comm = array.inner.data.team.lamellae.comm();
        let dummy_val = array.dummy_val();
        UnsafeAtomicOpSupport {
            load: comm
                .atomic_op_avail::<T>(AtomicOp::Read(unsafe { Box::pin(std::mem::zeroed()) })),
            store: comm.atomic_op_avail::<T>(AtomicOp::Write(Box::pin(dummy_val))),
            swap: comm.atomic_op_avail::<T>(AtomicOp::Write(Box::pin(dummy_val))),
            cas: comm.atomic_op_avail::<T>(AtomicOp::Cas),
            add: comm.atomic_op_avail::<T>(AtomicOp::Sum(Box::pin(dummy_val))),
            fetch_add: comm.atomic_op_avail::<T>(AtomicOp::Sum(Box::pin(dummy_val))),
            prod: comm.atomic_op_avail::<T>(AtomicOp::Prod(Box::pin(dummy_val))),
            fetch_prod: comm.atomic_op_avail::<T>(AtomicOp::Prod(Box::pin(dummy_val))),
            bit_or: comm.atomic_op_avail::<T>(AtomicOp::BitOr(Box::pin(dummy_val))),
            fetch_bit_or: comm.atomic_op_avail::<T>(AtomicOp::BitOr(Box::pin(dummy_val))),
            bit_xor: comm.atomic_op_avail::<T>(AtomicOp::BitXor(Box::pin(dummy_val))),
            fetch_bit_xor: comm.atomic_op_avail::<T>(AtomicOp::BitXor(Box::pin(dummy_val))),
            bit_and: comm.atomic_op_avail::<T>(AtomicOp::BitAnd(Box::pin(dummy_val))),
            fetch_bit_and: comm.atomic_op_avail::<T>(AtomicOp::BitAnd(Box::pin(dummy_val))),
        }
    }

    pub(crate) fn detect_collective_support(array: &UnsafeArray<T>) -> UnsafeCollectiveSupport {
        let comm = array.inner.data.team.lamellae.comm();
        let dummy_val = array.dummy_val();
        UnsafeArray::detect_collective_support(comm, dummy_val)
    }

    pub fn network_type(&self) -> NetworkAtomicType {
        self.orig_t
    }
    pub(crate) fn get_element(&self, index: usize) -> Option<NetworkAtomicElement<T>> {
        if index < unsafe { self.__local_as_slice().len() } {
            //We are only directly accessing the local slice for its len
            Some(NetworkAtomicElement {
                array: self.clone(),
                local_index: index,
            })
        } else {
            None
        }
    }

    pub(crate) fn get_element_ref(&self, index: usize) -> Option<NetworkAtomicElementRef<'_, T>> {
        if index < unsafe { self.__local_as_slice().len() } {
            Some(NetworkAtomicElementRef {
                array: self,
                local_index: index,
            })
        } else {
            None
        }
    }
}

#[doc(hidden)]
impl<T: Dist> NetworkAtomicArray<T> {
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        NetworkAtomicArray {
            array: self.array.use_distribution(distribution),
            orig_t: self.orig_t,
            op_support: self.op_support,
            collective_support: self.collective_support,
        }
    }

    pub fn local_data(&self) -> __NetworkAtomicLocalData<T> {
        __NetworkAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    pub fn mut_local_data(&self) -> __NetworkAtomicLocalData<T> {
        __NetworkAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }

    pub unsafe fn __local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }

    pub fn into_unsafe(self) -> IntoUnsafeArrayHandle<T> {
        // println!("Network into_unsafe");
        // self.array.into()

        IntoUnsafeArrayHandle {
            team: self.array.inner.data.team.clone(),
            launched: false,
            outstanding_future: Box::pin(self.async_into()),
        }
    }

    pub fn into_read_only(self) -> IntoReadOnlyArrayHandle<T> {
        // println!("Network into_read_only");
        self.array.into_read_only()
    }
}

impl<T: Dist + ArrayOps + Default> AsyncTeamFrom<(Vec<T>, Distribution)> for NetworkAtomicArray<T> {
    async fn team_from(input: (Vec<T>, Distribution), team: &Arc<LamellarTeam>) -> Self {
        let array: UnsafeArray<T> = AsyncTeamInto::team_into(input, team).await;
        array.async_into().await
    }
}

//#[doc(hidden)]
#[async_trait]
impl<T: Dist> AsyncFrom<UnsafeArray<T>> for NetworkAtomicArray<T> {
    async fn async_from(array: UnsafeArray<T>) -> Self {
        // println!("Network from unsafe");
        array
            .await_on_outstanding(DarcMode::NetworkAtomicArray)
            .await;
        let op_support = Self::detect_op_support(&array);
        let collective_support = Self::detect_collective_support(&array);
        NetworkAtomicArray {
            array: array,
            orig_t: NetworkAtomicType::of::<T>(),
            op_support,
            collective_support,
        }
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicArray<T>> for __NetworkAtomicByteArray {
    fn from(array: NetworkAtomicArray<T>) -> Self {
        __NetworkAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicArray<T>> for LamellarByteArray {
    fn from(array: NetworkAtomicArray<T>) -> Self {
        LamellarByteArray::NetworkAtomicArray(__NetworkAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        })
    }
}

//#[doc(hidden)]
impl<T: Dist> From<LamellarByteArray> for NetworkAtomicArray<T> {
    fn from(array: LamellarByteArray) -> Self {
        if let LamellarByteArray::NetworkAtomicArray(array) = array {
            array.into()
        } else {
            panic!("Expected LamellarByteArray::NetworkAtomicArray")
        }
    }
}

impl From<__NetworkAtomicByteArray> for __NativeAtomicByteArray {
    fn from(array: __NetworkAtomicByteArray) -> Self {
        __NativeAtomicByteArray {
            array: array.array,
            orig_t: array.orig_t.into(),
        }
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicArray<T>> for __AtomicByteArray {
    fn from(array: NetworkAtomicArray<T>) -> Self {
        __AtomicByteArray::NetworkAtomicByteArray(__NetworkAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        })
    }
}

//#[doc(hidden)]
impl<T: Dist> From<__NetworkAtomicByteArray> for NetworkAtomicArray<T> {
    fn from(array: __NetworkAtomicByteArray) -> Self {
        let array: UnsafeArray<T> = array.array.into();
        NetworkAtomicArray {
            orig_t: NetworkAtomicType::of::<T>(),
            op_support: Self::detect_op_support(&array),
            collective_support: Self::detect_collective_support(&array),
            array,
        }
    }
}

impl<T: Dist> From<&__NetworkAtomicByteArray> for NetworkAtomicArray<T> {
    fn from(array: &__NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}
impl<T: Dist> From<&mut __NetworkAtomicByteArray> for NetworkAtomicArray<T> {
    fn from(array: &mut __NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}

//#[doc(hidden)]
impl<T: Dist> From<__NetworkAtomicByteArray> for AtomicArray<T> {
    fn from(array: __NetworkAtomicByteArray) -> Self {
        let array: UnsafeArray<T> = array.array.into();
        NetworkAtomicArray {
            orig_t: NetworkAtomicType::of::<T>(),
            op_support: NetworkAtomicArray::<T>::detect_op_support(&array),
            collective_support: NetworkAtomicArray::<T>::detect_collective_support(&array),
            array,
        }
        .into()
    }
}
impl<T: Dist> From<&__NetworkAtomicByteArray> for AtomicArray<T> {
    fn from(array: &__NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}
impl<T: Dist> From<&mut __NetworkAtomicByteArray> for AtomicArray<T> {
    fn from(array: &mut __NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}

// //#[doc(hidden)]
impl<T: Dist> private::ArrayExecAm<T> for NetworkAtomicArray<T> {
    fn team_rt(&self) -> Darc<LamellarTeamRT> {
        self.array.team_rt()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

//#[doc(hidden)]
impl<T: Dist> private::LamellarArrayPrivate<T> for NetworkAtomicArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        &self.array
    }
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }
    unsafe fn into_inner(self) -> UnsafeArray<T> {
        self.array
    }
    fn as_lamellar_byte_array(&self) -> LamellarByteArray {
        self.clone().into()
    }
}

impl<T: Dist> ActiveMessaging for NetworkAtomicArray<T> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_all_tg(am)
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_pe_tg(pe, am)
    }
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.array.exec_am_local_tg(am)
    }
    fn wait_all(&self) {
        self.array.wait_all()
    }
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        self.array.await_all()
    }
    fn barrier(&self) {
        self.array.barrier()
    }
    fn async_barrier(&self) -> BarrierHandle {
        self.array.async_barrier()
    }
    fn spawn<F: Future>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.array.spawn(f)
    }
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.array.block_on(f)
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        self.array.block_on_all(iter)
    }
}

//#[doc(hidden)]
impl<T: Dist> LamellarArray<T> for NetworkAtomicArray<T> {
    fn len(&self) -> usize {
        self.array.len()
    }
    fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.first_global_index_for_pe(pe)
    }

    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.last_global_index_for_pe(pe)
    }
}

impl<T: Dist> LamellarEnv for NetworkAtomicArray<T> {
    fn my_pe(&self) -> usize {
        LamellarEnv::my_pe(&self.array)
    }

    fn num_pes(&self) -> usize {
        LamellarEnv::num_pes(&self.array)
    }

    fn num_threads_per_pe(&self) -> usize {
        self.array.team_rt().num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().team()
    }
}

//#[doc(hidden)]
impl<T: Dist> LamellarWrite for NetworkAtomicArray<T> {}

//#[doc(hidden)]
impl<T: Dist> LamellarRead for NetworkAtomicArray<T> {}

//#[doc(hidden)]
impl<T: Dist> SubArray<T> for NetworkAtomicArray<T> {
    type Array = NetworkAtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        NetworkAtomicArray {
            array: self.array.sub_array(range),
            orig_t: self.orig_t,
            op_support: self.op_support,
            collective_support: self.collective_support,
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

//#[doc(hidden)]
impl<T: Dist + std::fmt::Debug> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn print(&self) {
        self.array.print();
    }
}

//#[doc(hidden)]
impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for NetworkAtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}

impl<T: Dist + AmDist + 'static> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn reduce(&self, op: &str) -> crate::array::ArrayReduceHandle<T> {
        self.array.reduce_data_user(op, self.clone().into())
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn sum(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::Sum))),
            None => self.array.reduce_data_user("sum", self.clone().into()),
        }
    }
    #[doc(hidden)]
    pub fn prod(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::Prod))),
            None => self.array.reduce_data_user("prod", self.clone().into()),
        }
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn max(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::Max))),
            None => self.array.reduce_data_user("max", self.clone().into()),
        }
    }
    #[doc(hidden)]
    pub fn min(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::Min))),
            None => self.array.reduce_data_user("min", self.clone().into()),
        }
    }
}
impl<T: Dist + AmDist + ElementBitWiseOps + 'static> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn and(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::And))),
            None => self.array.reduce_data_user("and", self.clone().into()),
        }
    }
    #[doc(hidden)]
    pub fn or(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::Or))),
            None => self.array.reduce_data_user("or", self.clone().into()),
        }
    }
    #[doc(hidden)]
    pub fn xor(&self) -> crate::array::ArrayReduceHandle<T> {
        match ScalarType::get_type::<T>() {
            Some((scalar_type,_)) => self.array.reduce_data(Arc::new(ScalarBuiltinReductionAm::new(self.clone().into(), scalar_type, BuiltinOp::Xor))),
            None => self.array.reduce_data_user("xor", self.clone().into()),
        }
    }
}

//for use within RDMA active messages to atomically read/write values
//#[doc(hidden)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
pub enum NetworkAtomicType {
    I8,
    I16,
    I32,
    I64,
    Isize,
    U8,
    U16,
    U32,
    U64,
    Usize,
}

//#[doc(hidden)]
impl NetworkAtomicType {
    pub(crate) fn of<T: 'static>() -> NetworkAtomicType {
        let t = TypeId::of::<T>();
        if t == TypeId::of::<i8>() {
            NetworkAtomicType::I8
        } else if t == TypeId::of::<i16>() {
            NetworkAtomicType::I16
        } else if t == TypeId::of::<i32>() {
            NetworkAtomicType::I32
        } else if t == TypeId::of::<i64>() {
            NetworkAtomicType::I64
        } else if t == TypeId::of::<isize>() {
            NetworkAtomicType::Isize
        } else if t == TypeId::of::<u8>() {
            NetworkAtomicType::U8
        } else if t == TypeId::of::<u16>() {
            NetworkAtomicType::U16
        } else if t == TypeId::of::<u32>() {
            NetworkAtomicType::U32
        } else if t == TypeId::of::<u64>() {
            NetworkAtomicType::U64
        } else if t == TypeId::of::<usize>() {
            NetworkAtomicType::Usize
        } else {
            panic!("invalid Network atomic type!")
        }
    }
    fn size(&self) -> usize {
        match self {
            NetworkAtomicType::I8 => std::mem::size_of::<i8>(),
            NetworkAtomicType::I16 => std::mem::size_of::<i16>(),
            NetworkAtomicType::I32 => std::mem::size_of::<i32>(),
            NetworkAtomicType::I64 => std::mem::size_of::<i64>(),
            NetworkAtomicType::Isize => std::mem::size_of::<isize>(),
            NetworkAtomicType::U8 => std::mem::size_of::<u8>(),
            NetworkAtomicType::U16 => std::mem::size_of::<u16>(),
            NetworkAtomicType::U32 => std::mem::size_of::<u32>(),
            NetworkAtomicType::U64 => std::mem::size_of::<u64>(),
            NetworkAtomicType::Usize => std::mem::size_of::<usize>(),
        }
    }
    fn load(&self, src_addr: *mut u8, dst_addr: *mut u8) {
        unsafe {
            match self {
                NetworkAtomicType::I8 => {
                    let dst = &mut *(dst_addr as *mut i8);
                    let src = &*(src_addr as *mut i8 as *mut AtomicI8);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::I16 => {
                    let dst = &mut *(dst_addr as *mut i16);
                    let src = &*(src_addr as *mut i16 as *mut AtomicI16);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::I32 => {
                    let dst = &mut *(dst_addr as *mut i32);
                    let src = &*(src_addr as *mut i32 as *mut AtomicI32);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::I64 => {
                    let dst = &mut *(dst_addr as *mut i64);
                    let src = &*(src_addr as *mut i64 as *mut AtomicI64);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::Isize => {
                    let dst = &mut *(dst_addr as *mut isize);
                    let src = &*(src_addr as *mut isize as *mut AtomicIsize);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::U8 => {
                    let dst = &mut *(dst_addr as *mut u8);
                    let src = &*(src_addr as *mut u8 as *mut AtomicU8);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::U16 => {
                    let dst = &mut *(dst_addr as *mut u16);
                    let src = &*(src_addr as *mut u16 as *mut AtomicU16);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::U32 => {
                    let dst = &mut *(dst_addr as *mut u32);
                    let src = &*(src_addr as *mut u32 as *mut AtomicU32);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::U64 => {
                    let dst = &mut *(dst_addr as *mut u64);
                    let src = &*(src_addr as *mut u64 as *mut AtomicU64);
                    *dst = src.load(Ordering::SeqCst);
                }
                NetworkAtomicType::Usize => {
                    let dst = &mut *(dst_addr as *mut usize);
                    let src = &*(src_addr as *mut usize as *mut AtomicUsize);
                    *dst = src.load(Ordering::SeqCst);
                }
            }
        }
    }

    // fn store(&self, src_addr: *const u8, dst_addr: *mut u8) {
    //     unsafe {
    //         match self {
    //             NetworkAtomicType::I8 => {
    //                 let dst = &*(dst_addr as *mut i8 as *mut AtomicI8);
    //                 let src = *(src_addr as *mut i8);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::I16 => {
    //                 let dst = &*(dst_addr as *mut i16 as *mut AtomicI16);
    //                 let src = *(src_addr as *mut i16);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::I32 => {
    //                 let dst = &*(dst_addr as *mut i32 as *mut AtomicI32);
    //                 let src = *(src_addr as *mut i32);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::I64 => {
    //                 let dst = &*(dst_addr as *mut i64 as *mut AtomicI64);
    //                 let src = *(src_addr as *mut i64);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::Isize => {
    //                 let dst = &*(dst_addr as *mut isize as *mut AtomicIsize);
    //                 let src = *(src_addr as *mut isize);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::U8 => {
    //                 let dst = &*(dst_addr as *mut u8 as *mut AtomicU8);
    //                 let src = *(src_addr as *mut u8);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::U16 => {
    //                 let dst = &*(dst_addr as *mut u16 as *mut AtomicU16);
    //                 let src = *(src_addr as *mut u16);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::U32 => {
    //                 let dst = &*(dst_addr as *mut u32 as *mut AtomicU32);
    //                 let src = *(src_addr as *mut u32);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::U64 => {
    //                 let dst = &*(dst_addr as *mut u64 as *mut AtomicU64);
    //                 let src = *(src_addr as *mut u64);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //             NetworkAtomicType::Usize => {
    //                 let dst = &*(dst_addr as *mut usize as *mut AtomicUsize);
    //                 let src = *(src_addr as *mut usize);
    //                 dst.store(src, Ordering::SeqCst);
    //             }
    //         }
    //     }
    // }
}

impl From<NetworkAtomicType> for NativeAtomicType {
    fn from(t: NetworkAtomicType) -> Self {
        match t {
            NetworkAtomicType::I8 => NativeAtomicType::I8,
            NetworkAtomicType::I16 => NativeAtomicType::I16,
            NetworkAtomicType::I32 => NativeAtomicType::I32,
            NetworkAtomicType::I64 => NativeAtomicType::I64,
            NetworkAtomicType::Isize => NativeAtomicType::Isize,
            NetworkAtomicType::U8 => NativeAtomicType::U8,
            NetworkAtomicType::U16 => NativeAtomicType::U16,
            NetworkAtomicType::U32 => NativeAtomicType::U32,
            NetworkAtomicType::U64 => NativeAtomicType::U64,
            NetworkAtomicType::Usize => NativeAtomicType::Usize,
        }
    }
}
