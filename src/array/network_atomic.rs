mod handle;
pub(crate) use handle::NetworkAtomicArrayHandle;

pub(crate) mod iteration;
pub(crate) mod operations;
mod rdma;
use crate::array::atomic::AtomicElement;
use crate::array::native_atomic::NativeAtomicType;
use crate::array::private::ArrayExecAm;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
use crate::array::*;
use crate::barrier::BarrierHandle;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;

use serde::ser::SerializeSeq;
use std::any::TypeId;
use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};

macro_rules! impl_atomic_ops{
    { $A:ty, $B:ty , $C:ident} => {
        pub(crate) struct $C<'a>(pub(crate) &'a $B);
        impl AddAssign<$A> for $C<'_>{
            fn add_assign(&mut self, val: $A) {
               self.0.fetch_add(val,Ordering::SeqCst);
            }
        }
        impl SubAssign<$A> for $C<'_>{
            fn sub_assign(&mut self, val: $A) {
               self.0.fetch_sub(val,Ordering::SeqCst);
            }
        }
        impl MulAssign<$A> for $C<'_>{
            fn mul_assign(&mut self, val: $A) {
                let mut cur = self.0.load(Ordering::SeqCst);
                let mut new = cur*val;
                while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                    std::thread::yield_now();
                    cur = self.0.load(Ordering::SeqCst);
                    new = cur*val;
                }
            }
        }
        impl DivAssign<$A> for $C<'_>{
            fn div_assign(&mut self, val: $A) {
                let mut cur = self.0.load(Ordering::SeqCst);
                let mut new = cur/val;
                while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                    std::thread::yield_now();
                    cur = self.0.load(Ordering::SeqCst);
                    new = cur/val;
                }
            }
        }
        impl RemAssign<$A> for $C<'_>{
            fn rem_assign(&mut self, val: $A) {
                let mut cur = self.0.load(Ordering::SeqCst);
                let mut new = cur%val;
                while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                    std::thread::yield_now();
                    cur = self.0.load(Ordering::SeqCst);
                    new = cur%val;
                }
            }
        }
        impl BitAndAssign<$A> for $C<'_>{
            fn bitand_assign(&mut self, val: $A) {
                self.0.fetch_and(val,Ordering::SeqCst);
            }
        }
        impl BitOrAssign<$A> for $C<'_>{
            fn bitor_assign(&mut self, val: $A) {
                self.0.fetch_or(val,Ordering::SeqCst);
            }
        }
        impl BitXorAssign<$A> for $C<'_>{
            fn bitxor_assign(&mut self, val: $A) {
                self.0.fetch_xor(val,Ordering::SeqCst);
            }
        }
        impl ShlAssign<$A> for $C<'_> {
            fn shl_assign(&mut self, val: $A ) {
                let mut cur = self.0.load(Ordering::SeqCst);
                let mut new = cur<<val;
                while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                    std::thread::yield_now();
                    cur = self.0.load(Ordering::SeqCst);
                    new = cur>>val;
                }
            }
        }
        impl ShrAssign<$A> for $C<'_> {
            fn shr_assign(&mut self, val: $A )  {
                let mut cur = self.0.load(Ordering::SeqCst);
                let mut new = cur<<val;
                while self.0.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                    std::thread::yield_now();
                    cur = self.0.load(Ordering::SeqCst);
                    new = cur>>val;
                }
            }
        }
    }
}

use std::sync::atomic::AtomicI8;
impl_atomic_ops! {i8,AtomicI8,MyAtomicI8}
use std::sync::atomic::AtomicI16;
impl_atomic_ops! {i16,AtomicI16,MyAtomicI16}
use std::sync::atomic::AtomicI32;
impl_atomic_ops! {i32,AtomicI32,MyAtomicI32}
use std::sync::atomic::AtomicI64;
impl_atomic_ops! {i64,AtomicI64,MyAtomicI64}
use std::sync::atomic::AtomicIsize;
impl_atomic_ops! {isize,AtomicIsize,MyAtomicIsize}
use std::sync::atomic::AtomicU8;
impl_atomic_ops! {u8,AtomicU8,MyAtomicU8}
use std::sync::atomic::AtomicU16;
impl_atomic_ops! {u16,AtomicU16,MyAtomicU16}
use std::sync::atomic::AtomicU32;
impl_atomic_ops! {u32,AtomicU32,MyAtomicU32}
use std::sync::atomic::AtomicU64;
impl_atomic_ops! {u64,AtomicU64,MyAtomicU64}
use std::sync::atomic::AtomicUsize;
impl_atomic_ops! {usize,AtomicUsize,MyAtomicUsize}
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

//#[doc(hidden)]
pub struct NetworkAtomicElement<T> {
    array: NetworkAtomicArray<T>,
    local_index: usize,
}

impl<T: Dist> From<NetworkAtomicElement<T>> for AtomicElement<T> {
    fn from(element: NetworkAtomicElement<T>) -> AtomicElement<T> {
        AtomicElement::NetworkAtomicElement(element)
    }
}

impl<T: Dist> NetworkAtomicElement<T> {
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
        impl_mul_div!(self, * , val)
    }
    pub fn fetch_div(&self, val: T) -> T {
        impl_mul_div!(self, /, val)
    }
    pub fn fetch_rem(&self, val: T) -> T {
        impl_mul_div!(self, %, val)
    }
    pub fn fetch_shl(&self, val: T) -> T {
        //result.0 is old value, result.1 is new value
        impl_shift!(self, <<, val)
    }
    pub fn fetch_shr(&self, val: T) -> T {
        //result.0 is old value, result.1 is new value
        impl_shift!(self, >>, val)
    }
}

impl<T: ElementBitWiseOps + 'static> NetworkAtomicElement<T> {
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
/// A variant of an [AtomicArray] providing atomic access for any integer type that has a corresponding Rust supported Atomic type (e.g. usize -> AtomicUsize)
///
/// Generally any operation on this array type will be performed via an internal runtime Active Message, i.e. direct RDMA operations are not allowed
///
/// You should not be directly interacting with this type, rather you should be operating on an [AtomicArray][crate::array::AtomicArray].
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct NetworkAtomicArray<T> {
    pub(crate) array: UnsafeArray<T>,
    pub(crate) orig_t: NetworkAtomicType,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct NetworkAtomicByteArray {
    pub(crate) array: UnsafeByteArray,
    pub(crate) orig_t: NetworkAtomicType,
}
impl NetworkAtomicByteArray {
    pub fn downgrade(array: &NetworkAtomicByteArray) -> NetworkAtomicByteArrayWeak {
        NetworkAtomicByteArrayWeak {
            array: UnsafeByteArray::downgrade(&array.array),
            orig_t: array.orig_t,
        }
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct NetworkAtomicByteArrayWeak {
    pub(crate) array: UnsafeByteArrayWeak,
    pub(crate) orig_t: NetworkAtomicType,
}

impl NetworkAtomicByteArrayWeak {
    pub fn upgrade(&self) -> Option<NetworkAtomicByteArray> {
        Some(NetworkAtomicByteArray {
            array: self.array.upgrade()?,
            orig_t: self.orig_t,
        })
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct NetworkAtomicLocalData<T> {
    // + NetworkAtomicOps> {
    pub(crate) array: NetworkAtomicArray<T>,
    start_index: usize,
    end_index: usize,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct NetworkAtomicLocalDataIter<T: Dist> {
    //+ NetworkAtomicOps> {
    array: NetworkAtomicArray<T>,
    index: usize,
    end_index: usize,
}

impl<T: Dist> NetworkAtomicLocalData<T> {
    pub fn at(&self, index: usize) -> NetworkAtomicElement<T> {
        NetworkAtomicElement {
            array: self.array.clone(),
            local_index: index,
        }
    }

    pub fn get_mut(&self, index: usize) -> Option<NetworkAtomicElement<T>> {
        Some(NetworkAtomicElement {
            array: self.array.clone(),
            local_index: index,
        })
    }

    pub fn len(&self) -> usize {
        self.end_index - self.start_index
    }

    pub fn iter(&self) -> NetworkAtomicLocalDataIter<T> {
        NetworkAtomicLocalDataIter {
            array: self.array.clone(),
            index: self.start_index,
            end_index: self.end_index,
        }
    }

    pub fn sub_data(&self, start_index: usize, end_index: usize) -> NetworkAtomicLocalData<T> {
        NetworkAtomicLocalData {
            array: self.array.clone(),
            start_index: start_index,
            end_index: std::cmp::min(end_index, self.array.num_elems_local()),
        }
    }

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

impl<T: Dist + serde::Serialize> serde::Serialize for NetworkAtomicLocalData<T> {
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

impl<T: Dist> IntoIterator for NetworkAtomicLocalData<T> {
    type Item = NetworkAtomicElement<T>;
    type IntoIter = NetworkAtomicLocalDataIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        NetworkAtomicLocalDataIter {
            array: self.array,
            index: self.start_index,
            end_index: self.end_index,
        }
    }
}

impl<T: Dist> Iterator for NetworkAtomicLocalDataIter<T> {
    type Item = NetworkAtomicElement<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end_index {
            let index = self.index;
            self.index += 1;
            Some(NetworkAtomicElement {
                array: self.array.clone(),
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
        println!("new Network atomic array 1");
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
    pub fn Network_type(&self) -> NetworkAtomicType {
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
}

#[doc(hidden)]
impl<T: Dist> NetworkAtomicArray<T> {
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        NetworkAtomicArray {
            array: self.array.use_distribution(distribution),
            orig_t: self.orig_t,
        }
    }

    pub fn local_data(&self) -> NetworkAtomicLocalData<T> {
        NetworkAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    pub fn mut_local_data(&self) -> NetworkAtomicLocalData<T> {
        NetworkAtomicLocalData {
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

impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for NetworkAtomicArray<T> {
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

        NetworkAtomicArray {
            array: array,
            orig_t: NetworkAtomicType::of::<T>(),
        }
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicArray<T>> for NetworkAtomicByteArray {
    fn from(array: NetworkAtomicArray<T>) -> Self {
        NetworkAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicArray<T>> for LamellarByteArray {
    fn from(array: NetworkAtomicArray<T>) -> Self {
        LamellarByteArray::NetworkAtomicArray(NetworkAtomicByteArray {
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

impl From<NetworkAtomicByteArray> for NativeAtomicByteArray {
    fn from(array: NetworkAtomicByteArray) -> Self {
        NativeAtomicByteArray {
            array: array.array,
            orig_t: array.orig_t.into(),
        }
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicArray<T>> for AtomicByteArray {
    fn from(array: NetworkAtomicArray<T>) -> Self {
        AtomicByteArray::NetworkAtomicByteArray(NetworkAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        })
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicByteArray> for NetworkAtomicArray<T> {
    fn from(array: NetworkAtomicByteArray) -> Self {
        NetworkAtomicArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }
    }
}

impl<T: Dist> From<&NetworkAtomicByteArray> for NetworkAtomicArray<T> {
    fn from(array: &NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}
impl<T: Dist> From<&mut NetworkAtomicByteArray> for NetworkAtomicArray<T> {
    fn from(array: &mut NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}

//#[doc(hidden)]
impl<T: Dist> From<NetworkAtomicByteArray> for AtomicArray<T> {
    fn from(array: NetworkAtomicByteArray) -> Self {
        NetworkAtomicArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }
        .into()
    }
}
impl<T: Dist> From<&NetworkAtomicByteArray> for AtomicArray<T> {
    fn from(array: &NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}
impl<T: Dist> From<&mut NetworkAtomicByteArray> for AtomicArray<T> {
    fn from(array: &mut NetworkAtomicByteArray) -> Self {
        array.clone().into()
    }
}

// //#[doc(hidden)]
impl<T: Dist> private::ArrayExecAm<T> for NetworkAtomicArray<T> {
    fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
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
    pub fn reduce(&self, op: &str) -> AmHandle<Option<T>> {
        self.array.reduce_data(op, self.clone().into())
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn sum(&self) -> AmHandle<Option<T>> {
        self.reduce("sum")
    }
    #[doc(hidden)]
    pub fn prod(&self) -> AmHandle<Option<T>> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> NetworkAtomicArray<T> {
    #[doc(hidden)]
    pub fn max(&self) -> AmHandle<Option<T>> {
        self.reduce("max")
    }
    #[doc(hidden)]
    pub fn min(&self) -> AmHandle<Option<T>> {
        self.reduce("min")
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
