use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};
use std::sync::atomic::{
    AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize,
    AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize,
    Ordering,
};

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::{InnerIter, Sealed},
    LamellarArrayIterators, LamellarArrayMutIterators,
};
use crate::array::native_atomic::*;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::{ElementArithmeticOps, ElementBitWiseOps, ElementShiftOps};
use crate::array::*;
use crate::memregion::Dist;

use self::iterator::IterLockFuture;

impl<T: Remote> InnerArray for NativeAtomicArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct NativeAtomicDistIter<T: Dist> {
    data: NativeAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for NativeAtomicDistIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        NativeAtomicDistIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for NativeAtomicDistIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NativeAtomicDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct NativeAtomicLocalIter<T: Dist> {
    data: NativeAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for NativeAtomicLocalIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        NativeAtomicLocalIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for NativeAtomicLocalIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NativeAtomicLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

// Macros for NativeAtomicDistIterElement operations via raw pointer.
// Cast self.value (*const T) directly to *const AtomicXxx — valid because
// NativeAtomicArray only exists for types with the same layout as their AtomicXxx.

macro_rules! ptr_nat_load {
    ($self:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8    => *(&(&*($self.value as *const AtomicI8   )).load(Ordering::SeqCst) as *const i8    as *const T),
                NativeAtomicType::I16   => *(&(&*($self.value as *const AtomicI16  )).load(Ordering::SeqCst) as *const i16   as *const T),
                NativeAtomicType::I32   => *(&(&*($self.value as *const AtomicI32  )).load(Ordering::SeqCst) as *const i32   as *const T),
                NativeAtomicType::I64   => *(&(&*($self.value as *const AtomicI64  )).load(Ordering::SeqCst) as *const i64   as *const T),
                NativeAtomicType::Isize => *(&(&*($self.value as *const AtomicIsize)).load(Ordering::SeqCst) as *const isize as *const T),
                NativeAtomicType::U8    => *(&(&*($self.value as *const AtomicU8   )).load(Ordering::SeqCst) as *const u8    as *const T),
                NativeAtomicType::U16   => *(&(&*($self.value as *const AtomicU16  )).load(Ordering::SeqCst) as *const u16   as *const T),
                NativeAtomicType::U32   => *(&(&*($self.value as *const AtomicU32  )).load(Ordering::SeqCst) as *const u32   as *const T),
                NativeAtomicType::U64   => *(&(&*($self.value as *const AtomicU64  )).load(Ordering::SeqCst) as *const u64   as *const T),
                NativeAtomicType::Usize => *(&(&*($self.value as *const AtomicUsize)).load(Ordering::SeqCst) as *const usize as *const T),
            }
        }
    };
}

macro_rules! ptr_nat_store {
    ($self:ident, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8    => (&*($self.value as *const AtomicI8   )).store(*(&$val as *const T as *const i8   ), Ordering::SeqCst),
                NativeAtomicType::I16   => (&*($self.value as *const AtomicI16  )).store(*(&$val as *const T as *const i16  ), Ordering::SeqCst),
                NativeAtomicType::I32   => (&*($self.value as *const AtomicI32  )).store(*(&$val as *const T as *const i32  ), Ordering::SeqCst),
                NativeAtomicType::I64   => (&*($self.value as *const AtomicI64  )).store(*(&$val as *const T as *const i64  ), Ordering::SeqCst),
                NativeAtomicType::Isize => (&*($self.value as *const AtomicIsize)).store(*(&$val as *const T as *const isize), Ordering::SeqCst),
                NativeAtomicType::U8    => (&*($self.value as *const AtomicU8   )).store(*(&$val as *const T as *const u8   ), Ordering::SeqCst),
                NativeAtomicType::U16   => (&*($self.value as *const AtomicU16  )).store(*(&$val as *const T as *const u16  ), Ordering::SeqCst),
                NativeAtomicType::U32   => (&*($self.value as *const AtomicU32  )).store(*(&$val as *const T as *const u32  ), Ordering::SeqCst),
                NativeAtomicType::U64   => (&*($self.value as *const AtomicU64  )).store(*(&$val as *const T as *const u64  ), Ordering::SeqCst),
                NativeAtomicType::Usize => (&*($self.value as *const AtomicUsize)).store(*(&$val as *const T as *const usize), Ordering::SeqCst),
            }
        }
    };
}

// fetch_add / fetch_sub / fetch_and / fetch_or / fetch_xor
macro_rules! ptr_nat_fetch_op {
    ($self:ident, $op:ident, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8    => *(&(&*($self.value as *const AtomicI8   )).$op(*(&$val as *const T as *const i8   ), Ordering::SeqCst) as *const i8    as *const T),
                NativeAtomicType::I16   => *(&(&*($self.value as *const AtomicI16  )).$op(*(&$val as *const T as *const i16  ), Ordering::SeqCst) as *const i16   as *const T),
                NativeAtomicType::I32   => *(&(&*($self.value as *const AtomicI32  )).$op(*(&$val as *const T as *const i32  ), Ordering::SeqCst) as *const i32   as *const T),
                NativeAtomicType::I64   => *(&(&*($self.value as *const AtomicI64  )).$op(*(&$val as *const T as *const i64  ), Ordering::SeqCst) as *const i64   as *const T),
                NativeAtomicType::Isize => *(&(&*($self.value as *const AtomicIsize)).$op(*(&$val as *const T as *const isize), Ordering::SeqCst) as *const isize as *const T),
                NativeAtomicType::U8    => *(&(&*($self.value as *const AtomicU8   )).$op(*(&$val as *const T as *const u8   ), Ordering::SeqCst) as *const u8    as *const T),
                NativeAtomicType::U16   => *(&(&*($self.value as *const AtomicU16  )).$op(*(&$val as *const T as *const u16  ), Ordering::SeqCst) as *const u16   as *const T),
                NativeAtomicType::U32   => *(&(&*($self.value as *const AtomicU32  )).$op(*(&$val as *const T as *const u32  ), Ordering::SeqCst) as *const u32   as *const T),
                NativeAtomicType::U64   => *(&(&*($self.value as *const AtomicU64  )).$op(*(&$val as *const T as *const u64  ), Ordering::SeqCst) as *const u64   as *const T),
                NativeAtomicType::Usize => *(&(&*($self.value as *const AtomicUsize)).$op(*(&$val as *const T as *const usize), Ordering::SeqCst) as *const usize as *const T),
            }
        }
    };
}

// swap / fetch_mul / fetch_div / fetch_rem / fetch_shl / fetch_shr — all CAS loops
macro_rules! ptr_nat_fetch_cas {
    ($self:ident, $op:tt, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8 => {
                    let a = &*($self.value as *const AtomicI8);
                    let v = *(&$val as *const T as *const i8);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const i8 as *const T)
                }
                NativeAtomicType::I16 => {
                    let a = &*($self.value as *const AtomicI16);
                    let v = *(&$val as *const T as *const i16);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const i16 as *const T)
                }
                NativeAtomicType::I32 => {
                    let a = &*($self.value as *const AtomicI32);
                    let v = *(&$val as *const T as *const i32);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const i32 as *const T)
                }
                NativeAtomicType::I64 => {
                    let a = &*($self.value as *const AtomicI64);
                    let v = *(&$val as *const T as *const i64);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const i64 as *const T)
                }
                NativeAtomicType::Isize => {
                    let a = &*($self.value as *const AtomicIsize);
                    let v = *(&$val as *const T as *const isize);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const isize as *const T)
                }
                NativeAtomicType::U8 => {
                    let a = &*($self.value as *const AtomicU8);
                    let v = *(&$val as *const T as *const u8);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const u8 as *const T)
                }
                NativeAtomicType::U16 => {
                    let a = &*($self.value as *const AtomicU16);
                    let v = *(&$val as *const T as *const u16);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const u16 as *const T)
                }
                NativeAtomicType::U32 => {
                    let a = &*($self.value as *const AtomicU32);
                    let v = *(&$val as *const T as *const u32);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const u32 as *const T)
                }
                NativeAtomicType::U64 => {
                    let a = &*($self.value as *const AtomicU64);
                    let v = *(&$val as *const T as *const u64);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const u64 as *const T)
                }
                NativeAtomicType::Usize => {
                    let a = &*($self.value as *const AtomicUsize);
                    let v = *(&$val as *const T as *const usize);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut new = cur $op v;
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                        new = cur $op v;
                    }
                    *(&cur as *const usize as *const T)
                }
            }
        }
    };
}

macro_rules! ptr_nat_swap {
    ($self:ident, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8 => {
                    let a = &*($self.value as *const AtomicI8);
                    let new = *(&$val as *const T as *const i8);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i8 as *const T)
                }
                NativeAtomicType::I16 => {
                    let a = &*($self.value as *const AtomicI16);
                    let new = *(&$val as *const T as *const i16);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i16 as *const T)
                }
                NativeAtomicType::I32 => {
                    let a = &*($self.value as *const AtomicI32);
                    let new = *(&$val as *const T as *const i32);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i32 as *const T)
                }
                NativeAtomicType::I64 => {
                    let a = &*($self.value as *const AtomicI64);
                    let new = *(&$val as *const T as *const i64);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i64 as *const T)
                }
                NativeAtomicType::Isize => {
                    let a = &*($self.value as *const AtomicIsize);
                    let new = *(&$val as *const T as *const isize);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const isize as *const T)
                }
                NativeAtomicType::U8 => {
                    let a = &*($self.value as *const AtomicU8);
                    let new = *(&$val as *const T as *const u8);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u8 as *const T)
                }
                NativeAtomicType::U16 => {
                    let a = &*($self.value as *const AtomicU16);
                    let new = *(&$val as *const T as *const u16);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u16 as *const T)
                }
                NativeAtomicType::U32 => {
                    let a = &*($self.value as *const AtomicU32);
                    let new = *(&$val as *const T as *const u32);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u32 as *const T)
                }
                NativeAtomicType::U64 => {
                    let a = &*($self.value as *const AtomicU64);
                    let new = *(&$val as *const T as *const u64);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u64 as *const T)
                }
                NativeAtomicType::Usize => {
                    let a = &*($self.value as *const AtomicUsize);
                    let new = *(&$val as *const T as *const usize);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a.compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const usize as *const T)
                }
            }
        }
    };
}

macro_rules! ptr_nat_compare_exchange {
    ($self:ident, $old:ident, $new:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8    => *(&(&*($self.value as *const AtomicI8   )).compare_exchange(*(&$old as *const T as *const i8   ), *(&$new as *const T as *const i8   ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<i8,    i8   > as *mut Result<T, T>),
                NativeAtomicType::I16   => *(&(&*($self.value as *const AtomicI16  )).compare_exchange(*(&$old as *const T as *const i16  ), *(&$new as *const T as *const i16  ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<i16,   i16  > as *mut Result<T, T>),
                NativeAtomicType::I32   => *(&(&*($self.value as *const AtomicI32  )).compare_exchange(*(&$old as *const T as *const i32  ), *(&$new as *const T as *const i32  ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<i32,   i32  > as *mut Result<T, T>),
                NativeAtomicType::I64   => *(&(&*($self.value as *const AtomicI64  )).compare_exchange(*(&$old as *const T as *const i64  ), *(&$new as *const T as *const i64  ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<i64,   i64  > as *mut Result<T, T>),
                NativeAtomicType::Isize => *(&(&*($self.value as *const AtomicIsize)).compare_exchange(*(&$old as *const T as *const isize), *(&$new as *const T as *const isize), Ordering::SeqCst, Ordering::SeqCst) as *const Result<isize, isize> as *mut Result<T, T>),
                NativeAtomicType::U8    => *(&(&*($self.value as *const AtomicU8   )).compare_exchange(*(&$old as *const T as *const u8   ), *(&$new as *const T as *const u8   ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<u8,    u8   > as *mut Result<T, T>),
                NativeAtomicType::U16   => *(&(&*($self.value as *const AtomicU16  )).compare_exchange(*(&$old as *const T as *const u16  ), *(&$new as *const T as *const u16  ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<u16,   u16  > as *mut Result<T, T>),
                NativeAtomicType::U32   => *(&(&*($self.value as *const AtomicU32  )).compare_exchange(*(&$old as *const T as *const u32  ), *(&$new as *const T as *const u32  ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<u32,   u32  > as *mut Result<T, T>),
                NativeAtomicType::U64   => *(&(&*($self.value as *const AtomicU64  )).compare_exchange(*(&$old as *const T as *const u64  ), *(&$new as *const T as *const u64  ), Ordering::SeqCst, Ordering::SeqCst) as *const Result<u64,   u64  > as *mut Result<T, T>),
                NativeAtomicType::Usize => *(&(&*($self.value as *const AtomicUsize)).compare_exchange(*(&$old as *const T as *const usize), *(&$new as *const T as *const usize), Ordering::SeqCst, Ordering::SeqCst) as *const Result<usize, usize> as *mut Result<T, T>),
            }
        }
    };
}

macro_rules! ptr_nat_compare_exchange_eps {
    ($self:ident, $old:ident, $val:ident, $eps:ident) => {
        unsafe {
            match $self.orig_t {
                NativeAtomicType::I8 => {
                    let a = &*($self.value as *const AtomicI8);
                    let old_a = *(&$old as *const T as *const i8);
                    let val_a = *(&$val as *const T as *const i8);
                    let eps_a = *(&$eps as *const T as *const i8);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i8) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i8, i8> as *mut Result<T, T>)
                }
                NativeAtomicType::I16 => {
                    let a = &*($self.value as *const AtomicI16);
                    let old_a = *(&$old as *const T as *const i16);
                    let val_a = *(&$val as *const T as *const i16);
                    let eps_a = *(&$eps as *const T as *const i16);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i16) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i16, i16> as *mut Result<T, T>)
                }
                NativeAtomicType::I32 => {
                    let a = &*($self.value as *const AtomicI32);
                    let old_a = *(&$old as *const T as *const i32);
                    let val_a = *(&$val as *const T as *const i32);
                    let eps_a = *(&$eps as *const T as *const i32);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i32) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i32, i32> as *mut Result<T, T>)
                }
                NativeAtomicType::I64 => {
                    let a = &*($self.value as *const AtomicI64);
                    let old_a = *(&$old as *const T as *const i64);
                    let val_a = *(&$val as *const T as *const i64);
                    let eps_a = *(&$eps as *const T as *const i64);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i64) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i64, i64> as *mut Result<T, T>)
                }
                NativeAtomicType::Isize => {
                    let a = &*($self.value as *const AtomicIsize);
                    let old_a = *(&$old as *const T as *const isize);
                    let val_a = *(&$val as *const T as *const isize);
                    let eps_a = *(&$eps as *const T as *const isize);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as isize) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<isize, isize> as *mut Result<T, T>)
                }
                NativeAtomicType::U8 => {
                    let a = &*($self.value as *const AtomicU8);
                    let old_a = *(&$old as *const T as *const u8);
                    let val_a = *(&$val as *const T as *const u8);
                    let eps_a = *(&$eps as *const T as *const u8);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u8) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u8, u8> as *mut Result<T, T>)
                }
                NativeAtomicType::U16 => {
                    let a = &*($self.value as *const AtomicU16);
                    let old_a = *(&$old as *const T as *const u16);
                    let val_a = *(&$val as *const T as *const u16);
                    let eps_a = *(&$eps as *const T as *const u16);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u16) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u16, u16> as *mut Result<T, T>)
                }
                NativeAtomicType::U32 => {
                    let a = &*($self.value as *const AtomicU32);
                    let old_a = *(&$old as *const T as *const u32);
                    let val_a = *(&$val as *const T as *const u32);
                    let eps_a = *(&$eps as *const T as *const u32);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u32) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u32, u32> as *mut Result<T, T>)
                }
                NativeAtomicType::U64 => {
                    let a = &*($self.value as *const AtomicU64);
                    let old_a = *(&$old as *const T as *const u64);
                    let val_a = *(&$val as *const T as *const u64);
                    let eps_a = *(&$eps as *const T as *const u64);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u64) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u64, u64> as *mut Result<T, T>)
                }
                NativeAtomicType::Usize => {
                    let a = &*($self.value as *const AtomicUsize);
                    let old_a = *(&$old as *const T as *const usize);
                    let val_a = *(&$val as *const T as *const usize);
                    let eps_a = *(&$eps as *const T as *const usize);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as usize) < eps_a && !done {
                        cur = match a.compare_exchange(old_a, val_a, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(c) => { done = true; c }
                            Err(c) => { std::thread::yield_now(); c }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<usize, usize> as *mut Result<T, T>)
                }
            }
        }
    };
}

/// Zero-clone element for framework iterators over `NativeAtomicArray`.
///
/// Stores a raw pointer into the array's backing memory plus the `NativeAtomicType`
/// discriminant (Copy). The iterator keeps `data: NativeAtomicArray<T>` alive so
/// the backing memory is valid for the whole iteration.
#[doc(hidden)]
pub struct NativeAtomicDistIterElement<'a, T: Dist> {
    pub(crate) value: *const T,
    pub(crate) orig_t: NativeAtomicType,
    pub(crate) _marker: std::marker::PhantomData<&'a T>,
}

unsafe impl<T: Dist> Send for NativeAtomicDistIterElement<'_, T> {}
unsafe impl<T: Dist> Sync for NativeAtomicDistIterElement<'_, T> {}

impl<'a, T: Dist> NativeAtomicDistIterElement<'a, T> {
    pub fn load(&self) -> T { ptr_nat_load!(self) }
    pub fn store(&self, val: T) { ptr_nat_store!(self, val); }
    pub fn swap(&self, val: T) -> T { ptr_nat_swap!(self, val) }
    pub fn compare_exchange(&self, old: T, new: T) -> Result<T, T> { ptr_nat_compare_exchange!(self, old, new) }
    pub fn compare_exchange_epsilon(&self, old: T, val: T, eps: T) -> Result<T, T> { ptr_nat_compare_exchange_eps!(self, old, val, eps) }
}

impl<'a, T: Dist + ElementArithmeticOps> NativeAtomicDistIterElement<'a, T> {
    pub fn fetch_add(&self, val: T) -> T { ptr_nat_fetch_op!(self, fetch_add, val) }
    pub fn fetch_sub(&self, val: T) -> T { ptr_nat_fetch_op!(self, fetch_sub, val) }
    pub fn fetch_mul(&self, val: T) -> T { ptr_nat_fetch_cas!(self, *, val) }
    pub fn fetch_div(&self, val: T) -> T { ptr_nat_fetch_cas!(self, /, val) }
    pub fn fetch_rem(&self, val: T) -> T { ptr_nat_fetch_cas!(self, %, val) }
}

impl<'a, T: Dist + ElementBitWiseOps + 'static> NativeAtomicDistIterElement<'a, T> {
    pub fn fetch_and(&self, val: T) -> T { ptr_nat_fetch_op!(self, fetch_and, val) }
    pub fn fetch_or(&self, val: T) -> T  { ptr_nat_fetch_op!(self, fetch_or,  val) }
    pub fn fetch_xor(&self, val: T) -> T { ptr_nat_fetch_op!(self, fetch_xor, val) }
}

impl<'a, T: Dist + ElementShiftOps + 'static> NativeAtomicDistIterElement<'a, T> {
    pub fn fetch_shl(&self, val: T) -> T { ptr_nat_fetch_cas!(self, <<, val) }
    pub fn fetch_shr(&self, val: T) -> T { ptr_nat_fetch_cas!(self, >>, val) }
}

impl<'a, T: Dist + ElementArithmeticOps> AddAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn add_assign(&mut self, val: T) { self.fetch_add(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> SubAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn sub_assign(&mut self, val: T) { self.fetch_sub(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> MulAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn mul_assign(&mut self, val: T) { self.fetch_mul(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> DivAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn div_assign(&mut self, val: T) { self.fetch_div(val); }
}
impl<'a, T: Dist + ElementArithmeticOps> RemAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn rem_assign(&mut self, val: T) { self.fetch_rem(val); }
}
impl<'a, T: Dist + ElementBitWiseOps> BitAndAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn bitand_assign(&mut self, val: T) { self.fetch_and(val); }
}
impl<'a, T: Dist + ElementBitWiseOps> BitOrAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn bitor_assign(&mut self, val: T) { self.fetch_or(val); }
}
impl<'a, T: Dist + ElementBitWiseOps> BitXorAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn bitxor_assign(&mut self, val: T) { self.fetch_xor(val); }
}
impl<'a, T: Dist + ElementShiftOps> ShlAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn shl_assign(&mut self, val: T) { self.fetch_shl(val); }
}
impl<'a, T: Dist + ElementShiftOps> ShrAssign<T> for NativeAtomicDistIterElement<'a, T> {
    fn shr_assign(&mut self, val: T) { self.fetch_shr(val); }
}

impl<'a, T: Dist + std::fmt::Debug> std::fmt::Debug for NativeAtomicDistIterElement<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.load())
    }
}

impl<T: Dist + 'static> DistributedIterator for NativeAtomicDistIter<T> {
    type Item = NativeAtomicDistIterElement<'static, T>;
    type Array = NativeAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        NativeAtomicDistIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            let local_i = self.cur_i;
            self.cur_i += 1;
            let value = unsafe { self.data.local_as_ptr().add(local_i) };
            Some(NativeAtomicDistIterElement {
                value,
                orig_t: self.data.orig_t,
                _marker: std::marker::PhantomData,
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist + 'static> IndexedDistributedIterator for NativeAtomicDistIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for NativeAtomicLocalIter<T> {
    type Item = NativeAtomicDistIterElement<'static, T>;
    type Array = NativeAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        NativeAtomicLocalIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            let local_i = self.cur_i;
            self.cur_i += 1;
            let value = unsafe { self.data.local_as_ptr().add(local_i) };
            Some(NativeAtomicDistIterElement {
                value,
                orig_t: self.data.orig_t,
                _marker: std::marker::PhantomData,
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist> LamellarArrayIterators<T> for NativeAtomicArray<T> {
    type DistIter = NativeAtomicDistIter<T>;
    type LocalIter = NativeAtomicLocalIter<T>;
    type OnesidedIter = OneSidedIter<T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        NativeAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        NativeAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self, 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(self, std::cmp::min(buf_size, self.len()))
    }
}

impl<T: Dist> LamellarArrayMutIterators<T> for NativeAtomicArray<T> {
    type DistIter = NativeAtomicDistIter<T>;
    type LocalIter = NativeAtomicLocalIter<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        NativeAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        NativeAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for NativeAtomicArray<T> {}

impl<T: Dist> LocalIteratorLauncher for NativeAtomicArray<T> {}
