use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};
use std::sync::atomic::{
    AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicU16, AtomicU32, AtomicU64,
    AtomicU8, AtomicUsize, Ordering,
};

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::{InnerIter, Sealed},
    LamellarArrayIterators, LamellarArrayMutIterators,
};
use crate::array::network_atomic::*;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::array::{ElementArithmeticOps, ElementBitWiseOps, ElementShiftOps};
use crate::memregion::Dist;
use crate::Remote;

use self::iterator::IterLockFuture;

impl<T: Remote> InnerArray for NetworkAtomicArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct NetworkAtomicDistIter<T: Dist> {
    data: NetworkAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for NetworkAtomicDistIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        NetworkAtomicDistIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for NetworkAtomicDistIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NetworkAtomicDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct NetworkAtomicLocalIter<T: Dist> {
    data: NetworkAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for NetworkAtomicLocalIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        NetworkAtomicLocalIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for NetworkAtomicLocalIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NetworkAtomicLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

// Macros for NetworkAtomicDistIterElement — identical pattern to NativeAtomicDistIterElement
// but matching on NetworkAtomicType instead of NativeAtomicType.

macro_rules! ptr_net_load {
    ($self:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => {
                    *(&(&*($self.value as *const AtomicI8)).load(Ordering::SeqCst) as *const i8
                        as *const T)
                }
                NetworkAtomicType::I16 => {
                    *(&(&*($self.value as *const AtomicI16)).load(Ordering::SeqCst) as *const i16
                        as *const T)
                }
                NetworkAtomicType::I32 => {
                    *(&(&*($self.value as *const AtomicI32)).load(Ordering::SeqCst) as *const i32
                        as *const T)
                }
                NetworkAtomicType::I64 => {
                    *(&(&*($self.value as *const AtomicI64)).load(Ordering::SeqCst) as *const i64
                        as *const T)
                }
                NetworkAtomicType::Isize => {
                    *(&(&*($self.value as *const AtomicIsize)).load(Ordering::SeqCst)
                        as *const isize as *const T)
                }
                NetworkAtomicType::U8 => {
                    *(&(&*($self.value as *const AtomicU8)).load(Ordering::SeqCst) as *const u8
                        as *const T)
                }
                NetworkAtomicType::U16 => {
                    *(&(&*($self.value as *const AtomicU16)).load(Ordering::SeqCst) as *const u16
                        as *const T)
                }
                NetworkAtomicType::U32 => {
                    *(&(&*($self.value as *const AtomicU32)).load(Ordering::SeqCst) as *const u32
                        as *const T)
                }
                NetworkAtomicType::U64 => {
                    *(&(&*($self.value as *const AtomicU64)).load(Ordering::SeqCst) as *const u64
                        as *const T)
                }
                NetworkAtomicType::Usize => {
                    *(&(&*($self.value as *const AtomicUsize)).load(Ordering::SeqCst)
                        as *const usize as *const T)
                }
            }
        }
    };
}

macro_rules! ptr_net_store {
    ($self:ident, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => (&*($self.value as *const AtomicI8))
                    .store(*(&$val as *const T as *const i8), Ordering::SeqCst),
                NetworkAtomicType::I16 => (&*($self.value as *const AtomicI16))
                    .store(*(&$val as *const T as *const i16), Ordering::SeqCst),
                NetworkAtomicType::I32 => (&*($self.value as *const AtomicI32))
                    .store(*(&$val as *const T as *const i32), Ordering::SeqCst),
                NetworkAtomicType::I64 => (&*($self.value as *const AtomicI64))
                    .store(*(&$val as *const T as *const i64), Ordering::SeqCst),
                NetworkAtomicType::Isize => (&*($self.value as *const AtomicIsize))
                    .store(*(&$val as *const T as *const isize), Ordering::SeqCst),
                NetworkAtomicType::U8 => (&*($self.value as *const AtomicU8))
                    .store(*(&$val as *const T as *const u8), Ordering::SeqCst),
                NetworkAtomicType::U16 => (&*($self.value as *const AtomicU16))
                    .store(*(&$val as *const T as *const u16), Ordering::SeqCst),
                NetworkAtomicType::U32 => (&*($self.value as *const AtomicU32))
                    .store(*(&$val as *const T as *const u32), Ordering::SeqCst),
                NetworkAtomicType::U64 => (&*($self.value as *const AtomicU64))
                    .store(*(&$val as *const T as *const u64), Ordering::SeqCst),
                NetworkAtomicType::Usize => (&*($self.value as *const AtomicUsize))
                    .store(*(&$val as *const T as *const usize), Ordering::SeqCst),
            }
        }
    };
}

macro_rules! ptr_net_fetch_op {
    ($self:ident, $op:ident, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => {
                    *(&(&*($self.value as *const AtomicI8))
                        .$op(*(&$val as *const T as *const i8), Ordering::SeqCst)
                        as *const i8 as *const T)
                }
                NetworkAtomicType::I16 => {
                    *(&(&*($self.value as *const AtomicI16))
                        .$op(*(&$val as *const T as *const i16), Ordering::SeqCst)
                        as *const i16 as *const T)
                }
                NetworkAtomicType::I32 => {
                    *(&(&*($self.value as *const AtomicI32))
                        .$op(*(&$val as *const T as *const i32), Ordering::SeqCst)
                        as *const i32 as *const T)
                }
                NetworkAtomicType::I64 => {
                    *(&(&*($self.value as *const AtomicI64))
                        .$op(*(&$val as *const T as *const i64), Ordering::SeqCst)
                        as *const i64 as *const T)
                }
                NetworkAtomicType::Isize => {
                    *(&(&*($self.value as *const AtomicIsize))
                        .$op(*(&$val as *const T as *const isize), Ordering::SeqCst)
                        as *const isize as *const T)
                }
                NetworkAtomicType::U8 => {
                    *(&(&*($self.value as *const AtomicU8))
                        .$op(*(&$val as *const T as *const u8), Ordering::SeqCst)
                        as *const u8 as *const T)
                }
                NetworkAtomicType::U16 => {
                    *(&(&*($self.value as *const AtomicU16))
                        .$op(*(&$val as *const T as *const u16), Ordering::SeqCst)
                        as *const u16 as *const T)
                }
                NetworkAtomicType::U32 => {
                    *(&(&*($self.value as *const AtomicU32))
                        .$op(*(&$val as *const T as *const u32), Ordering::SeqCst)
                        as *const u32 as *const T)
                }
                NetworkAtomicType::U64 => {
                    *(&(&*($self.value as *const AtomicU64))
                        .$op(*(&$val as *const T as *const u64), Ordering::SeqCst)
                        as *const u64 as *const T)
                }
                NetworkAtomicType::Usize => {
                    *(&(&*($self.value as *const AtomicUsize))
                        .$op(*(&$val as *const T as *const usize), Ordering::SeqCst)
                        as *const usize as *const T)
                }
            }
        }
    };
}

macro_rules! ptr_net_fetch_cas {
    ($self:ident, $op:tt, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => {
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
                NetworkAtomicType::I16 => {
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
                NetworkAtomicType::I32 => {
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
                NetworkAtomicType::I64 => {
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
                NetworkAtomicType::Isize => {
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
                NetworkAtomicType::U8 => {
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
                NetworkAtomicType::U16 => {
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
                NetworkAtomicType::U32 => {
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
                NetworkAtomicType::U64 => {
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
                NetworkAtomicType::Usize => {
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

macro_rules! ptr_net_swap {
    ($self:ident, $val:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => {
                    let a = &*($self.value as *const AtomicI8);
                    let new = *(&$val as *const T as *const i8);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i8 as *const T)
                }
                NetworkAtomicType::I16 => {
                    let a = &*($self.value as *const AtomicI16);
                    let new = *(&$val as *const T as *const i16);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i16 as *const T)
                }
                NetworkAtomicType::I32 => {
                    let a = &*($self.value as *const AtomicI32);
                    let new = *(&$val as *const T as *const i32);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i32 as *const T)
                }
                NetworkAtomicType::I64 => {
                    let a = &*($self.value as *const AtomicI64);
                    let new = *(&$val as *const T as *const i64);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const i64 as *const T)
                }
                NetworkAtomicType::Isize => {
                    let a = &*($self.value as *const AtomicIsize);
                    let new = *(&$val as *const T as *const isize);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const isize as *const T)
                }
                NetworkAtomicType::U8 => {
                    let a = &*($self.value as *const AtomicU8);
                    let new = *(&$val as *const T as *const u8);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u8 as *const T)
                }
                NetworkAtomicType::U16 => {
                    let a = &*($self.value as *const AtomicU16);
                    let new = *(&$val as *const T as *const u16);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u16 as *const T)
                }
                NetworkAtomicType::U32 => {
                    let a = &*($self.value as *const AtomicU32);
                    let new = *(&$val as *const T as *const u32);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u32 as *const T)
                }
                NetworkAtomicType::U64 => {
                    let a = &*($self.value as *const AtomicU64);
                    let new = *(&$val as *const T as *const u64);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const u64 as *const T)
                }
                NetworkAtomicType::Usize => {
                    let a = &*($self.value as *const AtomicUsize);
                    let new = *(&$val as *const T as *const usize);
                    let mut cur = a.load(Ordering::SeqCst);
                    while a
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = a.load(Ordering::SeqCst);
                    }
                    *(&cur as *const usize as *const T)
                }
            }
        }
    };
}

macro_rules! ptr_net_compare_exchange {
    ($self:ident, $old:ident, $new:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => {
                    *(&(&*($self.value as *const AtomicI8)).compare_exchange(
                        *(&$old as *const T as *const i8),
                        *(&$new as *const T as *const i8),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<i8, i8> as *mut Result<T, T>)
                }
                NetworkAtomicType::I16 => {
                    *(&(&*($self.value as *const AtomicI16)).compare_exchange(
                        *(&$old as *const T as *const i16),
                        *(&$new as *const T as *const i16),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<i16, i16> as *mut Result<T, T>)
                }
                NetworkAtomicType::I32 => {
                    *(&(&*($self.value as *const AtomicI32)).compare_exchange(
                        *(&$old as *const T as *const i32),
                        *(&$new as *const T as *const i32),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<i32, i32> as *mut Result<T, T>)
                }
                NetworkAtomicType::I64 => {
                    *(&(&*($self.value as *const AtomicI64)).compare_exchange(
                        *(&$old as *const T as *const i64),
                        *(&$new as *const T as *const i64),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<i64, i64> as *mut Result<T, T>)
                }
                NetworkAtomicType::Isize => {
                    *(&(&*($self.value as *const AtomicIsize)).compare_exchange(
                        *(&$old as *const T as *const isize),
                        *(&$new as *const T as *const isize),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<isize, isize> as *mut Result<T, T>)
                }
                NetworkAtomicType::U8 => {
                    *(&(&*($self.value as *const AtomicU8)).compare_exchange(
                        *(&$old as *const T as *const u8),
                        *(&$new as *const T as *const u8),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<u8, u8> as *mut Result<T, T>)
                }
                NetworkAtomicType::U16 => {
                    *(&(&*($self.value as *const AtomicU16)).compare_exchange(
                        *(&$old as *const T as *const u16),
                        *(&$new as *const T as *const u16),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<u16, u16> as *mut Result<T, T>)
                }
                NetworkAtomicType::U32 => {
                    *(&(&*($self.value as *const AtomicU32)).compare_exchange(
                        *(&$old as *const T as *const u32),
                        *(&$new as *const T as *const u32),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<u32, u32> as *mut Result<T, T>)
                }
                NetworkAtomicType::U64 => {
                    *(&(&*($self.value as *const AtomicU64)).compare_exchange(
                        *(&$old as *const T as *const u64),
                        *(&$new as *const T as *const u64),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<u64, u64> as *mut Result<T, T>)
                }
                NetworkAtomicType::Usize => {
                    *(&(&*($self.value as *const AtomicUsize)).compare_exchange(
                        *(&$old as *const T as *const usize),
                        *(&$new as *const T as *const usize),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) as *const Result<usize, usize> as *mut Result<T, T>)
                }
            }
        }
    };
}

macro_rules! ptr_net_compare_exchange_eps {
    ($self:ident, $old:ident, $val:ident, $eps:ident) => {
        unsafe {
            match $self.orig_t {
                NetworkAtomicType::I8 => {
                    let a = &*($self.value as *const AtomicI8);
                    let old_a = *(&$old as *const T as *const i8);
                    let val_a = *(&$val as *const T as *const i8);
                    let eps_a = *(&$eps as *const T as *const i8);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i8) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i8, i8>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::I16 => {
                    let a = &*($self.value as *const AtomicI16);
                    let old_a = *(&$old as *const T as *const i16);
                    let val_a = *(&$val as *const T as *const i16);
                    let eps_a = *(&$eps as *const T as *const i16);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i16) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i16, i16>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::I32 => {
                    let a = &*($self.value as *const AtomicI32);
                    let old_a = *(&$old as *const T as *const i32);
                    let val_a = *(&$val as *const T as *const i32);
                    let eps_a = *(&$eps as *const T as *const i32);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i32) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i32, i32>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::I64 => {
                    let a = &*($self.value as *const AtomicI64);
                    let old_a = *(&$old as *const T as *const i64);
                    let val_a = *(&$val as *const T as *const i64);
                    let eps_a = *(&$eps as *const T as *const i64);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as i64) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<i64, i64>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::Isize => {
                    let a = &*($self.value as *const AtomicIsize);
                    let old_a = *(&$old as *const T as *const isize);
                    let val_a = *(&$val as *const T as *const isize);
                    let eps_a = *(&$eps as *const T as *const isize);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as isize) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<isize, isize>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::U8 => {
                    let a = &*($self.value as *const AtomicU8);
                    let old_a = *(&$old as *const T as *const u8);
                    let val_a = *(&$val as *const T as *const u8);
                    let eps_a = *(&$eps as *const T as *const u8);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u8) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u8, u8>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::U16 => {
                    let a = &*($self.value as *const AtomicU16);
                    let old_a = *(&$old as *const T as *const u16);
                    let val_a = *(&$val as *const T as *const u16);
                    let eps_a = *(&$eps as *const T as *const u16);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u16) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u16, u16>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::U32 => {
                    let a = &*($self.value as *const AtomicU32);
                    let old_a = *(&$old as *const T as *const u32);
                    let val_a = *(&$val as *const T as *const u32);
                    let eps_a = *(&$eps as *const T as *const u32);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u32) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u32, u32>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::U64 => {
                    let a = &*($self.value as *const AtomicU64);
                    let old_a = *(&$old as *const T as *const u64);
                    let val_a = *(&$val as *const T as *const u64);
                    let eps_a = *(&$eps as *const T as *const u64);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as u64) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<u64, u64>
                        as *mut Result<T, T>)
                }
                NetworkAtomicType::Usize => {
                    let a = &*($self.value as *const AtomicUsize);
                    let old_a = *(&$old as *const T as *const usize);
                    let val_a = *(&$val as *const T as *const usize);
                    let eps_a = *(&$eps as *const T as *const usize);
                    let mut cur = a.load(Ordering::SeqCst);
                    let mut done = false;
                    while (cur.abs_diff(old_a) as usize) < eps_a && !done {
                        cur = match a.compare_exchange(
                            old_a,
                            val_a,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(c) => {
                                done = true;
                                c
                            }
                            Err(c) => {
                                std::thread::yield_now();
                                c
                            }
                        };
                    }
                    *(&(if done { Ok(cur) } else { Err(cur) }) as *const Result<usize, usize>
                        as *mut Result<T, T>)
                }
            }
        }
    };
}

/// Zero-clone element for framework iterators over `NetworkAtomicArray`.
///
/// Stores a raw pointer into the array's backing memory plus the `NetworkAtomicType`
/// discriminant (Copy). The iterator keeps `data: NetworkAtomicArray<T>` alive.
#[doc(hidden)]
pub struct NetworkAtomicDistIterElement<'a, T: Dist> {
    pub(crate) value: *const T,
    pub(crate) orig_t: NetworkAtomicType,
    pub(crate) _marker: std::marker::PhantomData<&'a T>,
}

unsafe impl<T: Dist> Send for NetworkAtomicDistIterElement<'_, T> {}
unsafe impl<T: Dist> Sync for NetworkAtomicDistIterElement<'_, T> {}

impl<'a, T: Dist> NetworkAtomicDistIterElement<'a, T> {
    pub fn load(&self) -> T {
        ptr_net_load!(self)
    }
    pub fn store(&self, val: T) {
        ptr_net_store!(self, val);
    }
    pub fn swap(&self, val: T) -> T {
        ptr_net_swap!(self, val)
    }
    pub fn compare_exchange(&self, old: T, new: T) -> Result<T, T> {
        ptr_net_compare_exchange!(self, old, new)
    }
    pub fn compare_exchange_epsilon(&self, old: T, val: T, eps: T) -> Result<T, T> {
        ptr_net_compare_exchange_eps!(self, old, val, eps)
    }
}

impl<'a, T: Dist + ElementArithmeticOps> NetworkAtomicDistIterElement<'a, T> {
    pub fn fetch_add(&self, val: T) -> T {
        ptr_net_fetch_op!(self, fetch_add, val)
    }
    pub fn fetch_sub(&self, val: T) -> T {
        ptr_net_fetch_op!(self, fetch_sub, val)
    }
    pub fn fetch_mul(&self, val: T) -> T {
        ptr_net_fetch_cas!(self, *, val)
    }
    pub fn fetch_div(&self, val: T) -> T {
        ptr_net_fetch_cas!(self, /, val)
    }
    pub fn fetch_rem(&self, val: T) -> T {
        ptr_net_fetch_cas!(self, %, val)
    }
}

impl<'a, T: Dist + ElementBitWiseOps + 'static> NetworkAtomicDistIterElement<'a, T> {
    pub fn fetch_and(&self, val: T) -> T {
        ptr_net_fetch_op!(self, fetch_and, val)
    }
    pub fn fetch_or(&self, val: T) -> T {
        ptr_net_fetch_op!(self, fetch_or, val)
    }
    pub fn fetch_xor(&self, val: T) -> T {
        ptr_net_fetch_op!(self, fetch_xor, val)
    }
}

impl<'a, T: Dist + ElementShiftOps + 'static> NetworkAtomicDistIterElement<'a, T> {
    pub fn fetch_shl(&self, val: T) -> T {
        ptr_net_fetch_cas!(self, <<, val)
    }
    pub fn fetch_shr(&self, val: T) -> T {
        ptr_net_fetch_cas!(self, >>, val)
    }
}

impl<'a, T: Dist + ElementArithmeticOps> AddAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn add_assign(&mut self, val: T) {
        self.fetch_add(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> SubAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn sub_assign(&mut self, val: T) {
        self.fetch_sub(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> MulAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn mul_assign(&mut self, val: T) {
        self.fetch_mul(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> DivAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn div_assign(&mut self, val: T) {
        self.fetch_div(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> RemAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn rem_assign(&mut self, val: T) {
        self.fetch_rem(val);
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitAndAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn bitand_assign(&mut self, val: T) {
        self.fetch_and(val);
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitOrAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn bitor_assign(&mut self, val: T) {
        self.fetch_or(val);
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitXorAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn bitxor_assign(&mut self, val: T) {
        self.fetch_xor(val);
    }
}
impl<'a, T: Dist + ElementShiftOps> ShlAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn shl_assign(&mut self, val: T) {
        self.fetch_shl(val);
    }
}
impl<'a, T: Dist + ElementShiftOps> ShrAssign<T> for NetworkAtomicDistIterElement<'a, T> {
    fn shr_assign(&mut self, val: T) {
        self.fetch_shr(val);
    }
}

impl<'a, T: Dist + std::fmt::Debug> std::fmt::Debug for NetworkAtomicDistIterElement<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.load())
    }
}

impl<T: Dist + 'static> DistributedIterator for NetworkAtomicDistIter<T> {
    type Item = NetworkAtomicDistIterElement<'static, T>;
    type Array = NetworkAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        NetworkAtomicDistIter {
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
            Some(NetworkAtomicDistIterElement {
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

impl<T: Dist + 'static> IndexedDistributedIterator for NetworkAtomicDistIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for NetworkAtomicLocalIter<T> {
    type Item = NetworkAtomicDistIterElement<'static, T>;
    type Array = NetworkAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        NetworkAtomicLocalIter {
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
            Some(NetworkAtomicDistIterElement {
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

impl<T: Dist> LamellarArrayIterators<T> for NetworkAtomicArray<T> {
    type DistIter = NetworkAtomicDistIter<T>;
    type LocalIter = NetworkAtomicLocalIter<T>;
    type OnesidedIter = OneSidedIter<T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        NetworkAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        NetworkAtomicLocalIter {
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

impl<T: Dist> LamellarArrayMutIterators<T> for NetworkAtomicArray<T> {
    type DistIter = NetworkAtomicDistIter<T>;
    type LocalIter = NetworkAtomicLocalIter<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        NetworkAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        NetworkAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for NetworkAtomicArray<T> {}

impl<T: Dist> LocalIteratorLauncher for NetworkAtomicArray<T> {}
