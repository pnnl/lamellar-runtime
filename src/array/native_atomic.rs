#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) mod buffered_operations;
mod iteration;
#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) use buffered_operations as operations;
mod rdma;
use crate::array::native_atomic::operations::BUFOPS;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::UnsafeByteArray;
use crate::array::atomic::{AtomicElement};
use crate::array::*;
// use crate::darc::Darc;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
// use parking_lot::{
//     Mutex,MutexGuard
// };
use std::any::TypeId;
// use std::ops::{Deref, DerefMut};


pub trait NativeAtomic {}

pub trait AsNativeAtomic{
    type Atomic;
    fn as_native_atomic(&self) -> &Self::Atomic;
}
// pub trait NativeAtomicOps {
//     type Atomic;
//     fn as_native_atomic(&self) -> &Self::Atomic;
//     fn fetch_add(&self, val: Self) -> Self;
//     fn fetch_sub(&mut self, val: Self) -> Self;
//     fn fetch_mul(&mut self, val: Self) -> Self;
//     fn fetch_div(&mut self, val: Self) -> Self;
//     fn fetch_bit_and(&mut self, val: Self) -> Self;
//     fn fetch_bit_or(&mut self, val: Self) -> Self;
//     fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self, Self>
//     where
//         Self: Sized;
//     fn load(&mut self) -> Self;
//     fn store(&mut self, val: Self);
//     fn swap(&mut self, val: Self) -> Self;
// }

macro_rules! impl_atomic_ops{
    { $A:ty, $B:ty , $C:ident} => {
        impl NativeAtomic for $A{}
        impl AsNativeAtomic for $A {
            // there is an equivalent call in nightly rust
            // Self::Atomic::from_mut()... we will switch to that once stablized;
            type Atomic = $B;
            fn as_native_atomic(&self) -> &Self::Atomic{
                use std::mem::align_of;
                let [] = [(); align_of::<$B>() - align_of::<$A>()];
                // SAFETY:
                //  - the mutable reference guarantees unique ownership.
                //  - the alignment of `$int_type` and `Self` is the
                //    same, as promised by $cfg_align and verified above.
                unsafe { &*(self as *const $A as *mut $A as *mut Self::Atomic) }
            }
        }
        pub struct $C<'a>(pub &'a $B);
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

        //     fn fetch_add(&self, val: Self) ->Self {
        //         self.as_native_atomic().fetch_add(val, Ordering::SeqCst)
        //     }
        //     fn fetch_sub(&mut self, val: Self,) ->Self{
        //         self.as_native_atomic().fetch_sub(val, Ordering::SeqCst)
        //     }
        //     fn fetch_mul(&mut self, val: Self,) -> Self{
        //         let mut cur = self.as_native_atomic().load(Ordering::SeqCst);
        //         let mut new = cur*val;
        //         while self.compare_exchange(cur,new).is_err(){
        //             std::thread::yield_now();
        //             cur = self.as_native_atomic().load(Ordering::SeqCst);
        //             new = cur*val;
        //         }
        //         cur
        //     }
        //     fn fetch_div(&mut self, val: Self,) -> Self{
        //         let mut cur = self.as_native_atomic().load(Ordering::SeqCst);
        //         let mut new = cur/val;
        //         while self.compare_exchange(cur,new).is_err(){
        //             std::thread::yield_now();
        //             cur = self.as_native_atomic().load(Ordering::SeqCst);
        //             new = cur/val;
        //         }
        //         cur
        //     }
        //     fn fetch_bit_and(&mut self, val: Self,) -> Self{
        //         // println!("fetch_bit_and: {:?}",val);
        //         self.as_native_atomic().fetch_and(val,Ordering::SeqCst)
        //     }
        //     fn fetch_bit_or(&mut self, val: Self,) -> Self{
        //         // println!("fetch_bit_or: {:?}",val);
        //         self.as_native_atomic().fetch_or(val,Ordering::SeqCst)
        //     }
        //     fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self,Self> {
        //         self.as_native_atomic().compare_exchange(current,new,Ordering::SeqCst,Ordering::SeqCst)
        //     }
        //     fn load(&mut self) ->Self{
        //         self.as_native_atomic().load(Ordering::SeqCst)
        //     }
        //     fn store(&mut self, val: Self){
        //         self.as_native_atomic().store(val,Ordering::SeqCst)
        //     }
        //     fn swap(&mut self, val: Self) ->Self{
        //         self.as_native_atomic().swap(val, Ordering::SeqCst)
        //     }
        // }
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

// fn as_atomic<T,A>(item: &T) -> &A{
//     use std::mem::align_of;
//     let [] = [(); align_of::<A>() - align_of::<T>()];
//     // SAFETY:
//     //  - the mutable reference guarantees unique ownership.
//     //  - the alignment of `$int_type` and `Self` is the
//     //    same, as promised by $cfg_align and verified above.
//     unsafe { &*(item as *const T as *mut T as *mut A) }
// }

macro_rules! as_atomic{
    { $A:ty, $B:ty, $item:ident , $val:ident } => {
        {
            use std::mem::align_of;
            let [] = [(); align_of::<$B>() - align_of::<$A>()];
            // SAFETY:
            //  - the mutable reference guarantees unique ownership.
            //  - the alignment of `$int_type` and `Self` is the
            //    same, as promised by $cfg_align and verified above.
            let item =  &*(&$item as *const T as *mut $A as *mut $B) ;
            let val =  *(&$val as *const T as *mut $A);
            (item,val)
        }
    }
}

macro_rules! compare_exchange_op{
    ($A:ty, $B:ty, $item:ident , $val:ident, $op:tt ) => {
        {
            let (item,val) = as_atomic!(i8,AtomicI8,$item,$val);
            let mut cur = item.load(Ordering::SeqCst);
            let mut new = cur $op val;
            while item.compare_exchange(cur,new,Ordering::SeqCst,Ordering::SeqCst).is_err(){
                std::thread::yield_now();
                cur = item.load(Ordering::SeqCst);
                new = cur $op val;
            }
            cur
        }
    }
}

macro_rules! impl_atomic_op{ 
    ($self:ident,$op:tt,$val:ident) =>{ // mul, div
        unsafe{
            let slice =  $self.array.__local_as_mut_slice();
            let item = slice[$self.local_index];
            let val = $val;
            match $self.array.orig_t{
                NativeAtomicType::I8 => {compare_exchange_op!(i8,AtomicI8,item,val,$op) as *const i8 as *mut T},
                NativeAtomicType::I16 => {compare_exchange_op!(i16,AtomicI16,item,val,$op) as *const i16 as *mut T},
                NativeAtomicType::I32 => {compare_exchange_op!(i32,AtomicI32,item,val,$op) as *const i32 as *mut T},
                NativeAtomicType::I64 => {compare_exchange_op!(i64,AtomicI64,item,val,$op) as *const i64 as *mut T},
                NativeAtomicType::Isize => {compare_exchange_op!(isize,AtomicIsize,item,val,$op) as *const isize as *mut T},
                NativeAtomicType::U8 => {compare_exchange_op!(u8,AtomicU8,item,val,$op) as *const u8 as *mut T},
                NativeAtomicType::U16 => {compare_exchange_op!(u16,AtomicU16,item,val,$op) as *const u16 as *mut T}
                NativeAtomicType::U32 => {compare_exchange_op!(u32,AtomicU32,item,val,$op) as *const u32 as *mut T},
                NativeAtomicType::U64 => {compare_exchange_op!(u64,AtomicU64,item,val,$op) as *const u64 as *mut T},
                NativeAtomicType::Usize => {compare_exchange_op!(usize,AtomicUsize,item,val,$op) as *const usize as *mut T},
            }
        }
    };
    ($self:ident,$op:ident,$val:ident,true) =>{//add,sub,and,or (returns value)
        unsafe{
            let slice =  $self.array.__local_as_mut_slice();
            let item = slice[$self.local_index];
            let val = $val;
            match $self.array.orig_t{
                NativeAtomicType::I8 => {let (item,val) = as_atomic!(i8,AtomicI8,item,val); *(&item.$op(val,Ordering::SeqCst) as *const i8 as *mut T)},
                NativeAtomicType::I16 => {let (item,val) = as_atomic!(i16,AtomicI16,item,val); *(&item.$op(val,Ordering::SeqCst) as *const i16 as *mut T)},
                NativeAtomicType::I32 => {let (item,val) = as_atomic!(i32,AtomicI32,item,val); *(&item.$op(val,Ordering::SeqCst) as *const i32 as *mut T)},
                NativeAtomicType::I64 => {let (item,val) = as_atomic!(i64,AtomicI64,item,val); *(&item.$op(val,Ordering::SeqCst) as *const i64 as *mut T)},
                NativeAtomicType::Isize => {let (item,val) = as_atomic!(isize,AtomicIsize,item,val); *(&item.$op(val,Ordering::SeqCst) as *const isize as *mut T)},
                NativeAtomicType::U8 => {let (item,val) = as_atomic!(u8,AtomicU8,item,val); *(&item.$op(val,Ordering::SeqCst) as *const u8 as *mut T)},
                NativeAtomicType::U16 => {let (item,val) = as_atomic!(u16,AtomicU16,item,val); *(&item.$op(val,Ordering::SeqCst) as *const u16 as *mut T)}
                NativeAtomicType::U32 => {let (item,val) = as_atomic!(u32,AtomicU32,item,val); *(&item.$op(val,Ordering::SeqCst) as *const u32 as *mut T)},
                NativeAtomicType::U64 => {let (item,val) = as_atomic!(u64,AtomicU64,item,val); *(&item.$op(val,Ordering::SeqCst) as *const u64 as *mut T)},
                NativeAtomicType::Usize => {let (item,val) = as_atomic!(usize,AtomicUsize,item,val); *(&item.$op(val,Ordering::SeqCst) as *const usize as *mut T)},
            }
        }
    };
    ($self:ident,$op:ident,$val:ident,false) =>{//store 
        unsafe{
            let slice =  $self.array.__local_as_mut_slice();
            let item = slice[$self.local_index];
            let val = $val;
            match $self.array.orig_t{
                NativeAtomicType::I8 => {let (item,val) = as_atomic!(i8,AtomicI8,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::I16 => {let (item,val) = as_atomic!(i16,AtomicI16,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::I32 => {let (item,val) = as_atomic!(i32,AtomicI32,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::I64 => {let (item,val) = as_atomic!(i64,AtomicI64,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::Isize => {let (item,val) = as_atomic!(isize,AtomicIsize,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::U8 => {let (item,val) = as_atomic!(u8,AtomicU8,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::U16 => {let (item,val) = as_atomic!(u16,AtomicU16,item,val); item.$op(val,Ordering::SeqCst);}
                NativeAtomicType::U32 => {let (item,val) = as_atomic!(u32,AtomicU32,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::U64 => {let (item,val) = as_atomic!(u64,AtomicU64,item,val); item.$op(val,Ordering::SeqCst);},
                NativeAtomicType::Usize => {let (item,val) = as_atomic!(usize,AtomicUsize,item,val); item.$op(val,Ordering::SeqCst);},
            }
        }
    };
    ($self:ident,$op:ident) =>{//load
        unsafe{
            let slice =  $self.array.__local_as_mut_slice();
            let item = slice[$self.local_index];
            let val = item;//dummy val
            match $self.array.orig_t{
                NativeAtomicType::I8 => {let (item,_) = as_atomic!(i8,AtomicI8,item,val); *(&item.$op(Ordering::SeqCst) as *const i8 as *mut T)},
                NativeAtomicType::I16 => {let (item,_) = as_atomic!(i16,AtomicI16,item,val); *(&item.$op(Ordering::SeqCst) as *const i16 as *mut T)},
                NativeAtomicType::I32 => {let (item,_) = as_atomic!(i32,AtomicI32,item,val); *(&item.$op(Ordering::SeqCst) as *const i32 as *mut T)},
                NativeAtomicType::I64 => {let (item,_) = as_atomic!(i64,AtomicI64,item,val); *(&item.$op(Ordering::SeqCst) as *const i64 as *mut T)},
                NativeAtomicType::Isize => {let (item,_) = as_atomic!(isize,AtomicIsize,item,val); *(&item.$op(Ordering::SeqCst) as *const isize as *mut T)},
                NativeAtomicType::U8 => {let (item,_) = as_atomic!(u8,AtomicU8,item,val); *(&item.$op(Ordering::SeqCst) as *const u8 as *mut T)},
                NativeAtomicType::U16 => {let (item,_) = as_atomic!(u16,AtomicU16,item,val); *(&item.$op(Ordering::SeqCst) as *const u16 as *mut T)}
                NativeAtomicType::U32 => {let (item,_) = as_atomic!(u32,AtomicU32,item,val); *(&item.$op(Ordering::SeqCst) as *const u32 as *mut T)},
                NativeAtomicType::U64 => {let (item,_) = as_atomic!(u64,AtomicU64,item,val); *(&item.$op(Ordering::SeqCst) as *const u64 as *mut T)},
                NativeAtomicType::Usize => {let (item,_) = as_atomic!(usize,AtomicUsize,item,val); *(&item.$op(Ordering::SeqCst) as *const usize as *mut T)},
            }
        }
    };
}

use std::ops::{AddAssign, BitAndAssign, BitOrAssign, DivAssign, MulAssign, SubAssign};
pub struct NativeAtomicElement<T: Dist > {
    array: NativeAtomicArray<T>,
    local_index: usize,
}

impl <T: Dist > From<NativeAtomicElement<T>> for AtomicElement<T>{
    fn from(element: NativeAtomicElement<T>) -> AtomicElement<T>{
        AtomicElement::NativeAtomicElement(element)
    }
}
 
impl <T: Dist > NativeAtomicElement<T>{
    pub fn load(&self) -> T {
        impl_atomic_op!(self,load)
    }
    pub fn store(&self, val: T) {
        impl_atomic_op!(self,store,val,false);
    }
}

impl<T: Dist + ElementArithmeticOps > AddAssign<T> for NativeAtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        impl_atomic_op!(self,fetch_add,val,true);
    }
}

impl<T: Dist + ElementArithmeticOps > SubAssign<T> for NativeAtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        impl_atomic_op!(self,fetch_sub,val,true);
    }
}

impl<T: Dist + ElementArithmeticOps > MulAssign<T> for NativeAtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        impl_atomic_op!(self,*,val);
    }
}

impl<T: Dist + ElementArithmeticOps > DivAssign<T> for NativeAtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        impl_atomic_op!(self,/,val);
    }
}

impl<T: Dist + ElementBitWiseOps > BitAndAssign<T> for NativeAtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        impl_atomic_op!(self,fetch_and,val,true);
    }
}

impl<T: Dist + ElementBitWiseOps > BitOrAssign<T> for NativeAtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        impl_atomic_op!(self,fetch_or,val,true);
    }
}


#[lamellar_impl::AmDataRT(Clone)]
pub struct NativeAtomicArray<T: Dist> {
    pub(crate) array: UnsafeArray<T>,
    pub(crate) orig_t: NativeAtomicType,
}

#[lamellar_impl::AmDataRT(Clone)]
pub struct NativeAtomicByteArray {
    pub(crate) array: UnsafeByteArray,
    pub(crate) orig_t: NativeAtomicType,
}



pub struct NativeAtomicLocalData<T: Dist> {// + NativeAtomicOps> {
    array: NativeAtomicArray<T>,
}

pub struct NativeAtomicLocalDataIter<T: Dist > {//+ NativeAtomicOps> {
    array: NativeAtomicArray<T>,
    index: usize,
}

impl<T: Dist > NativeAtomicLocalData<T> {
    pub fn at(&self, index: usize) -> NativeAtomicElement<T> {
        NativeAtomicElement {
            array: self.array.clone(),
            local_index: index,
        }
    }

    pub fn get_mut(&self, index: usize) -> Option<NativeAtomicElement<T>> {
        Some(NativeAtomicElement {
            array: self.array.clone(),
            local_index: index,
        })
    }

    pub fn len(&self) -> usize {
        unsafe { self.array.__local_as_mut_slice().len() }
    }

    pub fn iter(&self) -> NativeAtomicLocalDataIter<T> {
        NativeAtomicLocalDataIter {
            array: self.array.clone(),
            index: 0,
        }
    }
}

impl<T: Dist > IntoIterator for NativeAtomicLocalData<T> {
    type Item = NativeAtomicElement<T>;
    type IntoIter = NativeAtomicLocalDataIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        NativeAtomicLocalDataIter {
            array: self.array,
            index: 0,
        }
    }
}

impl<T: Dist > Iterator for NativeAtomicLocalDataIter<T> {
    type Item = NativeAtomicElement<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.array.num_elems_local() {
            let index = self.index;
            self.index += 1;
            Some(NativeAtomicElement {
                array: self.array.clone(),
                local_index: index,
            })
        } else {
            None
        }
    }
}

impl<T: Dist + NativeAtomic + std::default::Default > NativeAtomicArray<T> {
    //Sync + Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> NativeAtomicArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = NativeAtomicByteArray {
                array: array.clone().into(),
                orig_t: NativeAtomicType::from::<T>(),
            };

            for pe in 0..op_bufs.len() {
                op_bufs[pe] = func(bytearray.clone());
            }
        }

        NativeAtomicArray {
            array: array,
            orig_t: NativeAtomicType::from::<T>(),
        }
    }
}

impl<T: Dist + std::default::Default> NativeAtomicArray<T> {
    //Sync + Send + Copy  == Dist
    pub(crate) fn new_internal<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> NativeAtomicArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = NativeAtomicByteArray {
                array: array.clone().into(),
                orig_t: NativeAtomicType::from::<T>(),
            };

            for pe in 0..op_bufs.len() {
                op_bufs[pe] = func(bytearray.clone());
            }
        }

        NativeAtomicArray {
            array: array,
            orig_t: NativeAtomicType::from::<T>(),
        }
    }
}

impl<T: Dist > NativeAtomicArray<T> {

    pub fn native_type(&self) -> NativeAtomicType{
        self.orig_t
    }
    pub(crate) fn get_element(&self, index: usize) -> NativeAtomicElement<T>{
        NativeAtomicElement{
            array: self.clone(),
            local_index: index,
        }
    }
}

impl<T: Dist> NativeAtomicArray<T> {
    pub fn wait_all(&self) {
        self.array.wait_all();
    }
    pub fn barrier(&self) {
        self.array.barrier();
    }
    pub(crate) fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }

    pub fn use_distribution(self, distribution: Distribution) -> Self {
        NativeAtomicArray {
            array: self.array.use_distribution(distribution),
            orig_t: self.orig_t
        }
    }

    pub fn num_pes(&self) -> usize {
        self.array.num_pes()
    }

    #[doc(hidden)]
    pub fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }

    #[doc(hidden)]
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }


    pub fn local_data(&self) -> NativeAtomicLocalData<T> {
        NativeAtomicLocalData {
            array: self.clone(),
        }
    }

    pub fn mut_local_data(&self) -> NativeAtomicLocalData<T> {
        NativeAtomicLocalData {
            array: self.clone(),
        }
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    #[doc(hidden)]
    pub unsafe fn __local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        NativeAtomicArray {
            array: self.array.sub_array(range),
            orig_t: self.orig_t,
        }
    }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        self.array.into()
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        self.array.into()
    }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        self.array.into()
    }

    pub fn into_atomic2(self) -> Atomic2Array<T> {
        self.array.into()
    }

}


impl<T: Dist> From<UnsafeArray<T>> for NativeAtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        array.block_on_outstanding(DarcMode::NativeAtomicArray);
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let bytearray = NativeAtomicByteArray {
                array: array.clone().into(),
                orig_t: NativeAtomicType::from::<T>(),
            };
            let mut op_bufs = array.inner.data.op_buffers.write();
            for _pe in 0..array.inner.data.num_pes {
                op_bufs.push(func(bytearray.clone()))
            }
        }
        NativeAtomicArray {
            array: array,
            orig_t: NativeAtomicType::from::<T>(),
        }
    }
}

impl<T: Dist > From<NativeAtomicArray<T>> for NativeAtomicByteArray {
    fn from(array: NativeAtomicArray<T>) -> Self {
        NativeAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }
    }
}
impl<T: Dist > From<NativeAtomicArray<T>> for AtomicByteArray {
    fn from(array: NativeAtomicArray<T>) -> Self {
        AtomicByteArray::NativeAtomicByteArray(NativeAtomicByteArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        })
    }
}
impl<T: Dist > From<NativeAtomicByteArray> for NativeAtomicArray<T> {
    fn from(array: NativeAtomicByteArray) -> Self {
        NativeAtomicArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }
    }
}
impl<T: Dist > From<NativeAtomicByteArray> for AtomicArray<T> {
    fn from(array: NativeAtomicByteArray) -> Self {
        NativeAtomicArray {
            array: array.array.into(),
            orig_t: array.orig_t,
        }.into()
    }
}

impl<T: Dist> private::ArrayExecAm<T> for NativeAtomicArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist > private::LamellarArrayPrivate<T> for NativeAtomicArray<T> {
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
}

impl<T: Dist > LamellarArray<T> for NativeAtomicArray<T> {
    fn my_pe(&self) -> usize {
        self.array.my_pe()
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn num_elems_local(&self) -> usize {
        self.num_elems_local()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn barrier(&self) {
        self.barrier();
    }
    fn wait_all(&self) {
        self.array.wait_all()
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
}

impl<T: Dist > LamellarWrite for NativeAtomicArray<T> {}
impl<T: Dist > LamellarRead for NativeAtomicArray<T> {}

impl<T: Dist > SubArray<T> for NativeAtomicArray<T> {
    type Array = NativeAtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist  + std::fmt::Debug> NativeAtomicArray<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist  + std::fmt::Debug> ArrayPrint<T> for NativeAtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}


impl<T: Dist + AmDist + 'static> NativeAtomicArray<T> {
    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce(op)
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce("sum")
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce("prod")
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce("max")
    }
}



//for use within RDMA active messages to atomically read/write values
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
pub enum NativeAtomicType{ 
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


impl NativeAtomicType{
    fn from<T: 'static>() -> NativeAtomicType{
        let t =  TypeId::of::<T>();
        if t == TypeId::of::<i8>() {NativeAtomicType::I8}
        else if t == TypeId::of::<i16>() {NativeAtomicType::I16}
        else if t == TypeId::of::<i32>() {NativeAtomicType::I32}
        else if t == TypeId::of::<i64>() {NativeAtomicType::I64}
        else if t == TypeId::of::<isize>() {NativeAtomicType::Isize}
        else if t == TypeId::of::<u8>() {NativeAtomicType::U8}
        else if t == TypeId::of::<u16>() {NativeAtomicType::U16}
        else if t == TypeId::of::<u32>() {NativeAtomicType::U32}
        else if t == TypeId::of::<u64>() {NativeAtomicType::U64}
        else if t == TypeId::of::<usize>() {NativeAtomicType::Usize} 
        else { panic!("invalid native atomic type!")} 
        
    }
    fn size(&self) -> usize{
        match self{
            NativeAtomicType::I8 => std::mem::size_of::<i8>(),
            NativeAtomicType::I16 => std::mem::size_of::<i16>(),
            NativeAtomicType::I32 => std::mem::size_of::<i32>(),
            NativeAtomicType::I64 => std::mem::size_of::<i64>(),
            NativeAtomicType::Isize => std::mem::size_of::<isize>(),
            NativeAtomicType::U8 => std::mem::size_of::<u8>(),
            NativeAtomicType::U16 => std::mem::size_of::<u16>(),
            NativeAtomicType::U32 => std::mem::size_of::<u32>(),
            NativeAtomicType::U64 => std::mem::size_of::<u64>(),
            NativeAtomicType::Usize => std::mem::size_of::<usize>(),
        }
    }
    fn load(&self,src_addr: *mut u8, dst_addr: *mut u8){
        unsafe{
            match self{
                NativeAtomicType::I8 =>{
                    let dst = &mut *(dst_addr as *mut i8);
                    let src = &*(src_addr as *mut i8 as *mut AtomicI8);
                    *dst = src.load(Ordering::SeqCst);
                },
                NativeAtomicType::I16 =>{
                    let dst = &mut*(dst_addr as *mut i16);
                    let src = &*(src_addr as *mut i16 as *mut AtomicI16);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::I32 =>{
                    let dst = &mut*(dst_addr as *mut i32);
                    let src = &*(src_addr as *mut i32 as *mut AtomicI32);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::I64 =>{
                    let dst = &mut*(dst_addr as *mut i64);
                    let src = &*(src_addr as *mut i64 as *mut AtomicI64);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::Isize =>{
                    let dst = &mut*(dst_addr as *mut isize);
                    let src = &*(src_addr as *mut isize as *mut AtomicIsize);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::U8 =>{
                    let dst = &mut*(dst_addr as *mut u8);
                    let src = &*(src_addr as *mut u8 as *mut AtomicU8);
                    *dst = src.load(Ordering::SeqCst);
                },
                NativeAtomicType::U16 =>{
                    let dst = &mut*(dst_addr as *mut u16);
                    let src = &*(src_addr as *mut u16 as *mut AtomicU16);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::U32 =>{
                    let dst = &mut*(dst_addr as *mut u32);
                    let src = &*(src_addr as *mut u32 as *mut AtomicU32);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::U64 =>{
                    let dst = &mut*(dst_addr as *mut u64);
                    let src = &*(src_addr as *mut u64 as *mut AtomicU64);
                    *dst = src.load(Ordering::SeqCst);
                }
                NativeAtomicType::Usize =>{
                    let dst = &mut*(dst_addr as *mut usize);
                    let src = &*(src_addr as *mut usize as *mut AtomicUsize);
                    *dst = src.load(Ordering::SeqCst);
                }
            }
        }
    }

    fn store(&self,src_addr: *const u8, dst_addr: *mut u8){
        unsafe{
            match self{
                NativeAtomicType::I8 =>{
                    let dst = &*(dst_addr as *mut i8 as *mut AtomicI8);
                    let src = *(src_addr as *mut i8);
                    dst.store(src,Ordering::SeqCst);
                },
                NativeAtomicType::I16 =>{
                    let dst = &*(dst_addr as *mut i16 as *mut AtomicI16);
                    let src = *(src_addr as *mut i16);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::I32 =>{
                    let dst = &*(dst_addr as *mut i32 as *mut AtomicI32);
                    let src = *(src_addr as *mut i32);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::I64 =>{
                    let dst = &*(dst_addr as *mut i64 as *mut AtomicI64);
                    let src = *(src_addr as *mut i64);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::Isize =>{
                    let dst = &*(dst_addr as *mut isize as *mut AtomicIsize);
                    let src = *(src_addr as *mut isize);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::U8 =>{
                    let dst = &*(dst_addr as *mut u8 as *mut AtomicU8);
                    let src = *(src_addr as *mut u8);
                    dst.store(src,Ordering::SeqCst);
                },
                NativeAtomicType::U16 =>{
                    let dst = &*(dst_addr as *mut u16 as *mut AtomicU16);
                    let src = *(src_addr as *mut u16);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::U32 =>{
                    let dst = &*(dst_addr as *mut u32 as *mut AtomicU32);
                    let src = *(src_addr as *mut u32);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::U64 =>{
                    let dst = &*(dst_addr as *mut u64 as *mut AtomicU64);
                    let src = *(src_addr as *mut u64);
                    dst.store(src,Ordering::SeqCst);
                }
                NativeAtomicType::Usize =>{
                    let dst = &*(dst_addr as *mut usize as *mut AtomicUsize);
                    let src = *(src_addr as *mut usize);
                    dst.store(src,Ordering::SeqCst);
                }
            }
        }
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for NativeAtomicArray<T>
// {
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         self.array.get_reduction_op(op)
//     }
//     fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.reduce(op)
//     }
//     fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.sum()
//     }
//     fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.max()
//     }
//     fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.prod()
//     }
// }
