
use crate::array::iterator::distributed_iterator::{
    DistIter,DistIterMut, DistIteratorLauncher, DistributedIterator,
};
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
// use crate::array::private::LamellarArrayPrivate;
use crate::darc::{Darc,DarcMode};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::{Dist};
use std::sync::Arc;
use core::marker::PhantomData;
use parking_lot::{Mutex,MutexGuard};
use std::sync::atomic::Ordering;
use std::any::TypeId;
use std::collections::HashSet;

type AddFn = fn(*const u8, AtomicArray<u8>, usize) -> LamellarArcAm;
type LocalAddFn = fn(*const u8, AtomicArray<u8>, usize);

lazy_static! {
    static ref ADD_OPS: HashMap<TypeId, (AddFn,LocalAddFn)> = {
        let mut map = HashMap::new();
        for add in crate::inventory::iter::<AtomicArrayAdd> {
            map.insert(add.id.clone(),(add.add,add.local));
        }
        map
        // map.insert(TypeId::of::<f64>(), f64_add::add as AddFn );
    };
}

lazy_static! {
    pub(crate) static ref NATIVE_ATOMICS: HashSet<TypeId> = {
        let mut map = HashSet::new();
        map.insert(TypeId::of::<u8>());
        map.insert(TypeId::of::<u16>());
        map.insert(TypeId::of::<u32>());
        map.insert(TypeId::of::<u64>());
        map.insert(TypeId::of::<usize>());
        map.insert(TypeId::of::<i8>());
        map.insert(TypeId::of::<i16>());
        map.insert(TypeId::of::<i32>());
        map.insert(TypeId::of::<i64>());
        map.insert(TypeId::of::<isize>());
        map
    };
}

pub struct AtomicArrayAdd{
    pub id: TypeId,
    pub add: AddFn,
    pub local: LocalAddFn,
}

crate::inventory::collect!(AtomicArrayAdd);

mod atomic_private{
    use parking_lot::Mutex;
    pub trait LocksInit{
        fn init(&self, local_len: usize) -> Option<Vec<Mutex<()>>>;
    }
}

pub trait AtomicOps {
    type Atomic;
    fn as_atomic(&self) -> &Self::Atomic;
    fn fetch_add(&self, val: Self)->Self;
    fn fetch_sub(&mut self, val: Self)->Self;
    fn load(&mut self) ->Self;
    fn store(&mut self, val: Self);
    fn swap(&mut self, val: Self) ->Self;
}


macro_rules! impl_atomic_ops{
    { $A:ty, $B:ty } => {
        impl AtomicOps for $A {
            // there is an equivalent call in nightly rust
            // Self::Atomic::from_mut()... we will switch to that once stablized; 
            type Atomic = $B;
            fn as_atomic(&self) -> &Self::Atomic{
                use std::mem::align_of;
                let [] = [(); align_of::<$B>() - align_of::<$A>()];
                // SAFETY:
                //  - the mutable reference guarantees unique ownership.
                //  - the alignment of `$int_type` and `Self` is the
                //    same, as promised by $cfg_align and verified above.
                unsafe { &*(self as *const $A as *mut $A as *mut Self::Atomic) }
            }
            fn fetch_add(&self, val: Self) ->Self {
                self.as_atomic().fetch_add(val, Ordering::SeqCst)
            }
            fn fetch_sub(&mut self, val: Self,) ->Self{
                self.as_atomic().fetch_sub(val, Ordering::SeqCst)
            }
            fn load(&mut self) ->Self{
                self.as_atomic().load(Ordering::SeqCst)
            }
            fn store(&mut self, val: Self){
                self.as_atomic().store(val,Ordering::SeqCst)
            }
            fn swap(&mut self, val: Self) ->Self{
                self.as_atomic().swap(val, Ordering::SeqCst)
            }
        }
    }
}

use std::sync::atomic::AtomicI8;
impl_atomic_ops!{i8,AtomicI8}
use std::sync::atomic::AtomicI16;
impl_atomic_ops!{i16,AtomicI16}
use std::sync::atomic::AtomicI32;
impl_atomic_ops!{i32,AtomicI32}
use std::sync::atomic::AtomicI64;
impl_atomic_ops!{i64,AtomicI64}
use std::sync::atomic::AtomicIsize;
impl_atomic_ops!{isize,AtomicIsize}
use std::sync::atomic::AtomicU8;
impl_atomic_ops!{u8,AtomicU8}
use std::sync::atomic::AtomicU16;
impl_atomic_ops!{u16,AtomicU16}
use std::sync::atomic::AtomicU32;
impl_atomic_ops!{u32,AtomicU32}
use std::sync::atomic::AtomicU64;
impl_atomic_ops!{u64,AtomicU64}
use std::sync::atomic::AtomicUsize;
impl_atomic_ops!{usize,AtomicUsize}

#[lamellar_impl::AmDataRT(Clone)]
pub struct AtomicArray<T: Dist > {
    locks: Darc<Option<Vec<Mutex<()>>>>,
    orig_t_size: usize,
    pub(crate) array: UnsafeArray<T>,
}

//#[prof]
impl<T: Dist + std::default::Default + 'static > AtomicArray<T> { //Sync + Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> AtomicArray<T> {
        let array =  UnsafeArray::new(team.clone(),array_size,distribution);
        let locks = if NATIVE_ATOMICS.get(&TypeId::of::<T>()).is_some(){
            None
        }
        else{
            let mut vec = vec!{};
            for _i in 0..array.num_elems_local(){
                vec.push(Mutex::new(()));
            }
            Some(vec)
        };
        AtomicArray{
            locks: Darc::new(team,locks).unwrap(),
            orig_t_size: std::mem::size_of::<T>(),
            array: array
        }
    }
}
impl<T: Dist > AtomicArray<T> {
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
        AtomicArray{
            locks: self.locks.clone(),
            orig_t_size: self.orig_t_size,
            array: self.array.use_distribution(distribution)
        }
    }

    pub fn num_pes(&self) -> usize {
        self.array.num_pes()
    }

    #[doc(hidden)]
    pub fn pe_for_dist_index(&self, index: usize) -> usize {
        self.array.pe_for_dist_index(index)
    }

    #[doc(hidden)]
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        self.array.pe_offset_for_dist_index(pe,index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }


    //these change into 
    // load, store, swap
    // pub unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
    //     // self.array.get_unchecked(index,buf)
    // }
    // pub  fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
    //     // self.array.get_unchecked(index,buf)
    // }
    // pub fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead >(&self, index: usize, buf: U) {
    //     // self.array.get_unchecked(index,buf)
    // }
    // pub fn at(&self, index: usize) -> T {
    //     self.array.at(index)
    // }
    
    #[doc(hidden)]
    pub unsafe fn local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    #[doc(hidden)]
    pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }

    #[doc(hidden)]
    pub unsafe fn to_base_inner<B: Dist + 'static>(self) -> AtomicArray<B> {
        let array =  self.array.to_base_inner();
        AtomicArray{
            locks: self.locks.clone(),
            orig_t_size: self.orig_t_size,
            array: array
        }
    }

    #[doc(hidden)]
    pub unsafe fn as_base_inner<B: Dist + 'static>(&self) -> AtomicArray<B> {
        // todo!("need to do some aliasing of the original lock");
        // println!();

        let array =  self.array.as_base_inner();
        // let temp: T = array.local_as_slice()[0];
        AtomicArray{
            locks: self.locks.clone(),
            orig_t_size: self.orig_t_size,
            array: array
        }
    }

    // pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
    //     self.array.local_as_mut_ptr()
    // }

    pub fn dist_iter(&self) -> DistIter<'static, T, AtomicArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    pub fn dist_iter_mut(&self) -> DistIterMut<'static, T, AtomicArray<T>> {
        DistIterMut::new(self.clone().into(), 0, 0)
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T,AtomicArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.array.team().clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T,AtomicArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.array.team().clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> AtomicArray<T> {
        AtomicArray {
            locks: self.locks.clone(),
            orig_t_size: self.orig_t_size,
            array: self.array.sub_array(range)
        }
    }
    // pub(crate) fn team(&self) -> Arc<LamellarTeamRT> {
    //     self.array.team()
    // }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        self.array.block_on_outstanding(DarcMode::UnsafeArray);
        self.array
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        self.array.block_on_outstanding(DarcMode::LocalOnlyArray);
        LocalOnlyArray{
            array: self.array,
            _unsync: PhantomData
        }
    }

    #[doc(hidden)]
    pub fn lock_index(&self, index: usize) -> Option<Vec<MutexGuard<()>>> {
        if let Some(ref locks) = *self.locks {
            let start_index = (index + std::mem::size_of::<T>()) / self.orig_t_size;
            let end_index = ((index+1) + std::mem::size_of::<T>()) / self.orig_t_size;
            let mut guards = vec![];
            for i in start_index..(end_index+1){
                guards.push(locks[i].lock())
            }
            Some(guards)
        }
        else{
            None
        }
    }
}

// impl <T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> AtomicArray<T> {
//     pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce(op)
//     }
//     pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce("sum")
//     }
//     pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce("prod")
//     }
//     pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce("max")
//     }
// }

impl<T: Dist> DistIteratorLauncher
    for AtomicArray<T>
{
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        self.array.for_each(iter,op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.array.for_each_async(iter,op)
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T>
    for AtomicArray<T>
{
    
    fn local_as_ptr(&self) -> *const T{
        self.array.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T{
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> usize {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        self.array.pe_offset_for_dist_index(pe,index)
    }
}

impl<T: Dist> LamellarArray<T>
    for AtomicArray<T>
{
    fn my_pe(&self) -> usize{
        self.array.my_pe()
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>{
        self.array.team().clone()
    }    
    fn num_elems_local(&self) -> usize{
        self.num_elems_local()
    }
    fn len(&self) -> usize{
        self.len()
    }
    fn barrier(&self){
        self.barrier();
    }
    fn wait_all(&self){
        self.array.wait_all()
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    
}
impl<T: Dist> LamellarWrite for AtomicArray<T> {}
impl<T: Dist> LamellarRead for AtomicArray<T> {}

impl<T: Dist> LamellarArrayRead<T> for AtomicArray<T> {
    unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.array.get_unchecked(index, buf)
    }
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.array.iget(index, buf)
    }
    fn iat(&self, index: usize) -> T {
        self.array.iat(index)
    }
}

impl<T: Dist> LamellarArrayWrite<T> for AtomicArray<T> {
    // fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.array.put_unchecked(index, buf)
    // }
}


impl<T: Dist> SubArray<T>
    for AtomicArray<T>
{
    type Array = AtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize{
        self.array.global_index(sub_index)
    }
}

// impl<T: Dist + std::ops::AddAssign> AtomicArray<T>
// // where
// // AtomicArray<T>: ArrayOps<T>,
// {
//     pub fn add(
//         &self,
//         index: usize,
//         val: T,
//     ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
//         <&AtomicArray<T> as ArrayOps<T>>::add(&self, index, val) // this is implemented automatically by a proc macro
//                                                                // because this gets implented as an active message, we need to know the full type
//                                                                // when the proc macro is called, all the integer/float times are handled by runtime,
//                                                                // but users are requried to call a proc macro on their type to get the functionality
//     }
// }

impl<T: Dist + std::ops::AddAssign + 'static> ArrayOps<T> for AtomicArray<T>{
    fn add(&self, index:usize, val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        println!("add ArrayOps<T> for &AtomicArray<T> ");
        if let Some(funcs) = ADD_OPS.get(&TypeId::of::<T>()){
            let pe = self.pe_for_dist_index(index);
            let local_index = self.pe_offset_for_dist_index(pe,index);
            let array: AtomicArray<u8> = unsafe {self.as_base_inner()};
            if pe == self.my_pe(){
                funcs.1(&val as *const T as *const u8, array,local_index);
                None
            }
            else{
                let am = funcs.0(&val as *const T as *const u8, array,local_index);
                Some(self.array.dist_add(index,am))
            }
        }
        else{
            let name = std::any::type_name::<T>().split("::").last().unwrap();
            panic!("the type {:?} has not been registered! this typically means you need to derive \"ArrayOps\" for the type . e.g. 
            #[derive(lamellar::ArrayOps)]
            struct {:?}{{
                ....
            }}
            note this also requires the type to impl Serialize + Deserialize, you can manually derive these, or use the lamellar::AmData attribute proc macro, e.g.
            #[lamellar::AMData(ArrayOps, any other traits you derive)]
            struct {:?}{{
                ....
            }}",name,name,name);
        }
    }
}



#[macro_export]
macro_rules! atomic_add{
    ($a:ty, $name:ident) => {
        impl ArrayAdd<$a> for AtomicArray<$a> { //for atomic array I need do this in the macro as well.
            fn dist_add(
                &self,
                index: usize,
                func: Arc<dyn RemoteActiveMessage + Send + Sync>,
            ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
                println!("native dist_add ArrayAdd<{}>  for AtomicArray<{}>  ",stringify!($a),stringify!($a));
                (&self).dist_add(index,func)
            }
            fn local_add(&self, index: usize, val: $a) {
                println!("native local_add ArrayAdd<{}>  for AtomicArray<{}>  ",stringify!($a),stringify!($a));
                use $crate::array::AtomicOps;
                unsafe {self.local_as_mut_slice()[index].fetch_add(val)};
            }
        }

        fn $name(val: *const u8, array: $crate::array::AtomicArray<u8>, index: usize){
            let val = unsafe {*(val as  *const $a)};
            println!("native {} ",stringify!($name));
            let array = unsafe {array.to_base_inner::<$a>()};
            use $crate::array::AtomicOps;
            unsafe {array.local_as_mut_slice()[index].fetch_add(val)};
        }
    };
}

#[macro_export]
macro_rules! non_atomic_add{
    ($a:ty, $name:ident) => {
        impl ArrayAdd<$a> for AtomicArray<$a> { //for atomic array I need do this in the macro as well.
            fn dist_add(
                &self,
                index: usize,
                func: Arc<dyn RemoteActiveMessage + Send + Sync>,
            ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
                println!("mutex dist_add ArrayAdd<{}> for AtomicArray<{}> ",stringify!($a),stringify!($a));
                (&self).dist_add(index,func)
            }
            fn local_add(&self, index: usize, val: $a) {
                println!("mutex local_add ArrayAdd<{}> for AtomicArray<{}>  ",stringify!($a),stringify!($a));
                let _lock = self.lock_index(index);
                unsafe {self.local_as_mut_slice()[index] += val};
            }
        }

        fn $name(val: *const u8, array: $crate::array::AtomicArray<u8>, index: usize){
            let val = unsafe {*(val as  *const $a)};
            let array = unsafe {array.to_base_inner::<$a>()};
            println!("mutex {} ",stringify!($name));
            let _lock = array.lock_index(index).expect("no lock exists!");
            unsafe {array.local_as_mut_slice()[index] += val};
        }
    };
}

#[macro_export]
macro_rules! AtomicArray_create_add{
    (u8, $name:ident) => {
       $crate::atomic_add!(u8, $name);
    };
    (u16, $name:ident) => {
        $crate::atomic_add!(u16, $name);
    };
    (u32, $name:ident) => {
        $crate::atomic_add!(u32, $name);
    };
    (u64, $name:ident) => {
        $crate::atomic_add!(u64, $name);
    };
    (usize, $name:ident) => {
        $crate::atomic_add!(usize, $name);
    };
    (i8, $name:ident) => {
        $crate::atomic_add!(i8, $name);
     };
     (i16, $name:ident) => {
         $crate::atomic_add!(i16, $name);
     };
     (i32, $name:ident) => {
         $crate::atomic_add!(i32, $name);
     };
     (i64, $name:ident) => {
         $crate::atomic_add!(i64, $name);
     };
     (isize, $name:ident) => {
         $crate::atomic_add!(isize, $name);
     };
    ($a:ty, $name:ident) => {
        $crate::non_atomic_add!($a, $name);
    };
}

#[macro_export]
macro_rules! AtomicArray_inventory_add{
    ($id:ident, $add:ident, $local:ident) => {
        inventory::submit!{
            #![crate = $crate]
            $crate::array::AtomicArrayAdd{
                id: std::any::TypeId::of::<$id>(),
                add: $add,
                local: $local
            }
        }
    };
}

impl<T: Dist + std::ops::AddAssign> ArrayAdd<T> for &AtomicArray<T> {
    fn dist_add(
        &self,
        index: usize,
        func: Arc<dyn RemoteActiveMessage + Send + Sync>,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        println!("dist_add ArrayAdd<T> for &AtomicArray<T>");
        self.array.dist_add(index,func)
    }
    fn local_add(&self, index: usize, val: T) {
        println!("local_add ArrayAdd<T> for &AtomicArray<T>");
        let _lock = self.lock_index(index);
        self.array.local_add(index,val);
    }
}

// impl<T: Dist + std::ops::AddAssign> ArrayOps<T> for AtomicArray<T>{
//     fn add(&self, index:usize, val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
//         let pe = self.pe_for_dist_index(index);
//         let local_index = self.pe_offset_for_dist_index(pe,index);
//         if pe == self.my_pe(){
//             self.local_add(local_index,val);
//             None
//         }
//         else{
//             None
//         }
//         // else{
//         //     Some(self.dist_add(
//         //         index,
//         //         Arc::new (#add_name_am{
//         //             data: self.clone(),
//         //             local_index: local_index,
//         //             val: val,
//         //         })
//         //     ))
//         // }
//     }
// }

impl<T: Dist + std::fmt::Debug>  AtomicArray<T> {
    pub fn print(&self) {
       self.array.print();
   }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for 
    AtomicArray<T>
{
    fn print(&self) {
        self.array.print()
    }
}


// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for AtomicArray<T>
// {
    
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         // unsafe {
//         REDUCE_OPS
//             .get(&(std::any::TypeId::of::<T>(), op))
//             .expect("unexpected reduction type")(
//             self.clone().to_base_inner::<u8>().into(),
//             self.inner.team.num_pes(),
//         )
//         // }
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

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> IntoIterator
//     for &'a AtomicArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }

// impl < T> Drop for AtomicArray<T>{
//     fn drop(&mut self){
//         println!("dropping array!!!");
//     }
// }
