use crate::{active_messaging::*, LamellarTeam, RemoteMemoryRegion}; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
                                                                    // use crate::lamellae::Lamellae;
                                                                    // use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion
};

use enum_dispatch::enum_dispatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::marker::PhantomData;

pub(crate) mod r#unsafe;
pub use r#unsafe::UnsafeArray;

pub(crate) type ReduceGen =
    fn(LamellarArray<u8>, usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>;

lazy_static! {
    pub(crate) static ref REDUCE_OPS: HashMap<(std::any::TypeId, String), ReduceGen> = {
        let mut temp = HashMap::new();
        for reduction_type in crate::inventory::iter::<ReduceKey> {
            temp.insert(
                (reduction_type.id.clone(), reduction_type.name.clone()),
                reduction_type.gen,
            );
        }
        temp
    };
}

pub struct ReduceKey {
    pub id: std::any::TypeId,
    pub name: String,
    pub gen: ReduceGen,
}
crate::inventory::collect!(ReduceKey);

lamellar_impl::generate_reductions_for_type_rt!(u8, u16, u32, u64, u128, usize);
lamellar_impl::generate_reductions_for_type_rt!(i8, i16, i32, i64, i128, isize);
lamellar_impl::generate_reductions_for_type_rt!(f32, f64);

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
pub enum Distribution {
    Block,
    Cyclic,
}

pub enum Array {
    Unsafe,
}

#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone)]
pub enum ArrayOp {
    Put,
    Get,
    Add,
}
#[derive(serde::Serialize, serde::Deserialize, Clone)]
enum ArrayOpInput {
    Add(usize, Vec<u8>),
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>)]
#[derive(Clone)]
pub enum LamellarArrayInput<T: Dist + 'static> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>),
    LocalMemRegion(LocalMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
    // Vec(Vec<T>),
}

impl<T: Dist + 'static> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Arc<LamellarTeam>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist + 'static> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Arc<LamellarTeam>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: Dist + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeam>) -> Self {
//         let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Arc<LamellarTeam>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Arc<LamellarTeam>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Arc<LamellarTeam>) -> U {
        U::my_from(self, team)
    }
}

pub trait ArrayOps<T> {
    fn add(&self, index: usize, val: T) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;
}

pub trait SubArray<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> {
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarArray<T>;
}


#[enum_dispatch(LamellarArrayRDMA<T>,LamellarArrayReduce<T>,ArrayOps<T>,SubArray<T>)] //,LamellarArrayIterator<T>)]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum LamellarArray<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> {
    UnsafeArray(UnsafeArray<T>),
}
impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArray<T> {
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarArray<T> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.sub_array(range).into(),
        }
    }
    // pub fn local_mem_region(&self) -> &MemoryRegion<T> {
    //     match self{
    //         LamellarArray::UnsafeArray(inner) => inner.local_mem_region(),
    //     }
    // }
    pub fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U){
        match self {
            LamellarArray::UnsafeArray(inner) => inner.put(index,buf),
        }
    }
    pub fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U){
        match self {
            LamellarArray::UnsafeArray(inner) => inner.get(index,buf),
        }
    }
    pub(crate) fn local_as_ptr(&self) -> *const T {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.local_as_ptr(),
        }
    }
    // pub fn local_as_mut_ptr(&self) -> *mut T {
    //     match self {
    //         LamellarArray::UnsafeArray(inner) => inner.local_as_mut_ptr(),
    //     }
    // }
    // pub(crate) fn for_each<I,F>(&self, iter: &I, op: F)
    // where
    //     I: LamellarIterator,
    //     F: Fn(&T) + Sync + Send + Clone + 'static,
    // {
    //     match self {
    //         LamellarArray::UnsafeArray(inner) => inner.for_each(iter,op),
    //     }
    // }
    // pub fn for_each_mut<F>(&self, op: F)
    // where
    //     F: Fn(&mut T) + Sync + Send + Clone + 'static,
    // {
    //     match self {
    //         LamellarArray::UnsafeArray(inner) => inner.for_each_mut(op),
    //     }
    // }
    pub fn dist_iter(&self) -> DistIter<'static, T> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.dist_iter(),
        }
    }
}

impl<
        T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::ops::AddAssign + 'static,
    > LamellarArray<T>
{
    pub fn dist_add(
        &self,
        index: usize,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.dist_add(index, func),
        }
    }
    pub fn local_add(&self, index: usize, val: T) {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.local_add(index, val),
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DarcSerde
    for LamellarArray<T>
{
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        match self {
            LamellarArray::UnsafeArray(inner) => DarcSerde::ser(inner, num_pes, cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        match self {
            LamellarArray::UnsafeArray(inner) => DarcSerde::des(inner, cur_pe),
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>  LamellarIteratorLauncher for LamellarArray<T>{
    fn for_each<I,F>(&self, iter: &I, op: F)
    where
        I: LamellarIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static
    {
        match self{
            LamellarArray::UnsafeArray(inner) => inner.for_each(iter,op)
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
struct ForEach<I, F>
where
    I: LamellarIterator,
    F: Fn(I::Item) + Sync + Send,
{
    op: F,
    // data: LamellarArray<T>,
    data: I,
    start_i: usize,
    end_i: usize,
}
#[lamellar_impl::rt_am_local]
impl< I, F> LamellarAm for ForEach<I, F>
where
    I: LamellarIterator + 'static,
    F: Fn(I::Item) + Sync + Send + 'static,
{
    fn exec(&self) {
        // self.data.local_as_slice()[self.start_i..self.end_i].iter().for_each((self.op));
        // println!("{:?} {:?} {:?}",lamellar::current_pe,self.start_i,self.end_i);
        // for elem in self.data.local_as_slice()[self.start_i..self.end_i].into_iter() {
        //     (&self.op)(elem)
        // }
        let mut iter = self.data.init(self.start_i,self.end_i);
        while let Some(elem) = iter.next(){
            (&self.op)(elem)
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
struct ForEachMut<T, F>
where
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(&mut T) + Sync + Send,
{
    op: F,
    data: LamellarArray<T>,
    start_i: usize,
    end_i: usize,
}
#[lamellar_impl::rt_am_local]
impl<T, F> LamellarAm for ForEachMut<T, F>
where
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(&mut T) + Sync + Send + 'static,
{
    fn exec(&self) {
        // self.data.local_as_slice()[self.start_i..self.end_i].iter().for_each((self.op));
        // println!("{:?} {:?} {:?}",lamellar::current_pe,self.start_i,self.end_i);
        for elem in self.data.local_as_mut_slice()[self.start_i..self.end_i].iter_mut() {
            (&self.op)(elem)
        }
    }
}

#[enum_dispatch]
pub trait LamellarArrayRDMA<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
{
    fn len(&self) -> usize;
    fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    fn local_as_slice(&self) -> &[T];
    fn local_as_mut_slice(&self) -> &mut [T];
    fn to_base<B: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>(
        self,
    ) -> LamellarArray<B>;
}

// #[enum_dispatch]
pub trait LamellarIteratorLauncher{
    fn for_each<I,F>(&self, iter: &I, op: F)
    where
        I: LamellarIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,;
}

pub trait LamellarArrayReduce<T>: LamellarArrayRDMA<T>
where
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
{
    fn wait_all(&self);
    fn get_reduction_op(&self, op: String) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}

#[derive(Clone)]
pub struct  DistIter<'a,T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>{
    data: LamellarArray<T>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl <T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIter<'static,T>{
    pub fn for_each<F>(&self,op: F)
    where
    F: Fn(&T) + Sync + Send + Clone + 'static{
        self.data.clone().for_each(self,op);
    }
}
impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> LamellarIterator for DistIter<'a,T>
// where  &'a T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned
{
    type Item = &'a T;
    type Array = LamellarArray<T>;
    fn init(&self,start_i: usize,end_i: usize) -> Self{
        DistIter{
            data: self.data.clone(),
            cur_i: start_i,
            end_i: end_i,
            _marker: PhantomData
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item>{
        if self.cur_i < self.end_i{
            self.cur_i += 1;
            unsafe {self.data.local_as_ptr().offset((self.cur_i -1)as isize).as_ref()}
        }
        else{
            None
        }
    }
}


#[derive(Clone)]
pub struct Enumerate<I>{
    iter: I,
    count: usize
}
impl <I> Enumerate<I> 
where I: LamellarIterator 
{
    pub(crate) fn new(iter: I, count: usize) -> Enumerate<I> {
        Enumerate {iter, count}
    }
}

impl <I> Enumerate<I> 
where I: LamellarIterator + 'static
{
    pub fn for_each<F>(&self,op: F)
    where
    F: Fn((usize,<I as LamellarIterator>::Item)) + Sync + Send + Clone + 'static{
        self.iter.array().for_each(self,op);
    }
}

impl<I> LamellarIterator for Enumerate<I>
where  
    I: LamellarIterator,
{
    type Item = (usize, <I as LamellarIterator>::Item);
    type Array = <I as LamellarIterator>::Array;
    fn init(&self,start_i: usize,end_i: usize) -> Enumerate<I>{
        Enumerate::new(self.iter.init(start_i,end_i),start_i)
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item>{
        let a = self.iter.next()?;
        let i = self.count;
        self.count += 1;
        Some((i, a))
    }
}



pub trait LamellarIterator: Sync + Send + Clone{
    type Item;
    type Array: LamellarIteratorLauncher;
    fn init(&self,start_i: usize, end_i: usize) -> Self;
    fn array(&self) -> Self::Array;
    fn next(&mut self) -> Option<Self::Item>;
    fn enumerate(self) -> Enumerate<Self>{
        Enumerate::new(self,0)
    }
    // fn for_each<F>(self, op: F)
    // where
    //     F: Fn(Self::Item) + Sync + Send + Clone
    // {
    //     self.array().for_each(self,op);
    // }
}

//need to think about iteration a bit more
// use core::ptr::NonNull;
// use core::slice::Iter;
// use std::marker::PhantomData;
// pub trait LamellarArrayIterator<T>: LamellarArrayRDMA<T>
// where
//     T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
// {
//     fn iter(&self) -> LamellarArrayIter<'_, T>;
//     fn dist_iter(&self) -> LamellarArrayDistIter<'_, T>;
// }
// pub struct LamellarArrayIter<
//     'a,
//     T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
// > {
//     array: LamellarArray<T>,
//     buf_0: LocalMemoryRegion<T>,
//     buf_1: LocalMemoryRegion<T>,
//     index: usize,
//     ptr: NonNull<T>,
//     _marker: PhantomData<&'a T>,
// }

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
//     LamellarArrayIter<'a, T>
// {
//     fn new(array: LamellarArray<T>, team: Arc<LamellarTeam>) -> LamellarArrayIter<'a, T> {
//         let buf_0 = team.alloc_local_mem_region(1);
//         let ptr = NonNull::new(buf_0.as_mut_ptr().unwrap()).unwrap();
//         LamellarArrayIter {
//             array: array,
//             buf_0: buf_0,
//             buf_1: team.alloc_local_mem_region(1),
//             index: 0,
//             ptr: ptr,
//             _marker: PhantomData,
//         }
//     }
// }

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> Iterator
//     for LamellarArrayIter<'a, T>
// {
//     type Item = &'a T;
//     fn next(&mut self) -> Option<Self::Item> {
//         let res = if self.index < self.array.len() {
//             let buf_0_u8 = self.buf_0.clone().to_base::<u8>();
//             let buf_0_slice = unsafe { buf_0_u8.as_mut_slice().unwrap() };
//             let buf_1_u8 = self.buf_1.clone().to_base::<u8>();
//             let buf_1_slice = unsafe { buf_1_u8.as_mut_slice().unwrap() };
//             for i in 0..buf_0_slice.len() {
//                 buf_0_slice[i] = 0;
//                 buf_1_slice[i] = 1;
//             }
//             // println!("{:?} {:?} {:?}", self.index, buf_0_slice, buf_1_slice);
//             self.array.get(self.index, &self.buf_0);
//             self.array.get(self.index, &self.buf_1);
//             for i in 0..buf_0_slice.len() {
//                 while buf_0_slice[i] != buf_1_slice[i] {
//                     std::thread::yield_now();
//                 }
//             }
//             // println!("{:?} {:?} {:?}", self.index, buf_0_slice, buf_1_slice);
//             // if buf_0_slice[0] == buf_1_slice[0] {
//             self.index += 1;
//             Some(unsafe { self.ptr.as_ref() })
//             // } else {
//             //     None
//             // }
//             // else if
//         } else {
//             None
//         };
//         res
//     }
// }

// pub struct LamellarArrayDistIter<
//     'a,
//     T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
// > {
//     array: LamellarArray<T>,
//     index: usize,
//     ptr: NonNull<T>,
//     _marker: PhantomData<&'a T>,
// }

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
//     LamellarArrayDistIter<'a, T>
// {
//     fn new(array: LamellarArray<T>) -> LamellarArrayDistIter<'a, T> {
//         LamellarArrayDistIter {
//             array: array.clone(),
//             index: 0,
//             ptr: NonNull::new(array.local_as_mut_ptr()).unwrap(),
//             _marker: PhantomData,
//         }
//     }
// }

// pub trait DistributedIterator: Dist + serde::ser::Serialize + serde::de::DeserializeOwned {
//     type Item: Dist + serde::ser::Serialize + serde::de::DeserializeOwned;
//     fn for_each<F>(self, op: F)
//     where F: Fn(Self::Item) + Sync + Send;
//     fn for_each_mut<F>(self, op: F)
//     where F: Fn(Self::Item) + Sync + Send;
// }

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistributedIterator
//     for LamellarArrayDistIter<'a, T>
// {
//     type Item = &'a T;
//     fn for_each<OP>(self, op: OP){
//         self.array.for_each(op)
//     }
// }
