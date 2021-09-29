use crate::{active_messaging::*, LamellarTeam, RemoteMemoryRegion}; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
                                                                    // use crate::lamellae::Lamellae;
                                                                    // use crate::lamellar_arch::LamellarArchRT;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};
use crate::lamellar_request::{LamellarRequest};

use std::collections::HashMap;
use std::sync::Arc;
use enum_dispatch::enum_dispatch;

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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum Distribution {
    Block,
    Cyclic,
}

pub enum Array{
    Unsafe,
}

#[derive(Hash,std::cmp::PartialEq,std::cmp::Eq,Clone)]
pub enum ArrayOp {
    Put,
    Get,
    Add,
}
#[derive(serde::Serialize, serde::Deserialize, Clone)]
enum ArrayOpInput{
    Add(usize,Vec<u8>)
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


#[enum_dispatch(LamellarArrayRDMA<T>,LamellarArrayReduce<T>)] //,LamellarArrayIterator<T>)]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum LamellarArray<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> {
    UnsafeArray(UnsafeArray<T>),
}
impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArray<T> {
    // pub fn local_as_ptr(&self) -> *const T {
    //     match self {
    //         LamellarArray::UnsafeArray(inner) => inner.local_as_ptr(),
    //     }
    // }
    // pub fn local_as_mut_ptr(&self) -> *mut T {
    //     match self {
    //         LamellarArray::UnsafeArray(inner) => inner.local_as_mut_ptr(),
    //     }
    // }
    pub fn for_each<F>(&self,op: F)
    where F: Fn(&T) + Sync + Send + Clone + 'static{
        match self {
            LamellarArray::UnsafeArray(inner) => inner.for_each(op),
        }
    }
    pub fn for_each_mut<F>(&self,op: F)
    where F: Fn(&mut T) + Sync + Send + Clone + 'static{
        match self {
            LamellarArray::UnsafeArray(inner) => inner.for_each_mut(op),
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::ops::AddAssign + 'static> LamellarArray<T> {
    pub fn dist_add(&self, index: usize, func: LamellarArcAm) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>{
        match self {
            LamellarArray::UnsafeArray(inner) => inner.dist_add(index, func)
        }
    }
    pub fn local_add(&self, index: usize, val: T){
        match self {
            LamellarArray::UnsafeArray(inner) => inner.local_add(index,val),
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



#[lamellar_impl::AmLocalDataRT(Clone)]
struct ForEach<T,F>
where  
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(&T) + Sync + Send 
{
    op: F,
    data: LamellarArray<T>,
    start_i: usize,
    end_i: usize,
}
#[lamellar_impl::rt_am_local]
impl<T,F> LamellarAm for ForEach<T,F>
where  
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(&T) + Sync + Send + 'static{
    fn exec(&self){
        // self.data.local_as_slice()[self.start_i..self.end_i].iter().for_each((self.op));
        // println!("{:?} {:?} {:?}",lamellar::current_pe,self.start_i,self.end_i);
        for elem in self.data.local_as_slice()[self.start_i..self.end_i].into_iter() {
            (&self.op)(elem)
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
struct ForEachMut<T,F>
where  
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(&mut T) + Sync + Send 
{
    op: F,
    data: LamellarArray<T>,
    start_i: usize,
    end_i: usize,
}
#[lamellar_impl::rt_am_local]
impl<T,F> LamellarAm for ForEachMut<T,F>
where  
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    F: Fn(&mut T) + Sync + Send + 'static{
    fn exec(&self){
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
    fn as_base<B: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>(
        self,
    ) -> LamellarArray<B>;
}

pub trait LamellarArrayReduce<T>: LamellarArrayRDMA<T>
where
    T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
{
    fn wait_all(&self);
    fn get_reduction_op(&self, op: String) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn sum(&self)-> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self)-> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self)-> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
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
//             let buf_0_u8 = self.buf_0.clone().as_base::<u8>();
//             let buf_0_slice = unsafe { buf_0_u8.as_mut_slice().unwrap() };
//             let buf_1_u8 = self.buf_1.clone().as_base::<u8>();
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
