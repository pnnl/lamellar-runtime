use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};
use crate::{active_messaging::*, LamellarTeamRT};
// use crate::Darc;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures_lite::Future;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
// use serde::de::DeserializeOwned;

pub(crate) mod r#unsafe;
pub use r#unsafe::{
    operations::UnsafeArrayOpBuf,
    UnsafeArray, UnsafeByteArray,
};
pub(crate) mod read_only;
pub use read_only::{ReadOnlyArray, ReadOnlyByteArray};

pub(crate) mod local_only;
pub use local_only::LocalOnlyArray;

pub(crate) mod atomic;
pub use atomic::{
    // operations::{AtomicArrayOp, AtomicArrayOpBuf},
    AtomicArray,
    AtomicByteArray, //AtomicOps
};

pub(crate) mod generic_atomic;
pub use generic_atomic::{
    operations::GenericAtomicArrayOpBuf,
    GenericAtomicArray, GenericAtomicByteArray, GenericAtomicLocalData,
};

pub(crate) mod native_atomic;
pub use native_atomic::{
    operations::NativeAtomicArrayOpBuf,
    NativeAtomicArray, NativeAtomicByteArray, NativeAtomicLocalData,
};

pub(crate) mod local_lock_atomic;
pub use local_lock_atomic::{
    operations::LocalLockAtomicArrayOpBuf,
    LocalLockAtomicArray, LocalLockAtomicByteArray, LocalLockAtomicLocalData,
};

pub mod iterator;
pub use iterator::distributed_iterator::DistributedIterator;
pub use iterator::serial_iterator::{SerialIterator, SerialIteratorIter};

pub(crate) type ReduceGen =
    fn(LamellarByteArray, usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>;

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

// lamellar_impl::generate_reductions_for_type_rt!(true, u8,usize);
// lamellar_impl::generate_ops_for_type_rt!(true, true, u8,usize);

lamellar_impl::generate_reductions_for_type_rt!(true, u8, u16, u32, u64, usize);
lamellar_impl::generate_reductions_for_type_rt!(false, u128);
lamellar_impl::generate_ops_for_type_rt!(true, true, u8, u16, u32, u64, usize);
lamellar_impl::generate_ops_for_type_rt!(true, false, u128);

lamellar_impl::generate_reductions_for_type_rt!(true, i8, i16, i32, i64, isize);
lamellar_impl::generate_reductions_for_type_rt!(false, i128);
lamellar_impl::generate_ops_for_type_rt!(true, true, i8, i16, i32, i64, isize);
lamellar_impl::generate_ops_for_type_rt!(true, false, i128);

lamellar_impl::generate_reductions_for_type_rt!(false, f32, f64);
lamellar_impl::generate_ops_for_type_rt!(false, false, f32, f64);

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum Distribution {
    Block,
    Cyclic,
}

#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone)]
pub enum ArrayRdmaCmd {
    Put,
    PutAm,
    Get(bool), //bool true == immediate, false = async
    GetAm,
}

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
pub enum ArrayOpCmd<T: Dist> {
    Add,
    FetchAdd,
    Sub,
    FetchSub,
    Mul,
    FetchMul,
    Div,
    FetchDiv,
    And,
    FetchAnd,
    Or,
    FetchOr,
    Store,
    Load,
    Swap,
    CompareExchange(T),
    CompareExchangeEps(T,T),  
    Put,
    Get,
}

impl<T: Dist> ArrayOpCmd<T> {
    pub fn result_size(&self) -> usize{
        match self {
            ArrayOpCmd::CompareExchange(_)  | ArrayOpCmd::CompareExchangeEps(_,_) => std::mem::size_of::<T>() + 1, //plus one to indicate this requires a result (0 for okay, 1 for error) 
            ArrayOpCmd::FetchAdd | ArrayOpCmd::FetchSub | ArrayOpCmd::FetchMul | ArrayOpCmd::FetchDiv | ArrayOpCmd::FetchAnd | ArrayOpCmd::FetchOr | ArrayOpCmd::Load  | ArrayOpCmd::Swap | ArrayOpCmd::Get => std::mem::size_of::<T>(), //just return value, assume never fails
            ArrayOpCmd::Add | ArrayOpCmd::Sub | ArrayOpCmd::Mul | ArrayOpCmd::Div | ArrayOpCmd::And | ArrayOpCmd::Or | ArrayOpCmd::Store | ArrayOpCmd::Put => 0, //we dont return anything
        }
    }
}

#[derive(serde::Serialize,Clone)]
pub enum InputToValue<'a, T: Dist > {
    OneToOne(usize,T),
    // #[serde(skip_deserializing,rename(serialize = "OwnedOneToMany"))]
    OneToMany(usize,OpInputEnum<'a, T>),
    // #[serde(skip_deserializing,rename(serialize = "ManyToOne"))]
    ManyToOne(OpInputEnum<'a, usize>,T),
    // #[serde(skip_deserializing,rename(serialize = "ManyToMany"))]
    ManyToMany(OpInputEnum<'a, usize>,OpInputEnum<'a, T>),
    // OwnedOneToMany(usize,Vec<T>),
    // OwnedManyToOne(Vec<usize>,T),
    // OwnedManyToMany(Vec<usize>,Vec<T>),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum OpAmInputToValue<T: Dist >{
    OneToOne(usize,T),
    OneToMany(usize,Vec<T>),
    ManyToOne(Vec<usize>,T),
    ManyToMany(Vec<usize>,Vec<T>),
}

#[derive(Clone,serde::Serialize)]
pub enum OpInputEnum<'a, T: Dist> {
    Val(T),
    Slice(&'a [T]),
    Vec(Vec<T>),
    #[serde(serialize_with = "LamellarMemoryRegion::serialize_local_data")]
    MemoryRegion(LamellarMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
    // ReadOnlyArray(ReadOnlyArray<T>),
    // AtomicArray(AtomicArray<T>),
    NativeAtomicArray(NativeAtomicLocalData<T>),
    GenericAtomicArray(GenericAtomicLocalData<T>),
    LocalLockAtomicArray(LocalLockAtomicLocalData<'a, T>),
}

impl<'a, T: Dist> OpInputEnum<'_, T> {
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + Sync + '_> {
        match self {
            OpInputEnum::Val(v) => Box::new(std::iter::repeat(v).map(|elem| *elem)),
            OpInputEnum::Slice(s) => Box::new(s.iter().map(|elem| *elem)),
            OpInputEnum::Vec(v) => Box::new(v.iter().map(|elem| *elem)),
            OpInputEnum::MemoryRegion(mr) => Box::new(
                unsafe { mr.as_slice() }
                    .expect("memregion not local")
                    .iter()
                    .map(|elem| *elem),
            ),
            // OpInputEnum::UnsafeArray(a) => Box::new(unsafe{a.local_data()}.iter().map(|elem| *elem)),
            // OpInputEnum::ReadOnlyArray(a) => Box::new(a.local_data().iter().map(|elem| *elem)),
            // OpInputEnum::AtomicArray(a) => Box::new(a.local_data().iter().map(|elem| elem.load())),
            OpInputEnum::NativeAtomicArray(a) => Box::new(a.iter().map(|elem| elem.load())),
            OpInputEnum::GenericAtomicArray(a) => Box::new(a.iter().map(|elem| elem.load())),
            OpInputEnum::LocalLockAtomicArray(a) => Box::new(a.iter().map(|elem| *elem)),
        }
    }
    pub(crate) fn len(&self) -> usize {
        match self {
            OpInputEnum::Val(_) => 1,
            OpInputEnum::Slice(s) => s.len(),
            OpInputEnum::Vec(v) => v.len(),
            OpInputEnum::MemoryRegion(mr) => {
                unsafe { mr.as_slice() }.expect("memregion not local").len()
            }
            // OpInputEnum::UnsafeArray(a) => unsafe{a.local_data()}.len(),
            // OpInputEnum::ReadOnlyArray(a) => a.local_data().len(),
            // OpInputEnum::AtomicArray(a) => unsafe{a.__local_as_slice().len()},
            OpInputEnum::NativeAtomicArray(a) => a.len(),
            OpInputEnum::GenericAtomicArray(a) => a.len(),
            OpInputEnum::LocalLockAtomicArray(a) => a.len(),
        }
    }

    pub(crate) fn first(&self) -> T {
        match self {
            OpInputEnum::Val(v) => *v,
            OpInputEnum::Slice(s) => *s.first().expect("slice is empty"),
            OpInputEnum::Vec(v) => *v.first().expect("vec is empty"),
            OpInputEnum::MemoryRegion(mr) => {
                *unsafe { mr.as_slice() }
                    .expect("memregion not local")
                    .first()
                    .expect("memregion is empty")
            }
            OpInputEnum::NativeAtomicArray(a) => a.at(0).load(),
            OpInputEnum::GenericAtomicArray(a) => a.at(0).load(),
            OpInputEnum::LocalLockAtomicArray(a) => *a.first().expect("array is empty"),
        }
    }
}


pub trait OpInput<'a, T: Dist> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize); //(Vec<(Box<dyn Iterator<Item = T> + Send  + Sync + '_>,usize)>,usize);
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
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let len = self.len();
        let mut iters = vec![];
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
            Err(_) => 10000,                      //+ 1 to account for main thread
        };
        let num = len / num_per_batch;
        for i in 0..num {
            let temp = &self[(i * num_per_batch)..((i + 1) * num_per_batch)];
            iters.push(OpInputEnum::Slice(temp));
        }
        let rem = len % num_per_batch;
        if rem > 0 {
            let temp = &self[(num * num_per_batch)..(num * num_per_batch) + rem];
            iters.push(OpInputEnum::Slice(temp));
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a Vec<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (&self[..]).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for Vec<T> {
    fn as_op_input(mut self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let len = self.len();
        let mut iters = vec![];
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), 
            Err(_) => 10000,                      
        };
        let num = len / num_per_batch;
        // println!("num: {}", num);
        for i in (0..num).rev() {
            let temp = self.split_off(i * num_per_batch);
            // println!("temp: {:?} {:?} {:?}", temp,i ,i * num_per_batch);
            iters.push(OpInputEnum::Vec(temp));
        }
        let rem = len % num_per_batch;
        // println!("rem: {} {:?}", rem,self);
        if rem > 0 || num == 1 {
            iters.push(OpInputEnum::Vec(self));
        }
        iters.reverse(); //the indice slices get pushed in from the back, but we want to return in order
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &LamellarMemoryRegion<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.as_slice() }.expect("mem region not local");
        let len = slice.len();
        let mut iters = vec![];
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), 
            Err(_) => 10000,                      
        };
        let num = len / num_per_batch;
        for i in 0..num {
            let sub_region = self.sub_region((i * num_per_batch)..((i + 1) * num_per_batch));
            iters.push(OpInputEnum::MemoryRegion(sub_region));
        }
        let rem = len % num_per_batch;
        if rem > 0 {
            let sub_region = self.sub_region((num * num_per_batch)..(num * num_per_batch) + rem);
            iters.push(OpInputEnum::MemoryRegion(sub_region));
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &LocalMemoryRegion<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for LocalMemoryRegion<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &SharedMemoryRegion<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for SharedMemoryRegion<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a UnsafeArray<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.local_as_slice() };
        // let slice = unsafe { std::mem::transmute::<&'_ [T], &'a [T]>(slice) }; //this is safe in the context of buffered_ops because we know we wait for all the requests to submit before we return
        slice.as_op_input()
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for UnsafeArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         unsafe{self.clone().local_as_slice().as_op_input()}
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for &'a ReadOnlyArray<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = self.local_as_slice();
        // let slice = unsafe { std::mem::transmute::<&'_ [T], &'a [T]>(slice) }; //this is safe in the context of buffered_ops because we know we wait for all the requests to submit before we return
        slice.as_op_input()
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for ReadOnlyArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for &'a LocalLockAtomicArray<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // let slice=unsafe{self.__local_as_slice()};
        let slice = self.read_local_data();
        let len = slice.len();
        let mut iters = vec![];
        let my_pe = self.my_pe();
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {//just to check that the array is local
            let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                Err(_) => 10000,                      //+ 1 to account for main thread
            };
            let num = len / num_per_batch;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = self
                    .read_local_data()
                    .into_sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                iters.push(OpInputEnum::LocalLockAtomicArray(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data = self
                    .read_local_data()
                    .into_sub_data(num * num_per_batch, num * num_per_batch + rem);
                iters.push(OpInputEnum::LocalLockAtomicArray(sub_data));
            }
        }
        (iters, len)
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for LocalLockAtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &AtomicArray<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        match self {
            &AtomicArray::GenericAtomicArray(ref a) => a.as_op_input(),
            &AtomicArray::NativeAtomicArray(ref a) => a.as_op_input(),
        }
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for AtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &GenericAtomicArray<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.__local_as_slice() };
        let len = slice.len();
        let local_data = self.local_data();
        let mut iters = vec![];
        let my_pe = self.my_pe();
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {//just to check that the array is local
            let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                Err(_) => 10000,                      //+ 1 to account for main thread
            };
            let num = len / num_per_batch;
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = local_data.sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                iters.push(OpInputEnum::GenericAtomicArray(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data =
                    local_data.sub_data(num * num_per_batch, (num * num_per_batch) + rem);
                iters.push(OpInputEnum::GenericAtomicArray(sub_data));
            }
        }
        (iters, len)
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for GenericAtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &NativeAtomicArray<T> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.__local_as_slice() };
        let len = slice.len();
        let local_data = self.local_data();
        let mut iters = vec![];
        let my_pe = self.my_pe();
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {//just to check that the array is local
            let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                Err(_) => 10000,                      //+ 1 to account for main thread
            };
            let num = len / num_per_batch;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = local_data.sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                // for j in sub_data.clone().into_iter() {
                //     println!("{:?} {:?}",i, j);
                // }
                iters.push(OpInputEnum::NativeAtomicArray(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data =
                    local_data.sub_data(num * num_per_batch, (num * num_per_batch) + rem);
                iters.push(OpInputEnum::NativeAtomicArray(sub_data));
            }
        }
        (iters, len)
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for NativeAtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

pub trait BufferOp: Sync + Send {
    fn add_ops(&self, op: *const u8, op_data: *const u8) -> (bool, Arc<AtomicBool>);
    fn add_fetch_ops(
        &self,
        pe: usize,
        op: *const u8,
        op_data: *const u8,
        req_ids: &Vec<usize>,
        res_map: OpResults,
    ) -> (bool, Arc<AtomicBool>, Option<OpResultOffsets>);

    fn into_arc_am(
        &self,
        sub_array: std::ops::Range<usize>,
    ) -> (
        Vec<LamellarArcAm>,
        usize,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<u8>>>,
    );
}

#[async_trait]
pub trait LamellarArrayRequest {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output>;
    fn wait(self: Box<Self>) -> Option<Self::Output>;
}

struct ArrayRdmaHandle {
    reqs: Vec<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>,
}
#[async_trait]
impl LamellarArrayRequest for ArrayRdmaHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        Some(())
    }
    fn wait(mut self: Box<Self>) -> Option<Self::Output> {
        for req in self.reqs.drain(0..) {
            req.get();
        }
        Some(())
    }
}

struct ArrayRdmaAtHandle<T: Dist> {
    reqs: Vec<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>,
    buf: LocalMemoryRegion<T>,
}
#[async_trait]
impl<T: Dist> LamellarArrayRequest for ArrayRdmaAtHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        Some(self.buf.as_slice().unwrap()[0])
    }
    fn wait(mut self: Box<Self>) -> Option<Self::Output> {
        for req in self.reqs.drain(0..) {
            req.get();
        }
        Some(self.buf.as_slice().unwrap()[0])
    }
}

pub type OpResultOffsets = Vec<(usize, usize, usize)>; //reqid,offset,len
pub struct OpReqOffsets(Arc<Mutex<HashMap<usize, OpResultOffsets>>>);//pe
impl OpReqOffsets {
    pub(crate) fn new() -> Self {
        OpReqOffsets(Arc::new(Mutex::new(HashMap::new())))
    }
    pub fn insert(&self, index: usize, indices: OpResultOffsets) {
        let mut map = self.0.lock();
        map.insert(index, indices);
    }

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

pub type PeOpResults = Arc<Mutex<Vec<u8>>>;
pub struct OpResults(Arc<Mutex<HashMap<usize, PeOpResults>>>);
impl OpResults {
    pub(crate) fn new() -> Self {
        OpResults(Arc::new(Mutex::new(HashMap::new())))
    }
    pub fn insert(&self, index: usize, val: PeOpResults) {
        let mut map = self.0.lock();
        map.insert(index, val);
    }
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


struct ArrayOpHandle{
    reqs: Vec<Box<ArrayOpHandleInner>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpHandleInner {
    complete: Vec<Arc<AtomicBool>>,
}

struct ArrayOpFetchHandle<T: Dist> {
    reqs: Vec<Box<ArrayOpFetchHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpFetchHandleInner<T: Dist> {
    indices: OpReqOffsets,
    complete: Vec<Arc<AtomicBool>>,
    results: OpResults,
    req_cnt: usize,
    _phantom: PhantomData<T>,
}

struct ArrayOpResultHandle<T: Dist> {
    reqs: Vec<Box<ArrayOpResultHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpResultHandleInner<T: Dist> {
    indices: OpReqOffsets,
    complete: Vec<Arc<AtomicBool>>,
    results: OpResults,
    req_cnt: usize,
    _phantom: PhantomData<T>,
}


#[async_trait]
impl LamellarRequest for ArrayOpHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        for req in self.reqs.drain(..) {
            req.into_future().await;
        }
        Some(())
    }
    fn get(&self) -> Option<Self::Output> {
        for req in &self.reqs {
            req.get();
        }
        Some(())
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![self.get()]
    }
}

#[async_trait]
impl LamellarRequest for ArrayOpHandleInner {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        for comp in self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        Some(())
    }
    fn get(&self) -> Option<Self::Output> {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        Some(())
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![self.get()]
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpFetchHandle<T> {
    type Output = Vec<T>;
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await.unwrap());
        }
        Some(res)
    }
    
    fn get(&self) -> Option<Self::Output> {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get().unwrap());
        }
        // println!("res: {:?}",res);
        Some(res)
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![self.get()]
    }
}

impl<T: Dist> ArrayOpFetchHandleInner<T> {
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
                    if len == std::mem::size_of::<T>()+1{
                        panic!("unexpected results len {:?} {:?}",len,std::mem::size_of::<T>()+1);
                    }
                    let res_t = unsafe{
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
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        Some(self.get_result())
    }
    fn get(&self) -> Option<Self::Output> {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        Some(self.get_result())
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![self.get()]
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpResultHandle<T> {
    type Output = Vec<Result<T,T>>;
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await.unwrap());
        }
        Some(res)
    }
    
    fn get(&self) -> Option<Self::Output> {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get().unwrap());
        }
        Some(res)
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![self.get()]
    }
}

impl<T: Dist> ArrayOpResultHandleInner<T> {
    fn get_result(&self) -> Vec<Result<T,T>> {
        // println!("req_cnt: {:?}", self.req_cnt);
        if self.req_cnt > 0 {
            let mut res_vec = Vec::with_capacity(self.req_cnt);
            unsafe {
                res_vec.set_len(self.req_cnt);
            }

            for (pe, res) in self.results.lock().iter() {
                let res = res.lock();
                // println!("{:?}",res.len());
                for (rid, offset, len) in self.indices.lock().get(pe).unwrap().iter() {
                    let ok: bool;
                    let mut offset = *offset;
                    let mut len = *len;
                    if len == std::mem::size_of::<T>()+1{
                        ok = res[offset] == 0;
                        offset+=1;
                        len -= 1;
                    }
                    else{
                        panic!("unexpected results len {:?} {:?}",len,std::mem::size_of::<T>()+1);
                    };
                    let res_t = unsafe{
                        std::slice::from_raw_parts(
                            res.as_ptr().offset(offset as isize) as *const T,
                            len / std::mem::size_of::<T>(),
                        )
                    };
                    
                    if ok{
                        res_vec[*rid] = Ok(res_t[0]);
                    }
                    else{
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
    type Output = Vec<Result<T,T>>;
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        Some(self.get_result())
    }
    fn get(&self) -> Option<Self::Output> {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        Some(self.get_result())
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![self.get()]
    }
}

pub trait ElementOps: AmDist + Dist + Sized {}
impl<T> ElementOps for T where T: AmDist + Dist {}

pub trait ElementArithmeticOps:
    std::ops::AddAssign
    + std::ops::SubAssign
    + std::ops::MulAssign
    + std::ops::DivAssign
    + AmDist
    + Dist
    + Sized
{
}
impl<T> ElementArithmeticOps for T where
    T: std::ops::AddAssign
        + std::ops::SubAssign
        + std::ops::MulAssign
        + std::ops::DivAssign
        + AmDist
        + Dist
{
}

pub trait ElementBitWiseOps:
    std::ops::BitAndAssign + std::ops::BitOrAssign + AmDist + Dist + Sized
{
}
impl<T> ElementBitWiseOps for T where
    T: std::ops::BitAndAssign + std::ops::BitOrAssign + AmDist + Dist
{
}

pub trait ArithmeticOps<T: Dist + ElementArithmeticOps> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync>;

    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync>;

    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;

    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync>;

    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;

    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync>;
}

pub trait BitWiseOps<T: ElementBitWiseOps> {
    fn bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;

    fn fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync>;

    fn bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;

    fn fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync>;
}

//perform the specified operation in place, returning the original value
pub trait LocalArithmeticOps<T: Dist + ElementArithmeticOps> {
    fn local_add(&self, index: usize, val: T) {
        self.local_fetch_add(index, val);
    }
    fn local_fetch_add(&self, index: usize, val: T) -> T;
    fn local_sub(&self, index: usize, val: T) {
        self.local_fetch_sub(index, val);
    }
    fn local_fetch_sub(&self, index: usize, val: T) -> T;
    fn local_mul(&self, index: usize, val: T) {
        self.local_fetch_mul(index, val);
    }
    fn local_fetch_mul(&self, index: usize, val: T) -> T;
    fn local_div(&self, index: usize, val: T) {
        self.local_fetch_div(index, val);
    }
    fn local_fetch_div(&self, index: usize, val: T) -> T;
}

pub trait LocalBitWiseOps<T: Dist + ElementBitWiseOps> {
    fn local_bit_and(&self, index: usize, val: T) {
        self.local_fetch_bit_and(index, val);
    }
    fn local_fetch_bit_and(&self, index: usize, val: T) -> T;
    fn local_bit_or(&self, index: usize, val: T) {
        self.local_fetch_bit_or(index, val);
    }
    fn local_fetch_bit_or(&self, index: usize, val: T) -> T;
}

pub trait LocalAtomicOps<T: Dist + ElementOps> {
    fn local_load(&self, index: usize, val: T) -> T;
    fn local_store(&self, index: usize, val: T);
    fn local_swap(&self, index: usize, val: T) -> T;
}

pub struct LocalOpResult<T: Dist> {
    val: T,
}

#[async_trait]
impl<T: Dist> LamellarArrayRequest for LocalOpResult<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Option<Self::Output> {
        Some(self.val)
    }
    fn wait(self: Box<Self>) -> Option<Self::Output> {
        Some(self.val)
    }
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone)]
pub enum LamellarArrayInput<T: Dist> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    LocalMemRegion(LocalMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
    // Vec(Vec<T>),
}

pub trait LamellarWrite {}
pub trait LamellarRead {}

impl<T: Dist> LamellarRead for T {}

impl<T: Dist> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: AmDist+ Clone + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
//         let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> U {
        U::my_from(self, team)
    }
}

impl<T: Dist> MyFrom<&LamellarArrayInput<T>> for LamellarArrayInput<T> {
    fn my_from(lai: &LamellarArrayInput<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        lai.clone()
    }
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum LamellarReadArray<T: Dist + 'static> {
    UnsafeArray(UnsafeArray<T>),
    ReadOnlyArray(ReadOnlyArray<T>),
    AtomicArray(AtomicArray<T>),
    // NativeAtomicArray(NativeAtomicArray<T>),
    // GenericAtomicArray(GenericAtomicArray<T>),
    LocalLockAtomicArray(LocalLockAtomicArray<T>),
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum LamellarByteArray {
    //we intentially do not include "byte" in the variant name to ease construciton in the proc macros
    UnsafeArray(UnsafeByteArray),
    ReadOnlyArray(ReadOnlyByteArray),
    AtomicArray(AtomicByteArray),
    NativeAtomicArray(NativeAtomicByteArray),
    GenericAtomicArray(GenericAtomicByteArray),
    LocalLockAtomicArray(LocalLockAtomicByteArray),
}

impl<T: Dist + 'static> crate::DarcSerde for LamellarReadArray<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared ser");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.ser(num_pes, cur_pe),
            LamellarReadArray::ReadOnlyArray(array) => array.ser(num_pes, cur_pe),
            LamellarReadArray::AtomicArray(array) => array.ser(num_pes, cur_pe),
            // LamellarReadArray::NativeAtomicArray(array) => array.ser(num_pes, cur_pe),
            // LamellarReadArray::GenericAtomicArray(array) => array.ser(num_pes, cur_pe),
            LamellarReadArray::LocalLockAtomicArray(array) => array.ser(num_pes, cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarReadArray::ReadOnlyArray(array) => array.des(cur_pe),
            LamellarReadArray::AtomicArray(array) => array.des(cur_pe),
            // LamellarReadArray::NativeAtomicArray(array) => array.des(cur_pe),
            // LamellarReadArray::GenericAtomicArray(array) => array.des(cur_pe),
            LamellarReadArray::LocalLockAtomicArray(array) => array.des(cur_pe),
        }
    }
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarWriteArray<T: Dist> {
    UnsafeArray(UnsafeArray<T>),
    AtomicArray(AtomicArray<T>),
    // NativeAtomicArray(NativeAtomicArray<T>),
    // GenericAtomicArray(GenericAtomicArray<T>),
    LocalLockAtomicArray(LocalLockAtomicArray<T>),
}

impl<T: ElementArithmeticOps> ArithmeticOps<T> for LamellarWriteArray<T> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.add(index, val),
            LamellarWriteArray::AtomicArray(array) => array.add(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.add(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.add(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.add(index, val),
        }
    }
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.fetch_add(index, val),
            LamellarWriteArray::AtomicArray(array) => array.fetch_add(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.fetch_add(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.fetch_add(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.fetch_add(index, val),
        }
    }

    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.sub(index, val),
            LamellarWriteArray::AtomicArray(array) => array.sub(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.sub(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.sub(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.sub(index, val),
        }
    }
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.fetch_sub(index, val),
            LamellarWriteArray::AtomicArray(array) => array.fetch_sub(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.fetch_sub(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.fetch_sub(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.fetch_sub(index, val),
        }
    }

    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.mul(index, val),
            LamellarWriteArray::AtomicArray(array) => array.mul(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.mul(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.mul(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.mul(index, val),
        }
    }

    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.fetch_mul(index, val),
            LamellarWriteArray::AtomicArray(array) => array.fetch_mul(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.fetch_mul(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.fetch_mul(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.fetch_mul(index, val),
        }
    }

    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.div(index, val),
            LamellarWriteArray::AtomicArray(array) => array.div(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.div(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.div(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.div(index, val),
        }
    }

    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.fetch_div(index, val),
            LamellarWriteArray::AtomicArray(array) => array.fetch_div(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.fetch_div(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.fetch_div(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.fetch_div(index, val),
        }
    }
}

impl<T: Dist + 'static> crate::DarcSerde for LamellarWriteArray<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared ser");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.ser(num_pes, cur_pe),
            LamellarWriteArray::AtomicArray(array) => array.ser(num_pes, cur_pe),
            // LamellarWriteArray::NativeAtomicArray(array) => array.ser(num_pes, cur_pe),
            // LamellarWriteArray::GenericAtomicArray(array) => array.ser(num_pes, cur_pe),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.ser(num_pes, cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarWriteArray::AtomicArray(array) => array.des(cur_pe),
            // LamellarWriteArray::NativeAtomicArray(array) => array.des(cur_pe),
            // LamellarWriteArray::GenericAtomicArray(array) => array.des(cur_pe),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.des(cur_pe),
        }
    }
}

pub(crate) mod private {
    use crate::active_messaging::*;
    use crate::array::{
        AtomicArray, /*NativeAtomicArray, GenericAtomicArray,*/ LamellarReadArray,
        LamellarWriteArray, LocalLockAtomicArray, ReadOnlyArray, UnsafeArray,
    };
    use crate::lamellar_request::LamellarRequest;
    use crate::memregion::Dist;
    use crate::LamellarTeamRT;
    use enum_dispatch::enum_dispatch;
    use std::pin::Pin;
    use std::sync::Arc;
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarArrayPrivate<T: Dist> {
        // // fn my_pe(&self) -> usize;

        fn local_as_ptr(&self) -> *const T;
        fn local_as_mut_ptr(&self) -> *mut T;
        fn pe_for_dist_index(&self, index: usize) -> Option<usize>;
        fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize>;
        unsafe fn into_inner(self) -> UnsafeArray<T>;
    }

    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub(crate) trait ArrayExecAm<T: Dist> {
        fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
        fn team_counters(&self) -> Arc<AMCounters>;
        fn exec_am_local<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: LamellarActiveMessage + LocalAM + Send + Sync + 'static,
        {
            self.team().exec_am_local_tg(am, Some(self.team_counters()))
        }
        fn exec_am_pe<F>(
            &self,
            pe: usize,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team()
                .exec_am_pe_tg(pe, am, Some(self.team_counters()))
        }
        fn exec_arc_am_pe<F>(
            &self,
            pe: usize,
            am: LamellarArcAm,
        ) -> Box<dyn LamellarRequest<Output = F> + Send + Sync>
        where
            F: AmDist,
        {
            self.team()
                .exec_arc_am_pe(pe, am, Some(self.team_counters()))
        }
        fn exec_am_all<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team().exec_am_all_tg(am, Some(self.team_counters()))
        }
    }
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArray<T: Dist>: private::LamellarArrayPrivate<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
    fn my_pe(&self) -> usize;
    fn num_elems_local(&self) -> usize;
    fn len(&self) -> usize;
    fn barrier(&self);
    fn wait_all(&self);
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)>;

    // /// Returns a distributed iterator for the LamellarArray
    // /// must be called accross all pes containing data in the array
    // /// iteration on a pe only occurs on the data which is locally present
    // /// with all pes iterating concurrently
    // /// blocking: true
    // pub fn dist_iter(&self) -> DistIter<'static, T>;

    // /// Returns a distributed iterator for the LamellarArray
    // /// must be called accross all pes containing data in the array
    // /// iteration on a pe only occurs on the data which is locally present
    // /// with all pes iterating concurrently
    // pub fn dist_iter_mut(&self) -> DistIterMut<'static, T>;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred
    // pub fn ser_iter(&self) -> LamellarArrayIter<'_, T> ;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred, array data is buffered to more efficiently make
    // /// use of network buffers
    // pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T> ;
}

// pub trait ArrayIterator{

// }
// #[doc(hidden)]
// // #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
// pub trait AsBytes<T: Dist, B: Dist>: LamellarArray<T> {
//     // #[doc(hidden)]
//     // unsafe fn to_base_inner<B: Dist>(self) -> LamellarArray<B>;
//     type Array: LamellarArray<B>;
//     #[doc(hidden)]
//     unsafe fn as_bytes(&self) -> Self::Array;
// }

// #[doc(hidden)]
// // #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
// pub trait FromBytes<T: Dist,B: Dist>: LamellarArray<B> {
//     // #[doc(hidden)]
//     // unsafe fn to_base_inner<B: Dist>(self) -> LamellarArray<B>;
//     type Array: LamellarArray<T>;
//     #[doc(hidden)]
//     unsafe fn from_bytes(self) -> Self::Array;
// }

// impl <T: Dist> FromBytes<T,u8> for LamellarReadArray<u8>{
//     type Array = LamellarReadArray<T>;
//     #[doc(hidden)]
//     unsafe fn from_bytes(self) -> Self::Array{
//         match self {
//             LamellarReadArray::UnsafeArray(array) => array.from_bytes().into(),
//             LamellarReadArray::ReadOnlyArray(array) => array.from_bytes().into(),
//             LamellarReadArray::AtomicArray(array) => array.from_bytes().into(),
//             LamellarReadArray::LocalLockAtomicArray(array) => array.from_bytes().into(),
//         }
//     }
// }

pub trait SubArray<T: Dist>: LamellarArray<T> {
    type Array: LamellarArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array;
    fn global_index(&self, sub_index: usize) -> usize;
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayGet<T: Dist + 'static>: LamellarArray<T> + Sync + Send {
    // this is non blocking call
    // the runtime does not manage checking for completion of message transmission
    // the user is responsible for ensuring the buffer remains valid
    // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // );

    // a safe synchronous call that blocks untils the data as all been transfered
    // get data from self and write into buf
    // fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, dst: U);

    // async get
    // get data from self and write into buf
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        dst: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()> + Send + Sync>;

    // blocking call that gets the value stored and the provided index
    fn at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T> + Send + Sync>;
}

#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayPut<T: Dist>: LamellarArray<T> + Sync + Send {
    //put data from buf into self
    fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        src: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()> + Send + Sync>;
}

pub trait ArrayPrint<T: Dist + std::fmt::Debug>: LamellarArray<T> {
    fn print(&self);
}

// #[enum_dispatch(LamellarWriteArray<T>,LamellarReadArray<T>)]
pub trait LamellarArrayReduce<T>: LamellarArrayGet<T>
where
    T: Dist + AmDist + 'static,
{
    fn get_reduction_op(&self, op: String) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}

impl<T: Dist + AmDist + 'static> LamellarWriteArray<T> {
    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.reduce(op),
            LamellarWriteArray::AtomicArray(array) => array.reduce(op),
            // LamellarWriteArray::NativeAtomicArray(array) => array.reduce(op),
            // LamellarWriteArray::GenericAtomicArray(array) => array.reduce(op),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.sum(),
            LamellarWriteArray::AtomicArray(array) => array.sum(),
            // LamellarWriteArray::NativeAtomicArray(array) => array.sum(),
            // LamellarWriteArray::GenericAtomicArray(array) => array.sum(),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.sum(),
        }
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.max(),
            LamellarWriteArray::AtomicArray(array) => array.max(),
            // LamellarWriteArray::NativeAtomicArray(array) => array.max(),
            // LamellarWriteArray::GenericAtomicArray(array) => array.max(),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.max(),
        }
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.prod(),
            LamellarWriteArray::AtomicArray(array) => array.prod(),
            // LamellarWriteArray::NativeAtomicArray(array) => array.prod(),
            // LamellarWriteArray::GenericAtomicArray(array) => array.prod(),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.prod(),
        }
    }
}

impl<T: Dist + AmDist + 'static> LamellarReadArray<T> {
    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.reduce(op),
            LamellarReadArray::AtomicArray(array) => array.reduce(op),
            // LamellarReadArray::NativeAtomicArray(array) => array.reduce(op),
            // LamellarReadArray::GenericAtomicArray(array) => array.reduce(op),
            LamellarReadArray::LocalLockAtomicArray(array) => array.reduce(op),
            LamellarReadArray::ReadOnlyArray(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.sum(),
            LamellarReadArray::AtomicArray(array) => array.sum(),
            // LamellarReadArray::NativeAtomicArray(array) => array.sum(),
            // LamellarReadArray::GenericAtomicArray(array) => array.sum(),
            LamellarReadArray::LocalLockAtomicArray(array) => array.sum(),
            LamellarReadArray::ReadOnlyArray(array) => array.sum(),
        }
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.max(),
            LamellarReadArray::AtomicArray(array) => array.max(),
            // LamellarReadArray::NativeAtomicArray(array) => array.max(),
            // LamellarReadArray::GenericAtomicArray(array) => array.max(),
            LamellarReadArray::LocalLockAtomicArray(array) => array.max(),
            LamellarReadArray::ReadOnlyArray(array) => array.max(),
        }
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.prod(),
            LamellarReadArray::AtomicArray(array) => array.prod(),
            // LamellarReadArray::NativeAtomicArray(array) => array.prod(),
            // LamellarReadArray::GenericAtomicArray(array) => array.prod(),
            LamellarReadArray::LocalLockAtomicArray(array) => array.prod(),
            LamellarReadArray::ReadOnlyArray(array) => array.prod(),
        }
    }
}

// impl<'a, T: AmDist + 'static> IntoIterator
//     for &'a LamellarArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }
