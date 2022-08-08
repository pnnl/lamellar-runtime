use crate::array::atomic::*;
use crate::array::generic_atomic::*;
use crate::array::local_lock_atomic::*;
use crate::array::native_atomic::*;
use crate::array::r#unsafe::*;
use crate::array::read_only::*;

use crate::array::*;

use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};
// use crate::Darc;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
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
    CompareExchangeEps(T, T),
    Put,
    Get,
}

impl<T: Dist> ArrayOpCmd<T> {
    pub fn result_size(&self) -> usize {
        match self {
            ArrayOpCmd::CompareExchange(_) | ArrayOpCmd::CompareExchangeEps(_, _) => {
                std::mem::size_of::<T>() + 1
            } //plus one to indicate this requires a result (0 for okay, 1 for error)
            ArrayOpCmd::FetchAdd
            | ArrayOpCmd::FetchSub
            | ArrayOpCmd::FetchMul
            | ArrayOpCmd::FetchDiv
            | ArrayOpCmd::FetchAnd
            | ArrayOpCmd::FetchOr
            | ArrayOpCmd::Load
            | ArrayOpCmd::Swap
            | ArrayOpCmd::Get => std::mem::size_of::<T>(), //just return value, assume never fails
            ArrayOpCmd::Add
            | ArrayOpCmd::Sub
            | ArrayOpCmd::Mul
            | ArrayOpCmd::Div
            | ArrayOpCmd::And
            | ArrayOpCmd::Or
            | ArrayOpCmd::Store
            | ArrayOpCmd::Put => 0, //we dont return anything
        }
    }
}

#[derive(serde::Serialize, Clone)]
pub enum InputToValue<'a, T: Dist> {
    OneToOne(usize, T),
    OneToMany(usize, OpInputEnum<'a, T>),
    ManyToOne(OpInputEnum<'a, usize>, T),
    ManyToMany(OpInputEnum<'a, usize>, OpInputEnum<'a, T>),
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum OpAmInputToValue<T: Dist> {
    OneToOne(usize, T),
    OneToMany(usize, Vec<T>),
    ManyToOne(Vec<usize>, T),
    ManyToMany(Vec<usize>, Vec<T>),
}

#[derive(Clone, serde::Serialize)]
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
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
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
            OpInputEnum::MemoryRegion(mr) => *unsafe { mr.as_slice() }
                .expect("memregion not local")
                .first()
                .expect("memregion is empty"),
            OpInputEnum::NativeAtomicArray(a) => a.at(0).load(),
            OpInputEnum::GenericAtomicArray(a) => a.at(0).load(),
            OpInputEnum::LocalLockAtomicArray(a) => *a.first().expect("array is empty"),
        }
    }
}

pub trait OpInput<'a, T: Dist> {
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
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {
            //just to check that the array is local
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
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {
            //just to check that the array is local
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
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {
            //just to check that the array is local
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

pub type OpResultOffsets = Vec<(usize, usize, usize)>; //reqid,offset,len
pub struct OpReqOffsets(Arc<Mutex<HashMap<usize, OpResultOffsets>>>); //pe
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

pub(crate) struct ArrayOpHandle {
    pub(crate) reqs: Vec<Box<ArrayOpHandleInner>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpHandleInner {
    pub(crate) complete: Vec<Arc<AtomicBool>>,
}

pub(crate) struct ArrayOpFetchHandle<T: Dist> {
    pub(crate) reqs: Vec<Box<ArrayOpFetchHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpFetchHandleInner<T: Dist> {
    pub(crate) indices: OpReqOffsets,
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) results: OpResults,
    pub(crate) req_cnt: usize,
    pub(crate) _phantom: PhantomData<T>,
}

pub(crate) struct ArrayOpResultHandle<T: Dist> {
    pub(crate) reqs: Vec<Box<ArrayOpResultHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpResultHandleInner<T> {
    pub(crate) indices: OpReqOffsets,
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) results: OpResults,
    pub(crate) req_cnt: usize,
    pub(crate) _phantom: PhantomData<T>,
}

#[async_trait]
impl LamellarRequest for ArrayOpHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.into_future().await;
        }
        ()
    }
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
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        ()
    }
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        ()
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpFetchHandle<T> {
    type Output = Vec<T>;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await);
        }
        res
    }

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
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        self.get_result()
    }
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        self.get_result()
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpResultHandle<T> {
    type Output = Vec<Result<T, T>>;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await);
        }
        res
    }

    fn get(&self) -> Self::Output {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get());
        }
        res
    }
}

impl<T: Dist> ArrayOpResultHandleInner<T> {
    fn get_result(&self) -> Vec<Result<T, T>> {
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
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        self.get_result()
    }
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        self.get_result()
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>>;

    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>>;

    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>>;

    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>>;
}

pub trait BitWiseOps<T: ElementBitWiseOps> {
    fn bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>>;

    fn bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>>;
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
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.val
    }
    fn wait(self: Box<Self>) -> Self::Output {
        self.val
    }
}

impl<T: ElementArithmeticOps> ArithmeticOps<T> for LamellarWriteArray<T> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.fetch_div(index, val),
            LamellarWriteArray::AtomicArray(array) => array.fetch_div(index, val),
            // LamellarWriteArray::NativeAtomicArray(array) => array.fetch_div(index, val),
            // LamellarWriteArray::GenericAtomicArray(array) => array.fetch_div(index, val),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.fetch_div(index, val),
        }
    }
}
