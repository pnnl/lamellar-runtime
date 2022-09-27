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
    operations::UnsafeArrayOpBuf, UnsafeArray, UnsafeByteArray, UnsafeByteArrayWeak,
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
    AtomicByteArrayWeak,
};

pub(crate) mod generic_atomic;
pub use generic_atomic::{
    operations::GenericAtomicArrayOpBuf, GenericAtomicArray, GenericAtomicByteArray,
    GenericAtomicByteArrayWeak, GenericAtomicLocalData,
};

pub(crate) mod native_atomic;
pub use native_atomic::{
    operations::NativeAtomicArrayOpBuf, NativeAtomicArray, NativeAtomicByteArray,
    NativeAtomicByteArrayWeak, NativeAtomicLocalData,
};

pub(crate) mod local_lock_atomic;
pub use local_lock_atomic::{
    operations::LocalLockAtomicArrayOpBuf, LocalLockAtomicArray, LocalLockAtomicByteArray,
    LocalLockAtomicByteArrayWeak, LocalLockAtomicLocalData,
};

pub mod iterator;
pub use iterator::distributed_iterator::DistributedIterator;
pub use iterator::serial_iterator::{SerialIterator, SerialIteratorIter};

pub(crate) mod operations;
pub use operations::*;

pub(crate) type ReduceGen = fn(LamellarByteArray, usize) -> LamellarArcAm;

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
impl Dist for bool {}

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

#[async_trait]
pub trait LamellarArrayRequest: Sync + Send {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn wait(self: Box<Self>) -> Self::Output;
}

struct ArrayRdmaHandle {
    reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
}
#[async_trait]
impl LamellarArrayRequest for ArrayRdmaHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        ()
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.get();
        }
        ()
    }
}

struct ArrayRdmaAtHandle<T: Dist> {
    reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
    buf: LocalMemoryRegion<T>,
}
#[async_trait]
impl<T: Dist> LamellarArrayRequest for ArrayRdmaAtHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        self.buf.as_slice().unwrap()[0]
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.get();
        }
        self.buf.as_slice().unwrap()[0]
    }
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone, Debug)]
pub enum LamellarArrayInput<T: Dist> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    LocalMemRegion(LocalMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
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

impl<T: Dist> MyFrom<Vec<T>> for LamellarArrayInput<T> {
    fn my_from(vals: Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(vals.as_ptr(), buf.as_mut_ptr().unwrap(), vals.len());
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}
impl<T: Dist> MyFrom<&Vec<T>> for LamellarArrayInput<T> {
    fn my_from(vals: &Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(vals.as_ptr(), buf.as_mut_ptr().unwrap(), vals.len());
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
    fn ser(&self, num_pes: usize) {
        // println!("in shared ser");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.ser(num_pes),
            LamellarReadArray::ReadOnlyArray(array) => array.ser(num_pes),
            LamellarReadArray::AtomicArray(array) => array.ser(num_pes),
            // LamellarReadArray::NativeAtomicArray(array) => array.ser(num_pes),
            // LamellarReadArray::GenericAtomicArray(array) => array.ser(num_pes),
            LamellarReadArray::LocalLockAtomicArray(array) => array.ser(num_pes),
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

impl<T: Dist + 'static> crate::DarcSerde for LamellarWriteArray<T> {
    fn ser(&self, num_pes: usize) {
        // println!("in shared ser");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.ser(num_pes),
            LamellarWriteArray::AtomicArray(array) => array.ser(num_pes),
            // LamellarWriteArray::NativeAtomicArray(array) => array.ser(num_pes),
            // LamellarWriteArray::GenericAtomicArray(array) => array.ser(num_pes),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.ser(num_pes),
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
    use crate::lamellar_request::{LamellarMultiRequest, LamellarRequest};
    use crate::memregion::Dist;
    use crate::LamellarTeamRT;
    use enum_dispatch::enum_dispatch;
    use std::pin::Pin;
    use std::sync::Arc;
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarArrayPrivate<T: Dist> {
        // // fn my_pe(&self) -> usize;
        fn inner_array(&self) -> &UnsafeArray<T>;
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
        fn exec_am_local<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output>>
        where
            F: LamellarActiveMessage + LocalAM + 'static,
        {
            self.team().exec_am_local_tg(am, Some(self.team_counters()))
        }
        fn exec_am_pe<F>(&self, pe: usize, am: F) -> Box<dyn LamellarRequest<Output = F::Output>>
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
        ) -> Box<dyn LamellarRequest<Output = F>>
        where
            F: AmDist,
        {
            self.team()
                .exec_arc_am_pe(pe, am, Some(self.team_counters()))
        }
        fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarMultiRequest<Output = F::Output>>
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

pub trait SubArray<T: Dist>: LamellarArray<T> {
    type Array: LamellarArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array;
    fn global_index(&self, sub_index: usize) -> usize;
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayGet<T: Dist + 'static>: LamellarArray<T> {
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
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // blocking call that gets the value stored and the provided index
    fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>>;
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayInternalGet<T: Dist + 'static>: LamellarArray<T> {
    fn internal_get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        dst: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>>;

    // blocking call that gets the value stored and the provided index
    fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>>;
}

#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayPut<T: Dist>: LamellarArray<T> {
    //put data from buf into self
    fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        src: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

#[enum_dispatch(LamellarWriteArray<T>)]
pub(crate) trait LamellarArrayInternalPut<T: Dist>: LamellarArray<T> {
    //put data from buf into self
    fn internal_put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        src: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>>;
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
    fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>>;
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>>;
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>>;
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>>;
}

impl<T: Dist + AmDist + 'static> LamellarWriteArray<T> {
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.reduce(op),
            LamellarWriteArray::AtomicArray(array) => array.reduce(op),
            // LamellarWriteArray::NativeAtomicArray(array) => array.reduce(op),
            // LamellarWriteArray::GenericAtomicArray(array) => array.reduce(op),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.sum(),
            LamellarWriteArray::AtomicArray(array) => array.sum(),
            // LamellarWriteArray::NativeAtomicArray(array) => array.sum(),
            // LamellarWriteArray::GenericAtomicArray(array) => array.sum(),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.sum(),
        }
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.max(),
            LamellarWriteArray::AtomicArray(array) => array.max(),
            // LamellarWriteArray::NativeAtomicArray(array) => array.max(),
            // LamellarWriteArray::GenericAtomicArray(array) => array.max(),
            LamellarWriteArray::LocalLockAtomicArray(array) => array.max(),
        }
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
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
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.reduce(op),
            LamellarReadArray::AtomicArray(array) => array.reduce(op),
            // LamellarReadArray::NativeAtomicArray(array) => array.reduce(op),
            // LamellarReadArray::GenericAtomicArray(array) => array.reduce(op),
            LamellarReadArray::LocalLockAtomicArray(array) => array.reduce(op),
            LamellarReadArray::ReadOnlyArray(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.sum(),
            LamellarReadArray::AtomicArray(array) => array.sum(),
            // LamellarReadArray::NativeAtomicArray(array) => array.sum(),
            // LamellarReadArray::GenericAtomicArray(array) => array.sum(),
            LamellarReadArray::LocalLockAtomicArray(array) => array.sum(),
            LamellarReadArray::ReadOnlyArray(array) => array.sum(),
        }
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.max(),
            LamellarReadArray::AtomicArray(array) => array.max(),
            // LamellarReadArray::NativeAtomicArray(array) => array.max(),
            // LamellarReadArray::GenericAtomicArray(array) => array.max(),
            LamellarReadArray::LocalLockAtomicArray(array) => array.max(),
            LamellarReadArray::ReadOnlyArray(array) => array.max(),
        }
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
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
