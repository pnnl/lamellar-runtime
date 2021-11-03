use crate::{active_messaging::*, LamellarTeamRT}; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
                                                                    // use crate::lamellae::Lamellae;
                                                                    // use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};



use enum_dispatch::enum_dispatch;
use futures_lite::Future;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) mod r#unsafe;
pub use r#unsafe::UnsafeArray;

pub mod iterator;
pub use iterator::distributed_iterator::DistributedIterator;
pub use iterator::serial_iterator::{SerialIterator,SerialIteratorIter};
use iterator::distributed_iterator::{DistIter,DistIterMut,DistIteratorLauncher};
use iterator::serial_iterator::{LamellarArrayIter};




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
    fn my_from(val: &T, team: &Arc<LamellarTeamRT>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist + 'static> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: Dist + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
//         let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Arc<LamellarTeamRT>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Arc<LamellarTeamRT>) -> U {
        U::my_from(self, team)
    }
}

pub trait ArrayOps<T> {
    fn add(&self, index: usize, val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;
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
    pub fn len(&self) -> usize{
        match self {
            LamellarArray::UnsafeArray(inner) => inner.len(),
        }
    }
    // pub(crate) fn size_of_elem(&self) -> usize{
    //     std::mem::size_of::<T>()
    // }
    pub(crate) fn team(&self) -> Arc<LamellarTeamRT> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.team(),
        }
    }
    pub fn barrier(&self) {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.barrier(),
        }
    }
    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarArray<T> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.sub_array(range).into(),
        }
    }
    pub fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.put(index, buf),
        }
    }
    pub fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.get(index, buf),
        }
    }
    pub fn at(&self,index: usize) -> T{
        let buf: LocalMemoryRegion<T> = self.team().alloc_local_mem_region(1);
        self.get(index,&buf);
        buf.as_slice().unwrap()[0].clone()
    }
    pub(crate) fn local_as_ptr(&self) -> *const T {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.local_as_ptr(),
        }
    }
    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.local_as_mut_ptr(),
        }
    }
    pub(crate) fn num_elems_local(&self) -> usize {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.num_elems_local(),
        }
    }

    /// Returns a distributed iterator for the LamellarArray
    /// must be called accross all pes containing data in the array
    /// iteration on a pe only occurs on the data which is locally present
    /// with all pes iterating concurrently
    /// blocking: true
    pub fn dist_iter(&self) -> DistIter<'static, T> {
        self.barrier();
        match self {
            LamellarArray::UnsafeArray(inner) => inner.dist_iter(),
        }
    }

    /// Returns a distributed iterator for the LamellarArray
    /// must be called accross all pes containing data in the array
    /// iteration on a pe only occurs on the data which is locally present
    /// with all pes iterating concurrently
    pub fn dist_iter_mut(&self) -> DistIterMut<'static, T> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.dist_iter_mut(),
        }
    }

    /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    /// where this was called, data that is not local to the PE is automatically
    /// copied and transferred
    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.ser_iter(),
        }
    }

    /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    /// where this was called, data that is not local to the PE is automatically
    /// copied and transferred, array data is buffered to more efficiently make
    /// use of network buffers
    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T> {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.buffered_iter(buf_size),
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

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIteratorLauncher
    for LamellarArray<T>
{
    fn for_each<I, F>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.for_each(iter, op),
        }
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.for_each_async(iter, op),
        }
    }
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        match self {
            LamellarArray::UnsafeArray(inner) => inner.global_index_from_local(index,chunk_size),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
struct ForEach<I, F>
where
    I: DistributedIterator,
    F: Fn(I::Item) + Sync + Send,
{
    op: F,
    data: I,
    start_i: usize,
    end_i: usize,
}
#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEach<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + Sync + Send + 'static,
{
    fn exec(&self) {
        // println!("in for each");
        let mut iter = self.data.init(self.start_i, self.end_i-self.start_i);
        while let Some(elem) = iter.next() {
            (&self.op)(elem)
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
struct ForEachAsync<I, F, Fut>
where
    I: DistributedIterator,
    F: Fn(I::Item) -> Fut + Sync + Send,
    Fut: Future<Output = ()> + Sync + Send,
{
    op: F,
    data: I,
    start_i: usize,
    end_i: usize,
}
#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsync<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + Sync + Send + 'static,
    Fut: Future<Output = ()> + Sync + Send + 'static,
{
    fn exec(&self) {
        let mut iter = self.data.init(self.start_i, self.end_i-self.start_i);
        while let Some(elem) = iter.next() {
            (&self.op)(elem).await;
        }
    }
}

#[enum_dispatch]
pub trait LamellarArrayRDMA<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
{
    fn len(&self) -> usize;
    fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    fn at(&self,index: usize) -> T;
    fn local_as_slice(&self) -> &[T];
    fn local_as_mut_slice(&self) -> &mut [T];
    fn to_base<B: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>(
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
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}

impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> IntoIterator
    for &'a LamellarArray<T>
{
    type Item = &'a T;
    type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
    fn into_iter(self) -> Self::IntoIter {
        SerialIteratorIter{ iter: self.ser_iter()}
    }
}
