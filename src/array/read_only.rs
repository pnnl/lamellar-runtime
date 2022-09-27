use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use std::sync::Arc;

#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct ReadOnlyArray<T> {
    pub(crate) array: UnsafeArray<T>,
}

#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct ReadOnlyByteArray {
    pub(crate) array: UnsafeByteArray,
}

//#[prof]
impl<T: Dist> ReadOnlyArray<T> {
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> ReadOnlyArray<T> {
        ReadOnlyArray {
            array: UnsafeArray::new(team, array_size, distribution),
        }
    }
    pub fn wait_all(&self) {
        self.array.wait_all();
    }
    pub fn barrier(&self) {
        self.array.barrier();
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.array.block_on(f)
    }

    pub(crate) fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }

    pub fn use_distribution(self, distribution: Distribution) -> Self {
        ReadOnlyArray {
            array: self.array.use_distribution(distribution),
        }
    }

    pub fn num_pes(&self) -> usize {
        self.array.num_pes()
    }

    // pub fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
    //     self.array.pe_for_dist_index(index)
    // }
    // pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
    //     self.array.pe_offset_for_dist_index(pe, index)
    // }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) {
        self.array.get_unchecked(index, buf)
    }
    pub fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.array.iget(index, buf)
    }
    pub fn internal_get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        self.array.internal_get(index, buf)
    }
    pub fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.get(index, buf)
    }
    pub fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>> {
        self.array.internal_at(index)
    }
    pub fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.array.at(index)
    }
    pub fn local_as_slice(&self) -> &[T] {
        unsafe { self.array.local_as_mut_slice() }
    }
    pub fn local_data(&self) -> &[T] {
        unsafe { self.array.local_as_mut_slice() }
    }
    pub fn dist_iter(&self) -> DistIter<'static, T, ReadOnlyArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T, ReadOnlyArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.array.team().clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T, ReadOnlyArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.array.team().clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> ReadOnlyArray<T> {
        ReadOnlyArray {
            array: self.array.sub_array(range),
        }
    }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("readonly into_unsafe");
        self.array.into()
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        // println!("readonly into_local_only");
        self.array.into()
    }

    pub fn into_local_lock_atomic(self) -> LocalLockAtomicArray<T> {
        // println!("readonly into_local_lock_atomic");
        self.array.into()
    }

    pub fn into_generic_atomic(self) -> GenericAtomicArray<T> {
        // println!("readonly into_generic_atomic");
        self.array.into()
    }
}

impl<T: Dist + 'static> ReadOnlyArray<T> {
    pub fn into_atomic(self) -> AtomicArray<T> {
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for ReadOnlyArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("readonly from UnsafeArray");
        array.block_on_outstanding(DarcMode::ReadOnlyArray);
        ReadOnlyArray { array: array }
    }
}

impl<T: Dist> From<LocalOnlyArray<T>> for ReadOnlyArray<T> {
    fn from(array: LocalOnlyArray<T>) -> Self {
        // println!("readonly from LocalOnlyArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<AtomicArray<T>> for ReadOnlyArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        // println!("readonly from AtomicArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<LocalLockAtomicArray<T>> for ReadOnlyArray<T> {
    fn from(array: LocalLockAtomicArray<T>) -> Self {
        // println!("readonly from LocalLockAtomicArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for ReadOnlyByteArray {
    fn from(array: ReadOnlyArray<T>) -> Self {
        ReadOnlyByteArray {
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<ReadOnlyByteArray> for ReadOnlyArray<T> {
    fn from(array: ReadOnlyByteArray) -> Self {
        ReadOnlyArray {
            array: array.array.into(),
        }
    }
}

impl<T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> ReadOnlyArray<T> {
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        self.array.reduce(op)
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.array.reduce("sum")
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.array.reduce("prod")
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.array.reduce("max")
    }
}

impl<T: Dist> DistIteratorLauncher for ReadOnlyArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.subarray_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + AmLocal + Clone + 'static,
    {
        self.array.for_each(iter, op)
    }
    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + AmLocal + Clone + 'static,
    {
        self.array.for_each_with_schedule(sched, iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + AmLocal + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array.for_each_async(iter, op)
    }
    fn for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + AmLocal + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array.for_each_async_with_schedule(sched, iter, op)
    }
    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + AmLocal + 'static,
    {
        self.array.collect(iter, d)
    }
    fn collect_async<I, A, B>(
        &self,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist,
        A: From<UnsafeArray<B>> + AmLocal + 'static,
    {
        self.array.collect_async(iter, d)
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
}

impl<T: Dist> private::ArrayExecAm<T> for ReadOnlyArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for ReadOnlyArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        &self.array
    }
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_ptr()
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

impl<T: Dist> LamellarArray<T> for ReadOnlyArray<T> {
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
impl<T: Dist + 'static> LamellarArrayInternalGet<T> for ReadOnlyArray<T> {
    // fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
    //     self.iget(index, buf)
    // }
    fn internal_get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        self.internal_get(index, buf)
    }
    fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>> {
        self.array.internal_at(index)
    }
}

impl<T: Dist + 'static> LamellarArrayGet<T> for ReadOnlyArray<T> {
    // fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
    //     self.iget(index, buf)
    // }
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.get(index, buf)
    }
    fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.array.at(index)
    }
}

impl<T: Dist> SubArray<T> for ReadOnlyArray<T> {
    type Array = ReadOnlyArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> ReadOnlyArray<T> {
    pub fn print(&self) {
        self.array.print()
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for ReadOnlyArray<T>
// {

//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         REDUCE_OPS
//             .get(&(std::any::TypeId::of::<T>(), op))
//             .expect("unexpected reduction type")(
//             self.clone().into(),
//             self.inner.team.num_pes(),
//         )
//     }
//     fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.reduce(op)
//     }
//     fn sum(&self) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.sum()
//     }
//     fn max(&self) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.max()
//     }
//     fn prod(&self) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.prod()
//     }
// }

// impl<'a, T: Dist > IntoIterator
//     for &'a ReadOnlyArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }

// impl < T> Drop for ReadOnlyArray<T>{
//     fn drop(&mut self){
//         println!("dropping array!!!");
//     }
// }
