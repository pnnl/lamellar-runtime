use crate::array::iterator::distributed_iterator::{
    DistIter, DistIteratorLauncher, DistributedIterator,
};
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
use crate::darc::DarcMode;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use std::sync::Arc;
use core::marker::PhantomData;

#[lamellar_impl::AmDataRT(Clone)]
pub struct ReadOnlyArray<T: Dist> {
    pub(crate) array: UnsafeArray<T>,
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

    pub fn pe_for_dist_index(&self, index: usize) -> usize {
        self.array.pe_for_dist_index(index)
    }
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        self.array.pe_offset_for_dist_index(pe, index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.array.get_unchecked(index, buf)
    }
    pub fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.array.iget(index, buf)
    }
    pub fn at(&self, index: usize) -> T {
        self.array.at(index)
    }
    pub fn local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    pub fn local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }
    pub fn to_base_inner<B: Dist>(self) -> ReadOnlyArray<B> {
        ReadOnlyArray {
            array: self.array.to_base_inner(),
        }
    }

    // pub fn local_mem_region(&self) -> &MemoryRegion<T> {
    //     &self.inner.mem_region
    // }

    // pub(crate) fn local_as_ptr(&self) -> *const T {
    //     self.array.local_as_ptr()
    // }
    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
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
}

impl <T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> ReadOnlyArray<T> {
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

impl<T: Dist> DistIteratorLauncher for ReadOnlyArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        self.array.for_each(iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.array.for_each_async(iter, op)
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T>
    for ReadOnlyArray<T>
{
    
    fn local_as_ptr(&self) -> *const T{
        self.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> usize {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        self.array.pe_offset_for_dist_index(pe,index)
    }
}

impl<T: Dist> LamellarArray<T> for ReadOnlyArray<T> {
    fn my_pe(&self) -> usize{
        self.array.my_pe()
    }
    fn team(&self) -> Arc<LamellarTeamRT>{
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
}
impl<T: Dist> LamellarArrayRead<T> for ReadOnlyArray<T> {
    unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.get_unchecked(index, buf)
    }
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.iget(index, buf)
    }
    fn at(&self, index: usize) -> T {
        self.at(index)
    }
}

impl<T: Dist> SubArray<T> for ReadOnlyArray<T> {
    type Array = ReadOnlyArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize{
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> ReadOnlyArray<T> {
    pub fn print(&self) {
        self.array.print()
    }
}

// impl<T: Dist > LamellarArrayReduce<T>
//     for ReadOnlyArray<T>
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
