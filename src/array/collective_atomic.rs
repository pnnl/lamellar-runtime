mod iteration;
pub(crate) mod operations;
mod rdma;
use crate::array::r#unsafe::UnsafeByteArray;
use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
use crate::darc::DarcMode;
use crate::darc::local_rw_darc::LocalRwDarc;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use core::marker::PhantomData;

#[lamellar_impl::AmDataRT(Clone)]
pub struct CollectiveAtomicArray<T: Dist> {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeArray<T>,
}

#[lamellar_impl::AmDataRT(Clone)]
pub struct CollectiveAtomicByteArray {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeByteArray,
}

impl<T: Dist + std::default::Default> CollectiveAtomicArray<T> {
    //Sync + Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> CollectiveAtomicArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        
        CollectiveAtomicArray {
            lock: LocalRwDarc::new(team, ()).unwrap(),
            array: array,
        }
    }
}

impl<T: Dist> CollectiveAtomicArray<T> {
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
        CollectiveAtomicArray {
            lock: self.lock.clone(),
            array: self.array.use_distribution(distribution),
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
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) ->  Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[doc(hidden)]
    pub unsafe fn local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    #[doc(hidden)]
    pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        CollectiveAtomicArray {
            lock: self.lock.clone(),
            array: self.array.sub_array(range),
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
}

impl<T: Dist + 'static> CollectiveAtomicArray<T>{
    pub fn into_atomic(self) -> AtomicArray<T>{
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for CollectiveAtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        array.block_on_outstanding(DarcMode::CollectiveAtomicArray);
        CollectiveAtomicArray {
            lock: LocalRwDarc::new(array.team(),()).unwrap(),
            array: array,
        }
    }
}

// impl <T: Dist> AsBytes<T,u8> for CollectiveAtomicArray<T>{
//     type Array = CollectiveAtomicArray<u8>;
//     #[doc(hidden)]
//     unsafe fn as_bytes(&self) -> Self::Array {
//         let array = self.array.as_bytes();
//         CollectiveAtomicArray {
//             lock: self.lock.clone(),
//             array: array,
//         }
//     }
// }
// impl <T: Dist> FromBytes<T,u8> for CollectiveAtomicArray<u8>{
//     type Array = CollectiveAtomicArray<T>;
//     #[doc(hidden)]
//     unsafe fn from_bytes(self) -> Self::Array {
//         let array = self.array.from_bytes();
//         CollectiveAtomicArray {
//             lock: self.lock.clone(),
//             array: array,
//         }
//     }
// }

impl <T: Dist> From<CollectiveAtomicArray<T>> for CollectiveAtomicByteArray{
    fn from(array: CollectiveAtomicArray<T>) -> Self {
        CollectiveAtomicByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}
impl <T: Dist> From<CollectiveAtomicByteArray> for CollectiveAtomicArray<T>{
    fn from(array: CollectiveAtomicByteArray) -> Self {
        CollectiveAtomicArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> private::ArrayExecAm<T> for CollectiveAtomicArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) ->Arc<AMCounters>{
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for CollectiveAtomicArray<T> {
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) ->  Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }
    unsafe fn into_inner(self) -> UnsafeArray<T>{
        self.array
    }
}

impl<T: Dist> LamellarArray<T> for CollectiveAtomicArray<T> {
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
}

impl<T: Dist> LamellarWrite for CollectiveAtomicArray<T> {}
impl<T: Dist> LamellarRead for CollectiveAtomicArray<T> {}

impl<T: Dist> SubArray<T> for CollectiveAtomicArray<T> {
    type Array = CollectiveAtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> CollectiveAtomicArray<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for CollectiveAtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}