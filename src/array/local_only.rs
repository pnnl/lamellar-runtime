use crate::array::*;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct LocalOnlyArray<T: Dist + 'static> {
    pub(crate) array: UnsafeArray<T>,
    // actually we should just use a read write lock here to enforce mutability exclusitivity
    pub(crate) _unsync: PhantomData<*const ()>, // because we allow mutable access to underlying slice but don't provide any protection to aquiring multiple mut slices
                                                // we must make this not sync by default.
                                                // either wrap the localonlyarray in a mutex/rwlock or use a localRwArray
}

//#[prof]
impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LocalOnlyArray<T> {
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> LocalOnlyArray<T> {
        LocalOnlyArray {
            array: UnsafeArray::new(team, array_size, distribution),
            _unsync: PhantomData,
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
        LocalOnlyArray {
            array: self.array.use_distribution(distribution),
            _unsync: PhantomData,
        }
    }

    pub fn num_pes(&self) -> usize {
        self.array.num_pes()
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { self.array.local_as_mut_slice() }
    }
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { self.array.local_as_mut_slice() }
    }
    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        self.array.into()
    }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        self.array.into()
    }

    pub fn into_atomic(self) -> AtomicArray<T> {
        self.array.into()
    }

    pub fn into_generic_atomic(self) -> GenericAtomicArray<T> {
        self.array.into()
    }

    pub fn into_local_lock_atomic(self) -> LocalLockAtomicArray<T> {
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for LocalOnlyArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        array.block_on_outstanding(DarcMode::LocalOnlyArray);
        LocalOnlyArray {
            array: array,
            _unsync: PhantomData,
        }
    }
}

impl<T: Dist> private::ArrayExecAm<T> for LocalOnlyArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
    private::LamellarArrayPrivate<T> for LocalOnlyArray<T>
{
    fn local_as_ptr(&self) -> *const T {
        self.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.local_as_mut_ptr()
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

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArray<T>
    for LocalOnlyArray<T>
{
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

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + 'static>
    LocalOnlyArray<T>
{
    pub fn print(&self) {
        self.array.print()
    }
}
