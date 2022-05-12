#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) mod buffered_operations;
mod iteration;
#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) use buffered_operations as operations;
mod rdma;
use crate::array::local_lock_atomic::operations::BUFOPS;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::UnsafeByteArray;
use crate::array::*;
use crate::darc::local_rw_darc::LocalRwDarc;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock,
};
use std::any::TypeId;
use std::ops::{Deref, DerefMut};

#[lamellar_impl::AmDataRT(Clone)]
pub struct LocalLockAtomicArray<T: Dist> {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeArray<T>,
}

#[lamellar_impl::AmDataRT(Clone)]
pub struct LocalLockAtomicByteArray {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeByteArray,
}

pub struct LocalLockAtomicMutLocalData<'a, T: Dist> {
    data: &'a mut [T],
    _index: usize,
    _lock_guard: ArcRwLockWriteGuard<RawRwLock, Box<()>>,
}

// impl<T: Dist> Drop for LocalLockAtomicMutLocalData<'_, T>{
//     fn drop(&mut self){
//         println!("dropping lla write lock");
//     }
// }

impl<T: Dist> Deref for LocalLockAtomicMutLocalData<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<T: Dist> DerefMut for LocalLockAtomicMutLocalData<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

pub struct LocalLockAtomicLocalData<'a, T: Dist> {
    pub(crate) data: &'a [T],
    index: usize,
    lock: LocalRwDarc<()>,
    _lock_guard: ArcRwLockReadGuard<RawRwLock, Box<()>>,
}

impl<'a, T: Dist> Clone for LocalLockAtomicLocalData<'a, T> {
    fn clone(&self) -> Self {
        LocalLockAtomicLocalData {
            data: self.data,
            index: self.index,
            lock: self.lock.clone(),
            _lock_guard: self.lock.read(),
        }
    }
}

impl<'a, T: Dist> LocalLockAtomicLocalData<'a, T> {
    pub fn into_sub_data(self, start: usize, end: usize) -> LocalLockAtomicLocalData<'a, T> {
        LocalLockAtomicLocalData {
            data: &self.data[start..end],
            index: 0,
            lock: self.lock,
            _lock_guard: self._lock_guard,
        }
    }
}

impl<'a, T: Dist + serde::Serialize> serde::Serialize for LocalLockAtomicLocalData<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data.serialize(serializer)
    }
}

impl<'a, T: Dist> Iterator for LocalLockAtomicLocalData<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data.len() {
            self.index += 1;
            Some(&self.data[self.index - 1])
        } else {
            None
        }
    }
}

impl<T: Dist> Deref for LocalLockAtomicLocalData<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<T: Dist + std::default::Default> LocalLockAtomicArray<T> {
    //Sync + Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> LocalLockAtomicArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        let lock = LocalRwDarc::new(team, ()).unwrap();

        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = LocalLockAtomicByteArray {
                lock: lock.clone(),
                array: array.clone().into(),
            };

            for pe in 0..op_bufs.len() {
                op_bufs[pe] = func(bytearray.clone());
            }
        }

        LocalLockAtomicArray {
            lock: lock,
            array: array,
        }
    }
}

impl<T: Dist> LocalLockAtomicArray<T> {
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
        LocalLockAtomicArray {
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
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn read_local_data(&self) -> LocalLockAtomicLocalData<'_, T> {
        LocalLockAtomicLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            index: 0,
            lock: self.lock.clone(),
            _lock_guard: self.lock.read(),
        }
    }

    pub fn write_local_data(&self) -> LocalLockAtomicMutLocalData<'_, T> {
        LocalLockAtomicMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: self.lock.write(),
        }
    }

    #[doc(hidden)] //todo create a custom macro to emit a warning saying use read_local_slice/write_local_slice intead
    pub fn local_as_slice(&self) -> LocalLockAtomicLocalData<'_, T> {
        LocalLockAtomicLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            index: 0,
            lock: self.lock.clone(),
            _lock_guard: self.lock.read(),
        }
    }
    // #[doc(hidden)]
    // pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
    //     self.array.local_as_mut_slice()
    // }

    #[doc(hidden)]
    pub fn local_as_mut_slice(&self) -> LocalLockAtomicMutLocalData<'_, T> {
        let lock = LocalLockAtomicMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: self.lock.write(),
        };
        // println!("have lla write lock");
        lock
    }

    #[doc(hidden)]
    pub fn local_data(&self) -> LocalLockAtomicLocalData<'_, T> {
        self.local_as_slice()
    }

    #[doc(hidden)]
    pub fn mut_local_data(&self) -> LocalLockAtomicMutLocalData<'_, T> {
        self.local_as_mut_slice()
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        LocalLockAtomicArray {
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

    pub fn into_generic_atomic(self) -> GenericAtomicArray<T> {
        self.array.into()
    }
}

impl<T: Dist + 'static> LocalLockAtomicArray<T> {
    pub fn into_atomic(self) -> AtomicArray<T> {
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for LocalLockAtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        array.block_on_outstanding(DarcMode::LocalLockAtomicArray);
        let lock = LocalRwDarc::new(array.team(), ()).unwrap();
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let bytearray = LocalLockAtomicByteArray {
                lock: lock.clone(),
                array: array.clone().into(),
            };
            let mut op_bufs = array.inner.data.op_buffers.write();
            for _pe in 0..array.inner.data.num_pes {
                op_bufs.push(func(bytearray.clone()))
            }
        }
        LocalLockAtomicArray {
            lock: lock,
            array: array,
        }
    }
}

impl<T: Dist> From<LocalLockAtomicArray<T>> for LocalLockAtomicByteArray {
    fn from(array: LocalLockAtomicArray<T>) -> Self {
        LocalLockAtomicByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<LocalLockAtomicByteArray> for LocalLockAtomicArray<T> {
    fn from(array: LocalLockAtomicByteArray) -> Self {
        LocalLockAtomicArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> private::ArrayExecAm<T> for LocalLockAtomicArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for LocalLockAtomicArray<T> {
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_mut_ptr()
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

impl<T: Dist> LamellarArray<T> for LocalLockAtomicArray<T> {
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

impl<T: Dist> LamellarWrite for LocalLockAtomicArray<T> {}
impl<T: Dist> LamellarRead for LocalLockAtomicArray<T> {}

impl<T: Dist> SubArray<T> for LocalLockAtomicArray<T> {
    type Array = LocalLockAtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> LocalLockAtomicArray<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for LocalLockAtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}

pub struct LocalLockAtomicArrayReduceHandle<T: Dist + AmDist> {
    req: Box<dyn LamellarRequest<Output = T> + Send + Sync>,
    _lock_guard: ArcRwLockReadGuard<RawRwLock, Box<()>>,
}

#[async_trait]
impl<T: Dist + AmDist> LamellarRequest for LocalLockAtomicArrayReduceHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req.into_future().await
    }
    fn get(&self) -> Self::Output {
        self.req.get()
    }
}

impl<T: Dist + AmDist + 'static> LocalLockAtomicArray<T> {
    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let lock = self.lock.read();
        Box::new(LocalLockAtomicArrayReduceHandle {
            req: self.array.reduce(op),
            _lock_guard: lock,
        })
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let lock = self.lock.read();
        Box::new(LocalLockAtomicArrayReduceHandle {
            req: self.array.reduce("sum"),
            _lock_guard: lock,
        })
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let lock = self.lock.read();
        Box::new(LocalLockAtomicArrayReduceHandle {
            req: self.array.reduce("prod"),
            _lock_guard: lock,
        })
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let lock = self.lock.read();
        Box::new(LocalLockAtomicArrayReduceHandle {
            req: self.array.reduce("max"),
            _lock_guard: lock,
        })
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for LocalLockAtomicArray<T>
// {
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         self.array.get_reduction_op(op)
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
