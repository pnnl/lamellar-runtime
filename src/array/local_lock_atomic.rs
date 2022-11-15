#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) mod buffered_operations;
mod iteration;
#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) use buffered_operations as operations;
mod rdma;
use crate::array::local_lock_atomic::operations::BUFOPS;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
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

#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct LocalLockArray<T> {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeArray<T>,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct LocalLockByteArray {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeByteArray,
}

impl LocalLockByteArray {
    pub fn downgrade(array: &LocalLockByteArray) -> LocalLockByteArrayWeak {
        LocalLockByteArrayWeak {
            lock: array.lock.clone(),
            array: UnsafeByteArray::downgrade(&array.array),
        }
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct LocalLockByteArrayWeak {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeByteArrayWeak,
}

impl LocalLockByteArrayWeak {
    pub fn upgrade(&self) -> Option<LocalLockByteArray> {
        Some(LocalLockByteArray {
            lock: self.lock.clone(),
            array: self.array.upgrade()?,
        })
    }
}

#[derive(Debug)]
pub struct LocalLockMutLocalData<'a, T: Dist> {
    data: &'a mut [T],
    _index: usize,
    _lock_guard: ArcRwLockWriteGuard<RawRwLock, Box<()>>,
}

// impl<T: Dist> Drop for LocalLockMutLocalData<'_, T>{
//     fn drop(&mut self){
//         println!("release lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
//     }
// }

impl<T: Dist> Deref for LocalLockMutLocalData<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<T: Dist> DerefMut for LocalLockMutLocalData<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

#[derive(Debug)]
pub struct LocalLockLocalData<'a, T: Dist> {
    pub(crate) array: LocalLockArray<T>,
    pub(crate) data: &'a [T],
    index: usize,
    lock: LocalRwDarc<()>,
    _lock_guard: ArcRwLockReadGuard<RawRwLock, Box<()>>,
}

impl<'a, T: Dist> Clone for LocalLockLocalData<'a, T> {
    fn clone(&self) -> Self {
        LocalLockLocalData {
            array: self.array.clone(),
            data: self.data,
            index: self.index,
            lock: self.lock.clone(),
            _lock_guard: self.lock.read(),
        }
    }
}

impl<'a, T: Dist> LocalLockLocalData<'a, T> {
    pub fn into_sub_data(self, start: usize, end: usize) -> LocalLockLocalData<'a, T> {
        LocalLockLocalData {
            array: self.array.clone(),
            data: &self.data[start..end],
            index: 0,
            lock: self.lock,
            _lock_guard: self._lock_guard,
        }
    }
}

impl<'a, T: Dist + serde::Serialize> serde::Serialize for LocalLockLocalData<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data.serialize(serializer)
    }
}

impl<'a, T: Dist> Iterator for LocalLockLocalData<'a, T> {
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

impl<T: Dist> Deref for LocalLockLocalData<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<T: Dist + std::default::Default> LocalLockArray<T> {
    // Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> LocalLockArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        let lock = LocalRwDarc::new(team, ()).unwrap();

        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = LocalLockByteArray {
                lock: lock.clone(),
                array: array.clone().into(),
            };

            for pe in 0..op_bufs.len() {
                op_bufs[pe] = func(LocalLockByteArray::downgrade(&bytearray));
            }
        }

        LocalLockArray {
            lock: lock,
            array: array,
        }
    }
}

impl<T: Dist> LocalLockArray<T> {
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
        LocalLockArray {
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

    pub fn read_local_data(&self) -> LocalLockLocalData<'_, T> {
        LocalLockLocalData {
            array: self.clone(),
            data: unsafe { self.array.local_as_mut_slice() },
            index: 0,
            lock: self.lock.clone(),
            _lock_guard: self.lock.read(),
        }
    }

    pub fn write_local_data(&self) -> LocalLockMutLocalData<'_, T> {
        let lock = self.lock.write();
        let data = LocalLockMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: lock,
        };
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        data
    }

    #[doc(hidden)] //todo create a custom macro to emit a warning saying use read_local_slice/write_local_slice intead
    pub fn local_as_slice(&self) -> LocalLockLocalData<'_, T> {
        LocalLockLocalData {
            array: self.clone(),
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
    pub fn local_as_mut_slice(&self) -> LocalLockMutLocalData<'_, T> {
        let the_lock = self.lock.write();
        let lock = LocalLockMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: the_lock,
        };
        // println!("have lla write lock");
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        lock
    }

    #[doc(hidden)]
    pub fn local_data(&self) -> LocalLockLocalData<'_, T> {
        self.local_as_slice()
    }

    #[doc(hidden)]
    pub fn mut_local_data(&self) -> LocalLockMutLocalData<'_, T> {
        self.local_as_mut_slice()
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        LocalLockArray {
            lock: self.lock.clone(),
            array: self.array.sub_array(range),
        }
    }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("locallock into_unsafe");
        self.array.into()
    }

    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("locallock into_local_only");
    //     self.array.into()
    // }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        // println!("locallock into_read_only");
        self.array.into()
    }

    // pub fn into_generic_atomic(self) -> GenericAtomicArray<T> {
    //     // println!("locallock into_generic_atomic");
    //     self.array.into()
    // }
}

impl<T: Dist + 'static> LocalLockArray<T> {
    pub fn into_atomic(self) -> AtomicArray<T> {
        // println!("locallock into_atomic");
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for LocalLockArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("locallock from unsafe");
        array.block_on_outstanding(DarcMode::LocalLockArray);
        let lock = LocalRwDarc::new(array.team(), ()).unwrap();
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let bytearray = LocalLockByteArray {
                lock: lock.clone(),
                array: array.clone().into(),
            };
            let mut op_bufs = array.inner.data.op_buffers.write();
            for _pe in 0..array.inner.data.num_pes {
                op_bufs.push(func(LocalLockByteArray::downgrade(&bytearray)))
            }
        }
        LocalLockArray {
            lock: lock,
            array: array,
        }
    }
}

// impl<T: Dist> From<LocalOnlyArray<T>> for LocalLockArray<T> {
//     fn from(array: LocalOnlyArray<T>) -> Self {
//         // println!("locallock from localonly");
//         unsafe { array.into_inner().into() }
//     }
// }

impl<T: Dist> From<AtomicArray<T>> for LocalLockArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        // println!("locallock from atomic");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for LocalLockArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self {
        // println!("locallock from readonly");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<LocalLockArray<T>> for LocalLockByteArray {
    fn from(array: LocalLockArray<T>) -> Self {
        LocalLockByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<LocalLockByteArray> for LocalLockArray<T> {
    fn from(array: LocalLockByteArray) -> Self {
        LocalLockArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> private::ArrayExecAm<T> for LocalLockArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for LocalLockArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        &self.array
    }
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

impl<T: Dist> LamellarArray<T> for LocalLockArray<T> {
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
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future {
            self.array.block_on(f)
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
}

impl<T: Dist> LamellarWrite for LocalLockArray<T> {}
impl<T: Dist> LamellarRead for LocalLockArray<T> {}

impl<T: Dist> SubArray<T> for LocalLockArray<T> {
    type Array = LocalLockArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> LocalLockArray<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for LocalLockArray<T> {
    fn print(&self) {
        self.array.print()
    }
}

#[doc(hidden)]
pub struct LocalLockArrayReduceHandle<T: Dist + AmDist> {
    req: Box<dyn LamellarRequest<Output = T>>,
    _lock_guard: ArcRwLockReadGuard<RawRwLock, Box<()>>,
}

#[async_trait]
impl<T: Dist + AmDist> LamellarRequest for LocalLockArrayReduceHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req.into_future().await
    }
    fn get(&self) -> Self::Output {
        self.req.get()
    }
}

impl<T: Dist + AmDist + 'static> LocalLockArray<T> {
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        let lock = self.lock.read();
        Box::new(LocalLockArrayReduceHandle {
            req: self.array.reduce_req(op),
            _lock_guard: lock,
        })
        .into_future()
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        let lock = self.lock.read();
        Box::new(LocalLockArrayReduceHandle {
            req: self.array.reduce_req("sum"),
            _lock_guard: lock,
        })
        .into_future()
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        let lock = self.lock.read();
        Box::new(LocalLockArrayReduceHandle {
            req: self.array.reduce_req("prod"),
            _lock_guard: lock,
        })
        .into_future()
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        let lock = self.lock.read();
        Box::new(LocalLockArrayReduceHandle {
            req: self.array.reduce_req("max"),
            _lock_guard: lock,
        })
        .into_future()
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for LocalLockArray<T>
// {
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         self.array.get_reduction_op(op)
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
