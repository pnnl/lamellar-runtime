mod handle;
pub(crate) use handle::GenericAtomicArrayHandle;

pub(crate) mod iteration;
pub(crate) mod operations;
mod rdma;
use crate::array::atomic::AtomicElement;
// use crate::array::private::LamellarArrayPrivate;
use crate::array::private::ArrayExecAm;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
use crate::array::*;
use crate::barrier::BarrierHandle;
use crate::darc::Darc;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;

use parking_lot::{Mutex, MutexGuard};
use serde::ser::SerializeSeq;
// use std::ops::{Deref, DerefMut};

use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};

#[doc(hidden)]
pub struct GenericAtomicElement<T> {
    array: GenericAtomicArray<T>,
    local_index: usize,
}

impl<T: Dist> From<GenericAtomicElement<T>> for AtomicElement<T> {
    fn from(element: GenericAtomicElement<T>) -> AtomicElement<T> {
        AtomicElement::GenericAtomicElement(element)
    }
}

impl<T: Dist> GenericAtomicElement<T> {
    pub fn load(&self) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] }
    }
    pub fn store(&self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] = val;
        }
    }
    pub fn swap(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] = val;
            old
        }
    }
}
impl<T: ElementArithmeticOps> GenericAtomicElement<T> {
    pub fn fetch_add(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] += val;
            old
        }
    }
    pub fn fetch_sub(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] -= val;
            old
        }
    }
    pub fn fetch_mul(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] *= val;
            old
        }
    }
    pub fn fetch_div(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] /= val;
            old
        }
    }
    pub fn fetch_rem(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] %= val;
            old
        }
    }
}

impl<T: Dist + std::cmp::Eq> GenericAtomicElement<T> {
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        let _lock = self.array.lock_index(self.local_index);
        let current_val = unsafe { self.array.__local_as_mut_slice()[self.local_index] };
        if current_val == current {
            unsafe {
                self.array.__local_as_mut_slice()[self.local_index] = new;
            }
            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}
impl<T: Dist + std::cmp::PartialEq + std::cmp::PartialOrd + std::ops::Sub<Output = T>>
    GenericAtomicElement<T>
{
    pub fn compare_exchange_epsilon(&self, current: T, new: T, eps: T) -> Result<T, T> {
        let _lock = self.array.lock_index(self.local_index);
        let current_val = unsafe { self.array.__local_as_mut_slice()[self.local_index] };
        let same = if current_val > current {
            current_val - current < eps
        } else {
            current - current_val < eps
        };
        if same {
            unsafe {
                self.array.__local_as_mut_slice()[self.local_index] = new;
            }
            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}

impl<T: ElementBitWiseOps + 'static> GenericAtomicElement<T> {
    pub fn fetch_and(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] &= val;
            old
        }
    }
    pub fn fetch_or(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] |= val;
            old
        }
    }
    pub fn fetch_xor(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] ^= val;
            old
        }
    }
}

impl<T: ElementShiftOps + 'static> GenericAtomicElement<T> {
    pub fn fetch_shl(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] <<= val;
            old
        }
    }
    pub fn fetch_shr(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] >>= val;
            old
        }
    }
}

impl<T: Dist + ElementArithmeticOps> AddAssign<T> for GenericAtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        // self.add(val)
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] += val }
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for GenericAtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] -= val }
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for GenericAtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] *= val }
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for GenericAtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] /= val }
    }
}

impl<T: Dist + ElementArithmeticOps> RemAssign<T> for GenericAtomicElement<T> {
    fn rem_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] %= val }
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for GenericAtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] &= val }
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for GenericAtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] |= val }
    }
}

impl<T: Dist + ElementBitWiseOps> BitXorAssign<T> for GenericAtomicElement<T> {
    fn bitxor_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] ^= val }
    }
}

impl<T: Dist + ElementShiftOps> ShlAssign<T> for GenericAtomicElement<T> {
    fn shl_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index].shl_assign(val) }
    }
}

impl<T: Dist + ElementShiftOps> ShrAssign<T> for GenericAtomicElement<T> {
    fn shr_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index].shr_assign(val) }
    }
}

impl<T: Dist + std::fmt::Debug> std::fmt::Debug for GenericAtomicElement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _lock = self.array.lock_index(self.local_index);
        let current_val = unsafe { self.array.__local_as_mut_slice()[self.local_index] };
        write!(f, "{current_val:?}")
    }
}

/// A variant of an [AtomicArray] providing atomic access for any type that implements [Dist][crate::memregion::Dist].
///
/// Atomicity is gauranteed by constructing a 1-Byte mutex for each element in the array.
///
/// Generally any operation on this array type will be performed via an internal runtime Active Message, i.e. direct RDMA operations are not allowed
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct GenericAtomicArray<T> {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeArray<T>,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct GenericAtomicByteArray {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeByteArray,
}

impl GenericAtomicByteArray {
    //#[doc(hidden)]
    pub fn lock_index(&self, index: usize) -> MutexGuard<()> {
        let index = self
            .array
            .inner
            .pe_full_offset_for_local_index(self.array.inner.data.my_pe, index)
            .expect("invalid local index");
        self.locks[index].lock()
    }

    //#[doc(hidden)]
    pub fn downgrade(array: &GenericAtomicByteArray) -> GenericAtomicByteArrayWeak {
        GenericAtomicByteArrayWeak {
            locks: array.locks.clone(),
            array: UnsafeByteArray::downgrade(&array.array),
        }
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct GenericAtomicByteArrayWeak {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeByteArrayWeak,
}

impl GenericAtomicByteArrayWeak {
    //#[doc(hidden)]
    pub fn upgrade(&self) -> Option<GenericAtomicByteArray> {
        Some(GenericAtomicByteArray {
            locks: self.locks.clone(),
            array: self.array.upgrade()?,
        })
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct GenericAtomicLocalData<T: Dist> {
    pub(crate) array: GenericAtomicArray<T>,
    start_index: usize,
    end_index: usize,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct GenericAtomicLocalDataIter<T: Dist> {
    array: GenericAtomicArray<T>,
    index: usize,
    end_index: usize,
}

impl<T: Dist> GenericAtomicLocalData<T> {
    pub fn at(&self, index: usize) -> GenericAtomicElement<T> {
        GenericAtomicElement {
            array: self.array.clone(),
            local_index: index,
        }
    }

    pub fn get_mut(&self, index: usize) -> Option<GenericAtomicElement<T>> {
        Some(GenericAtomicElement {
            array: self.array.clone(),
            local_index: index,
        })
    }

    pub fn len(&self) -> usize {
        unsafe { self.array.__local_as_mut_slice().len() }
    }

    pub fn iter(&self) -> GenericAtomicLocalDataIter<T> {
        GenericAtomicLocalDataIter {
            array: self.array.clone(),
            index: self.start_index,
            end_index: self.end_index,
        }
    }

    pub fn sub_data(&self, start_index: usize, end_index: usize) -> GenericAtomicLocalData<T> {
        GenericAtomicLocalData {
            array: self.array.clone(),
            start_index: start_index,
            end_index: std::cmp::min(end_index, self.array.num_elems_local()),
        }
    }
}

impl<T: Dist + serde::Serialize> serde::Serialize for GenericAtomicLocalData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_seq(Some(self.len()))?;
        for i in 0..self.len() {
            s.serialize_element(&self.at(i).load())?;
        }
        s.end()
    }
}

impl<T: Dist> IntoIterator for GenericAtomicLocalData<T> {
    type Item = GenericAtomicElement<T>;
    type IntoIter = GenericAtomicLocalDataIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        GenericAtomicLocalDataIter {
            array: self.array,
            index: self.start_index,
            end_index: self.end_index,
        }
    }
}

impl<T: Dist> Iterator for GenericAtomicLocalDataIter<T> {
    type Item = GenericAtomicElement<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end_index {
            let index = self.index;
            self.index += 1;
            Some(GenericAtomicElement {
                array: self.array.clone(),
                local_index: index,
            })
        } else {
            None
        }
    }
}

impl<T: Dist + ArrayOps + std::default::Default> GenericAtomicArray<T> {
    pub(crate) fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> GenericAtomicArrayHandle<T> {
        // println!("new generic_atomic array");

        let team = team.into().team.clone();
        GenericAtomicArrayHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(async move {
                let array = UnsafeArray::async_new(
                    team.clone(),
                    array_size,
                    distribution,
                    DarcMode::LocalLockArray,
                )
                .await;
                let mut vec = vec![];
                for _i in 0..array.num_elems_local() {
                    vec.push(Mutex::new(()));
                }
                GenericAtomicArray {
                    locks: Darc::new(team, vec).await.expect("pe exists in team"),
                    array,
                }
            }),
        }
    }
}

impl<T: Dist> GenericAtomicArray<T> {
    pub(crate) fn get_element(&self, index: usize) -> Option<GenericAtomicElement<T>> {
        if index < unsafe { self.__local_as_slice().len() } {
            //We are only directly accessing the local slice for its len
            Some(GenericAtomicElement {
                array: self.clone(),
                local_index: index,
            })
        } else {
            None
        }
    }
}
#[doc(hidden)]
impl<T: Dist> GenericAtomicArray<T> {
    //#[doc(hidden)]
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        GenericAtomicArray {
            locks: self.locks.clone(),
            array: self.array.use_distribution(distribution),
        }
    }

    //#[doc(hidden)]
    pub fn local_data(&self) -> GenericAtomicLocalData<T> {
        GenericAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    //#[doc(hidden)]
    pub fn mut_local_data(&self) -> GenericAtomicLocalData<T> {
        GenericAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    //#[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    //#[doc(hidden)]
    pub unsafe fn __local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }

    //#[doc(hidden)]
    pub fn into_unsafe(self) -> IntoUnsafeArrayHandle<T> {
        // println!("generic into_unsafe");
        // self.array.into()
        IntoUnsafeArrayHandle {
            team: self.array.inner.data.team.clone(),
            launched: false,
            outstanding_future: Box::pin(self.async_into()),
        }
    }

    //#[doc(hidden)]
    pub fn into_read_only(self) -> IntoReadOnlyArrayHandle<T> {
        // println!("generic into_read_only");
        self.array.into_read_only()
    }

    //#[doc(hidden)]
    pub fn into_local_lock(self) -> IntoLocalLockArrayHandle<T> {
        // println!("generic into_local_lock");
        self.array.into_local_lock()
    }

    //#[doc(hidden)]
    pub fn into_global_lock(self) -> IntoGlobalLockArrayHandle<T> {
        // println!("generic into_local_lock");
        self.array.into_global_lock()
    }

    //#[doc(hidden)]
    pub fn lock_index(&self, index: usize) -> MutexGuard<()> {
        // if let Some(ref locks) = *self.locks {
        //     let start_index = (index * std::mem::size_of::<T>()) / self.orig_t_size;
        //     let end_index = ((index + 1) * std::mem::size_of::<T>()) / self.orig_t_size;
        //     let mut guards = vec![];
        //     for i in start_index..end_index {
        //         guards.push(locks[i].lock())
        //     }
        //     Some(guards)
        // } else {
        //     None
        // }
        // println!("trying to lock {:?}",index);
        let index = self
            .array
            .inner
            .pe_full_offset_for_local_index(self.array.inner.data.my_pe, index)
            .expect("invalid local index");
        self.locks[index].lock()
    }
}

impl<T: Dist + 'static> GenericAtomicArray<T> {
    #[doc(hidden)]
    pub fn into_atomic(self) -> IntoAtomicArrayHandle<T> {
        // println!("generic into_atomic");
        self.array.into_atomic()
    }
}

// #[async_trait]
impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for GenericAtomicArray<T> {
    async fn team_from(input: (Vec<T>, Distribution), team: &Arc<LamellarTeam>) -> Self {
        let array: UnsafeArray<T> = AsyncTeamInto::team_into(input, team).await;
        array.async_into().await
    }
}

#[async_trait]
impl<T: Dist> AsyncFrom<UnsafeArray<T>> for GenericAtomicArray<T> {
    async fn async_from(array: UnsafeArray<T>) -> Self {
        // println!("generic from unsafe array");
        array
            .await_on_outstanding(DarcMode::GenericAtomicArray)
            .await;
        let mut vec = vec![];
        for _i in 0..array.num_elems_local() {
            vec.push(Mutex::new(()));
        }
        let locks = Darc::new(array.team_rt(), vec).await.expect("PE in team");

        GenericAtomicArray {
            locks: locks,
            array: array,
        }
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for GenericAtomicByteArray {
    fn from(array: GenericAtomicArray<T>) -> Self {
        GenericAtomicByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for LamellarByteArray {
    fn from(array: GenericAtomicArray<T>) -> Self {
        LamellarByteArray::GenericAtomicArray(GenericAtomicByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        })
    }
}

impl<T: Dist> From<LamellarByteArray> for GenericAtomicArray<T> {
    fn from(array: LamellarByteArray) -> Self {
        if let LamellarByteArray::GenericAtomicArray(array) = array {
            array.into()
        } else {
            panic!("Expected LamellarByteArray::GenericAtomicArray")
        }
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for AtomicByteArray {
    fn from(array: GenericAtomicArray<T>) -> Self {
        AtomicByteArray::GenericAtomicByteArray(GenericAtomicByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        })
    }
}
impl<T: Dist> From<GenericAtomicByteArray> for GenericAtomicArray<T> {
    fn from(array: GenericAtomicByteArray) -> Self {
        GenericAtomicArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<GenericAtomicByteArray> for AtomicArray<T> {
    fn from(array: GenericAtomicByteArray) -> Self {
        GenericAtomicArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
        .into()
    }
}

impl<T: Dist> private::ArrayExecAm<T> for GenericAtomicArray<T> {
    fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for GenericAtomicArray<T> {
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
    fn as_lamellar_byte_array(&self) -> LamellarByteArray {
        self.clone().into()
    }
}

impl<T: Dist> ActiveMessaging for GenericAtomicArray<T> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_all_tg(am)
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_pe_tg(pe, am)
    }
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.array.exec_am_local_tg(am)
    }
    fn wait_all(&self) {
        self.array.wait_all()
    }
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        self.array.await_all()
    }
    fn barrier(&self) {
        self.array.barrier()
    }
    fn async_barrier(&self) -> BarrierHandle {
        self.array.async_barrier()
    }
    fn spawn<F: Future>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.array.spawn(f)
    }
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.array.block_on(f)
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        self.array.block_on_all(iter)
    }
}

impl<T: Dist> LamellarArray<T> for GenericAtomicArray<T> {
    // fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
    //     self.array.team_rt()
    // }
    // fn my_pe(&self) -> usize {
    //     LamellarArray::my_pe(&self.array)
    // }
    // fn num_pes(&self) -> usize {
    //     LamellarArray::num_pes(&self.array)
    // }
    fn len(&self) -> usize {
        self.array.len()
    }
    fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }
    // fn barrier(&self) {
    //     self.array.barrier();
    // }

    // fn wait_all(&self) {
    //     self.array.wait_all()
    //     // println!("done in wait all {:?}",std::time::SystemTime::now());
    // }
    // fn block_on<F: Future>(&self, f: F) -> F::Output {
    //     self.array.block_on(f)
    // }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.first_global_index_for_pe(pe)
    }

    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.last_global_index_for_pe(pe)
    }
}

impl<T: Dist> LamellarEnv for GenericAtomicArray<T> {
    fn my_pe(&self) -> usize {
        LamellarEnv::my_pe(&self.array)
    }

    fn num_pes(&self) -> usize {
        LamellarEnv::num_pes(&self.array)
    }
    fn num_threads_per_pe(&self) -> usize {
        self.array.team_rt().num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().team()
    }
}

impl<T: Dist> LamellarWrite for GenericAtomicArray<T> {}
impl<T: Dist> LamellarRead for GenericAtomicArray<T> {}

impl<T: Dist> SubArray<T> for GenericAtomicArray<T> {
    type Array = GenericAtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        GenericAtomicArray {
            locks: self.locks.clone(),
            array: self.array.sub_array(range),
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> GenericAtomicArray<T> {
    #[doc(alias = "Collective")]
    /// Print the data within a lamellar array
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the print call otherwise deadlock will occur (i.e. barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let block_array = AtomicArray::<f32>::new(&world,100,Distribution::Block).block();
    /// let cyclic_array = AtomicArray::<f32>::new(&world,100,Distribution::Block).block();
    ///
    /// block_array.print();
    /// println!();
    /// cyclic_array.print();
    ///```
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for GenericAtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}

impl<T: Dist + AmDist + 'static> GenericAtomicArray<T> {
    #[doc(hidden)]
    pub fn reduce(&self, op: &str) -> AmHandle<Option<T>> {
        self.array.reduce_data(op, self.clone().into())
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> GenericAtomicArray<T> {
    #[doc(hidden)]
    pub fn sum(&self) -> AmHandle<Option<T>> {
        self.reduce("sum")
    }
    #[doc(hidden)]
    pub fn prod(&self) -> AmHandle<Option<T>> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> GenericAtomicArray<T> {
    #[doc(hidden)]
    pub fn max(&self) -> AmHandle<Option<T>> {
        self.reduce("max")
    }
    #[doc(hidden)]
    pub fn min(&self) -> AmHandle<Option<T>> {
        self.reduce("min")
    }
}

#[doc(hidden)]
pub struct LocalGenericAtomicElement<T> {
    pub(crate) val: Mutex<T>,
}

impl<T: Dist> From<LocalGenericAtomicElement<T>> for AtomicElement<T> {
    fn from(element: LocalGenericAtomicElement<T>) -> AtomicElement<T> {
        AtomicElement::LocalGenericAtomicElement(element)
    }
}

impl<T: Dist> LocalGenericAtomicElement<T> {
    pub fn load(&self) -> T {
        *self.val.lock()
    }
    pub fn store(&self, val: T) {
        *self.val.lock() = val
    }
    pub fn swap(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() = val;
        old
    }
}
impl<T: ElementArithmeticOps> LocalGenericAtomicElement<T> {
    pub fn fetch_add(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() += val;
        old
    }
    pub fn fetch_sub(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() -= val;
        old
    }
    pub fn fetch_mul(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() *= val;
        old
    }
    pub fn fetch_div(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() /= val;
        old
    }
    pub fn fetch_rem(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() %= val;
        old
    }
}

impl<T: Dist + std::cmp::Eq> LocalGenericAtomicElement<T> {
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        let current_val = *self.val.lock();
        if current_val == current {
            *self.val.lock() = new;

            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}
impl<T: Dist + std::cmp::PartialEq + std::cmp::PartialOrd + std::ops::Sub<Output = T>>
    LocalGenericAtomicElement<T>
{
    pub fn compare_exchange_epsilon(&self, current: T, new: T, eps: T) -> Result<T, T> {
        let current_val = *self.val.lock();
        let same = if current_val > current {
            current_val - current < eps
        } else {
            current - current_val < eps
        };
        if same {
            *self.val.lock() = new;

            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}

impl<T: ElementBitWiseOps + 'static> LocalGenericAtomicElement<T> {
    pub fn fetch_and(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() &= val;
        old
    }
    pub fn fetch_or(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() |= val;
        old
    }
    pub fn fetch_xor(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() ^= val;
        old
    }
}

impl<T: ElementShiftOps + 'static> LocalGenericAtomicElement<T> {
    pub fn fetch_shl(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() <<= val;
        old
    }
    pub fn fetch_shr(&self, val: T) -> T {
        let old = *self.val.lock();
        *self.val.lock() >>= val;
        old
    }
}

impl<T: Dist + ElementArithmeticOps> AddAssign<T> for LocalGenericAtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        // self.add(val)
        *self.val.lock() += val
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for LocalGenericAtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        *self.val.lock() -= val
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for LocalGenericAtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        *self.val.lock() *= val
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for LocalGenericAtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        *self.val.lock() /= val
    }
}

impl<T: Dist + ElementArithmeticOps> RemAssign<T> for LocalGenericAtomicElement<T> {
    fn rem_assign(&mut self, val: T) {
        *self.val.lock() %= val
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for LocalGenericAtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        *self.val.lock() &= val
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for LocalGenericAtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        *self.val.lock() |= val
    }
}

impl<T: Dist + ElementBitWiseOps> BitXorAssign<T> for LocalGenericAtomicElement<T> {
    fn bitxor_assign(&mut self, val: T) {
        *self.val.lock() ^= val
    }
}

impl<T: Dist + ElementShiftOps> ShlAssign<T> for LocalGenericAtomicElement<T> {
    fn shl_assign(&mut self, val: T) {
        self.val.lock().shl_assign(val)
    }
}

impl<T: Dist + ElementShiftOps> ShrAssign<T> for LocalGenericAtomicElement<T> {
    fn shr_assign(&mut self, val: T) {
        self.val.lock().shr_assign(val)
    }
}

impl<T: Dist + std::fmt::Debug> std::fmt::Debug for LocalGenericAtomicElement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let current_val = *self.val.lock();
        write!(f, "{current_val:?}")
    }
}
